"""Atributos derivados y viabilidad del SCD2 (ADR-007)."""
from __future__ import annotations

from datetime import date, datetime

import pytest
from pyspark.sql import functions as F

from iol_pipeline import config, segmentation

SCHEMA = "client_id string, channel_code string, trade_ts_utc timestamp, effective_trade_date date"
CUTS = [date(2026, 1, 31), date(2026, 2, 28), date(2026, 3, 13)]


def _trade(client, channel, day, hour=12):
    return client, channel, datetime(day.year, day.month, day.day, hour), day


def _trades(spark, rows):
    return spark.createDataFrame(rows, SCHEMA)


@pytest.mark.parametrize(("n", "segment"), [
    (1, "OCASIONAL"), (2, "RECURRENTE"), (3, "RECURRENTE"), (4, "ACTIVO"), (9, "ACTIVO"), (10, "MUY_ACTIVO"), (676, "MUY_ACTIVO"),
])
def test_activity_segment_cuts(spark, n, segment):
    row = spark.range(1).select(segmentation.activity_segment(F.lit(n)).alias("s")).first()
    assert row.s == segment


def test_channel_code_maps_origin_and_marks_unknown(spark):
    df = spark.createDataFrame([("Sitio Web Responsive",), ("Otro",)], "origen string")
    assert [r[0] for r in df.select(segmentation.channel_code(F.col("origen"))).collect()] == ["WEB_RESPONSIVE", config.CHANNEL_UNKNOWN]


def test_preferred_channel_most_trades_wins(spark):
    rows = [_trade("C", "APP_MOBILE", date(2026, 1, 5)), _trade("C", "APP_MOBILE", date(2026, 1, 6)), _trade("C", "API", date(2026, 1, 7))]
    assert segmentation.preferred_channel(_trades(spark, rows)).first().preferred_channel == "APP_MOBILE"


def test_preferred_channel_tie_goes_to_most_recent_trade(spark):
    rows = [_trade("C", "API", date(2026, 1, 5)), _trade("C", "WEB_DESKTOP", date(2026, 1, 7))]
    assert segmentation.preferred_channel(_trades(spark, rows)).first().preferred_channel == "WEB_DESKTOP"


def test_preferred_channel_full_tie_is_alphabetical_and_ignores_row_order(spark):
    rows = [_trade("C", "WEB_DESKTOP", date(2026, 1, 5)), _trade("C", "API", date(2026, 1, 5))]
    assert segmentation.preferred_channel(_trades(spark, rows)).first().preferred_channel == "API"
    assert segmentation.preferred_channel(_trades(spark, rows[::-1])).first().preferred_channel == "API"


def test_attributes_at_cut_ignore_later_trades(spark):
    rows = [_trade("C", "API", date(2026, 1, 5)), _trade("C", "APP_MOBILE", date(2026, 2, 5)), _trade("C", "APP_MOBILE", date(2026, 2, 6))]
    row = segmentation.attributes_at_cut(_trades(spark, rows), date(2026, 1, 31)).first()
    assert (row.cum_trades, row.activity_segment, row.preferred_channel) == (1, "OCASIONAL", "API")


def test_initial_attributes_use_only_the_first_trade(spark):
    rows = [_trade("C", "API", date(2026, 1, 5)), _trade("C", "APP_MOBILE", date(2026, 1, 6)), _trade("C", "APP_MOBILE", date(2026, 1, 7))]
    row = segmentation.initial_attributes(_trades(spark, rows)).first()
    assert (row.activity_segment, row.preferred_channel) == ("OCASIONAL", "API")


def test_client_does_not_exist_at_cuts_before_its_first_trade(spark):
    points = segmentation.client_points(_trades(spark, [_trade("C", "API", date(2026, 2, 5))]), CUTS)
    assert sorted(r.point for r in points.collect()) == ["2026-02-28", "2026-03-13", "inicial"]


def test_client_without_changes_has_one_version(spark):
    points = segmentation.client_points(_trades(spark, [_trade("C", "API", date(2026, 1, 5))]), CUTS)
    assert segmentation.versions_per_client(points).first().versions == 1


def test_versions_count_segment_and_channel_changes(spark):
    rows = [
        _trade("C", "API", date(2026, 1, 5)),         # inicial: OCASIONAL, API
        _trade("C", "API", date(2026, 1, 6)),         # 31/01: RECURRENTE, API -> cambia segmento
        _trade("C", "APP_MOBILE", date(2026, 2, 5)),
        _trade("C", "APP_MOBILE", date(2026, 2, 6)),
        _trade("C", "APP_MOBILE", date(2026, 2, 7)),  # 28/02: ACTIVO, APP_MOBILE -> cambian los dos
    ]                                                 # 13/03: sin cambios
    points = segmentation.client_points(_trades(spark, rows), CUTS)
    assert segmentation.versions_per_client(points).first().versions == 3


def test_transitions_only_count_clients_present_in_both_points(spark):
    rows = [
        _trade("A", "API", date(2026, 1, 5)), _trade("A", "APP_MOBILE", date(2026, 2, 5)), _trade("A", "APP_MOBILE", date(2026, 2, 6)),
        _trade("B", "API", date(2026, 2, 10)),
    ]
    result = {(r.from_point, r.point): r for r in segmentation.transitions(segmentation.client_points(_trades(spark, rows), CUTS)).collect()}

    assert result[("2026-01-31", "2026-02-28")].clients == 1  # B todavía no existía el 31/01
    assert result[("2026-01-31", "2026-02-28")].channel_changes == 1
    assert result[("2026-01-31", "2026-02-28")].segment_changes == 1
    assert result[("inicial", "2026-02-28")].clients == 1     # B nace después del 31/01


def test_segment_matrix_adds_up_to_transitions(spark):
    rows = [_trade("A", "API", date(2026, 1, 5)), _trade("A", "API", date(2026, 2, 5)), _trade("B", "API", date(2026, 1, 6))]
    points = segmentation.client_points(_trades(spark, rows), CUTS)
    total_matrix = segmentation.segment_transition_matrix(points).agg(F.sum("clients")).first()[0]
    total_transitions = segmentation.transitions(points).agg(F.sum("clients")).first()[0]
    assert total_matrix == total_transitions


def test_usable_trades_follow_the_adr_007_criteria(spark, trades_small_csv):
    from iol_pipeline import calendar

    raw = spark.read.option("header", True).csv(str(trades_small_csv))
    usable = segmentation.usable_trades(calendar.with_trade_dates(raw))
    # 40 filas - 1 duplicado exacto - 2 del ID en conflicto - 1 sin id_cliente - 1 fecha no parseable
    assert usable.count() == 35
    assert usable.where("channel_code = 'UNKNOWN'").count() == 0
    assert usable.where("client_id = 'CLIFX000011'").count() == 1  # precio de escala anómala: ERROR, cuenta
