"""Tests de humo: el entorno local soporta Delta MERGE y la config es coherente."""
from __future__ import annotations

from datetime import date

from iol_pipeline import config


def test_delta_merge_is_idempotent(spark, schema):
    tbl = f"{schema}.trades_raw"
    rows = [("TXN1", "h1"), ("TXN2", "h2")]
    spark.createDataFrame(rows, "id_transaccion string, record_hash string").write.format("delta").saveAsTable(tbl)

    staged = spark.createDataFrame(rows + [("TXN1", "h1_changed")], "id_transaccion string, record_hash string")
    staged.createOrReplaceTempView("staged")
    merge = f"""
        MERGE INTO {tbl} t USING staged s
        ON t.id_transaccion = s.id_transaccion AND t.record_hash = s.record_hash
        WHEN NOT MATCHED THEN INSERT *
    """
    spark.sql(merge)
    spark.sql(merge)  # segunda corrida: no-op

    assert spark.table(tbl).count() == 3  # 2 originales + 1 conflicto preservado
    fmt = spark.sql(f"DESCRIBE DETAIL {tbl}").first()["format"]
    assert fmt == "delta"


def test_activity_segments_are_ordered_and_cover_one_trade():
    mins = [m for m, _ in config.ACTIVITY_SEGMENTS]
    assert mins == sorted(mins, reverse=True)
    assert mins[-1] == 1


def test_gate_thresholds_consistent():
    g = config.GATE_PARAMS
    assert g["unmapped_yellow_pct"] < g["unmapped_red_pct"]
    assert g["coverage_inconclusive_pct"] < g["coverage_green_pct"]


def test_market_holidays_are_weekdays_inside_calendar_range():
    for day in config.MARKET_HOLIDAYS:
        assert config.CALENDAR_FROM <= day <= config.CALENDAR_TO
        assert day.weekday() < 5
    assert date(2026, 2, 16) in config.MARKET_HOLIDAYS and date(2026, 2, 17) in config.MARKET_HOLIDAYS
    assert date(2026, 3, 23) not in config.MARKET_HOLIDAYS  # no laborable turístico: BYMA operó


def test_contract_has_ten_columns():
    assert len(config.TRADES_CONTRACT) == 10
    assert len(set(config.TRADES_CONTRACT)) == 10
