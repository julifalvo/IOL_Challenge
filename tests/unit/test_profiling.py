"""Funciones del notebook 01 sobre el fixture trades_small.csv."""
from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from iol_pipeline import config, profiling


@pytest.fixture(scope="module")
def raw(spark, trades_small_csv):
    return profiling.read_trades_csv(spark, str(trades_small_csv))


@pytest.fixture(scope="module")
def trades(raw):
    return profiling.prepare(raw)


def test_general_summary_counts_the_fixture(raw, trades):
    summary = profiling.general_summary(raw, trades)
    assert summary["rows"] == 40
    assert summary["duplicate_trade_ids"] == 2  # ...001 y ...002 aparecen dos veces
    assert summary["unparseable_ts"] == 1
    assert summary["nulls_by_column"]["id_cliente"] == 1
    assert summary["nulls_by_column"]["fecha"] == 0


def test_weekday_distribution_uses_local_and_utc(trades):
    rows = {r["name"]: r for r in profiling.weekday_distribution(trades).collect()}
    assert len(rows) == 7
    assert sum(r.ops_utc for r in rows.values()) == sum(r.ops_local for r in rows.values()) == 39
    assert rows["dom"].ops_local == 1
    # …008: sábado 01:30 UTC es viernes local
    assert rows["sab"].ops_utc == rows["sab"].ops_local + 1


def test_weekdays_without_trades_flag_holidays(trades):
    missing = {d["date"]: d["market_holiday"] for d in profiling.weekdays_without_trades(trades)}
    assert missing["2026-02-17"] == "Carnaval"
    assert missing["2026-01-16"] is None
    assert "2026-02-16" not in missing  # el fixture tiene una operación ese día


def test_trades_on_market_holidays(trades):
    assert profiling.trades_on_market_holidays(trades) == 1


def test_hour_bounds_and_histogram_agree(trades):
    hours = profiling.hourly_by_weekday(trades)
    day_columns = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    assert hours.select(sum(F.sum(c) for c in day_columns)).first()[0] == 39
    bounds = {r.weekday: r for r in profiling.hour_bounds_by_weekday(trades).collect()}
    assert bounds["Sat"].first_hour_utc == 1


def test_share_in_session_is_a_proportion(trades):
    assert 0 < profiling.share_in_session(trades) < 1


def test_channel_counts_cover_every_row(trades):
    assert sum(r.ops for r in profiling.channel_counts(trades).collect()) == 40


def test_multichannel_clients_includes_the_fixture_case(trades):
    assert profiling.multichannel_clients(trades) >= 1


def test_client_activity_percentiles(trades):
    activity = profiling.client_activity(trades)
    assert set(activity["percentiles"]) == set(config.CLIENT_PERCENTILES)
    assert activity["clients"] == 25
    assert activity["one_trade"] < activity["clients"]


def test_instrument_summary_finds_suffix_false_positives(trades):
    summary = profiling.instrument_summary(trades)
    assert {"YPFD", "CARC", "BA.C"} <= set(summary["ars_ending_in_d_or_c"])
    assert summary["usd_with_ars_base"] >= 1
    assert "AL30D" not in summary["ars_ending_in_d_or_c"]


def test_option_is_classified_before_other_rules(trades):
    types = {r.simbolo_titulo: r.asset_type_rule for r in trades.select("simbolo_titulo", "asset_type_rule").distinct().collect()}
    assert types["GFGC72000F"] == "OPCION"
    assert types["AAPL"] == "CEDEAR"


def test_settlement_medians_give_implied_fx(trades):
    rows = {r.simbolo_titulo: r for r in profiling.settlement_price_medians(trades).collect()}
    assert rows["AL30"].implied_fx is None
    assert rows["AL30D"].implied_fx > 1


def test_top_by_amount_ranks_inside_each_currency(trades):
    rows = profiling.top_by_amount(trades, 2).collect()
    assert {r.moneda for r in rows} >= {"ARS", "USD"}
    assert all(r["rank"] <= 2 for r in rows)


@pytest.mark.parametrize("fn", [profiling.top_by_quantity, profiling.top_by_amount])
def test_extreme_outputs_do_not_expose_identifiers(trades, fn):
    assert not {"id_cliente", "id_transaccion"} & set(fn(trades, 1).columns)


def test_scale_anomalies_find_t30a7_without_exposing_identifiers(trades):
    anomalies = profiling.scale_anomalies(trades)
    assert "T30A7" in {r.simbolo_titulo for r in anomalies.collect()}
    assert not {"id_cliente", "id_transaccion"} & set(anomalies.columns)


def test_treasury_family_shows_t30a7_first(trades):
    assert profiling.treasury_capitalizable_prices(trades).first().simbolo_titulo == "T30A7"


def test_monthly_activity_uses_effective_date(trades):
    rows = {r.month: r for r in profiling.monthly_activity(trades).collect()}
    assert sum(r.ops for r in rows.values()) == 39
    assert all(r.ops_per_business_day == r.ops / r.business_days_observed for r in rows.values())
