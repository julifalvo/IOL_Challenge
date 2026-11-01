"""De los proveedores a `fact_trade_market` y la cobertura (spec 05)."""
from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from iol_pipeline import config, instruments, quotes

pytestmark = pytest.mark.integration
EMPTY_OVERRIDES = Path(__file__).parents[1] / "fixtures" / "overrides_empty.csv"
TRADES_SCHEMA = ("trade_id string, symbol string, currency_code string, side string, price decimal(18,6), quality_status string, effective_trade_date date, description_raw string")
FROM, TO = date(2026, 1, 1), date(2026, 3, 13)


def _row(day, close, high=None, low=None):
    return {"date": day, "open": close, "high": high or close, "low": low or close, "close": close, "volume": 10.0}


class FakeProvider:
    """Filas por símbolo. Un símbolo en `broken` falla como un proveedor caído."""

    def __init__(self, name, rows=None, broken=()):
        self.name, self.rows, self.broken, self.calls = name, rows or {}, set(broken), []

    def fetch_daily(self, provider_symbol, date_from, date_to):
        self.calls.append(provider_symbol)
        if provider_symbol in self.broken:
            raise quotes.ProviderError("caído", status=503, retryable=False)
        return self.rows.get(provider_symbol, [])


def _trade(trade_id, symbol, day, price, side="BUY", currency="ARS", status="OK", description="Cedear Test"):
    return (trade_id, symbol, currency, side, Decimal(str(price)), status, day, description)


def _tables(schema):
    return {n: f"{schema}.{n}" for n in ("trades", "instrument_mapping", "quotes_raw", "quote_requests", "quote_series", "fact_market_quotes", "fact_trade_market", "quote_coverage")}


def _run(spark, tables, trades, data912=None, byma=None, yahoo=None, mapping_rows=None):
    """Corre la etapa con proveedores simulados. El tipo sale de `mapping_rows`: acá no se prueba la clasificación."""
    spark.createDataFrame(trades, TRADES_SCHEMA).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tables["trades"])
    rows = [(s, c, t) for s, c, t in mapping_rows]
    spark.createDataFrame(rows, "symbol string, currency_code string, asset_type string").write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tables["instrument_mapping"])
    symbols = [{"symbol": s, "asset_type": t} for s, _, t in mapping_rows]
    candidates = quotes.candidate_series(symbols)
    providers = {"data912": data912 or FakeProvider("data912"), "byma_open_data": byma or FakeProvider("byma_open_data"), "yahoo": yahoo or FakeProvider("yahoo")}
    fetched = quotes.run_fetch(spark, candidates, providers, tables["quotes_raw"], tables["quote_requests"], "test", "f1", FROM, TO)
    quotes.run_series(spark, candidates, tables["quotes_raw"], spark.table(tables["trades"]), tables["quote_series"])
    result = quotes.run_market(spark, tables["trades"], tables["instrument_mapping"], tables["quotes_raw"], tables["quote_requests"], tables["quote_series"],
                               tables["fact_market_quotes"], tables["fact_trade_market"], tables["quote_coverage"], "r1")
    return fetched, result, candidates, providers


def _matched(spark, tables):
    return {r.trade_id: r for r in spark.table(tables["fact_trade_market"]).collect()}


JAN_DAYS = [date(2026, 1, d) for d in (5, 6, 7, 8, 9, 12, 13)]


def _steady(close=100.0, days=JAN_DAYS, high=None, low=None):
    return [_row(d, close, high, low) for d in days]


def _trades_on(symbol, days, price, prefix="v"):
    """Operaciones que validan la serie: sin ellas, no hay días comparables."""
    return [_trade(f"{prefix}{symbol}{i}", symbol, d, price) for i, d in enumerate(days)]


def test_saturday_uses_the_close_of_friday(spark, schema):
    tables = _tables(schema)
    trades = _trades_on("AAA", JAN_DAYS, 100.0) + [_trade("sat", "AAA", date(2026, 1, 9), 100.0)]  # effective_trade_date del sábado 10/01 es el viernes 09/01
    _run(spark, tables, trades, data912=FakeProvider("data912", {"AAA": _steady()}), mapping_rows=[("AAA", "ARS", "CEDEAR")])
    row = _matched(spark, tables)["sat"]
    assert (row.match_method, row.quote_date_used, row.quote_age_business_days) == ("EXACT_DAY", date(2026, 1, 9), 0)


def test_holiday_is_imputed_with_its_age(spark, schema):
    """Sin cotización el 16/02 ni el 17/02 (Carnaval): el 18/02 usa el 13/02, a 1 día hábil, porque los feriados no cuentan."""
    tables = _tables(schema)
    days = JAN_DAYS + [date(2026, 2, 13)]
    trades = _trades_on("AAA", days, 100.0) + [_trade("h", "AAA", date(2026, 2, 18), 100.0)]
    _run(spark, tables, trades, data912=FakeProvider("data912", {"AAA": _steady(days=days)}), mapping_rows=[("AAA", "ARS", "CEDEAR")])
    row = _matched(spark, tables)["h"]
    assert (row.match_method, row.quote_date_used, row.quote_age_business_days) == ("PRIOR_DAY_IMPUTED", date(2026, 2, 13), 1)
    assert (row.day_low, row.day_high, row.position_vs_range) == (None, None, "NO_RANGE")  # el rango de otro día no dice nada
    assert row.is_comparable is True


def test_gap_of_five_business_days_is_unavailable(spark, schema):
    tables = _tables(schema)
    trades = _trades_on("AAA", JAN_DAYS, 100.0) + [_trade("gap", "AAA", date(2026, 1, 20), 100.0)]  # 13/01 + 5 días hábiles
    _run(spark, tables, trades, data912=FakeProvider("data912", {"AAA": _steady()}), mapping_rows=[("AAA", "ARS", "CEDEAR")])
    row = _matched(spark, tables)["gap"]
    assert row.match_method == "UNAVAILABLE"
    assert (row.benchmark_close, row.deviation_pct, row.is_above_market, row.is_comparable) == (None, None, None, False)


def test_a_later_quote_is_never_used(spark, schema):
    """Hay cotización el día siguiente y ninguna previa: no hay benchmark."""
    tables = _tables(schema)
    days = JAN_DAYS[1:]
    trades = _trades_on("AAA", days, 100.0) + [_trade("early", "AAA", JAN_DAYS[0], 100.0)]
    _run(spark, tables, trades, data912=FakeProvider("data912", {"AAA": _steady(days=days)}), mapping_rows=[("AAA", "ARS", "CEDEAR")])
    assert _matched(spark, tables)["early"].match_method == "UNAVAILABLE"


def test_scale_factor_brings_the_quote_to_the_price_of_the_trade(spark, schema):
    tables = _tables(schema)
    trades = _trades_on("BON", JAN_DAYS, 876.0)
    _run(spark, tables, trades, data912=FakeProvider("data912", {"BON": _steady(87600.0, high=88000.0, low=87000.0)}), mapping_rows=[("BON", "ARS", "BONO_SOBERANO")])
    row = next(iter(_matched(spark, tables).values()))
    assert (row.match_method, row.benchmark_close, row.day_high, row.day_low, row.position_vs_range) == ("EXACT_DAY", 876.0, 880.0, 870.0, "WITHIN")
    assert row.deviation_pct == pytest.approx(0.0)
    quote = spark.table(tables["fact_market_quotes"]).first()
    assert (quote.close, quote.close_raw, quote.price_scale_factor_applied, quote.currency_code) == (876.0, 87600.0, 0.01, "ARS")


def test_a_series_without_a_scale_is_left_out_and_flagged(spark, schema):
    tables = _tables(schema)
    _run(spark, tables, _trades_on("AAA", JAN_DAYS, 2500.0), byma=FakeProvider("byma_open_data", {"AAA 24HS": _steady()}), mapping_rows=[("AAA", "ARS", "CEDEAR")])
    assert {r.match_method for r in _matched(spark, tables).values()} == {"SCALE_SUSPECT"}
    row = spark.table(tables["quote_coverage"]).first()
    assert (row.excluded_by_validation, row.matched_exact, row.coverage_pct, row.provider) == (len(JAN_DAYS), 0, 0.0, None)


def test_a_biased_series_is_left_out_and_stays_in_the_denominator(spark, schema):
    """AL30 en BYMA: 1,15 pasa el rango de escala, y compararlo marcaría todo "por encima"."""
    tables = _tables(schema)
    _run(spark, tables, _trades_on("BON", JAN_DAYS, 1000.0), byma=FakeProvider("byma_open_data", {"BON 24HS": _steady(87000.0)}), mapping_rows=[("BON", "ARS", "BONO_SOBERANO")])
    assert {r.match_method for r in _matched(spark, tables).values()} == {"SOURCE_BIASED"}
    row = spark.table(tables["quote_coverage"]).first()
    assert (row.trades_eligible, row.excluded_by_validation, row.coverage_pct) == (len(JAN_DAYS), len(JAN_DAYS), 0.0)


def test_above_market_uses_the_tolerance_of_the_type_and_the_side(spark, schema):
    tables = _tables(schema)
    day = JAN_DAYS[0]
    tau = config.ABOVE_MARKET_TOLERANCE_BY_TYPE["CEDEAR"]
    above, inside, below = 100.0 * (1 + tau + 0.001), 100.0 * (1 + tau - 0.001), 100.0 * (1 - tau - 0.001)
    trades = _trades_on("AAA", JAN_DAYS, 100.0) + [
        _trade("buy_above", "AAA", day, above, "BUY"), _trade("sell_above", "AAA", day, above, "SELL"), _trade("buy_inside", "AAA", day, inside, "BUY"),
        _trade("sell_below", "AAA", day, below, "SELL"), _trade("buy_below", "AAA", day, below, "BUY"),
    ]
    _run(spark, tables, trades, data912=FakeProvider("data912", {"AAA": _steady(high=100.5, low=99.5)}), mapping_rows=[("AAA", "ARS", "CEDEAR")])
    rows = _matched(spark, tables)
    assert (rows["buy_above"].is_above_market, rows["buy_above"].is_unfavorable_for_client, rows["buy_above"].position_vs_range) == (True, True, "ABOVE_HIGH")
    assert (rows["sell_above"].is_above_market, rows["sell_above"].is_unfavorable_for_client) == (True, False)
    assert (rows["buy_inside"].is_above_market, rows["buy_inside"].is_unfavorable_for_client) == (False, False)
    assert (rows["sell_below"].is_above_market, rows["sell_below"].is_unfavorable_for_client, rows["sell_below"].position_vs_range) == (False, True, "BELOW_LOW")
    assert rows["buy_below"].is_unfavorable_for_client is False
    assert rows["buy_above"].above_market_tolerance == tau


def test_no_row_is_lost_and_each_method_has_its_reason(spark, schema):
    """1:1 con las operaciones, incluidas las de calidad ERROR, sin mapeo y con otra moneda."""
    tables = _tables(schema)
    day = JAN_DAYS[0]
    trades = _trades_on("AAA", JAN_DAYS, 100.0) + [
        _trade("err", "AAA", day, 1.0, status="ERROR"), _trade("usd", "AAA", day, 1.0, currency="USD"), _trade("nomap", "ZZZ", day, 1.0),
        _trade("opt", "OPT", day, 1.0), _trade("nodate", "AAA", None, 100.0),
    ]
    _run(spark, tables, trades, data912=FakeProvider("data912", {"AAA": _steady()}), mapping_rows=[("AAA", "ARS", "CEDEAR"), ("OPT", "ARS", "OPCION")])
    rows = _matched(spark, tables)
    assert len(rows) == len(trades) == spark.table(tables["trades"]).count()
    assert rows["usd"].match_method == "CURRENCY_MISMATCH"
    assert rows["nomap"].match_method == "UNMAPPED"
    assert rows["opt"].match_method == "UNAVAILABLE"
    assert rows["nodate"].match_method == "UNAVAILABLE"
    assert (rows["err"].match_method, rows["err"].is_comparable) == ("EXACT_DAY", False)


def test_a_failing_symbol_does_not_stop_the_rest_and_is_retried(spark, schema):
    tables = _tables(schema)
    trades = _trades_on("AAA", JAN_DAYS, 100.0) + _trades_on("BBB", JAN_DAYS, 50.0)
    mapping = [("AAA", "ARS", "CEDEAR"), ("BBB", "ARS", "CEDEAR")]
    down = FakeProvider("data912", {"BBB": _steady(50.0)}, broken={"AAA"})
    fetched, _, candidates, providers = _run(spark, tables, trades, data912=down, mapping_rows=mapping)
    assert fetched["errors"] == 1
    logged = {(r.provider, r.provider_symbol): r for r in spark.table(tables["quote_requests"]).collect()}
    assert (logged[("data912", "AAA")].http_status, logged[("data912", "AAA")].rows_returned) == (503, 0) and "caído" in logged[("data912", "AAA")].error
    rows = _matched(spark, tables)
    assert rows["vBBB0"].match_method == "EXACT_DAY"
    assert rows["vAAA0"].match_method == "UNAVAILABLE"
    coverage = {r.symbol: r for r in spark.table(tables["quote_coverage"]).collect()}
    assert "caído" in coverage["AAA"].last_error and coverage["BBB"].last_error is None

    # La corrida siguiente pide solo lo que falló, y el símbolo se recupera.
    recovered = FakeProvider("data912", {"AAA": _steady()})
    again = quotes.run_fetch(spark, candidates, {**providers, "data912": recovered}, tables["quotes_raw"], tables["quote_requests"], "test", "f2", FROM, TO)
    assert recovered.calls == ["AAA"]
    assert (again["requested"], again["errors"]) == (1, 0)


def test_a_replay_without_the_file_of_a_symbol_does_not_stop_the_run(spark, schema, tmp_path):
    """Criterio del spec: el proveedor caído se demuestra con `FileReplayProvider` vacío para un símbolo."""
    tables = _tables(schema)
    quotes.save_replay(tmp_path, "data912", "BBB", _steady(50.0))
    trades = _trades_on("AAA", JAN_DAYS, 100.0) + _trades_on("BBB", JAN_DAYS, 50.0)
    fetched, *_ = _run(spark, tables, trades, data912=quotes.FileReplayProvider("data912", tmp_path), mapping_rows=[("AAA", "ARS", "CEDEAR"), ("BBB", "ARS", "CEDEAR")])
    assert fetched["errors"] == 1
    rows = _matched(spark, tables)
    assert (rows["vBBB0"].match_method, rows["vAAA0"].match_method) == ("EXACT_DAY", "UNAVAILABLE")


def test_coverage_is_consistent_with_requests_and_trades(spark, schema):
    tables = _tables(schema)
    trades = _trades_on("AAA", JAN_DAYS, 100.0) + [_trade("err", "AAA", JAN_DAYS[0], 100.0, status="ERROR"), _trade("gap", "AAA", date(2026, 1, 26), 100.0), _trade("o", "OPT", JAN_DAYS[0], 1.0)]
    _run(spark, tables, trades, data912=FakeProvider("data912", {"AAA": _steady()}), mapping_rows=[("AAA", "ARS", "CEDEAR"), ("OPT", "ARS", "OPCION")])
    coverage = spark.table(tables["quote_coverage"])
    row = {r.symbol: r for r in coverage.collect()}["AAA"]
    assert (row.trades_total, row.trades_eligible, row.matched_exact, row.unavailable, row.provider, row.series_status) == (len(JAN_DAYS) + 2, len(JAN_DAYS) + 1, len(JAN_DAYS), 1, "data912", quotes.OK)
    assert row.coverage_pct == round(len(JAN_DAYS) / (len(JAN_DAYS) + 1), 4)
    overall = quotes.overall_coverage(coverage)
    assert overall["trades_total"] == len(trades)
    assert overall["trades_eligible"] == len(trades) - 1  # OPT sigue en el denominador
    assert overall["exact"] + overall["imputed"] + overall["unavailable"] + overall["excluded_by_validation"] + overall["unmapped"] == overall["trades_eligible"]

    by_provider = {r.provider: r for r in quotes.reconcile_requests(spark.table(tables["quote_requests"]), spark.table(tables["quotes_raw"])).collect()}
    assert by_provider["data912"].rows_returned == by_provider["data912"].rows_stored == len(JAN_DAYS)


def test_rerun_changes_nothing_and_does_not_request_again(spark, schema):
    tables = _tables(schema)
    trades = _trades_on("AAA", JAN_DAYS, 100.0) + _trades_on("BON", JAN_DAYS, 876.0)
    provider = FakeProvider("data912", {"AAA": _steady(), "BON": _steady(87600.0)})
    _, first, candidates, providers = _run(spark, tables, trades, data912=provider, mapping_rows=[("AAA", "ARS", "CEDEAR"), ("BON", "ARS", "BONO_SOBERANO")])
    assert first["trade_market"]["rows_inserted"] == len(trades)
    before = {t: sorted(map(tuple, spark.table(tables[t]).drop("gold_run_id").collect())) for t in ("quote_series", "fact_market_quotes", "fact_trade_market", "quote_coverage", "quotes_raw", "quote_requests")}
    calls = len(provider.calls)

    again = quotes.run_fetch(spark, candidates, providers, tables["quotes_raw"], tables["quote_requests"], "test", "f2", FROM, TO)
    series = quotes.run_series(spark, candidates, tables["quotes_raw"], spark.table(tables["trades"]), tables["quote_series"])
    second = quotes.run_market(spark, tables["trades"], tables["instrument_mapping"], tables["quotes_raw"], tables["quote_requests"], tables["quote_series"],
                               tables["fact_market_quotes"], tables["fact_trade_market"], tables["quote_coverage"], "r2")
    assert again["requested"] == 0 and len(provider.calls) == calls
    assert series == {"rows_inserted": 0, "rows_updated": 0}
    assert all(result == {"rows_inserted": 0, "rows_updated": 0} for result in second.values())
    assert before == {t: sorted(map(tuple, spark.table(tables[t]).drop("gold_run_id").collect())) for t in before}


def test_a_series_that_stops_being_chosen_is_deactivated_and_not_deleted(spark, schema):
    tables = _tables(schema)
    trades = _trades_on("AAA", JAN_DAYS, 100.0)
    _, _, candidates, _ = _run(spark, tables, trades, data912=FakeProvider("data912"), yahoo=FakeProvider("yahoo", {"AAA.BA": _steady()}), mapping_rows=[("AAA", "ARS", "CEDEAR")])
    assert {r.provider for r in spark.table(tables["fact_market_quotes"]).where("is_active").collect()} == {"yahoo"}

    # data912, primero en la cadena, aparece después: la serie de Yahoo queda inactiva y fact_trade_market usa la nueva.
    raw = spark.createDataFrame([("data912", "AAA", "AAA", d, 101.0, 101.0, 101.0, 101.0, 1.0, "f2", datetime.now(timezone.utc)) for d in JAN_DAYS], quotes.RAW_SCHEMA)
    raw.write.format("delta").mode("append").saveAsTable(tables["quotes_raw"])
    quotes.run_series(spark, candidates, tables["quotes_raw"], spark.table(tables["trades"]), tables["quote_series"])
    quotes.run_market(spark, tables["trades"], tables["instrument_mapping"], tables["quotes_raw"], tables["quote_requests"], tables["quote_series"],
                      tables["fact_market_quotes"], tables["fact_trade_market"], tables["quote_coverage"], "r2")
    table = spark.table(tables["fact_market_quotes"])
    assert {r.provider for r in table.where("is_active").collect()} == {"data912"}
    assert table.where("provider = 'yahoo' AND NOT is_active").count() == len(JAN_DAYS)
    assert {r.benchmark_close for r in spark.table(tables["fact_trade_market"]).collect()} == {101.0}


def test_mapping_takes_the_chosen_series_and_a_rebuild_keeps_it(spark, schema, trades_small_csv):
    """La validación llega al mapeo, y reconstruirlo desde la etapa 04 no la borra."""
    from iol_pipeline import bronze, silver

    t = {n: f"{schema}.{n}" for n in ("trades_raw", "ingestion_runs", "trades", "quality_baselines", "quality_events", "instrument_mapping", "quote_series")}
    bronze.ingest_trades(spark, str(trades_small_csv), t["trades_raw"], t["ingestion_runs"])
    silver.run_silver(spark, t["trades_raw"], t["trades"], t["quality_baselines"], t["quality_events"], "p1")
    instruments.run_instruments(spark, t["trades"], t["instrument_mapping"], overrides_path=EMPTY_OVERRIDES)
    before = {r.symbol: r for r in spark.table(t["instrument_mapping"]).collect()}
    assert before["AL30"].mapping_status == "UNVERIFIED"

    candidate = [(s, "yahoo", s + ".BA", 0, 40, 30, 1.0, 1.0, 1.0, 10.0, 0.0, quotes.OK, True, True) for s in ("AL30",)] + [("AL30D", "data912", "AL30D", 0, 40, 30, 1.0, 1.15, 1.0, 0.01, 0.15, quotes.BIASED, False, True)]
    spark.createDataFrame(candidate, quotes.SERIES_SCHEMA).write.format("delta").saveAsTable(t["quote_series"])
    for _ in range(2):
        instruments.run_instruments(spark, t["trades"], t["instrument_mapping"], overrides_path=EMPTY_OVERRIDES, quote_series_table=t["quote_series"])
        rows = {r.symbol: r for r in spark.table(t["instrument_mapping"]).collect()}
        assert (rows["AL30"].provider, rows["AL30"].provider_symbol, rows["AL30"].price_scale_factor, rows["AL30"].mapping_status) == ("yahoo", "AL30.BA", 10.0, "MAPPED")
        assert (rows["AL30D"].provider, rows["AL30D"].provider_symbol, rows["AL30D"].mapping_status) == (None, None, "UNSUPPORTED")
        other = next(s for s in rows if s not in ("AL30", "AL30D"))
        assert (rows[other].provider, rows[other].mapping_status) == (before[other].provider, before[other].mapping_status)  # sin evaluar: queda como estaba
