"""Cada regla de docs/quality_rules.md: un caso positivo y uno negativo, sobre filas armadas para eso.

Las reglas por fila corren juntas en un solo DataFrame, con un escenario por fila y sin estadísticas: armar el
plan de Silver para cada test tarda ~10 s. Las reglas con estadísticas dependen del grupo y van en tests propios.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from iol_pipeline import bronze, config, instruments, quality, silver

NOW = datetime(2026, 11, 1, tzinfo=timezone.utc)
BASELINES_SCHEMA = "rule_code string, group_key string, median_value double, mad_value double, limit_value double, n long, rules_version string"
CLEAN = {
    "fecha": "2026-01-06T15:00:00.000Z",  # martes, 12:00 local: dentro de la rueda
    "tipoTran": "Compra", "id_cliente": "CLI1", "descripcion_titulo": "Cedear Apple Inc.", "moneda": "ARS",
    "simbolo_titulo": "AAPL", "cantidad": "10", "precio": "100.5", "id_transaccion": "TXN1", "origen": "App Mobile",
}
# Nombre del escenario -> campos que pisa de CLEAN. El ID de cada fila es el nombre, salvo que el escenario lo pise.
SCENARIOS = {
    "clean": {},
    "normalized": {"tipoTran": " Venta ", "simbolo_titulo": " msft ", "moneda": " usd ", "origen": "Sitio Web Responsive", "descripcion_titulo": "Cedear Microsoft"},
    "empty_side": {"tipoTran": "  "}, "empty_client": {"id_cliente": "  "}, "empty_channel": {"origen": "  "},
    "empty_date": {"fecha": ""}, "empty_qty": {"cantidad": ""},
    "bad_date": {"fecha": "2026-02-30T15:00:00.000Z"},
    "qty_text": {"cantidad": "abc"}, "qty_comma": {"cantidad": "1,5"}, "qty_nan": {"cantidad": "NaN"},
    "price_text": {"precio": "abc"}, "price_comma": {"precio": "1,5"}, "price_nan": {"precio": "NaN"},
    "qty_decimals": {"cantidad": "10.5"}, "qty_zero_decimals": {"cantidad": "10.00"},
    "qty_zero": {"cantidad": "0"}, "price_zero": {"precio": "0"}, "qty_negative": {"cantidad": "-1"}, "price_negative": {"precio": "-0.5"},
    "bad_side": {"tipoTran": "Canje"}, "bad_currency": {"moneda": "EUR"}, "bad_channel": {"origen": "Fax"},
    "extra_fields": {"extra_fields": '["de más"]'},
    "saturday": {"fecha": "2026-01-10T15:00:00.000Z"},
    "utc_saturday_local_friday": {"fecha": "2026-01-10T01:30:00.000Z"},
    "sunday": {"fecha": "2026-02-22T16:00:00.000Z"},
    "carnival_tuesday": {"fecha": "2026-02-17T15:00:00.000Z"},
    "before_session": {"fecha": "2026-01-06T13:59:59.000Z"},   # 10:59 local
    "session_start": {"fecha": "2026-01-06T14:00:00.000Z"},    # 11:00 local
    "session_end": {"fecha": "2026-01-06T19:59:59.000Z"},      # 16:59 local
    "after_session": {"fecha": "2026-01-06T20:00:00.000Z"},    # 17:00 local
    "outside_calendar": {"fecha": "2026-04-06T15:00:00.000Z"},
    "after_maturity": {"descripcion_titulo": "BONCAP VTO 05/01/2026", "simbolo_titulo": "T1"},
    "before_maturity": {"descripcion_titulo": "BONCAP VTO 30/06/26", "simbolo_titulo": "T2"},
    "warning_and_critical": {"fecha": "2026-01-10T15:00:00.000Z", "extra_fields": '["x"]'},
    "amount_does_not_fit": {"cantidad": "9" * 20, "precio": "9" * 16},
    "price_too_precise": {"precio": "1.123456789"},
    "price_eight_decimals": {"cantidad": "1", "precio": "0.00000001"},
    "many_desc_a": {"simbolo_titulo": "AZN", "descripcion_titulo": "Cedear Astrazeneca"},
    "many_desc_b": {"simbolo_titulo": "AZN", "descripcion_titulo": "Cedear Astra"},
    "conflict_a": {"id_transaccion": "CONFLICT", "precio": "100"}, "conflict_b": {"id_transaccion": "CONFLICT", "precio": "101"},
    "no_id_a": {"id_transaccion": "", "precio": "100"}, "no_id_b": {"id_transaccion": "", "precio": "101"},
}


def _bronze(spark, rows):
    """`rows`: nombre -> campos que pisan a CLEAN. `extra_fields` es opcional."""
    data = []
    for name, row in rows.items():
        merged = {**CLEAN, "id_transaccion": name, **row}
        data.append((*[merged[c] for c in config.TRADES_CONTRACT], merged.get("extra_fields")))
    return bronze.add_lineage(spark, data, "/x.csv", "fh", "run1", NOW)


@pytest.fixture(scope="module")
def results(spark):
    """Escenario -> fila de Silver. Se cruza por `record_hash`, porque varios escenarios comparten o no tienen ID."""
    br = _bronze(spark, SCENARIOS)
    names = {r.source_row_number: name for r, name in zip(br.orderBy("source_row_number").collect(), SCENARIOS)}
    no_stats = spark.createDataFrame([], BASELINES_SCHEMA)
    out = silver.build_silver(silver.type_trades(br), no_stats, "s1", NOW).join(br.select("record_hash", "source_row_number"), "record_hash").collect()
    return {names[r.source_row_number]: r for r in out}


def _stats(spark, rows):
    """Silver con estadísticas calculadas sobre `rows`, una lista de campos que pisan a CLEAN."""
    typed = silver.type_trades(_bronze(spark, {f"TXN{i}": row for i, row in enumerate(rows)}))
    return silver.build_silver(typed, quality.compute_baselines(typed), "s1", NOW).collect()


def _flagged(rows, code):
    return [r for r in rows if code in r.quality_flags]


def test_no_row_is_lost_whatever_its_problems(results):
    assert set(results) == set(SCENARIOS)


def test_clean_row_has_no_flags_and_is_ok(results):
    assert results["clean"].quality_flags == []
    assert results["clean"].quality_status == "OK"


def test_clean_row_is_typed(results):
    row = results["clean"]
    assert row.qty == Decimal(10) and row.price == Decimal("100.5")
    assert row.gross_amount == Decimal("1005")
    assert row.asset_type_rule == "CEDEAR"
    assert row.side == "BUY" and row.channel_code == "APP_MOBILE"


def test_fields_are_normalized(results):
    row = results["normalized"]
    assert (row.side, row.symbol, row.currency_code, row.channel_code) == ("SELL", "MSFT", "USD", "WEB_RESPONSIVE")


@pytest.mark.parametrize("name", ["empty_side", "empty_client", "empty_channel", "no_id_a"])
def test_dq002_empty_field(results, name):
    assert "DQ002_REQUIRED_NULL" in results[name].quality_flags
    assert results[name].quality_status == "CRITICAL"


@pytest.mark.parametrize("name", ["empty_date", "empty_qty"])
def test_dq002_empty_is_not_also_reported_as_unparseable(results, name):
    assert results[name].quality_flags == ["DQ002_REQUIRED_NULL"]


def test_dq003_unparseable_date(results):
    row = results["bad_date"]
    assert row.quality_flags == ["DQ003_TS_UNPARSEABLE"]
    assert row.quality_status == "CRITICAL"
    assert row.effective_trade_date is None


@pytest.mark.parametrize("name", ["conflict_a", "conflict_b"])
def test_dq004_marks_both_versions(results, name):
    assert "DQ004_ID_CONFLICT" in results[name].quality_flags
    assert results[name].quality_status == "CRITICAL"


def test_dq004_negative_and_rows_without_id_are_not_a_conflict(results):
    assert "DQ004_ID_CONFLICT" not in results["clean"].quality_flags
    assert "DQ004_ID_CONFLICT" not in results["no_id_a"].quality_flags
    assert "DQ004_ID_CONFLICT" not in results["no_id_b"].quality_flags


@pytest.mark.parametrize("name", ["qty_text", "qty_comma", "qty_nan", "price_text", "price_comma", "price_nan"])
def test_dq005_not_numeric(results, name):
    assert "DQ005_NUMERIC_UNPARSEABLE" in results[name].quality_flags
    assert results[name].quality_status == "CRITICAL"
    assert results[name].gross_amount is None


def test_dq005_quantity_with_decimals_is_not_rounded(results):
    assert "DQ005_NUMERIC_UNPARSEABLE" in results["qty_decimals"].quality_flags
    assert results["qty_decimals"].qty is None


def test_quantity_with_zero_decimals_is_valid(results):
    assert results["qty_zero_decimals"].qty == Decimal(10)
    assert "DQ005_NUMERIC_UNPARSEABLE" not in results["qty_zero_decimals"].quality_flags


@pytest.mark.parametrize("name", ["qty_zero", "price_zero", "qty_negative", "price_negative"])
def test_dq006_non_positive(results, name):
    assert "DQ006_NON_POSITIVE" in results[name].quality_flags
    assert results[name].quality_status == "ERROR"


@pytest.mark.parametrize("name", ["bad_side", "bad_currency", "bad_channel"])
def test_dq007_domain_invalid(results, name):
    assert "DQ007_DOMAIN_INVALID" in results[name].quality_flags
    assert results[name].quality_status == "ERROR"


def test_dq007_unknown_channel_maps_to_unknown(results):
    assert results["bad_channel"].channel_code == config.CHANNEL_UNKNOWN


def test_dq008_malformed_row(results):
    assert results["extra_fields"].quality_flags == ["DQ008_MALFORMED_ROW"]
    assert results["extra_fields"].quality_status == "CRITICAL"


def test_dq010_saturday_gets_effective_friday(results):
    row = results["saturday"]
    assert "DQ010_SATURDAY" in row.quality_flags
    assert row.effective_trade_date == date(2026, 1, 9)
    assert row.quality_status == "WARNING"


def test_utc_saturday_that_is_local_friday_is_not_flagged(results):
    row = results["utc_saturday_local_friday"]
    assert "DQ010_SATURDAY" not in row.quality_flags
    assert row.effective_trade_date == date(2026, 1, 9)


def test_dq011_sunday_is_not_reassigned(results):
    row = results["sunday"]
    assert "DQ011_SUNDAY" in row.quality_flags
    assert row.quality_status == "ERROR"
    assert row.effective_trade_date == date(2026, 2, 22)


def test_dq012_carnival_tuesday(results):
    assert "DQ012_MARKET_HOLIDAY" in results["carnival_tuesday"].quality_flags
    assert results["carnival_tuesday"].quality_status == "ERROR"
    assert "DQ012_MARKET_HOLIDAY" not in results["clean"].quality_flags


@pytest.mark.parametrize(("name", "outside"), [("before_session", True), ("session_start", False), ("session_end", False), ("after_session", True)])
def test_dq013_session_limits_and_status_stays_ok(results, name, outside):
    assert ("DQ013_OUTSIDE_SESSION" in results[name].quality_flags) is outside
    assert results[name].quality_status == "OK"  # INFO no cambia el estado


def test_dq014_outside_calendar(results):
    row = results["outside_calendar"]
    assert "DQ014_OUTSIDE_CALENDAR" in row.quality_flags
    assert row.effective_trade_date is None
    assert row.quality_status == "ERROR"
    assert "DQ014_OUTSIDE_CALENDAR" not in results["clean"].quality_flags


def test_dq031_symbol_with_many_descriptions(results):
    assert "DQ031_SYMBOL_MULTI_DESC" in results["many_desc_a"].quality_flags
    assert "DQ031_SYMBOL_MULTI_DESC" in results["many_desc_b"].quality_flags
    assert results["many_desc_a"].quality_status == "OK"  # INFO
    assert "DQ031_SYMBOL_MULTI_DESC" not in results["normalized"].quality_flags


def test_dq032_after_maturity(results):
    assert "DQ032_AFTER_MATURITY" in results["after_maturity"].quality_flags
    assert results["after_maturity"].quality_status == "ERROR"
    assert "DQ032_AFTER_MATURITY" not in results["before_maturity"].quality_flags


def test_highest_severity_wins(results):
    row = results["warning_and_critical"]
    assert {"DQ010_SATURDAY", "DQ008_MALFORMED_ROW"} <= set(row.quality_flags)
    assert row.quality_status == "CRITICAL"


def test_dq005_amount_that_does_not_fit_is_flagged(results):
    row = results["amount_does_not_fit"]
    assert row.qty is not None and row.price is not None and row.gross_amount is None
    assert "DQ005_NUMERIC_UNPARSEABLE" in row.quality_flags
    assert row.quality_status == "CRITICAL"


def test_dq005_price_with_more_than_eight_decimals_is_not_rounded(results):
    assert "DQ005_NUMERIC_UNPARSEABLE" in results["price_too_precise"].quality_flags
    assert results["price_too_precise"].price is None


def test_amount_keeps_the_eight_decimals_of_the_price(results):
    row = results["price_eight_decimals"]
    assert row.gross_amount == Decimal("0.00000001")
    assert "DQ005_NUMERIC_UNPARSEABLE" not in row.quality_flags


def test_maturity_date_from_description(spark):
    cases = {"BONCAP T30J6 VTO 30/06/2026": date(2026, 6, 30), "BONO V.30/04/27": date(2027, 4, 30), "BONO 31/02/27": None, "YPF": None}
    df = spark.createDataFrame([(d,) for d in cases], "d string").withColumn("m", instruments.maturity_date(F.col("d")))
    assert {r.d: r.m for r in df.collect()} == cases


def test_every_rule_has_a_severity_and_deferred_rules_are_not_here(spark):
    assert set(quality.row_rules()) == set(config.RULE_SEVERITY)
    assert {"DQ030_INSTRUMENT_UNMAPPED", "DQ033_CURRENCY_VARIANT_MISMATCH"}.isdisjoint(config.RULE_SEVERITY)  # etapa 04


# --- Reglas con estadísticas ------------------------------------------------------
def _amount_rows(extreme):
    return [{"cantidad": str(10 + i), "precio": "100"} for i in range(12)] + [{"cantidad": extreme, "precio": "100"}]


@pytest.mark.parametrize(("extreme", "flagged"), [("100000000", True), ("16", False)])
def test_dq020_extreme_amount(spark, extreme, flagged):
    found = _flagged(_stats(spark, _amount_rows(extreme)), "DQ020_EXTREME_AMOUNT")
    assert [r.qty for r in found] == ([Decimal(extreme)] if flagged else [])
    if flagged:
        assert found[0].quality_status == "WARNING"


def test_dq020_without_dispersion_flags_nothing(spark):
    assert not _flagged(_stats(spark, [{"cantidad": "10", "precio": "100"}] * 5), "DQ020_EXTREME_AMOUNT")


def test_dq020_does_not_mix_currencies(spark):
    ars = [{"cantidad": str(10 + i), "precio": "100"} for i in range(12)]
    usd = [{"cantidad": str(10 + i), "precio": "0.07", "moneda": "USD"} for i in range(12)]
    assert not _flagged(_stats(spark, ars + usd), "DQ020_EXTREME_AMOUNT")  # juntos, las dos escalas parecerían extremas


def _scale_rows(price, description, symbol):
    base = {"simbolo_titulo": symbol, "descripcion_titulo": description}
    return [{**base, "precio": "1.08"}, {**base, "precio": "1.07"}, {**base, "precio": "1.09"}, {**base, "precio": price, "cantidad": "11"}]


def test_dq021_error_in_bonds_warning_in_options_and_negative(spark):
    rows = _stats(spark, _scale_rows("0.0101", "BONO TESORO", "T30A7") + _scale_rows("0.0101", "GFGC72000F", "GFGC72000F") + _scale_rows("1.10", "BONO TESORO DOS", "T31Y7"))
    found = {r.symbol: r for r in _flagged(rows, "DQ021_POSSIBLE_SCALE_ERROR")}
    assert set(found) == {"T30A7", "GFGC72000F"}
    assert found["T30A7"].price == Decimal("0.0101") and found["T30A7"].quality_status == "ERROR"
    assert found["GFGC72000F"].asset_type_rule == "OPCION" and found["GFGC72000F"].quality_status == "WARNING"


def test_dq021_does_not_mix_currencies_of_a_symbol(spark):
    ars = [{"simbolo_titulo": "AL30", "precio": str(876 + i)} for i in range(3)]
    usd = [{"simbolo_titulo": "AL30", "moneda": "USD", "precio": str(0.61 + i / 100)} for i in range(3)]
    assert not _flagged(_stats(spark, ars + usd), "DQ021_POSSIBLE_SCALE_ERROR")  # una mediana única marcaría los dólares


def test_dq022_extreme_quantity_only_flags_the_tail(spark):
    rows = [{"cantidad": str(10 + i), "precio": "100"} for i in range(20)] + [{"cantidad": "99999", "precio": "100"}]
    assert [r.qty for r in _flagged(_stats(spark, rows), "DQ022_EXTREME_QTY")] == [Decimal(99999)]


def test_stats_ignore_rows_that_are_not_eligible(spark):
    rows = [{"precio": "100"}, {"precio": "100"}, {"precio": "100"}, {"precio": "0"}, {"moneda": "EUR", "precio": "900"}, {"id_cliente": "", "precio": "900"}]
    typed = silver.type_trades(_bronze(spark, {f"TXN{i}": row for i, row in enumerate(rows)}))
    scale = quality.compute_baselines(typed).where("rule_code = 'DQ021_POSSIBLE_SCALE_ERROR'").first()
    assert scale.n == 3 and scale.median_value == 100


# --- Eventos -----------------------------------------------------------------------
def test_events_only_above_info_with_details_without_client_and_deterministic(spark):
    br = _bronze(spark, {"TXN0": {"fecha": "2026-01-10T15:00:00.000Z"}, "TXN1": {"fecha": "2026-01-06T22:00:00.000Z"}})
    typed = silver.type_trades(br)
    baselines = quality.compute_baselines(typed)
    silver_df = silver.build_silver(typed, baselines, "s1", NOW)
    events = quality.build_events(silver_df, br, baselines, "p1").collect()
    assert [e.rule_code for e in events] == ["DQ010_SATURDAY"]  # DQ013 es INFO: sin evento
    assert json.loads(events[0].details_json) == {"trade_date_local": "2026-01-10", "effective_trade_date": "2026-01-09"}
    assert events[0].severity == "WARNING"
    assert "CLI" not in events[0].details_json
    assert events[0].is_active is True
    assert quality.build_events(silver_df, br, baselines, "otra").first().event_id == events[0].event_id
