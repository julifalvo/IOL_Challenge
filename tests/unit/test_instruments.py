"""Clasificación, variantes, overrides y mapeo a proveedor (ADR-004). Corren juntos en un solo DataFrame."""
from __future__ import annotations

from datetime import date, datetime, timezone

import pytest
from pyspark.sql import functions as F

from iol_pipeline import config, instruments

NOW = datetime(2026, 11, 1, tzinfo=timezone.utc)
SCHEMA = "symbol string, description_raw string, currency_code string"
TRADES = [
    ("AL30", "Bono Rep. Argentina Usd Step Up 2030", "ARS"),
    ("AL30D", "Bono Rep. Argentina Usd Step Up 2030", "USD"),
    ("AL30C", "Bono Rep. Argentina Usd Step Up 2030", "USD"),
    ("YPFD", "YPF", "ARS"),
    ("YPFDD", "Ypf", "USD"),
    ("C", "Cedear Citigroup Inc", "ARS"),
    ("BA.C", "Cedear Bank Of America Corpora", "ARS"),
    ("BA.CD", "Cedear Bank Of America Corp.", "USD"),
    ("BMA", "Banco Macro", "ARS"),
    ("BMA.D", "Banco Macro", "USD"),
    ("CARC", "Carboclor S.A.", "ARS"),
    ("YM34D", "On Ypf Cl. Xxxiv Regs 8.25% V 17/01/34", "USD"),
    ("YM34O", "On Ypf Cl. Xxxiv Regs 8.25% V 17/01/34", "ARS"),
    ("GFGC72000F", "GFGC72000F", "ARS"),
    ("COMC47.0AB", "COMC47.0AB", "ARS"),
    ("COMV61.0FE", "COMV61.0FE", "ARS"),
    ("ZZZC1000", "ZZZC1000", "ARS"),
    ("T30J6", "BONCAP T30J6 VTO 30/06/2026", "ARS"),
    ("X29Y6", "L. Tes. Nac. Aj. Cer A Desc. 29/05/26 $", "ARS"),
    ("D30E6", "LELINK VTO 30/01/26 (D30E6)", "ARS"),
    ("TZXD7", "BONTES $ A DESC AJ CER Vto. 15/12/27", "ARS"),
    ("RUCDO", "Msu Energy S.A. Regs 9.75% Vto. 05/12/30", "ARS"),
    ("YMCJO", "YPF S.A. - CLASE XVIII- Vto. 30/9/2033", "ARS"),
    ("BACGO", "O N Banco Macro S A 8% Vto.23/06/2029", "ARS"),
    ("BB37D", "Bono Pcia. Bs. As. REGS New 2037", "ARS"),
    ("CRE3W", "WARRANT CRESUD S.A.C.I.F. VTO 08/03/26", "ARS"),
    ("D30A6", "D30a6 Vto 30/04/2026", "ARS"),
    ("MISTD", "Otra Cosa Sa", "USD"),
    ("MIST", "Empresa Distinta", "ARS"),
    ("SOLOEUR", "Algo", "EUR"),
    ("AZN", "Cedear Astrazeneca", "ARS"),
    ("AZN", "Cedear Astrazeneca", "ARS"),
    ("AZN", "Cedear Astra", "ARS"),
    ("MIX", "Empresa", "ARS"),
    ("MIX", "Empresa", "ARS"),
    ("MIX", "Empresa", "USD"),
]


@pytest.fixture(scope="module")
def mapping(spark):
    rows = instruments.build_mapping(spark.createDataFrame(TRADES, SCHEMA), {}, NOW).collect()
    return {r.symbol: r for r in rows}


def test_one_row_per_symbol(mapping):
    assert set(mapping) == {t[0] for t in TRADES}


@pytest.mark.parametrize(("symbol", "asset_type"), [
    ("AL30", "BONO_SOBERANO"), ("TZXD7", "BONO_SOBERANO"), ("BB37D", "BONO_SUBSOBERANO"), ("C", "CEDEAR"), ("BA.C", "CEDEAR"),
    ("YPFD", "ACCION_LOCAL"), ("CARC", "ACCION_LOCAL"), ("BMA", "ACCION_LOCAL"),
    ("T30J6", "LETRA_LECAP_BONCAP"), ("X29Y6", "LETRA_LECAP_BONCAP"), ("D30E6", "LETRA_LECAP_BONCAP"),
    ("YM34D", "ON"), ("RUCDO", "ON"), ("YMCJO", "ON"), ("BACGO", "ON"), ("CRE3W", "WARRANT"),
    ("GFGC72000F", "OPCION"), ("COMC47.0AB", "OPCION"), ("COMV61.0FE", "OPCION"), ("D30A6", "NEEDS_REVIEW"),
])
def test_asset_type_by_rules(mapping, symbol, asset_type):
    assert mapping[symbol].asset_type == asset_type
    assert mapping[symbol].asset_type_source == "RULE"


@pytest.mark.parametrize(("symbol", "variant", "underlying"), [
    ("AL30D", "USD_MEP", "AL30"), ("AL30C", "USD_CCL", "AL30"), ("AL30", "ARS", "AL30"),
    ("YPFD", "ARS", "YPFD"), ("C", "ARS", "C"), ("BA.C", "ARS", "BA.C"), ("CARC", "ARS", "CARC"),
    ("YM34D", "USD_OTHER", "YM34D"), ("BMA.D", "USD_MEP", "BMA"), ("BA.CD", "USD_MEP", "BA.C"), ("YPFDD", "USD_MEP", "YPFD"),
])
def test_settlement_variant_and_underlying(mapping, symbol, variant, underlying):
    assert mapping[symbol].settlement_variant == variant
    assert mapping[symbol].underlying_key == underlying


def test_suffix_with_a_different_base_is_not_a_variant(mapping):
    assert mapping["MISTD"].settlement_variant == "USD_OTHER"  # existe MIST, pero es otra empresa
    assert mapping["MISTD"].underlying_key == "MISTD"
    assert mapping["MISTD"].asset_type == "NEEDS_REVIEW"


def test_variant_inherits_the_type_of_its_base(mapping):
    assert mapping["YPFDD"].asset_type == "ACCION_LOCAL"


def test_currency_outside_the_domain_is_unknown_variant(mapping):
    assert mapping["SOLOEUR"].settlement_variant == "UNKNOWN"


def test_option_underlying_uses_the_prefix(mapping):
    assert mapping["GFGC72000F"].underlying_key == "GGAL"
    assert mapping["COMC47.0AB"].underlying_key == "COME"
    assert mapping["ZZZC1000"].underlying_key == "ZZZC1000"  # prefijo desconocido: no se inventa


def test_canonical_description_and_currency_are_the_most_frequent(mapping):
    assert mapping["AZN"].description_canonical == "Cedear Astrazeneca"
    assert mapping["MIX"].currency_code == "ARS"


def test_maturity_date_accepts_single_digit_day_and_month(mapping):
    assert mapping["YMCJO"].maturity_date == date(2033, 9, 30)
    assert mapping["T30J6"].maturity_date == date(2026, 6, 30)
    assert mapping["C"].maturity_date is None


@pytest.mark.parametrize(("symbol", "provider", "provider_symbol", "factor", "status"), [
    ("AL30", "data912", "AL30", 0.01, "UNVERIFIED"),
    ("AL30D", "data912", "AL30D", 0.01, "UNVERIFIED"),
    ("YPFD", "data912", "YPFD", 1.0, "UNVERIFIED"),
    ("C", "data912", "C", 1.0, "UNVERIFIED"),
    ("BA.CD", "yahoo", "BA.CD.BA", 1.0, "UNVERIFIED"),
    ("YPFDD", "yahoo", "YPFDD.BA", 1.0, "UNVERIFIED"),
    ("T30J6", "byma_open_data", "T30J6 24HS", 0.01, "UNVERIFIED"),
    ("YM34D", "byma_open_data", "YM34D 24HS", 0.01, "UNVERIFIED"),
    ("BB37D", "byma_open_data", "BB37D 24HS", 0.01, "UNVERIFIED"),
    ("GFGC72000F", None, None, 1.0, "UNSUPPORTED"),
    ("CRE3W", None, None, 1.0, "UNSUPPORTED"),
    ("D30A6", None, None, 1.0, "UNMAPPED"),
])
def test_provider_follows_adr_008(mapping, symbol, provider, provider_symbol, factor, status):
    row = mapping[symbol]
    assert (row.provider, row.provider_symbol, row.price_scale_factor, row.mapping_status) == (provider, provider_symbol, factor, status)


def test_override_wins_over_the_rule(spark):
    overrides = {"asset_type": {"D30A6": "LETRA_LECAP_BONCAP"}, "price_scale_factor": {"YPFD": "0.1"}, "settlement_variant": {"YM34D": "USD_MEP"}}
    rows = {r.symbol: r for r in instruments.build_mapping(spark.createDataFrame(TRADES, SCHEMA), overrides, NOW).collect()}
    assert rows["D30A6"].asset_type == "LETRA_LECAP_BONCAP" and rows["D30A6"].asset_type_source == "MANUAL"
    assert rows["YPFD"].price_scale_factor == 0.1
    assert rows["YM34D"].settlement_variant == "USD_MEP"
    assert rows["AL30"].asset_type_source == "RULE"  # los demás no cambian


def _overrides_file(tmp_path, *lines):
    path = tmp_path / "overrides.csv"
    path.write_text("\n".join([",".join(instruments.OVERRIDE_COLUMNS), *lines]) + "\n", encoding="utf-8")
    return path


def test_read_overrides_groups_by_field(tmp_path):
    path = _overrides_file(tmp_path, "AAA,asset_type,ON,motivo,autor,2026-11-01", "BBB,asset_type,OTRO,motivo,autor,2026-11-01")
    assert instruments.read_overrides(path) == {"asset_type": {"AAA": "ON", "BBB": "OTRO"}}


@pytest.mark.parametrize(("lines", "message"), [
    (["AAA,campo_raro,x,m,a,2026-11-01"], "campo desconocido"),
    (["AAA,asset_type,ON,m,a,2026-11-01", "AAA,asset_type,OTRO,m,a,2026-11-01"], "repetido"),
])
def test_read_overrides_rejects_mistakes(tmp_path, lines, message):
    with pytest.raises(ValueError, match=message):
        instruments.read_overrides(_overrides_file(tmp_path, *lines))


def test_repository_files_are_valid():
    assert instruments.read_overrides() is not None
    stocks = instruments.local_stocks()
    assert {"YPFD", "GGAL", "PAMP"} <= set(stocks)
    assert "AL30" not in stocks and "AAPL" not in stocks


def test_every_option_prefix_points_to_a_known_stock():
    assert set(config.OPTION_UNDERLYING.values()) <= set(instruments.local_stocks())


def test_notebook_outputs(spark, mapping):
    trades = spark.createDataFrame(TRADES, SCHEMA).withColumn("price", F.lit(100.0))
    df = instruments.build_mapping(spark.createDataFrame(TRADES, SCHEMA), {}, NOW)
    variants = {r.symbol: r for r in instruments.variants_with_implied_fx(df, trades).collect()}
    assert {"AL30", "AL30D", "AL30C"} <= set(variants)
    assert variants["AL30D"].implied_fx == 1.0 and variants["AL30"].implied_fx is None
    assert "GFGC72000F" not in variants
    assert {"YPFD", "C", "BA.C", "CARC", "BB37D"} <= {r.symbol for r in instruments.suffix_false_positives(df).collect()}
    assert "D30A6" in {r.symbol for r in instruments.needs_review(df).collect()}
    assert sum(r.symbols for r in instruments.type_summary(df).collect()) == len(mapping)
