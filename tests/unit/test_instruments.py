"""Clasificación, variantes, overrides y mapeo a proveedor (ADR-004). Corren juntos en un solo DataFrame."""
from __future__ import annotations

import csv
from datetime import date, datetime, timezone
from types import SimpleNamespace

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
    ("TGSU2", "Transportadora Gas del Sur", "ARS"),
    ("TGSUD", "Transportadora Gas Del Sur", "USD"),
    ("DOSA", "Mismo Nombre", "ARS"),
    ("DOSB", "Mismo Nombre", "ARS"),
    ("DOSUD", "Mismo Nombre", "USD"),
    ("NOSUFU", "Banco Macro", "USD"),
    ("AB", "Cedear Una Empresa", "ARS"),
    ("ABD", "Cedear Otra Empresa", "USD"),
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
    ("BMA.D", "USD_MEP", "BMA"), ("BA.CD", "USD_MEP", "BA.C"), ("YPFDD", "USD_MEP", "YPFD"),
    ("YM34D", "USD_MEP", "YM34O"), ("TGSUD", "USD_MEP", "TGSU2"),  # por descripción: la base no sale del sufijo
    ("DOSUD", "USD_OTHER", "DOSUD"),  # dos hermanos con la misma descripción: no se elige
    ("NOSUFU", "USD_OTHER", "NOSUFU"),  # misma descripción, pero sin sufijo D o C
    ("ABD", "USD_MEP", "AB"),  # mismo tipo con otra descripción: lo muestra el control de cambio implícito
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
    ("TGSUD", "yahoo", "TGSUD.BA", 1.0, "UNVERIFIED"),
    ("BB37D", "byma_open_data", "BB37D 24HS", 0.01, "UNVERIFIED"),
    ("GFGC72000F", None, None, 1.0, "UNSUPPORTED"),
    ("CRE3W", None, None, 1.0, "UNSUPPORTED"),
    ("D30A6", None, None, 1.0, "UNMAPPED"),
])
def test_provider_follows_adr_008(mapping, symbol, provider, provider_symbol, factor, status):
    row = mapping[symbol]
    assert (row.provider, row.provider_symbol, row.price_scale_factor, row.mapping_status) == (provider, provider_symbol, factor, status)


def test_override_wins_over_the_rule(spark):
    overrides = {"asset_type": {"D30A6": "LETRA_LECAP_BONCAP"}, "price_scale_factor": {"YPFD": "0.1"}, "settlement_variant": {"YM34D": "USD_OTHER"}}
    rows = {r.symbol: r for r in instruments.build_mapping(spark.createDataFrame(TRADES, SCHEMA), overrides, NOW).collect()}
    assert rows["D30A6"].asset_type == "LETRA_LECAP_BONCAP" and rows["D30A6"].asset_type_source == "MANUAL"
    # El proveedor sale del tipo final: si no, nunca se bajarían sus cotizaciones.
    assert (rows["D30A6"].provider, rows["D30A6"].mapping_status, rows["D30A6"].price_scale_factor) == ("byma_open_data", "UNVERIFIED", 0.01)
    assert rows["YPFD"].price_scale_factor == 0.1
    assert rows["YM34D"].settlement_variant == "USD_OTHER"
    assert rows["AL30"].asset_type_source == "RULE"  # los demás no cambian


def test_override_of_a_symbol_that_does_not_exist_fails(spark):
    with pytest.raises(ValueError, match="NOEXISTE"):
        instruments.build_mapping(spark.createDataFrame(TRADES, SCHEMA), {"asset_type": {"NOEXISTE": "ON"}}, NOW)


def test_ai_only_fills_what_the_rules_left_open_and_override_wins(spark):
    ai = spark.createDataFrame(
        [("D30A6", "LETRA_LECAP_BONCAP", "Tesoro", "SOBERANO", None), ("YPFD", "ON", "YPF S.A.", "ENERGIA", None), ("C", "CEDEAR", "Citigroup", "FINANCIERO", "C"), ("CRE3W", "OTRO", None, None, None)],
        "symbol string, asset_type string, issuer string, sector string, underlying_ticker_foreign string")
    rows = {r.symbol: r for r in instruments.build_mapping(spark.createDataFrame(TRADES, SCHEMA), {"asset_type": {"D30A6": "BONO_SOBERANO"}}, NOW, ai).collect()}
    assert rows["YPFD"].asset_type == "ACCION_LOCAL" and rows["YPFD"].asset_type_source == "RULE"  # la IA no pisa la regla
    assert rows["YPFD"].issuer == "YPF S.A." and rows["C"].underlying_ticker_foreign == "C"
    assert rows["D30A6"].asset_type == "BONO_SOBERANO" and rows["D30A6"].asset_type_source == "MANUAL"  # el override le gana a la IA
    assert rows["D30A6"].issuer == "Tesoro"
    assert rows["AL30"].issuer is None


def test_ai_type_for_needs_review_reaches_the_provider(spark):
    ai = spark.createDataFrame([("D30A6", "LETRA_LECAP_BONCAP", None, None, None)], "symbol string, asset_type string, issuer string, sector string, underlying_ticker_foreign string")
    row = instruments.build_mapping(spark.createDataFrame(TRADES, SCHEMA), {}, NOW, ai).where("symbol = 'D30A6'").first()
    assert (row.asset_type, row.asset_type_source, row.provider, row.mapping_status) == ("LETRA_LECAP_BONCAP", "AI", "byma_open_data", "UNVERIFIED")


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
    (["AAA,asset_type,ACCIONN,m,a,2026-11-01"], "ACCIONN"),
    (["AAA,settlement_variant,MEP,m,a,2026-11-01"], "MEP"),
    (["AAA,mapping_status,OK,m,a,2026-11-01"], "mapping_status"),
    (["AAA,price_scale_factor,0.1x,m,a,2026-11-01"], "price_scale_factor"),
    (["AAA,price_scale_factor,-1,m,a,2026-11-01"], "positivo"),
    (["AAA,maturity_date,30/06/2026,m,a,2026-11-01"], "maturity_date"),
    (["AAA,issuer,,m,a,2026-11-01"], "vacío"),
])
def test_read_overrides_rejects_mistakes(tmp_path, lines, message):
    with pytest.raises(ValueError, match=message):
        instruments.read_overrides(_overrides_file(tmp_path, *lines))


def test_repository_files_are_valid():
    assert instruments.read_overrides() is not None


def test_every_override_of_the_repository_says_why_and_who():
    with instruments.OVERRIDES_PATH.open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    assert rows
    assert all(row["reason"].strip() and row["author"].strip() and row["date"].strip() for row in rows)
    stocks = instruments.local_stocks()
    assert {"YPFD", "GGAL", "PAMP"} <= set(stocks)
    assert "AL30" not in stocks and "AAPL" not in stocks


def test_every_option_prefix_points_to_a_known_stock():
    assert set(config.OPTION_UNDERLYING.values()) <= set(instruments.local_stocks())


def test_agreement_with_provider_lists(spark):
    df = spark.createDataFrame([("AAPL", "CEDEAR"), ("AL30", "BONO_SOBERANO"), ("RARO", "NEEDS_REVIEW"), ("YPFD", "ACCION_LOCAL"), ("MAL", "ON")], "symbol string, asset_type string")

    def get(url, timeout):
        if "arg_notes" in url:
            raise ConnectionError("caído")
        symbols = {"arg_cedears": ["AAPL", "RARO", "MAL", "NOESTA"], "arg_bonds": ["AL30"], "arg_corp": [], "arg_stocks": ["YPFD"]}
        name = url.rsplit("/", 1)[1]
        return SimpleNamespace(json=lambda: [{"symbol": s} for s in symbols[name]])

    rows = {r.list_name: r for r in instruments.agreement_with_provider_lists(spark, df, get).collect()}
    cedears = rows["arg_cedears"]
    assert (cedears.symbols_in_dataset, cedears.agree, cedears.needs_review, cedears.disagree, cedears.disagree_examples) == (3, 1, 1, 1, "MAL")
    assert rows["arg_notes"].error.startswith("ConnectionError")  # una lista caída se informa y no corta
    assert rows["arg_cedears"].independent is True and rows["arg_stocks"].independent is False


def test_notebook_outputs(spark, mapping):
    trades = spark.createDataFrame(TRADES, SCHEMA).withColumn("price", F.lit(100.0))
    df = instruments.build_mapping(spark.createDataFrame(TRADES, SCHEMA), {}, NOW)
    variants = {r.symbol: r for r in instruments.variants_with_implied_fx(df, trades).collect()}
    assert {"AL30", "AL30D", "AL30C"} <= set(variants)
    assert variants["AL30D"].implied_fx == 1.0 and variants["AL30"].implied_fx is None
    assert "GFGC72000F" not in variants
    assert instruments.variants_fx_out_of_range(instruments.variants_with_implied_fx(df, trades)).count() > 0  # 1,0 está fuera de rango
    assert {"YPFD", "C", "BA.C", "CARC", "BB37D"} <= {r.symbol for r in instruments.suffix_false_positives(df).collect()}
    assert "D30A6" in {r.symbol for r in instruments.needs_review(df).collect()}
    assert sum(r.symbols for r in instruments.type_summary(df).collect()) == len(mapping)
