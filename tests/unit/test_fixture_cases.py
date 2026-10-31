"""El fixture trades_small.csv cubre los casos que exige el spec 00 (y los que usan las etapas 02-06).

Se verifica con la librería estándar, sin depender de la lógica del pipeline que todavía no existe:
si un caso desaparece del CSV, falla acá y no en un test de etapa con un mensaje confuso.
"""
from __future__ import annotations

import csv
import statistics
from collections import Counter, defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from iol_pipeline import config

TZ = ZoneInfo(config.TZ_MARKET)


@pytest.fixture(scope="module")
def rows(trades_small_csv):
    with trades_small_csv.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        assert reader.fieldnames == config.TRADES_CONTRACT
        return list(reader)


def _local_date(row):
    try:
        ts = datetime.strptime(row["fecha"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=ZoneInfo("UTC"))
    except ValueError:
        return None
    return ts.astimezone(TZ)


def _ids(rows, predicate):
    return {r["id_transaccion"] for r in rows if predicate(r)}


def test_size_and_synthetic_ids(rows):
    assert 30 <= len(rows) <= 60
    assert all(r["id_transaccion"].startswith("TXNfixture") for r in rows)
    assert all(r["id_cliente"] == "" or r["id_cliente"].startswith("CLIFX") for r in rows)


def test_saturday_holiday_and_sunday(rows):
    local = {r["id_transaccion"]: _local_date(r) for r in rows}
    assert _ids(rows, lambda r: local[r["id_transaccion"]] and local[r["id_transaccion"]].weekday() == 5)
    assert _ids(rows, lambda r: local[r["id_transaccion"]] and local[r["id_transaccion"]].date() in config.MARKET_HOLIDAYS)
    assert _ids(rows, lambda r: local[r["id_transaccion"]] and local[r["id_transaccion"]].weekday() == 6)
    # sábado en UTC que en hora local todavía es viernes (ADR-003: manda la fecha local)
    assert "TXNfixture000008" in _ids(rows, lambda r: local[r["id_transaccion"]] and local[r["id_transaccion"]].weekday() == 4)


def test_scale_anomalies_like_t30a7(rows):
    prices = defaultdict(list)
    for r in rows:
        try:
            price = float(r["precio"])
        except ValueError:
            continue
        if price > 0:
            prices[r["simbolo_titulo"]].append((r, price))
    low, high = config.QUALITY_PARAMS["scale_ratio_low"], config.QUALITY_PARAMS["scale_ratio_high"]
    anomalous = []
    for items in prices.values():
        median = statistics.median(p for _, p in items)
        anomalous += [r for r, p in items if p / median < low or p / median > high]
    by_symbol = {r["simbolo_titulo"] for r in anomalous}
    assert "T30A7" in by_symbol
    assert any(r["origen"] == "IOLnet" for r in anomalous)
    assert "GFGC72000F" in by_symbol  # opción: DQ021 como WARNING


def test_ars_symbols_ending_in_d_or_c(rows):
    ars_dc = {r["simbolo_titulo"] for r in rows if r["moneda"] == "ARS" and r["simbolo_titulo"][-1] in "DC"}
    assert "YPFD" in ars_dc
    assert ars_dc - {"YPFD"}, "falta otro símbolo ARS terminado en D/C además de YPFD"


def test_al30_settlement_variants(rows):
    currency = {r["simbolo_titulo"]: r["moneda"] for r in rows if r["simbolo_titulo"].startswith("AL30")}
    assert {"AL30", "AL30D", "AL30C"} <= set(currency)
    assert currency["AL30"] == "ARS" and currency["AL30D"] == "USD" and currency["AL30C"] == "USD"


def test_repeated_ids_same_and_different_content(rows):
    by_id = defaultdict(list)
    for r in rows:
        by_id[r["id_transaccion"]].append(tuple(r[c] for c in config.TRADES_CONTRACT))
    repeated = {k: v for k, v in by_id.items() if len(v) > 1}
    assert any(len(set(v)) == 1 for v in repeated.values()), "falta un ID repetido con mismo contenido"
    assert any(len(set(v)) > 1 for v in repeated.values()), "falta un ID repetido con distinto contenido"


def test_quality_rule_positives_and_parsing_cases(rows):
    assert any(r["id_cliente"] == "" for r in rows)                                  # DQ002
    assert any(_local_date(r) is None for r in rows)                                 # DQ003
    assert any(r["precio"] in {"0", "0.0"} for r in rows)                            # DQ006
    assert any(r["moneda"] not in config.CURRENCIES for r in rows)                   # DQ007
    assert any("," in r["descripcion_titulo"] for r in rows)                         # comillas
    assert set(Counter(r["origen"] for r in rows)) == set(config.CHANNELS)           # 5 canales


def test_spark_reads_fixture_like_the_pipeline(spark, trades_small_csv):
    df = spark.read.option("header", True).csv(str(trades_small_csv))
    assert df.columns == config.TRADES_CONTRACT
    assert df.count() == 40
    descriptions = {r[0] for r in df.select("descripcion_titulo").collect()}
    assert "Cedear Amazon.Com, Inc" in descriptions
