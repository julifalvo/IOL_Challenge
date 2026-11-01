"""Bronze a Silver sobre el fixture: 1:1, idempotencia, reconciliación y salidas del notebook (spec 03).

Los tests que solo leen comparten una corrida: `run_silver` tarda decenas de segundos.
"""
from __future__ import annotations

import pytest

from iol_pipeline import bronze, config, silver

pytestmark = pytest.mark.integration
TABLES = ("trades_raw", "ingestion_runs", "trades", "quality_baselines", "quality_events")


def _tables(schema):
    return {name: f"{schema}.{name}" for name in TABLES}


def _load(spark, t, csv_path):
    bronze.ingest_trades(spark, str(csv_path), t["trades_raw"], t["ingestion_runs"])


def _run(spark, t, run_id="p1"):
    return silver.run_silver(spark, t["trades_raw"], t["trades"], t["quality_baselines"], t["quality_events"], run_id)


def _snapshot(spark, table):
    return sorted(tuple(str(v) for v in r) for r in spark.table(table).collect())


@pytest.fixture(scope="module")
def shared(spark, trades_small_csv):
    """Una corrida sobre el fixture, y la segunda con otro run id. Devuelve tablas, resultados y fotos intermedias."""
    name = "t_silver_shared"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {name}")
    t = _tables(name)
    _load(spark, t, trades_small_csv)
    first = _run(spark, t, "p1")
    before = {n: _snapshot(spark, t[n]) for n in ("trades", "quality_baselines", "quality_events")}
    second = _run(spark, t, "p2")
    yield {"tables": t, "first": first, "second": second, "before": before}
    spark.sql(f"DROP DATABASE IF EXISTS {name} CASCADE")


def test_silver_is_one_to_one_with_bronze(shared):
    first = shared["first"]
    assert first["bronze_rows"] == first["silver_rows"] == first["silver_inserted"] == 39


def test_fixture_cases_get_their_flags(spark, shared):
    rows = {r.trade_id: r for r in spark.table(shared["tables"]["trades"]).collect() if r.trade_id}
    flags = lambda n: rows[f"TXNfixture{n:06d}"].quality_flags  # noqa: E731
    assert "DQ010_SATURDAY" in flags(7)
    assert "DQ010_SATURDAY" not in flags(8)
    assert "DQ012_MARKET_HOLIDAY" in flags(9)
    assert "DQ011_SUNDAY" in flags(10)
    assert "DQ021_POSSIBLE_SCALE_ERROR" in flags(11)
    assert rows["TXNfixture000011"].quality_status == "ERROR"
    assert "DQ021_POSSIBLE_SCALE_ERROR" in flags(22)
    assert rows["TXNfixture000022"].quality_status == "WARNING"  # opción
    assert "DQ002_REQUIRED_NULL" in flags(30)
    assert "DQ003_TS_UNPARSEABLE" in flags(31)
    assert "DQ006_NON_POSITIVE" in flags(32)
    assert "DQ007_DOMAIN_INVALID" in flags(33)
    assert "DQ004_ID_CONFLICT" in flags(2)
    assert "DQ004_ID_CONFLICT" not in flags(1)  # duplicado exacto: una sola fila, sin conflicto


def test_rerun_leaves_the_three_tables_identical(spark, shared):
    second = shared["second"]
    assert second["silver_inserted"] == second["silver_updated"] == second["events_inserted"] == 0
    assert second["baselines_inserted"] == second["baselines_updated"] == 0
    assert {n: _snapshot(spark, shared["tables"][n]) for n in shared["before"]} == shared["before"]  # otro run id: no toca nada


def test_reconciliation_has_no_differences(spark, shared):
    t = shared["tables"]
    check = silver.reconciliation(spark.table(t["trades_raw"]), spark.table(t["trades"]), spark.table(t["quality_events"]))
    assert check == {"bronze_rows": 39, "silver_rows": 39, "bronze_not_in_silver": 0, "silver_not_in_bronze": 0,
                     "not_ok_without_flag": 0, "severe_flags_without_event": 0}


def test_baselines_are_persisted_with_their_version(spark, shared):
    rows = spark.table(shared["tables"]["quality_baselines"]).collect()
    assert {r.rule_code for r in rows} == {"DQ020_EXTREME_AMOUNT", "DQ021_POSSIBLE_SCALE_ERROR", "DQ022_EXTREME_QTY"}
    assert {r.rules_version for r in rows} == {config.RULES_VERSION}
    scale = next(r for r in rows if r.rule_code == "DQ021_POSSIBLE_SCALE_ERROR" and r.group_key == "T30A7")
    assert scale.median_value == pytest.approx(1.0802)


def test_rule_summary_lists_every_rule_without_identifiers(spark, shared):
    summary = silver.rule_summary(spark.table(shared["tables"]["trades"]))
    rows = summary.collect()
    assert {r.rule_code for r in rows} == set(config.RULE_SEVERITY)
    assert next(r for r in rows if r.rule_code == "DQ008_MALFORMED_ROW").rows == 0  # una regla sin casos también se muestra
    assert not {"trade_id", "client_id", "id_cliente", "id_transaccion"} & set(summary.columns)
    example = next(r for r in rows if r.rule_code == "DQ010_SATURDAY").example_record_hash
    assert len(example) == config.EXAMPLE_HASH_CHARS and "TXN" not in example


def test_status_distribution_adds_up(spark, shared):
    assert sum(r.rows for r in silver.status_distribution(spark.table(shared["tables"]["trades"])).collect()) == 39


def test_new_version_in_bronze_flags_the_row_that_was_already_there(spark, schema, tmp_path, trades_small_csv):
    t = _tables(schema)
    _load(spark, t, trades_small_csv)
    _run(spark, t, "p1")
    old = spark.table(t["trades"]).where("trade_id = 'TXNfixture000004'").first()
    assert "DQ004_ID_CONFLICT" not in old.quality_flags

    lines = trades_small_csv.read_text(encoding="utf-8").splitlines()
    changed = next(line for line in lines if "TXNfixture000004" in line).replace("54598.2747", "60000.0000")
    second_file = tmp_path / "segundo.csv"
    second_file.write_text(lines[0] + "\n" + changed + "\n", encoding="utf-8")
    _load(spark, t, second_file)

    result = _run(spark, t, "p2")

    versions = spark.table(t["trades"]).where("trade_id = 'TXNfixture000004'").collect()
    assert result["silver_inserted"] == 1 and result["silver_updated"] >= 1
    assert len(versions) == 2
    assert all("DQ004_ID_CONFLICT" in r.quality_flags and r.quality_status == "CRITICAL" for r in versions)

    # Y una versión nueva de las reglas se reaplica a todas las filas.
    config_version = config.RULES_VERSION
    config.RULES_VERSION = "rules_v2"
    try:
        assert _run(spark, t, "p3")["silver_updated"] == 40
    finally:
        config.RULES_VERSION = config_version
    assert {r.rules_version for r in spark.table(t["trades"]).collect()} == {"rules_v2"}
