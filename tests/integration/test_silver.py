"""Bronze a Silver sobre el fixture: 1:1, idempotencia, reconciliación y salidas del notebook (spec 03).

Los tests que solo leen comparten una corrida: `run_silver` tarda decenas de segundos.
"""
from __future__ import annotations

from datetime import date

import pytest

from iol_pipeline import bronze, config, instruments, quality, silver

pytestmark = pytest.mark.integration
TABLES = ("trades_raw", "ingestion_runs", "trades", "quality_baselines", "quality_events", "instrument_mapping")


def _tables(schema):
    return {name: f"{schema}.{name}" for name in TABLES}


def _load(spark, t, csv_path):
    bronze.ingest_trades(spark, str(csv_path), t["trades_raw"], t["ingestion_runs"])


def _run(spark, t, run_id="p1"):
    return silver.run_silver(spark, t["trades_raw"], t["trades"], t["quality_baselines"], t["quality_events"], run_id, t["instrument_mapping"])


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
    assert second["silver_inserted"] == second["silver_updated"] == second["events_inserted"] == second["events_updated"] == 0
    assert second["baselines_inserted"] == second["baselines_updated"] == 0
    assert {n: _snapshot(spark, shared["tables"][n]) for n in shared["before"]} == shared["before"]  # otro run id: no toca nada


def test_reconciliation_has_no_differences(spark, shared):
    t = shared["tables"]
    check = silver.reconciliation(spark.table(t["trades_raw"]), spark.table(t["trades"]), spark.table(t["quality_events"]))
    assert check == {"bronze_rows": 39, "silver_rows": 39, "bronze_not_in_silver": 0, "silver_not_in_bronze": 0,
                     "not_ok_without_flag": 0, "severe_flags_without_event": 0, "active_events_without_flag": 0}


def test_baselines_are_persisted_with_their_version(spark, shared):
    rows = spark.table(shared["tables"]["quality_baselines"]).collect()
    assert {r.rule_code for r in rows} == {"DQ020_EXTREME_AMOUNT", "DQ021_POSSIBLE_SCALE_ERROR", "DQ022_EXTREME_QTY"}
    assert {r.rules_version for r in rows} == {config.RULES_VERSION}
    scale = next(r for r in rows if r.rule_code == "DQ021_POSSIBLE_SCALE_ERROR" and r.group_key == "T30A7|ARS")
    assert scale.median_value == pytest.approx(1.0802)


def test_rule_summary_lists_every_rule_without_identifiers(spark, shared):
    summary = silver.rule_summary(spark.table(shared["tables"]["trades"]))
    rows = summary.collect()
    assert {r.rule_code for r in rows} == set(config.RULE_SEVERITY)
    assert next(r for r in rows if r.rule_code == "DQ008_MALFORMED_ROW").rows == 0  # una regla sin casos también se muestra
    assert not {"trade_id", "client_id", "id_cliente", "id_transaccion"} & set(summary.columns)
    example = next(r for r in rows if r.rule_code == "DQ010_SATURDAY").example_record_hash
    assert len(example) == config.EXAMPLE_HASH_CHARS and "TXN" not in example


def test_event_examples_cover_every_example_rule_without_identifiers(spark, shared):
    examples = silver.event_examples(spark.table(shared["tables"]["quality_events"]))
    rows = examples.collect()
    assert "DQ010_SATURDAY" in {r.rule_code for r in rows} and "DQ021_POSSIBLE_SCALE_ERROR" in {r.rule_code for r in rows}
    assert max(sum(1 for r in rows if r.rule_code == code) for code in config.EVENT_EXAMPLE_RULES) <= config.EVENT_EXAMPLES_PER_RULE
    assert examples.columns == ["rule_code", "severity", "details_json"]


def test_missing_baselines_fail_instead_of_silently_skipping_rules(spark, shared, monkeypatch):
    t = shared["tables"]
    monkeypatch.setattr(config, "RULES_VERSION", "rules_sin_estadisticas")
    monkeypatch.setattr(quality, "compute_baselines", lambda typed: spark.table(t["quality_baselines"]).limit(0))
    with pytest.raises(ValueError, match="no tiene estadísticas"):
        _run(spark, t, "p9")


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



def test_new_rules_version_reapplies_everything_and_keeps_the_statistics(spark, schema, trades_small_csv, monkeypatch):
    t = _tables(schema)
    _load(spark, t, trades_small_csv)
    _run(spark, t, "p1")
    scale = lambda: spark.table(t["trades"]).where("array_contains(quality_flags, 'DQ021_POSSIBLE_SCALE_ERROR')").count()  # noqa: E731
    before = scale()
    assert before > 0

    monkeypatch.setattr(config, "RULES_VERSION", "rules_v2")
    result = _run(spark, t, "p2")

    assert result["silver_updated"] == 39
    assert {r.rules_version for r in spark.table(t["trades"]).collect()} == {"rules_v2"}
    assert scale() == before  # las estadísticas de la versión nueva existen: las reglas no se apagan
    events = spark.table(t["quality_events"])
    assert events.where("rules_version = 'rules_v2' AND is_active").count() > 0
    assert events.where("rules_version != 'rules_v2' AND is_active").count() == 0  # los viejos no se borran, quedan inactivos


def test_calendar_change_reaches_rows_that_are_already_there(spark, schema, trades_small_csv, monkeypatch):
    t = _tables(schema)
    _load(spark, t, trades_small_csv)
    _run(spark, t, "p1")
    saturday = lambda: spark.table(t["trades"]).where("trade_id = 'TXNfixture000007'").first().effective_trade_date  # noqa: E731
    assert saturday() == date(2026, 1, 9)

    monkeypatch.setitem(config.MARKET_HOLIDAYS, date(2026, 1, 9), "feriado de prueba")
    _run(spark, t, "p2")

    assert saturday() == date(2026, 1, 8)


def test_new_file_deactivates_events_of_flags_that_are_gone(spark, schema, tmp_path, trades_small_csv):
    t = _tables(schema)
    _load(spark, t, trades_small_csv)
    _run(spark, t, "p1")
    total_before = spark.table(t["quality_events"]).count()

    lines = trades_small_csv.read_text(encoding="utf-8").splitlines()
    base = next(line for line in lines if "TXNfixture000007" in line)
    extra = [base.replace("TXNfixture000007", f"TXNnuevo{i:04d}").replace(",2,19625.8112,", f",{100000 + i},19625.8112,") for i in range(30)]
    second_file = tmp_path / "mas.csv"
    second_file.write_text("\n".join([lines[0], *extra]) + "\n", encoding="utf-8")
    _load(spark, t, second_file)
    _run(spark, t, "p2")

    events = spark.table(t["quality_events"])
    assert events.where("NOT is_active").count() > 0
    assert events.count() >= total_before  # no se borra ninguno
    check = silver.reconciliation(spark.table(t["trades_raw"]), spark.table(t["trades"]), events)
    assert check["active_events_without_flag"] == check["severe_flags_without_event"] == 0


def test_mapping_then_silver_is_stable_and_detects_a_missing_symbol(spark, schema, trades_small_csv):
    t = _tables(schema)
    _load(spark, t, trades_small_csv)
    _run(spark, t, "p1")  # primera vez: todavía no hay mapeo

    first = instruments.run_instruments(spark, t["trades"], t["instrument_mapping"])
    assert first["rows_inserted"] == first["symbols"] == spark.table(t["trades"]).select("symbol").distinct().where("symbol IS NOT NULL").count()
    assert instruments.run_instruments(spark, t["trades"], t["instrument_mapping"])["rows_updated"] == 0

    reapplied = _run(spark, t, "p2")
    stable = _run(spark, t, "p3")
    assert stable["silver_inserted"] == stable["silver_updated"] == stable["events_inserted"] == stable["events_updated"] == 0
    flags = {flag for r in spark.table(t["trades"]).collect() for flag in r.quality_flags}
    assert "DQ030_INSTRUMENT_UNMAPPED" not in flags
    assert reapplied["silver_rows"] == 39

    spark.sql(f"DELETE FROM {t['instrument_mapping']} WHERE symbol = 'AAPL'")
    _run(spark, t, "p4")
    aapl = spark.table(t["trades"]).where("symbol = 'AAPL'").collect()
    assert aapl and all("DQ030_INSTRUMENT_UNMAPPED" in r.quality_flags for r in aapl)

    instruments.run_instruments(spark, t["trades"], t["instrument_mapping"])
    _run(spark, t, "p5")
    assert all("DQ030_INSTRUMENT_UNMAPPED" not in r.quality_flags for r in spark.table(t["trades"]).where("symbol = 'AAPL'").collect())
    assert spark.table(t["quality_events"]).where("rule_code = 'DQ030_INSTRUMENT_UNMAPPED' AND is_active").count() == 0


def test_enrichment_survives_rerunning_the_mapping(spark, schema, trades_small_csv):
    t = _tables(schema)
    _load(spark, t, trades_small_csv)
    _run(spark, t, "p1")
    enrichment = f"{schema}.instrument_enrichment"
    spark.createDataFrame([("AAPL", "CEDEAR", "Apple Inc.", "TECNOLOGIA", "AAPL")], "symbol string, asset_type string, issuer string, sector string, underlying_ticker_foreign string").write.format("delta").saveAsTable(enrichment)

    instruments.run_instruments(spark, t["trades"], t["instrument_mapping"], enrichment_table=enrichment)
    again = instruments.run_instruments(spark, t["trades"], t["instrument_mapping"], enrichment_table=enrichment)

    assert again["rows_updated"] == again["rows_inserted"] == 0  # estable: no deshace a la IA
    row = spark.table(t["instrument_mapping"]).where("symbol = 'AAPL'").first()
    assert row.issuer == "Apple Inc." and row.underlying_ticker_foreign == "AAPL"


def test_rows_without_id_go_through_and_rerun_changes_nothing(spark, schema, tmp_path, trades_small_csv):
    t = _tables(schema)
    lines = trades_small_csv.read_text(encoding="utf-8").splitlines()
    path = tmp_path / "sin_id.csv"
    path.write_text("\n".join([lines[0], lines[1].replace("TXNfixture000001", ""), lines[2]]) + "\n", encoding="utf-8")
    _load(spark, t, path)

    first = _run(spark, t, "p1")
    second = _run(spark, t, "p2")

    assert first["silver_inserted"] == 2
    assert second["silver_inserted"] == second["silver_updated"] == 0
    row = spark.table(t["trades"]).where("trade_id IS NULL").first()
    assert "DQ002_REQUIRED_NULL" in row.quality_flags and row.quality_status == "CRITICAL"
