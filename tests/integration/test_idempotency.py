"""Ingesta completa a Bronze sobre archivos: reejecución, archivo nuevo y fallas (spec 02)."""
from __future__ import annotations

import pytest

from iol_pipeline import bronze, config

pytestmark = pytest.mark.integration


def _tables(schema):
    return f"{schema}.trades_raw", f"{schema}.ingestion_runs"


def _counters(run):
    return {k: run[k] for k in ("rows_read", "rows_inserted", "rows_already_present", "rows_duplicate_in_file", "rows_conflict")}


def _table_fingerprint(spark, table):
    rows = spark.table(table).select("id_transaccion", "record_hash", "ingestion_run_id", "source_row_number").collect()
    return sorted((r.id_transaccion, r.record_hash, r.ingestion_run_id, r.source_row_number) for r in rows)


def test_rerun_with_the_same_file_changes_nothing(spark, schema, trades_small_csv):
    trades, runs = _tables(schema)

    first = bronze.ingest_trades(spark, str(trades_small_csv), trades, runs, "p1")
    after_first = _table_fingerprint(spark, trades)
    second = bronze.ingest_trades(spark, str(trades_small_csv), trades, runs, "p1")

    assert first["status"] == second["status"] == "SUCCESS"
    assert _counters(first) == {"rows_read": 40, "rows_inserted": 39, "rows_already_present": 0, "rows_duplicate_in_file": 1, "rows_conflict": 1}
    assert _counters(second) == {"rows_read": 40, "rows_inserted": 0, "rows_already_present": 39, "rows_duplicate_in_file": 1, "rows_conflict": 0}
    assert _table_fingerprint(spark, trades) == after_first  # ni filas ni linaje: conserva la primera corrida
    assert spark.table(runs).count() == 2  # el registro sí crece: una fila por corrida


def test_fixture_keeps_one_row_for_exact_duplicate_and_two_for_conflict(spark, schema, trades_small_csv):
    trades, runs = _tables(schema)
    bronze.ingest_trades(spark, str(trades_small_csv), trades, runs)
    table = spark.table(trades)
    assert table.where("id_transaccion = 'TXNfixture000001'").count() == 1
    assert table.where("id_transaccion = 'TXNfixture000002'").count() == 2
    assert table.where("id_transaccion = 'TXNfixture000001'").first().source_row_number == 1
    assert spark.sql(f"DESCRIBE DETAIL {trades}").first()["format"] == "delta"


def test_new_file_adds_only_new_rows_and_preserves_the_changed_version(spark, schema, tmp_path, trades_small_csv):
    trades, runs = _tables(schema)
    bronze.ingest_trades(spark, str(trades_small_csv), trades, runs)

    lines = trades_small_csv.read_text(encoding="utf-8").splitlines()
    changed = lines[2].replace("0.6073", "0.7000")  # otra versión de una operación ya cargada
    new_row = lines[1].replace("TXNfixture000001", "TXNfixtureNUEVA")
    second_file = tmp_path / "segundo.csv"
    second_file.write_text("\n".join([lines[0], lines[1], changed, new_row]) + "\n", encoding="utf-8")

    run = bronze.ingest_trades(spark, str(second_file), trades, runs)

    assert _counters(run) == {"rows_read": 3, "rows_inserted": 2, "rows_already_present": 1, "rows_duplicate_in_file": 0, "rows_conflict": 1}
    assert spark.table(trades).count() == 41
    assert spark.table(trades).where("id_transaccion = 'TXNfixture000002'").count() == 3  # no se pisa ninguna versión


def test_same_content_in_another_path_inserts_nothing(spark, schema, tmp_path, trades_small_csv):
    trades, runs = _tables(schema)
    bronze.ingest_trades(spark, str(trades_small_csv), trades, runs)
    copy = tmp_path / "copia.csv"
    copy.write_bytes(trades_small_csv.read_bytes())

    assert bronze.ingest_trades(spark, str(copy), trades, runs)["rows_inserted"] == 0


def test_missing_column_fails_schema_and_leaves_the_table_untouched(spark, schema, tmp_path, trades_small_csv):
    trades, runs = _tables(schema)
    bronze.ingest_trades(spark, str(trades_small_csv), trades, runs)
    before = _table_fingerprint(spark, trades)
    bad = tmp_path / "malo.csv"
    bad.write_text(",".join(config.TRADES_CONTRACT[:-1]) + "\n" + ",".join(["x"] * 9) + "\n", encoding="utf-8")

    run = bronze.ingest_trades(spark, str(bad), trades, runs)

    assert run["status"] == "FAILED_SCHEMA"
    assert "DQ001" in run["error"]
    assert _table_fingerprint(spark, trades) == before
    recorded = spark.table(runs).where(f"ingestion_run_id = '{run['ingestion_run_id']}'").first()
    assert recorded.status == "FAILED_SCHEMA" and recorded.rows_inserted == 0 and recorded.ended_at is not None


def test_first_file_with_bad_schema_does_not_create_the_table(spark, schema, tmp_path):
    trades, runs = _tables(schema)
    bad = tmp_path / "malo.csv"
    bad.write_text("a,b\n1,2\n", encoding="utf-8")

    assert bronze.ingest_trades(spark, str(bad), trades, runs)["status"] == "FAILED_SCHEMA"
    assert not spark.catalog.tableExists(trades)


def test_malformed_file_records_failed_and_raises(spark, schema, tmp_path):
    trades, runs = _tables(schema)
    bad = tmp_path / "roto.csv"
    bad.write_text(",".join(config.TRADES_CONTRACT) + "\n" + ",".join(["x"] * 12) + "\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Línea 2"):
        bronze.ingest_trades(spark, str(bad), trades, runs)

    recorded = spark.table(runs).first()
    assert recorded.status == "FAILED"
    assert recorded.source_file_hash is not None and recorded.error
    assert not spark.catalog.tableExists(trades)
