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
    assert first["duplicate_row_numbers"] == second["duplicate_row_numbers"] == [16]
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


def test_line_with_extra_fields_enters_and_does_not_stop_the_others(spark, schema, tmp_path):
    trades, runs = _tables(schema)
    good = [",".join(["x"] * 8 + [f"TXN{i}", "API"]) for i in range(3)]
    path = tmp_path / "una_mala.csv"
    path.write_text("\n".join([",".join(config.TRADES_CONTRACT), good[0], good[1] + ",de más", good[2]]) + "\n", encoding="utf-8")

    run = bronze.ingest_trades(spark, str(path), trades, runs)

    assert run["status"] == "SUCCESS" and run["rows_inserted"] == 3
    malformed = spark.table(trades).where("extra_fields IS NOT NULL").collect()
    assert [(r.id_transaccion, r.origen, r.extra_fields, r.source_row_number) for r in malformed] == [("TXN1", "API", '["de más"]', 2)]


@pytest.mark.parametrize(("name", "content", "expected"), [
    ("comilla", (",".join(config.TRADES_CONTRACT) + "\n\"x" + ",y" * 9 + "\n").encode("utf-8"), "unexpected end of data"),
    ("codificacion", (",".join(config.TRADES_CONTRACT) + "\nDescripción" + ",y" * 9 + "\n").encode("latin-1"), "codec"),
])
def test_unreadable_file_records_failed_and_raises(spark, schema, tmp_path, name, content, expected):
    trades, runs = _tables(schema)
    bad = tmp_path / f"{name}.csv"
    bad.write_bytes(content)

    with pytest.raises(Exception, match=expected):
        bronze.ingest_trades(spark, str(bad), trades, runs)

    recorded = spark.table(runs).first()
    assert recorded.status == "FAILED"
    assert recorded.source_file_hash is not None and recorded.error
    assert not spark.catalog.tableExists(trades)


def test_file_without_rows_fails_empty_and_writes_nothing(spark, schema, tmp_path):
    trades, runs = _tables(schema)
    header_only = tmp_path / "solo_header.csv"
    header_only.write_text(",".join(config.TRADES_CONTRACT) + "\n", encoding="utf-8")

    run = bronze.ingest_trades(spark, str(header_only), trades, runs)

    assert run["status"] == "FAILED_EMPTY"
    assert not spark.catalog.tableExists(trades)
    assert spark.table(runs).first().status == "FAILED_EMPTY"


def test_failure_after_the_merge_still_records_what_was_written(spark, schema, trades_small_csv, monkeypatch):
    trades, runs = _tables(schema)

    def broken_stats(*args):
        raise ValueError("no cuadra (simulado)")

    monkeypatch.setattr(bronze, "ingestion_stats", broken_stats)
    with pytest.raises(ValueError, match="simulado"):
        bronze.ingest_trades(spark, str(trades_small_csv), trades, runs)

    recorded = spark.table(runs).first()
    assert recorded.status == "FAILED"
    assert recorded.rows_inserted == 39 == spark.table(trades).count()


def test_lost_run_record_is_detected_as_an_orphan(spark, schema, trades_small_csv, monkeypatch):
    trades, runs = _tables(schema)
    assert bronze.orphan_ingestion_runs(spark, trades, runs) == []  # todavía no hay nada

    with monkeypatch.context() as patch:
        def broken_record(*args):
            raise RuntimeError("falla al registrar (simulado)")

        patch.setattr(bronze, "_record_run", broken_record)
        with pytest.raises(RuntimeError, match="simulado"):
            bronze.ingest_trades(spark, str(trades_small_csv), trades, runs)

    assert len(bronze.orphan_ingestion_runs(spark, trades, runs)) == 1  # sin tabla de corridas
    bronze.ingest_trades(spark, str(trades_small_csv), trades, runs)
    assert len(bronze.orphan_ingestion_runs(spark, trades, runs)) == 1  # el reintento no lo tapa


def test_recorded_runs_are_not_orphans(spark, schema, trades_small_csv):
    trades, runs = _tables(schema)
    bronze.ingest_trades(spark, str(trades_small_csv), trades, runs)
    assert bronze.orphan_ingestion_runs(spark, trades, runs) == []
