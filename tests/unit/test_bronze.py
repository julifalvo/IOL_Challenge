"""Lectura, hash, dedupe y contadores de la ingesta a Bronze (ADR-002)."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from iol_pipeline import bronze, config

ROW = ["2026-01-05T14:10:00.000Z", "Compra", "CLI1", "Desc", "ARS", "AL30", "100", "876.3", "TXN1", "App Mobile"]
INGESTED_AT = datetime(2026, 11, 1, tzinfo=timezone.utc)


def _lineage(spark, rows):
    return bronze.add_lineage(spark, [(*r, None) for r in rows], "/x.csv", "fh", "run1", INGESTED_AT)


def _write(tmp_path, *lines):
    path = tmp_path / "t.csv"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return str(path)


HEADER = ",".join(config.TRADES_CONTRACT)


def _hash(spark, row):
    return _lineage(spark, [row]).first().record_hash


def test_read_keeps_everything_as_text(tmp_path):
    columns, rows = bronze.read_trades_csv(_write(tmp_path, HEADER, "2026-01-05T14:10:00.000Z,Compra,,NA,ARS,AL30,0100,876.30,TXN1,API"))
    row = dict(zip(columns, rows[0]))
    assert row["cantidad"] == "0100" and row["precio"] == "876.30"  # sin perder ceros: el tipado es de Silver
    assert row["id_cliente"] == "" and row["descripcion_titulo"] == "NA"  # "NA" no se interpreta como nulo


def test_read_fills_missing_trailing_fields_with_empty(tmp_path):
    columns, rows = bronze.read_trades_csv(_write(tmp_path, HEADER, "2026-01-05T14:10:00.000Z,Compra"))
    assert len(rows[0]) == len(columns) + 1  # + extra_fields
    assert rows[0][-1] is None
    assert dict(zip(columns, rows[0]))["origen"] == ""


@pytest.mark.parametrize("position", [0, 1])  # 0: con pandas, en la primera fila las columnas se corren sin error (ADR-002)
def test_read_keeps_extra_fields_without_shifting_columns(tmp_path, position):
    lines = [",".join(ROW), ",".join(ROW)]
    lines[position] = ",".join(ROW + ["de más", "otro"])
    columns, rows = bronze.read_trades_csv(_write(tmp_path, HEADER, *lines))
    assert rows[position][:len(columns)] == tuple(ROW)
    assert rows[position][-1] == '["de más", "otro"]'
    assert rows[1 - position][-1] is None


def test_read_ignores_blank_lines_and_a_utf8_bom(tmp_path):
    path = tmp_path / "t.csv"
    path.write_bytes((HEADER + "\n\n" + ",".join(ROW) + "\n\n").encode("utf-8-sig"))
    columns, rows = bronze.read_trades_csv(str(path))
    assert columns == config.TRADES_CONTRACT
    assert rows == [(*ROW, None)]


def test_read_keeps_quoted_commas(trades_small_csv):
    columns, rows = bronze.read_trades_csv(str(trades_small_csv))
    assert len(rows) == 40
    assert "Cedear Amazon.Com, Inc" in {r[columns.index("descripcion_titulo")] for r in rows}


def test_file_hash_changes_with_content(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    a.write_bytes(b"uno")
    b.write_bytes(b"dos")
    assert bronze.file_sha256(str(a)) == bronze.file_sha256(str(a))
    assert bronze.file_sha256(str(a)) != bronze.file_sha256(str(b))


def test_schema_contract_requires_same_columns_in_same_order():
    assert bronze.schema_matches(config.TRADES_CONTRACT)
    assert not bronze.schema_matches(config.TRADES_CONTRACT[:-1])
    assert not bronze.schema_matches(config.TRADES_CONTRACT + ["extra"])
    assert not bronze.schema_matches(list(reversed(config.TRADES_CONTRACT)))


def test_hash_is_stable_and_changes_with_any_column(spark):
    base = _hash(spark, ROW)
    assert _hash(spark, ROW) == base
    for i in range(len(ROW)):
        changed = list(ROW)
        changed[i] = ROW[i] + "x"
        assert _hash(spark, changed) != base, config.TRADES_CONTRACT[i]


def test_extra_fields_change_the_hash_and_null_does_not(spark):
    with_extra = bronze.add_lineage(spark, [(*ROW, '["de más"]')], "/x.csv", "fh", "run1", INGESTED_AT).first()
    assert with_extra.extra_fields == '["de más"]'
    assert with_extra.record_hash != _hash(spark, ROW)


def test_hash_does_not_confuse_values_moved_between_columns(spark):
    a, b = list(ROW), list(ROW)
    a[2], a[3] = "", "CLI1"
    b[2], b[3] = "CLI1", ""
    assert _hash(spark, a) != _hash(spark, b)


def test_lineage_numbers_rows_from_one(spark):
    rows = _lineage(spark, [ROW, ROW[:8] + ["TXN2", "API"]]).orderBy("source_row_number").collect()
    assert [r.source_row_number for r in rows] == [1, 2]
    assert rows[0].id_transaccion == "TXN1"
    assert (rows[0].source_file, rows[0].source_file_hash, rows[0].ingestion_run_id) == ("/x.csv", "fh", "run1")
    assert rows[0].schema_version == config.SCHEMA_VERSION


def test_stage_keeps_first_exact_duplicate_and_every_version(spark):
    other_version = list(ROW)
    other_version[7] = "999"
    staged = bronze.stage_for_merge(_lineage(spark, [ROW, ROW, other_version])).orderBy("source_row_number").collect()
    assert [r.source_row_number for r in staged] == [1, 3]


def test_duplicate_row_numbers_list_only_the_discarded_rows(spark):
    other_version = list(ROW)
    other_version[7] = "999"
    assert bronze.duplicate_row_numbers(_lineage(spark, [ROW, other_version, ROW, ROW])) == [3, 4]
    assert bronze.duplicate_row_numbers(_lineage(spark, [ROW, other_version])) == []


def test_conflicts_in_the_same_file_when_table_does_not_exist(spark, schema):
    other_version = list(ROW)
    other_version[7] = "999"
    staged = bronze.stage_for_merge(_lineage(spark, [ROW, other_version]))
    assert bronze.count_conflicts(spark, staged, f"{schema}.no_existe") == 1


def test_empty_id_is_not_a_conflict(spark, schema):
    a, b = list(ROW), list(ROW)
    a[8], b[8] = "", ""
    b[7] = "999"
    staged = bronze.stage_for_merge(_lineage(spark, [a, b]))
    assert staged.count() == 2  # no se pierde ninguna
    assert bronze.count_conflicts(spark, staged, f"{schema}.no_existe") == 0


def test_stats_follow_the_invariant():
    stats = bronze.ingestion_stats(40, {"rows_source": 39, "rows_inserted": 39, "rows_already_present": 0}, 1)
    assert stats == {"rows_read": 40, "rows_inserted": 39, "rows_already_present": 0, "rows_conflict": 1, "rows_duplicate_in_file": 1}


def test_stats_fail_if_they_do_not_add_up():
    with pytest.raises(ValueError, match="no cuadra"):
        bronze.ingestion_stats(40, {"rows_source": 39, "rows_inserted": 30, "rows_already_present": 0}, 0)


def test_list_csv_files_only_returns_csv(tmp_path):
    (tmp_path / "b.csv").write_text("")
    (tmp_path / "a.CSV").write_text("")
    (tmp_path / "nota.txt").write_text("")
    assert [Path(p).name for p in bronze.list_csv_files(str(tmp_path))] == ["a.CSV", "b.csv"]


def test_record_hash_uses_the_separator_of_the_data_model(spark):
    row = _lineage(spark, [ROW]).select(F.sha2(F.concat_ws("\u001f", *config.TRADES_CONTRACT), 256).alias("h"), "record_hash").first()
    assert row.h == row.record_hash
