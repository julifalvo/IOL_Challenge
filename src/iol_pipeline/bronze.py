"""Ingesta del CSV a Bronze: texto original + linaje, MERGE idempotente y una fila por corrida (ADR-002)."""
from __future__ import annotations

import csv
import hashlib
import json
import os
import uuid
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from iol_pipeline import config, io

MERGE_KEYS = ["id_transaccion", "record_hash"]
RUNS_SCHEMA = (
    "ingestion_run_id string, pipeline_run_id string, source_file string, source_file_hash string, "
    "rows_read long, rows_inserted long, rows_already_present long, rows_conflict long, "
    "rows_duplicate_in_file long, duplicate_row_numbers array<long>, status string, error string, started_at timestamp, ended_at timestamp"
)


def list_csv_files(directory: str) -> list[str]:
    return [os.path.join(directory, name) for name in sorted(os.listdir(directory)) if name.lower().endswith(".csv")]


def file_sha256(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(config.HASH_CHUNK_BYTES), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_trades_csv(path: str) -> tuple[list[str], list[tuple[str, ...]]]:
    """Devuelve (columnas, filas), todo como texto y sin interpretar nada. Cada fila lleva un campo más al
    final: `extra_fields`, JSON con lo que excede al header, o None.

    Una línea con campos de más entra igual: saltearla o recortarla perdería datos, y la marca Silver (DQ008).
    Un campo que falta queda vacío (DQ002). Las líneas en blanco no son filas.
    """
    with open(path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.reader(fh, strict=True)
        columns = next(reader, [])
        rows = []
        size = len(columns)
        for row in reader:
            if row:
                extra = json.dumps(row[size:], ensure_ascii=False) if len(row) > size else None
                rows.append((*row[:size], *[""] * (size - len(row)), extra))
    return columns, rows


def schema_matches(columns: list[str]) -> bool:
    return list(columns) == config.TRADES_CONTRACT


def add_lineage(spark: SparkSession, rows: list[tuple[str | None, ...]], source_file: str, source_file_hash: str,
                ingestion_run_id: str, ingested_at: datetime) -> DataFrame:
    """Columnas originales + linaje. `source_row_number`: 1 = primera fila de datos del archivo.

    Linaje con literales, no con `current_timestamp()`: serverless no tiene cache, el origen se evalúa
    más de una vez y tiene que dar siempre lo mismo.
    """
    data = [(*row, number) for number, row in enumerate(rows, start=1)]
    schema = ", ".join(f"{c} string" for c in config.TRADES_CONTRACT) + ", extra_fields string, source_row_number long"
    separator = config.RECORD_HASH_SEPARATOR
    return (spark.createDataFrame(data, schema)
            .withColumn("record_hash", F.sha2(F.concat_ws(separator, *config.TRADES_CONTRACT, "extra_fields"), config.RECORD_HASH_BITS))
            .withColumn("source_file", F.lit(source_file))
            .withColumn("source_file_hash", F.lit(source_file_hash))
            .withColumn("ingestion_run_id", F.lit(ingestion_run_id))
            .withColumn("ingested_at", F.lit(ingested_at))
            .withColumn("schema_version", F.lit(config.SCHEMA_VERSION)))


def _with_duplicate_rank(df: DataFrame) -> DataFrame:
    order = Window.partitionBy(*MERGE_KEYS).orderBy("source_row_number")
    return df.withColumn("_rank", F.row_number().over(order))


def stage_for_merge(df: DataFrame) -> DataFrame:
    """Una fila por (id, hash): la primera del archivo. Dos filas con mismo ID y distinto hash quedan las dos."""
    return _with_duplicate_rank(df).where("_rank = 1").drop("_rank")


def duplicate_row_numbers(df: DataFrame) -> list[int]:
    """Filas del archivo descartadas por ser duplicados exactos de una anterior."""
    rows = _with_duplicate_rank(df).where("_rank > 1").select("source_row_number").collect()
    return sorted(r[0] for r in rows)


def count_conflicts(spark: SparkSession, staged: DataFrame, target: str) -> int:
    """Filas nuevas cuyo ID ya tenía otra versión, en la tabla o antes en el mismo archivo. Un ID vacío no
    identifica una operación, así que no es un conflicto: lo marca DQ002. Se calcula antes del MERGE."""
    with_id = staged.where("id_transaccion != ''").select(*MERGE_KEYS)
    if spark.catalog.tableExists(target):
        existing = spark.table(target).select(*MERGE_KEYS)
        new = with_id.join(existing, MERGE_KEYS, "left_anti")
        first_versions = new.select("id_transaccion").distinct().join(existing.select("id_transaccion").distinct(), "id_transaccion", "left_anti")
    else:
        new = with_id
        first_versions = new.select("id_transaccion").distinct()
    return new.count() - first_versions.count()


def merge_bronze(spark: SparkSession, staged: DataFrame, target: str) -> dict[str, int]:
    """Nunca actualiza: una fila con otro contenido entra como versión nueva, porque la clave incluye el hash."""
    return io.write_delta_merge(spark, staged, target, MERGE_KEYS)


def ingestion_stats(rows_read: int, merge_result: dict[str, int], rows_conflict: int) -> dict[str, int]:
    """Contadores de la corrida. Falla si no cuadran con la invariante de ADR-002."""
    rows_duplicate_in_file = rows_read - merge_result["rows_source"]
    if rows_read != merge_result["rows_inserted"] + merge_result["rows_already_present"] + rows_duplicate_in_file:
        raise ValueError(f"La ingesta no cuadra: read={rows_read}, merge={merge_result}")
    return {"rows_read": rows_read, "rows_inserted": merge_result["rows_inserted"],
            "rows_already_present": merge_result["rows_already_present"], "rows_conflict": rows_conflict,
            "rows_duplicate_in_file": rows_duplicate_in_file}


def orphan_ingestion_runs(spark: SparkSession, trades_table: str, runs_table: str) -> list[str]:
    """`ingestion_run_id` de Bronze sin corrida registrada: filas insertadas de las que no quedó rastro (ADR-002)."""
    if not spark.catalog.tableExists(trades_table):
        return []
    in_bronze = spark.table(trades_table).select("ingestion_run_id").distinct()
    if spark.catalog.tableExists(runs_table):
        in_bronze = in_bronze.join(spark.table(runs_table).select("ingestion_run_id"), "ingestion_run_id", "left_anti")
    return sorted(r[0] for r in in_bronze.collect())


def _record_run(spark: SparkSession, runs_table: str, run: dict) -> None:
    columns = [c.split()[0] for c in RUNS_SCHEMA.split(", ")]
    io.write_delta_merge(spark, spark.createDataFrame([tuple(run[c] for c in columns)], RUNS_SCHEMA), runs_table, ["ingestion_run_id"])


def ingest_trades(spark: SparkSession, path: str, trades_table: str, runs_table: str, pipeline_run_id: str = "") -> dict:
    """Ingesta un archivo y registra la corrida en `runs_table`, también si falla.

    Un esquema distinto al contrato devuelve FAILED_SCHEMA, y un archivo sin filas FAILED_EMPTY, sin escribir
    `trades_table`. Cualquier otro error registra FAILED y se relanza.
    """
    started_at = datetime.now(timezone.utc)
    run = {"ingestion_run_id": str(uuid.uuid4()), "pipeline_run_id": pipeline_run_id, "source_file": path, "source_file_hash": None,
           "rows_read": 0, "rows_inserted": 0, "rows_already_present": 0, "rows_conflict": 0, "rows_duplicate_in_file": 0,
           "duplicate_row_numbers": [],
           "status": "SUCCESS", "error": None, "started_at": started_at, "ended_at": None}
    try:
        run["source_file_hash"] = file_sha256(path)
        columns, rows = read_trades_csv(path)
        run["rows_read"] = len(rows)
        if not schema_matches(columns):
            run["status"] = "FAILED_SCHEMA"
            run["error"] = f"DQ001_SCHEMA_MISMATCH: columnas leídas {columns}"
        elif not rows:
            run["status"] = "FAILED_EMPTY"
            run["error"] = "El archivo no tiene filas de datos"
        else:
            lineage = add_lineage(spark, rows, path, run["source_file_hash"], run["ingestion_run_id"], started_at)
            staged = stage_for_merge(lineage)
            run["duplicate_row_numbers"] = duplicate_row_numbers(lineage)
            conflicts = count_conflicts(spark, staged, trades_table)
            merged = merge_bronze(spark, staged, trades_table)
            # Antes de validar: si la validación falla, la corrida igual dice lo que se escribió.
            run["rows_inserted"], run["rows_already_present"] = merged["rows_inserted"], merged["rows_already_present"]
            run.update(ingestion_stats(len(rows), merged, conflicts))
    except Exception as exc:
        run["status"], run["error"] = "FAILED", f"{type(exc).__name__}: {str(exc)[:config.RUN_ERROR_MAX_CHARS]}"
        run["ended_at"] = datetime.now(timezone.utc)
        _record_run(spark, runs_table, run)
        raise
    run["ended_at"] = datetime.now(timezone.utc)
    _record_run(spark, runs_table, run)
    return run
