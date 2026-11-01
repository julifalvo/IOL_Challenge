# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Ingesta a Bronze
# MAGIC **Spec:** docs/specs/02_bronze_ingesta.md · **Requisitos:** R-06, R-07, R-08 · **Escribe:** `iol_bronze.trades_raw`, `iol_ops.ingestion_runs`
# MAGIC
# MAGIC Guarda las columnas originales como texto, con linaje. El MERGE usa `(id_transaccion, record_hash)` y nunca actualiza (ADR-002).
# MAGIC `demo_rerun = true` corre la ingesta dos veces para demostrar la idempotencia. El orquestador la corre una vez.

# COMMAND ----------

dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.dropdown("demo_rerun", "true", ["true", "false"])

import json
import os
import sys
import uuid

sys.path.insert(0, os.path.abspath("../src"))
from iol_pipeline import bronze, io

RUN_ID = dbutils.widgets.get("pipeline_run_id") or str(uuid.uuid4())
CATALOG = dbutils.widgets.get("catalog")
RUNS = 2 if dbutils.widgets.get("demo_rerun") == "true" else 1
print("pipeline_run_id:", RUN_ID)

trades_table = io.table_name("bronze", "trades_raw", catalog=CATALOG)
runs_table = io.table_name("ops", "ingestion_runs", catalog=CATALOG)
csv_files = bronze.list_csv_files(io.landing_path("trades", catalog=CATALOG))
print("Archivos:", csv_files)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingesta

# COMMAND ----------

results = [bronze.ingest_trades(spark, path, trades_table, runs_table, RUN_ID) for _ in range(RUNS) for path in csv_files]
for r in results:
    print({k: r[k] for k in ("status", "rows_read", "rows_inserted", "rows_already_present", "rows_conflict", "rows_duplicate_in_file", "duplicate_row_numbers", "error")})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Corridas registradas
# MAGIC `rows_read = rows_inserted + rows_already_present + rows_duplicate_in_file`; `rows_conflict` es parte de `rows_inserted`.
# MAGIC Esta tabla crece una fila por corrida, a propósito. La que queda idéntica es `trades_raw`.

# COMMAND ----------

display(spark.table(runs_table).orderBy("started_at"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabla Bronze

# COMMAND ----------

rows_out = spark.table(trades_table).count()
print(f"{trades_table}: {rows_out:,} filas")
detail = spark.sql(f"DESCRIBE DETAIL {trades_table}").first()
print("format:", detail["format"], "· numFiles:", detail["numFiles"], "· sizeInBytes:", detail["sizeInBytes"])
display(spark.table(trades_table).select("source_file", "source_file_hash", "schema_version", "ingestion_run_id").distinct())
orphans = bronze.orphan_ingestion_runs(spark, trades_table, runs_table)
print("Corridas de Bronze sin registro en ingestion_runs:", orphans or "ninguna")

# COMMAND ----------

ok = bool(results) and all(r["status"] == "SUCCESS" for r in results) and not orphans
dbutils.notebook.exit(json.dumps({"status": "SUCCESS" if ok else "FAILED", "pipeline_run_id": RUN_ID, "rows_out": rows_out,
                                  "runs": [{"status": r["status"], "rows_inserted": r["rows_inserted"]} for r in results]}))
