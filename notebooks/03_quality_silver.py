# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Silver y calidad
# MAGIC **Spec:** docs/specs/03_silver_calidad.md · **Requisitos:** R-08, R-09, R-10 · **Escribe:** `iol_silver.trades`, `iol_ops.quality_baselines`, `iol_ops.quality_events`
# MAGIC
# MAGIC Silver es 1:1 con Bronze: ninguna fila se descarta, cada problema queda en `quality_flags` y `quality_status`.
# MAGIC Catálogo y argumentos de cada regla: `docs/quality_rules.md`. Los outputs no muestran IDs de cliente ni de operación.

# COMMAND ----------

dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("catalog", "workspace")

import json
import os
import sys
import uuid

sys.path.insert(0, os.path.abspath("../src"))
from iol_pipeline import config, io, silver

RUN_ID = dbutils.widgets.get("pipeline_run_id") or str(uuid.uuid4())
CATALOG = dbutils.widgets.get("catalog")
print("pipeline_run_id:", RUN_ID, "· rules_version:", config.RULES_VERSION)

io.ensure_schemas(spark, catalog=CATALOG)
bronze_table = io.table_name("bronze", "trades_raw", catalog=CATALOG)
silver_table = io.table_name("silver", "trades", catalog=CATALOG)
baselines_table = io.table_name("ops", "quality_baselines", catalog=CATALOG)
events_table = io.table_name("ops", "quality_events", catalog=CATALOG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Corrida
# MAGIC Se corre dos veces: la segunda tiene que dar 0 filas insertadas, 0 actualizadas y 0 eventos nuevos.

# COMMAND ----------

first = silver.run_silver(spark, bronze_table, silver_table, baselines_table, events_table, RUN_ID)
second = silver.run_silver(spark, bronze_table, silver_table, baselines_table, events_table, RUN_ID)
print("1ra corrida:", first)
print("2da corrida:", second)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Reglas: filas afectadas
# MAGIC `example_record_hash` es un prefijo de `record_hash`: lleva a la fila de Bronze sin publicar el ID.

# COMMAND ----------

trades = spark.table(silver_table)
display(silver.rule_summary(trades))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Distribución de `quality_status`

# COMMAND ----------

display(silver.status_distribution(trades))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Reconciliación Bronze contra Silver
# MAGIC Todas las diferencias tienen que dar 0.

# COMMAND ----------

check = silver.reconciliation(spark.table(bronze_table), trades, spark.table(events_table))
for key, value in check.items():
    print(f"{key:30} {value:,}")
reconciled = check["bronze_rows"] == check["silver_rows"] and not any(v for k, v in check.items() if k not in ("bronze_rows", "silver_rows"))
print("Reconciliación:", "OK" if reconciled else "CON DIFERENCIAS")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Ejemplos de `quality_events`

# COMMAND ----------

from pyspark.sql import functions as F

display(spark.table(events_table).where(F.col("rule_code").isin("DQ010_SATURDAY", "DQ020_EXTREME_AMOUNT", "DQ021_POSSIBLE_SCALE_ERROR"))
        .select("rule_code", "severity", "details_json").orderBy("rule_code", "details_json").limit(config.EVENT_EXAMPLES))

# COMMAND ----------

idempotent = second["silver_inserted"] == second["silver_updated"] == second["events_inserted"] == 0
ok = reconciled and idempotent
dbutils.notebook.exit(json.dumps({"status": "SUCCESS" if ok else "FAILED", "pipeline_run_id": RUN_ID, "rows_out": check["silver_rows"]}))
