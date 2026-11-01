# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup del entorno
# MAGIC **Spec:** docs/specs/00_entorno.md · **Requisitos:** R-37, R-38 · **Crea:** schemas `iol_bronze`, `iol_silver`, `iol_gold`, `iol_ops` y Volume `iol_bronze.landing`
# MAGIC
# MAGIC Idempotente (`CREATE ... IF NOT EXISTS`): se puede correr las veces que haga falta.
# MAGIC El CSV original se sube a mano a `landing/trades/` (no está en el repo).

# COMMAND ----------

dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("catalog", "workspace")

import json
import os
import platform
import sys
import uuid

sys.path.insert(0, os.path.abspath("../src"))
from iol_pipeline import config, io, smoke

RUN_ID = dbutils.widgets.get("pipeline_run_id") or str(uuid.uuid4())
CATALOG = dbutils.widgets.get("catalog")
print("pipeline_run_id:", RUN_ID)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Runtime

# COMMAND ----------

runtime = {
    "databricks_runtime_version": os.environ.get("DATABRICKS_RUNTIME_VERSION", "<no definido>"),
    "spark_version": spark.version,
    "python_version": platform.python_version(),
    "catalog": CATALOG,
    "current_user": spark.sql("SELECT current_user() AS u").first()["u"],
}
for key, value in runtime.items():
    print(f"{key:28} {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schemas y Volume

# COMMAND ----------

created = io.ensure_schemas(spark, catalog=CATALOG)
print("Asegurados:", created)
display(spark.sql(f"SHOW SCHEMAS IN {CATALOG} LIKE 'iol_*'"))
display(spark.sql(f"SHOW VOLUMES IN {CATALOG}.{config.SCHEMAS['bronze']}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Landing: subcarpetas y CSV de operaciones

# COMMAND ----------

for subdir in config.LANDING_SUBDIRS:
    dbutils.fs.mkdirs(io.landing_path(subdir, catalog=CATALOG))

trades_dir = io.landing_path("trades", catalog=CATALOG)
csv_files = [f for f in dbutils.fs.ls(trades_dir) if f.name.lower().endswith(".csv")]

if not csv_files:
    print(f"CSV_MISSING: subir el CSV original a {trades_dir}/ (Catalog Explorer > Volumes > Upload) y volver a correr.")
contract_ok = {}
for f in csv_files:
    columns = spark.read.option("header", True).csv(f.path).columns
    contract_ok[f.name] = columns == config.TRADES_CONTRACT
    print(f"{f.name}: {f.size:,} bytes · columnas = contrato {config.SCHEMA_VERSION}: {contract_ok[f.name]}")
    if not contract_ok[f.name]:
        print("  columnas leídas:", columns)

status = smoke.setup_status(contract_ok)
print("status:", status)

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    "status": status,
    "pipeline_run_id": RUN_ID,
    "schemas": created,
    "csv_contract_ok": contract_ok,
}))
