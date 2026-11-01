# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Instrumentos y variantes de liquidación
# MAGIC **Spec:** docs/specs/04_instrumentos_mapeo.md · **Requisitos:** R-22 · **Escribe:** `iol_ops.instrument_mapping`, y reaplica `iol_silver.trades`
# MAGIC
# MAGIC Una fila por símbolo: tipo de activo, variante de liquidación, subyacente y proveedor. El sufijo D/C no alcanza: `YPFD` está en pesos (ADR-004).
# MAGIC Lo que las reglas no resuelven queda en `NEEDS_REVIEW` para la IA (notebook 09). El proveedor y la escala quedan `UNVERIFIED` hasta el notebook 05.

# COMMAND ----------

dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.dropdown("demo_rerun", "true", ["true", "false"])

import json
import os
import sys
import uuid

import requests

sys.path.insert(0, os.path.abspath("../src"))
from iol_pipeline import instruments, io, silver

RUN_ID = dbutils.widgets.get("pipeline_run_id") or str(uuid.uuid4())
CATALOG = dbutils.widgets.get("catalog")
print("pipeline_run_id:", RUN_ID)

io.ensure_schemas(spark, catalog=CATALOG)
bronze_table = io.table_name("bronze", "trades_raw", catalog=CATALOG)
silver_table = io.table_name("silver", "trades", catalog=CATALOG)
mapping_table = io.table_name("ops", "instrument_mapping", catalog=CATALOG)
enrichment_table = io.table_name("ops", "instrument_enrichment", catalog=CATALOG)  # la escribe el notebook 09
quote_series_table = io.table_name("ops", "quote_series", catalog=CATALOG)  # la escribe el notebook 05
baselines_table = io.table_name("ops", "quality_baselines", catalog=CATALOG)
events_table = io.table_name("ops", "quality_events", catalog=CATALOG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Corrida
# MAGIC Después del mapeo se reaplica Silver: `DQ030` y `DQ033` lo necesitan, y Silver corre antes. Con `demo_rerun = true`, la segunda corrida del mapeo tiene que dar 0 insertadas y 0 actualizadas.

# COMMAND ----------

first = instruments.run_instruments(spark, silver_table, mapping_table, enrichment_table=enrichment_table, quote_series_table=quote_series_table)
print("1ra corrida:", first)
second = None
if dbutils.widgets.get("demo_rerun") == "true":
    second = instruments.run_instruments(spark, silver_table, mapping_table, enrichment_table=enrichment_table, quote_series_table=quote_series_table)
    print("2da corrida:", second)
reapplied = silver.run_silver(spark, bronze_table, silver_table, baselines_table, events_table, RUN_ID, mapping_table)
print("Silver reaplicado:", reapplied)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Símbolos por tipo, moneda y origen de la clasificación

# COMMAND ----------

mapping = spark.table(mapping_table)
trades = spark.table(silver_table)
display(instruments.type_summary(mapping))
print("Símbolos en el mapeo:", mapping.count(), "· en Silver:", trades.select("symbol").distinct().where("symbol IS NOT NULL").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Variantes de liquidación
# MAGIC `implied_fx` = mediana en ARS / mediana de la variante: el MEP o CCL implícito. Si es coherente entre instrumentos, las variantes están bien identificadas.

# COMMAND ----------

variants = instruments.variants_with_implied_fx(mapping, trades)
display(variants)
print("Variantes con cambio implícito fuera de rango:")
display(instruments.variants_fx_out_of_range(variants))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Falsos positivos del sufijo: terminan en D o C y están en pesos

# COMMAND ----------

display(instruments.suffix_false_positives(mapping))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. `NEEDS_REVIEW` para la IA

# COMMAND ----------

pending = instruments.needs_review(mapping)
print("Símbolos en NEEDS_REVIEW:", pending.count())
display(pending)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validación independiente contra data912
# MAGIC Compara la clasificación con las listas por tipo de data912, que son de hoy. La de acciones no cuenta: es la misma que usa la regla.

# COMMAND ----------

display(instruments.agreement_with_provider_lists(spark, mapping, requests.get))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Reconciliación de Silver después de reaplicar

# COMMAND ----------

check = silver.reconciliation(spark.table(bronze_table), trades, spark.table(events_table))
for key, value in check.items():
    print(f"{key:30} {value:,}")
reconciled = check["bronze_rows"] == check["silver_rows"] and not any(v for k, v in check.items() if k not in ("bronze_rows", "silver_rows"))
print("Reconciliación:", "OK" if reconciled else "CON DIFERENCIAS")

# COMMAND ----------

idempotent = second is None or second["rows_inserted"] == second["rows_updated"] == 0
complete = first["symbols"] == trades.select("symbol").distinct().where("symbol IS NOT NULL").count()
dbutils.notebook.exit(json.dumps({"status": "SUCCESS" if idempotent and complete and reconciled else "FAILED", "pipeline_run_id": RUN_ID, "rows_out": first["symbols"]}))
