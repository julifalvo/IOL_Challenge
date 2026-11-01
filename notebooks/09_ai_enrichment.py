# Databricks notebook source
# MAGIC %md
# MAGIC # 09 — IA: enriquecimiento de instrumentos
# MAGIC **Spec:** docs/specs/09_ia.md · **Requisitos:** R-28, R-29, R-30 · **Escribe:** `iol_ops.ai_runs`, `iol_ops.instrument_enrichment`, y reconstruye `iol_ops.instrument_mapping`
# MAGIC
# MAGIC Completa lo que una regla no puede inferir: emisor, sector y ticker del subyacente, más los instrumentos que quedaron en `NEEDS_REVIEW`.
# MAGIC Solo se envían metadatos de instrumentos: ningún dato de clientes. La IA no pisa reglas, no decide calidad ni excluye filas (ADR-011).

# COMMAND ----------

dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.dropdown("run_evaluation", "true", ["true", "false"])

import json
import os
import sys
import uuid

sys.path.insert(0, os.path.abspath("../src"))
from iol_pipeline import ai, config, instruments, io

RUN_ID = dbutils.widgets.get("pipeline_run_id") or str(uuid.uuid4())
CATALOG = dbutils.widgets.get("catalog")
print("pipeline_run_id:", RUN_ID, "· modelo:", config.AI_MODEL, "· prompt:", config.AI_PROMPT_VERSION)

io.ensure_schemas(spark, catalog=CATALOG)
silver_table = io.table_name("silver", "trades", catalog=CATALOG)
mapping_table = io.table_name("ops", "instrument_mapping", catalog=CATALOG)
runs_table = io.table_name("ops", "ai_runs", catalog=CATALOG)
enrichment_table = io.table_name("ops", "instrument_enrichment", catalog=CATALOG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Evaluación a ciegas de los modelos
# MAGIC Muestra etiquetada de `tests/fixtures/instrument_gold_labels.csv`. El modelo no recibe el tipo de las reglas: si no, lo copiaría y la accuracy saldría inflada.
# MAGIC Las respuestas quedan en caché: repetir la evaluación no vuelve a consultar.

# COMMAND ----------

if dbutils.widgets.get("run_evaluation") == "true":
    evaluation = ai.run_evaluation(spark, runs_table, config.AI_EVAL_MODELS, lambda model: ai.ai_query_caller(spark, model), RUN_ID)
    display(spark.createDataFrame(evaluation))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Enriquecimiento

# COMMAND ----------

first = ai.run_enrichment(spark, mapping_table, runs_table, enrichment_table, ai.ai_query_caller(spark, config.AI_MODEL), RUN_ID)
print("1ra corrida:", first)
second = ai.run_enrichment(spark, mapping_table, runs_table, enrichment_table, ai.ai_query_caller(spark, config.AI_MODEL), RUN_ID)
print("2da corrida (tiene que consultar solo lo que falló):", second)
print("Mapeo:", instruments.run_instruments(spark, silver_table, mapping_table, enrichment_table=enrichment_table))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Resultado de la validación

# COMMAND ----------

runs = spark.table(runs_table)
display(ai.status_summary(runs))
display(ai.problem_examples(runs))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Mapeo después de la IA

# COMMAND ----------

mapping = spark.table(mapping_table)
display(instruments.type_summary(mapping))
pending = instruments.needs_review(mapping)
print("Símbolos en NEEDS_REVIEW:", pending.count())
display(pending)
display(mapping.where("issuer IS NOT NULL").select("symbol", "asset_type", "asset_type_source", "issuer", "sector", "underlying_ticker_foreign").orderBy("symbol").limit(config.AI_EXAMPLE_ROWS))
print("Con emisor:", mapping.where("issuer IS NOT NULL").count(), "· con ticker extranjero:", mapping.where("underlying_ticker_foreign IS NOT NULL").count(), "· de", mapping.count())

# COMMAND ----------

dbutils.notebook.exit(json.dumps({"status": "SUCCESS", "pipeline_run_id": RUN_ID, "rows_out": first["enriched_symbols"], "statuses": first["statuses"]}))
