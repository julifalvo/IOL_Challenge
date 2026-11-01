# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Cotizaciones de mercado
# MAGIC **Spec:** docs/specs/05_cotizaciones.md · **Requisitos:** R-16 a R-21 · **ADR:** 005, 008, 009
# MAGIC **Escribe:** `iol_bronze.quotes_raw`, `iol_ops.quote_requests`, `iol_ops.quote_series`, `iol_ops.quote_coverage`, `iol_gold.fact_market_quotes`, `iol_gold.fact_trade_market`, y reconstruye `iol_ops.instrument_mapping`
# MAGIC
# MAGIC Compara cada operación contra el **cierre diario** de su símbolo y moneda. Es un benchmark de cierre, no el precio al instante: la hora del dataset no es confiable (ADR-003).
# MAGIC Ninguna fila se descarta: una operación sin cotización queda en `fact_trade_market` y `match_method` dice por qué.

# COMMAND ----------

dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.dropdown("quotes_source", "api", ["api", "volume"])
dbutils.widgets.dropdown("demo_rerun", "true", ["true", "false"])

import json
import os
import sys
import uuid
from pathlib import Path

import requests
from pyspark.sql import functions as F

sys.path.insert(0, os.path.abspath("../src"))
from iol_pipeline import config, instruments, io, quotes

RUN_ID = dbutils.widgets.get("pipeline_run_id") or str(uuid.uuid4())
CATALOG = dbutils.widgets.get("catalog")
SOURCE = dbutils.widgets.get("quotes_source")
print("pipeline_run_id:", RUN_ID, "· fuente:", SOURCE)

io.ensure_schemas(spark, catalog=CATALOG)
silver_table = io.table_name("silver", "trades", catalog=CATALOG)
mapping_table = io.table_name("ops", "instrument_mapping", catalog=CATALOG)
enrichment_table = io.table_name("ops", "instrument_enrichment", catalog=CATALOG)
raw_table = io.table_name("bronze", "quotes_raw", catalog=CATALOG)
requests_table = io.table_name("ops", "quote_requests", catalog=CATALOG)
series_table = io.table_name("ops", "quote_series", catalog=CATALOG)
coverage_table = io.table_name("ops", "quote_coverage", catalog=CATALOG)
market_table = io.table_name("gold", "fact_market_quotes", catalog=CATALOG)
trade_market_table = io.table_name("gold", "fact_trade_market", catalog=CATALOG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Descarga
# MAGIC Una fila por símbolo y proveedor de su cadena (ADR-008). Lo ya pedido sin error no se repite: si la corrida se corta, la siguiente sigue desde ahí.
# MAGIC Con `quotes_source = volume` lee lo que `tools/quotes_fetcher` dejó en el Volume, con el mismo contrato.

# COMMAND ----------

symbols = [r.asDict() for r in spark.table(mapping_table).select("symbol", "asset_type").collect()]
candidates = quotes.candidate_series(symbols)
names = sorted({c["provider"] for c in candidates})
if SOURCE == "api":
    providers = {name: quotes.build_provider(name, symbols, requests.get) for name in names}
    pause_s = config.QUOTE_PAUSE_S
else:
    providers = {name: quotes.FileReplayProvider(name, Path(io.landing_path("quotes", catalog=CATALOG))) for name in names}
    pause_s = 0.0
print("Series candidatas:", len(candidates), "de", len(symbols), "símbolos")
fetched = quotes.run_fetch(spark, candidates, providers, raw_table, requests_table, SOURCE, RUN_ID, pause_s=pause_s)
print("Descarga:", fetched)
display(quotes.reconcile_requests(spark.table(requests_table), spark.table(raw_table)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Validación de las series
# MAGIC De cada símbolo se usa la primera serie de su cadena que valida. Nunca la que más se parece a las operaciones: sesgaría la comparación (ADR-008).

# COMMAND ----------

trades = spark.table(silver_table)
print("Series:", quotes.run_series(spark, candidates, raw_table, trades, series_table))
print("Mapeo:", instruments.run_instruments(spark, silver_table, mapping_table, enrichment_table=enrichment_table, quote_series_table=series_table))
series = spark.table(series_table)
display(quotes.series_summary(series))
display(spark.table(mapping_table).groupBy("mapping_status").count().orderBy("mapping_status"))
display(series.where("series_status = 'SCALE_SUSPECT'").select("symbol", "provider", "days_compared", "ratio_p50").orderBy("symbol"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Benchmark y matching
# MAGIC Misma fecha efectiva → `EXACT_DAY`. Si no, el cierre previo con hasta 3 días hábiles → `PRIOR_DAY_IMPUTED`. Nunca una cotización posterior (ADR-009).

# COMMAND ----------

first = quotes.run_market(spark, silver_table, mapping_table, raw_table, requests_table, series_table, market_table, trade_market_table, coverage_table, RUN_ID)
print("1ra corrida:", first)
if dbutils.widgets.get("demo_rerun") == "true":
    second = quotes.run_market(spark, silver_table, mapping_table, raw_table, requests_table, series_table, market_table, trade_market_table, coverage_table, RUN_ID)
    print("2da corrida (tiene que dar 0 en todo):", second)

matched = spark.table(trade_market_table)
trades_count, matched_count = trades.count(), matched.count()
print("Operaciones en Silver:", trades_count, "· en fact_trade_market:", matched_count)
assert trades_count == matched_count, "fact_trade_market tiene que tener una fila por operación"
display(matched.groupBy("match_method").count().orderBy(F.col("count").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Cobertura (R-21)
# MAGIC El denominador son las operaciones elegibles, sin excluir los tipos sin fuente. "Excluidas" son las que tienen serie, pero sesgada o con otra escala.

# COMMAND ----------

coverage = spark.table(coverage_table)
overall = quotes.overall_coverage(coverage)
print("Cobertura ponderada por operaciones:", overall)
display(quotes.coverage_by_type(coverage))
display(coverage.groupBy("provider", "series_status").agg(F.count("*").alias("symbols"), F.sum("trades_eligible").alias("eligible")).orderBy(F.col("eligible").desc()))
display(coverage.orderBy(F.col("trades_eligible").desc()).drop("last_error").limit(config.QUOTE_COVERAGE_TOP_N))
display(coverage.where("last_error IS NOT NULL").select("symbol", "asset_type", "provider", "coverage_pct", "last_error").orderBy("symbol"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Comparación contra el benchmark y calibración de τ
# MAGIC τ es la mitad de la mediana del rango diario de cada tipo (ADR-009). `share_above` no es sobreprecio: incluye el azar intradiario. La consulta por cliente es de la etapa 08.

# COMMAND ----------

mapping = spark.table(mapping_table)
display(quotes.daily_range_by_type(spark.table(market_table).where("is_active"), mapping))
display(quotes.deviation_summary(matched, mapping))

# COMMAND ----------

dbutils.notebook.exit(json.dumps({"status": "SUCCESS", "pipeline_run_id": RUN_ID, "rows_out": matched_count, "coverage_pct": overall["coverage_pct"], "fetched": fetched}))
