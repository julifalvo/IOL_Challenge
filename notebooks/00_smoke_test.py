# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Smoke test de Free Edition
# MAGIC **Spec:** docs/specs/00_entorno.md · **Requisitos:** R-37, R-17, R-28 · **Escribe:** `iol_ops.smoke_test_results`
# MAGIC
# MAGIC Verifica lo que puede cambiar el plan: orquestación con `dbutils.notebook.run`, salida a internet hacia los
# MAGIC proveedores de cotizaciones (ADR-008), `ai_query` (ADR-011), liquid clustering (ADR-012) y el MERGE Delta de `src/`.
# MAGIC Ningún chequeo corta el notebook: cada uno registra OK, INCONCLUSO o FAIL con el detalle. Pasar la tabla final a ADR-001/008/011.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("ai_model", "")

import json
import os
import sys
import uuid
from datetime import datetime, timezone

sys.path.insert(0, os.path.abspath("../src"))
from iol_pipeline import config, io, smoke

CATALOG = dbutils.widgets.get("catalog")
SMOKE_RUN_ID = str(uuid.uuid4())
results = []


def record(check: str, status: str, detail: str) -> None:
    results.append({"check": check, "status": status, "detail": detail[:config.SMOKE_DETAIL_MAX_CHARS],
                    "checked_at": datetime.now(timezone.utc).isoformat(timespec="seconds")})
    print(f"[{status}] {check}: {detail[:config.SMOKE_PRINT_MAX_CHARS]}")


io.ensure_schemas(spark, catalog=CATALOG)
print("smoke_run_id:", SMOKE_RUN_ID, "· DBR:", os.environ.get("DATABRICKS_RUNTIME_VERSION"), "· Spark:", spark.version)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Orquestación: `dbutils.notebook.run`

# COMMAND ----------

try:
    raw = dbutils.notebook.run("./00_smoke_test_child", config.SMOKE_CHILD_TIMEOUT_S, {"ping": SMOKE_RUN_ID})
    echo = json.loads(raw).get("echo")
    record("orchestration:notebook_run", "OK" if echo == SMOKE_RUN_ID else "FAIL", f"child devolvió {raw}")
except Exception as exc:
    record("orchestration:notebook_run", "FAIL", f"{type(exc).__name__}: {exc}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Egress a proveedores de cotizaciones (ADR-008)
# MAGIC HTTP 403, 407 o 429 = INCONCLUSO (pudo responder un intermediario). Otra respuesta HTTP = host alcanzable. Error de conexión o timeout = bloqueado.

# COMMAND ----------

import requests

for r in smoke.check_egress(config.SMOKE_EGRESS_URLS, requests.get, config.HTTP_TIMEOUT_S,
                            config.SMOKE_EGRESS_INCONCLUSIVE_HTTP):
    record(r["check"], r["status"], r["detail"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. IA: endpoints disponibles y `ai_query` (ADR-011)

# COMMAND ----------

endpoints = []
try:
    from databricks.sdk import WorkspaceClient

    endpoints = sorted(e.name for e in WorkspaceClient().serving_endpoints.list())
    record("ai:serving_endpoints", "OK" if endpoints else "FAIL", ", ".join(endpoints) or "ninguno visible")
except Exception as exc:
    record("ai:serving_endpoints", "FAIL", f"{type(exc).__name__}: {exc}")

model = dbutils.widgets.get("ai_model") or config.SMOKE_AI_MODEL
try:
    answer = spark.sql(f"SELECT ai_query('{model}', 'Respond with the single word: pong') AS r").first()["r"]
    record(f"ai:ai_query:{model}", "OK", str(answer))
except Exception as exc:
    record(f"ai:ai_query:{model}", "FAIL", f"{type(exc).__name__}: {exc}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Liquid clustering (ADR-012)

# COMMAND ----------

probe = io.table_name("ops", "smoke_cluster_probe", catalog=CATALOG)
try:
    spark.sql(f"CREATE OR REPLACE TABLE {probe} (id INT, d DATE) CLUSTER BY (d)")
    clustering = spark.sql(f"DESCRIBE DETAIL {probe}").first()["clusteringColumns"]
    record("delta:liquid_clustering", "OK" if list(clustering) == ["d"] else "FAIL", f"clusteringColumns={clustering}")
except Exception as exc:
    record("delta:liquid_clustering", "FAIL", f"{type(exc).__name__}: {exc}")
finally:
    spark.sql(f"DROP TABLE IF EXISTS {probe}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. MERGE Delta de `src/iol_pipeline/io.py` en serverless + resultados persistidos

# COMMAND ----------

from pyspark.sql import functions as F

target = io.table_name("ops", "smoke_test_results", catalog=CATALOG)
keys = ["smoke_run_id", "check"]


def to_df(rows):
    return (spark.createDataFrame(rows, "check string, status string, detail string, checked_at string")
            .select(F.lit(SMOKE_RUN_ID).alias("smoke_run_id"), "check", "status", "detail",
                    F.to_timestamp("checked_at").alias("checked_at")))


try:
    first = io.write_delta_merge(spark, to_df(results), target, keys)
    rerun = io.write_delta_merge(spark, to_df(results), target, keys)
    idempotent = first["rows_inserted"] == len(results) and rerun["rows_inserted"] == 0
    record("delta:write_delta_merge_idempotent", "OK" if idempotent else "FAIL", f"1ra corrida={first} · 2da corrida={rerun}")
except Exception as exc:
    record("delta:write_delta_merge_idempotent", "FAIL", f"{type(exc).__name__}: {exc}")

try:
    io.write_delta_merge(spark, to_df(results[-1:]), target, keys)
except Exception as exc:
    record("delta:persist_results", "FAIL", f"{type(exc).__name__}: {exc}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resultado (copiar a ADR-001 / ADR-008 / ADR-011)

# COMMAND ----------

display(to_df(results).orderBy("check"))  # desde memoria: se ve aunque el MERGE haya fallado
failed = [r["check"] for r in results if r["status"] == "FAIL"]
inconclusive = [r["check"] for r in results if r["status"] == "INCONCLUSO"]
print(f"{len(results) - len(failed) - len(inconclusive)}/{len(results)} OK · "
      f"INCONCLUSO: {inconclusive or 'ninguno'} · FAIL: {failed or 'ninguno'}")
status = smoke.run_status(results)
print("status:", status)

# COMMAND ----------

dbutils.notebook.exit(json.dumps({"status": status, "smoke_run_id": SMOKE_RUN_ID, "failed_checks": failed,
                                 "inconclusive_checks": inconclusive}))
