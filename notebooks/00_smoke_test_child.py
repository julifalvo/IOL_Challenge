# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Hijo del smoke test
# MAGIC Lo invoca `00_smoke_test` con `dbutils.notebook.run` para probar la orquestación en serverless. Devuelve el `ping` recibido.

# COMMAND ----------

import json

dbutils.widgets.text("ping", "")
dbutils.notebook.exit(json.dumps({"status": "SUCCESS", "echo": dbutils.widgets.get("ping")}))
