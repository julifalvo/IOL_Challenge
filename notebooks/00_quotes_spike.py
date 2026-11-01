# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Spike de cotizaciones desde Free Edition
# MAGIC **Spec:** docs/specs/05_cotizaciones.md (Paso 0) · **Requisitos:** R-17 · **Escribe:** nada
# MAGIC
# MAGIC Prueba desde serverless los endpoints de datos de cada proveedor, para los símbolos del spike. No guarda datos crudos.
# MAGIC Completa la columna "Desde Free Edition" de ADR-008. La relación contra operaciones se mide desde local; acá queda vacía. La API de IOL no se evalúa.

# COMMAND ----------

import os
import sys

import requests

sys.path.insert(0, os.path.abspath(".."))
from tools.quotes_fetcher import spike

display(spark.createDataFrame([{k: str(v) for k, v in row.items()} for row in spike.run_spike(requests.get)]))
