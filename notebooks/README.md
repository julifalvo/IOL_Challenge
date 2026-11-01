# Notebooks

Formato: Databricks source (`.py`) para que git muestre diffs legibles. Los ejecutados con outputs van en `../notebooks_executed/` como `.ipynb`.

Plantilla de cabecera:

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # NN — Título
# MAGIC **Spec:** docs/specs/NN_*.md · **Requisitos:** R-xx · **Escribe:** tablas…

# COMMAND ----------
dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("catalog", "workspace")

import sys, os, uuid
sys.path.insert(0, os.path.abspath("../src"))
from iol_pipeline import config

RUN_ID = dbutils.widgets.get("pipeline_run_id") or str(uuid.uuid4())
print("pipeline_run_id:", RUN_ID)

# COMMAND ----------
# lógica: solo llamadas a src/iol_pipeline/*

# COMMAND ----------
# outputs visibles: display(...) de métricas y controles

# COMMAND ----------
import json
dbutils.notebook.exit(json.dumps({"status": "SUCCESS", "rows_out": 0}))
```

Antes de la primera corrida (una vez por workspace): `00_setup` (schemas + Volume + chequeo del CSV) y `00_smoke_test` (evidencia del entorno; usa `00_smoke_test_child`). `00_quotes_spike` prueba los proveedores de cotizaciones (ADR-008).

Orden: `00_run_pipeline` → `01_data_profiling` → `02_ingest_bronze` → `03_quality_silver` → `04_instruments` → `09_ai_enrichment` → `05_market_quotes` → `06_model_gold` → `07_quality_gate_monitoring` → `08_business_eda_sql` → `09b_ai_eda_summary` · `90_benchmark` (aparte).
