# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Profiling del dataset
# MAGIC **Spec:** docs/specs/01_profiling_y_arquitectura.md · **Requisitos:** R-01, R-09, R-15, R-40 · **Escribe:** nada
# MAGIC
# MAGIC Evidencia de cada decisión de diseño, recalculada en Spark sobre el CSV completo. Cada celda cierra con el ADR que justifica.
# MAGIC Los outputs no muestran `id_cliente` ni `id_transaccion`: este notebook se publica.

# COMMAND ----------

dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("catalog", "workspace")

import json
import os
import sys
import uuid

sys.path.insert(0, os.path.abspath("../src"))
from iol_pipeline import config, io, profiling, segmentation

RUN_ID = dbutils.widgets.get("pipeline_run_id") or str(uuid.uuid4())
CATALOG = dbutils.widgets.get("catalog")
print("pipeline_run_id:", RUN_ID)

trades_dir = io.landing_path("trades", catalog=CATALOG)
raw = profiling.read_trades_csv(spark, trades_dir)
trades = profiling.prepare(raw)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Conteos, nulos, duplicados y rango temporal

# COMMAND ----------

summary = profiling.general_summary(raw, trades)
for key, value in summary.items():
    print(f"{key:24} {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC → **Decisión:** marzo es un período parcial, cortado en `DATA_CUTOFF`. Las comparaciones entre meses se normalizan por día hábil (celda 9).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Día de semana y días hábiles sin operaciones

# COMMAND ----------

display(profiling.weekday_distribution(trades))
print("Días lun-vie sin operaciones:", profiling.weekdays_without_trades(trades))
print("Operaciones en feriados del calendario:", profiling.trades_on_market_holidays(trades))

# COMMAND ----------

# MAGIC %md
# MAGIC → **Decisión: ADR-003.** Si el dataset respeta el calendario, un sábado no se explica como operación fuera de mercado cualquiera.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Hora UTC por día de semana

# COMMAND ----------

display(profiling.hourly_by_weekday(trades))
display(profiling.hour_bounds_by_weekday(trades))
print(f"Proporción dentro de la rueda {config.SESSION_HOURS_LOCAL} local: {profiling.share_in_session(trades):.1%}")

# COMMAND ----------

# MAGIC %md
# MAGIC → **Decisión: ADR-003.** Se compara dónde arranca el lunes y dónde corta el sábado. La hora no se usa para análisis intradiario y `DQ013` queda como métrica INFO.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Canales

# COMMAND ----------

display(profiling.channel_counts(trades))
print("Clientes multicanal:", profiling.multichannel_clients(trades))

# COMMAND ----------

# MAGIC %md
# MAGIC → **Decisión:** el canal va en el hecho, y `preferred_channel` en el SCD2 (ADR-007).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Actividad por cliente

# COMMAND ----------

activity = profiling.client_activity(trades)
for key, value in activity.items():
    print(f"{key:18} {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC → **Decisión: ADR-007.** Los cortes de `activity_segment` se ubican en la cola de la distribución.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Instrumentos, sufijos y descripciones
# MAGIC La clasificación por tipo es preliminar; la definitiva sale del spec 04.

# COMMAND ----------

display(profiling.asset_type_by_currency(trades))
for key, value in profiling.instrument_summary(trades).items():
    print(f"{key:32} {value}")
print(f"Top {config.PROFILE_TOP_SYMBOLS} símbolos: {profiling.top_symbols_share(trades, config.PROFILE_TOP_SYMBOLS):.1%} de las operaciones")

# COMMAND ----------

# MAGIC %md
# MAGIC → **Decisión: ADR-004.** El sufijo D/C no alcanza para identificar variantes: el mapeo es una tabla explícita.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Precios por variante de liquidación y escala
# MAGIC `implied_fx` = mediana del símbolo base / mediana de la variante. No se comparan precios entre monedas para sumarlos: solo se muestra la relación.

# COMMAND ----------

display(profiling.settlement_price_medians(trades))

# COMMAND ----------

# MAGIC %md
# MAGIC → **Decisión: ADR-005.** Si el `implied_fx` de los bonos es coherente con el de los CEDEARs, la escala del dataset es por 1 VN. Cada variante es un instrumento distinto (ADR-004).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Extremos y posibles errores de escala
# MAGIC El top por importe va por moneda: ARS y USD no se comparan.

# COMMAND ----------

display(profiling.top_by_quantity(trades, config.PROFILE_TOP_N))
display(profiling.top_by_amount(trades, config.PROFILE_TOP_N))
display(profiling.treasury_capitalizable_prices(trades))
anomalies = profiling.scale_anomalies(trades)
print("Filas con precio fuera del rango de DQ021:", anomalies.count())
display(anomalies)

# COMMAND ----------

# MAGIC %md
# MAGIC → **Decisión: DQ021 y ADR-005.** Se marcan y no se corrigen: no se sabe si el error está en el precio, en la cantidad o en ambos.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Operaciones por día hábil observado

# COMMAND ----------

display(profiling.monthly_activity(trades))

# COMMAND ----------

# MAGIC %md
# MAGIC → **Decisión:** las comparaciones entre meses (q3, q4) usan operaciones por día hábil observado, porque marzo está cortado.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Viabilidad del SCD2 (ADR-007)
# MAGIC Mide si hay cambios reales de atributos para demostrar el SCD2 con datos del dataset. **No hay valores esperados.**

# COMMAND ----------

print(f"""Definición aplicada ({config.SEGMENT_RULE_VERSION}):
- Versión inicial en la primera operación del cliente, con atributos de esa sola operación.
- Cortes acumulados: {[c.isoformat() for c in config.SCD2_SNAPSHOTS]}, por fecha local efectiva.
- activity_segment con los cortes congelados: {config.ACTIVITY_SEGMENTS}.
- preferred_channel = channel_code con más operaciones hasta el corte; desempate: operación más reciente, después alfabético.
- Una versión nueva = cambia activity_segment o preferred_channel. dominant_asset_type y currency_profile no entran acá
  (dependen del spec 04): sumarlos solo puede agregar versiones, así que (a) es un piso.""")

usable = segmentation.usable_trades(trades)
points = segmentation.client_points(usable)
distribution = segmentation.versions_distribution(points)

# COMMAND ----------

# MAGIC %md
# MAGIC ### (a) Clientes por número de versiones

# COMMAND ----------

display(distribution)
print("Clientes con 2 o más versiones:", sum(r.clients for r in distribution.collect() if r.versions >= 2))

# COMMAND ----------

# MAGIC %md
# MAGIC ### (b) Cambios entre puntos consecutivos
# MAGIC Entre clientes presentes en ambos puntos. Las filas `inicial → 2026-02-28` e `inicial → 2026-03-13` son los clientes nacidos después del corte anterior.

# COMMAND ----------

display(segmentation.transitions(points))

# COMMAND ----------

# MAGIC %md
# MAGIC ### (c) Matriz de transición de `activity_segment`

# COMMAND ----------

display(segmentation.segment_transition_matrix(points))

# COMMAND ----------

# MAGIC %md
# MAGIC → **Decisión: ADR-007.** Si (b) da 0 o (a) da muy poco, se replantea ADR-007 antes del spec 06.

# COMMAND ----------

dbutils.notebook.exit(json.dumps({"status": "SUCCESS", "pipeline_run_id": RUN_ID, "rows_out": 0, "rows_read": summary["rows"]}))
