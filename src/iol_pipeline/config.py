"""Configuración única del pipeline. Ningún umbral vive fuera de este archivo.

Los valores reflejan docs/quality_rules.md, docs/quality_gates.md y los ADR.
Si cambia un valor, actualizar el doc/ADR correspondiente en el mismo commit.
"""
from __future__ import annotations

import os
from datetime import date

# --- Entorno -----------------------------------------------------------------
CATALOG = os.environ.get("IOL_CATALOG", "workspace")
SCHEMAS = {"bronze": "iol_bronze", "silver": "iol_silver", "gold": "iol_gold", "ops": "iol_ops"}
LANDING_VOLUME_NAME = "landing"  # Volume en iol_bronze; ruta: io.landing_path()
LANDING_SUBDIRS = ("trades", "quotes", "ai")
TZ_MARKET = "America/Argentina/Buenos_Aires"


def is_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


# --- Versiones -----------------------------------------------------------------
SCHEMA_VERSION = "trades_v1"
RULES_VERSION = "rules_v1"
MAPPING_VERSION = "map_v1"
SEGMENT_RULE_VERSION = "seg_v1"

TRADES_CONTRACT = [
    "fecha", "tipoTran", "id_cliente", "descripcion_titulo", "moneda",
    "simbolo_titulo", "cantidad", "precio", "id_transaccion", "origen",
]
SIDES = {"Compra": "BUY", "Venta": "SELL"}
CURRENCIES = {"ARS", "USD"}
CHANNELS = {  # origen -> channel_code (dim_channel, preferred_channel)
    "App Mobile": "APP_MOBILE",
    "Sitio Web Desktop": "WEB_DESKTOP",
    "Sitio Web Responsive": "WEB_RESPONSIVE",
    "API": "API",
    "IOLnet": "IOLNET",
}
CHANNEL_FAMILIES = {  # channel_code -> channel_family: agrupación descriptiva (data_model.md)
    "APP_MOBILE": "APP",
    "WEB_DESKTOP": "WEB",
    "WEB_RESPONSIVE": "WEB",
    "API": "API",
    "IOLNET": "IOLNET",
}

# --- Calendario (ADR-003) -----------------------------------------------------
# Feriados bursátiles = feriados nacionales (no los "días no laborables") en el rango de
# dim_date (2025-12-01..2026-03-31). Fuente oficial: calendario de feriados nacionales de
# Jefatura de Gabinete, https://www.argentina.gob.ar/jefatura/feriados-nacionales-2026
# (datos: /sites/default/files/holidays-2025-es.json y holidays-2026-es.json), consultado 2026-10-31.
# Excluidos a propósito: 2026-03-20 (no laborable, Ramadán) y 2026-03-23 (no laborable con fines
# turísticos): BYMA operó ese día, sin liquidación (cierre registrado en
# https://www.rava.com/cotizaciones/historico/byma/2026/03/23).
MARKET_HOLIDAYS = {
    date(2025, 12, 8): "Inmaculada Concepción de María",
    date(2025, 12, 25): "Navidad",
    date(2026, 1, 1): "Año Nuevo",
    date(2026, 2, 16): "Carnaval",
    date(2026, 2, 17): "Carnaval",
    date(2026, 3, 24): "Día Nacional de la Memoria por la Verdad y la Justicia",
}
CALENDAR_FROM = date(2025, 12, 1)
CALENDAR_TO = date(2026, 3, 31)
SESSION_HOURS_LOCAL = ("11:00", "17:00")
DATA_CUTOFF = date(2026, 3, 13)

# --- Calidad (docs/quality_rules.md) ---------------------------------------------
SEVERITY_RANK = {"OK": 0, "INFO": 0, "WARNING": 1, "ERROR": 2, "CRITICAL": 3}
QUALITY_PARAMS = {
    "robust_z_threshold": 3.5,         # DQ020, Iglewicz-Hoaglin
    "scale_ratio_low": 0.02,           # DQ021
    "scale_ratio_high": 50.0,          # DQ021
    "extreme_qty_quantile": 0.999,     # DQ022
}

# --- Cotizaciones (ADR-005, ADR-009) ----------------------------------------------
MAX_QUOTE_AGE_BDAYS = 3
ABOVE_MARKET_TOLERANCE = 0.005         # [SUPUESTO] calibrar con rango diario por asset_type
SCALE_SANITY_RANGE = (0.5, 2.0)
QUOTES_FROM = date(2025, 12, 15)
QUOTES_TO = DATA_CUTOFF
HTTP_TIMEOUT_S = 10
HTTP_MAX_RETRIES = 3

# --- Smoke test de entorno (spec 00; resultados en ADR-001/008/011) ----------------
# Raíz de cada proveedor candidato de ADR-008: solo se prueba si el host es alcanzable.
SMOKE_EGRESS_URLS = {
    "iol_api": "https://api.invertironline.com",
    "byma_open_data": "https://open.bymadata.com.ar",
    "data912": "https://data912.com",
    "yahoo_finance": "https://query1.finance.yahoo.com",
}
SMOKE_EGRESS_INCONCLUSIVE_HTTP = frozenset({403, 407, 429})  # también los puede devolver un intermediario (ADR-008)
SMOKE_AI_MODEL = "databricks-meta-llama-3-3-70b-instruct"  # [MEDIDO] disponible en Free Edition (smoke 2026-11-01, ADR-011)
SMOKE_CHILD_TIMEOUT_S = 60
SMOKE_DETAIL_MAX_CHARS = 500  # lo que se guarda en smoke_test_results
SMOKE_PRINT_MAX_CHARS = 300   # lo que se imprime en el output del notebook
SMOKE_ERROR_MAX_CHARS = 200   # mensaje de una excepción de egress

# --- Segmentación SCD2 (ADR-007) -------------------------------------------------
# Cortes congelados en seg_v1 a partir de p75/p90-p95/p99 de ops por cliente.
ACTIVITY_SEGMENTS = [  # (mínimo de operaciones acumuladas, segmento)
    (10, "MUY_ACTIVO"),
    (4, "ACTIVO"),
    (2, "RECURRENTE"),
    (1, "OCASIONAL"),
]
SCD2_SNAPSHOTS = [date(2026, 1, 31), date(2026, 2, 28), DATA_CUTOFF]
SCD2_OPEN_END = date(9999, 12, 31)

# --- Quality gates (docs/quality_gates.md) -----------------------------------------
GATE_PARAMS = {
    "unmapped_yellow_pct": 0.01,
    "unmapped_red_pct": 0.05,
    "coverage_green_pct": 0.80,
    "coverage_inconclusive_pct": 0.50,
    "top_n_symbols": 20,
    "top_symbol_coverage_pct": 0.95,
    "drift_relative_tolerance": 0.50,
    "max_pipeline_minutes": 30,
}

# --- EDA (spec 08) ----------------------------------------------------------------
Q1_MIN_COMPARABLE_TRADES = 5
Q1_MIN_SHARE_ABOVE = 0.70
Q1_ALPHA = 0.05
Q2_MIN_COMPARABLE_TRADES = 20
