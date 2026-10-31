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
LANDING_VOLUME = f"/Volumes/{CATALOG}/iol_bronze/landing"
TZ_MARKET = "America/Argentina/Buenos_Aires"


def is_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def table(layer: str, name: str, catalog: str | None = None) -> str:
    return f"{catalog or CATALOG}.{SCHEMAS[layer]}.{name}"


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
CHANNELS = {
    "App Mobile": "APP",
    "Sitio Web Desktop": "WEB",
    "Sitio Web Responsive": "WEB",
    "API": "API",
    "IOLnet": "IOLNET",
}

# --- Calendario (ADR-003) -----------------------------------------------------
# Verificar contra el calendario oficial de BYMA y citar la fuente en ADR-003.
MARKET_HOLIDAYS = {
    date(2026, 1, 1): "Año Nuevo",
    date(2026, 2, 16): "Carnaval",
    date(2026, 2, 17): "Carnaval",
    date(2026, 3, 24): "Día de la Memoria",
    # date(2026, 3, 23): "Feriado puente"  # VERIFICAR
}
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
