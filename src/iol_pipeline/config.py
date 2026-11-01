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
RECORD_HASH_SEPARATOR = "\u001f"
RECORD_HASH_BITS = 256
BRONZE_DEMO_RUNS = 2  # corridas de 02_ingest_bronze con demo_rerun, para mostrar la idempotencia
HASH_CHUNK_BYTES = 1024 * 1024
RUN_ERROR_MAX_CHARS = 500
TRADES_TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSX"
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
CHANNEL_UNKNOWN = "UNKNOWN"
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
RULE_SEVERITY = {  # DQ021 baja a WARNING en OPCION (quality.rule_severity)
    "DQ002_REQUIRED_NULL": "CRITICAL",
    "DQ003_TS_UNPARSEABLE": "CRITICAL",
    "DQ004_ID_CONFLICT": "CRITICAL",
    "DQ005_NUMERIC_UNPARSEABLE": "CRITICAL",
    "DQ006_NON_POSITIVE": "ERROR",
    "DQ007_DOMAIN_INVALID": "ERROR",
    "DQ008_MALFORMED_ROW": "CRITICAL",
    "DQ010_SATURDAY": "WARNING",
    "DQ011_SUNDAY": "ERROR",
    "DQ012_MARKET_HOLIDAY": "ERROR",
    "DQ013_OUTSIDE_SESSION": "INFO",
    "DQ014_OUTSIDE_CALENDAR": "ERROR",
    "DQ020_EXTREME_AMOUNT": "WARNING",
    "DQ021_POSSIBLE_SCALE_ERROR": "ERROR",
    "DQ022_EXTREME_QTY": "WARNING",
    "DQ030_INSTRUMENT_UNMAPPED": "WARNING",
    "DQ031_SYMBOL_MULTI_DESC": "INFO",
    "DQ032_AFTER_MATURITY": "ERROR",
    "DQ033_CURRENCY_VARIANT_MISMATCH": "ERROR",
}
DECIMAL_PARSE_TYPE = "decimal(38,18)"  # para detectar si convertir a QTY_TYPE o PRICE_TYPE cambiaría el dato
QTY_TYPE = "decimal(20,0)"
PRICE_TYPE = "decimal(24,8)"
AMOUNT_TYPE = "decimal(38,8)"
AMOUNT_PRICE_TYPE = "decimal(17,8)"  # QTY_TYPE × este tipo entra en AMOUNT_TYPE sin que Spark recorte decimales
EXAMPLE_HASH_CHARS = 12
EVENT_EXAMPLES_PER_RULE = 3
EVENT_EXAMPLE_RULES = ("DQ010_SATURDAY", "DQ020_EXTREME_AMOUNT", "DQ021_POSSIBLE_SCALE_ERROR")
QUALITY_PARAMS = {
    "robust_z_threshold": 3.5,         # DQ020, Iglewicz-Hoaglin
    "robust_z_constant": 0.6745,       # DQ020, Iglewicz-Hoaglin
    "scale_ratio_low": 0.02,           # DQ021
    "scale_ratio_high": 50.0,          # DQ021
    "extreme_qty_quantile": 0.999,     # DQ022
}

# --- Cotizaciones (ADR-005, ADR-009) ----------------------------------------------
MAX_QUOTE_AGE_BDAYS = 3
ABOVE_MARKET_TOLERANCE = 0.005         # [SUPUESTO] tipos sin rango medido: hoy no tienen cotizaciones
ABOVE_MARKET_TOLERANCE_BY_TYPE = {     # mitad de la mediana del rango diario (high - low) / close, medido (ADR-009)
    "ACCION_LOCAL": 0.026, "CEDEAR": 0.015, "BONO_SOBERANO": 0.008, "BONO_SUBSOBERANO": 0.013, "LETRA_LECAP_BONCAP": 0.006, "ON": 0.009, "WARRANT": 0.038,
}
ANALYTICS_ELIGIBLE_STATUSES = ("OK", "WARNING")
QUOTE_PROVIDERS_BY_TYPE = {            # prioridad fijada de antemano: se usa la primera serie que valida (ADR-008)
    "CEDEAR": ("data912", "yahoo", "byma_open_data"),
    "ACCION_LOCAL": ("data912", "yahoo", "byma_open_data"),
    "BONO_SOBERANO": ("data912", "byma_open_data"),
    "BONO_SUBSOBERANO": ("data912", "byma_open_data"),
    "LETRA_LECAP_BONCAP": ("byma_open_data",),
    "ON": ("byma_open_data",),
    "OPCION": ("byma_open_data",),
    "WARRANT": ("byma_open_data",),
    "OTRO": ("byma_open_data",),
}
QUOTE_SCALE_CANDIDATES = (0.01, 0.1, 1 / 3, 1.0, 3.0, 10.0, 100.0)  # los factores medidos (ADR-005)
QUOTE_MIN_DAYS = 5                     # días comparables para validar una serie; con menos queda THIN (ADR-008)
QUOTE_PRICE_DECIMALS = 6
QUOTE_LOOKBACK_CALENDAR_DAYS = 10      # corta el join por rango; alcanza con holgura para MAX_QUOTE_AGE_BDAYS y un feriado largo
QUOTE_INDEX_MARGIN_DAYS = 10
QUOTE_WRITE_BATCH = 200                # pedidos por proveedor entre escrituras
QUOTE_COVERAGE_TOP_N = 30              # filas de la tabla de cobertura en el notebook y el README (spec 05)
SCALE_SANITY_RANGE = (0.5, 2.0)
QUOTES_FROM = date(2025, 12, 15)
QUOTES_TO = DATA_CUTOFF
HTTP_TIMEOUT_S = 10
HTTP_MAX_RETRIES = 3
HTTP_BACKOFF_BASE_S = 1.0
HTTP_BACKOFF_JITTER_S = 0.5
QUOTE_PAUSE_S = 1.5  # entre pedidos a un mismo proveedor
DATA912_KIND_BY_TYPE = {  # dónde está el histórico de cada tipo de activo en data912
    "CEDEAR": "cedears", "ACCION_LOCAL": "stocks", "BONO_SOBERANO": "bonds", "BONO_SUBSOBERANO": "bonds", "LETRA_LECAP_BONCAP": "bonds", "ON": "bonds",
}

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

# --- Instrumentos y proveedores (ADR-004, ADR-005, ADR-008) ------------------------
OPTION_UNDERLYING = {  # prefijo del símbolo de la opción -> acción subyacente
    "ALU": "ALUA", "BBA": "BBAR", "BHI": "BHIP", "BMA": "BMA", "BYM": "BYMA", "CEP": "CEPU", "COM": "COME", "CRE": "CRES",
    "EDN": "EDN", "GFG": "GGAL", "MET": "METR", "PAM": "PAMP", "TGN": "TGNO4", "TGS": "TGSU2", "TRA": "TRAN", "TXA": "TXAR", "YPF": "YPFD",
}
# (asset_type, es USD; None = cualquiera, proveedor). Lo marcado como provisional no está decidido en ADR-008.
PROVIDER_RULES = [
    ("BONO_SOBERANO", None, "data912"),
    ("ACCION_LOCAL", False, "data912"),
    ("ACCION_LOCAL", True, "yahoo"),  # provisional
    ("CEDEAR", False, "data912"),
    ("CEDEAR", True, "yahoo"),  # provisional
    ("LETRA_LECAP_BONCAP", None, "byma_open_data"),
    ("ON", None, "byma_open_data"),
    ("BONO_SUBSOBERANO", None, "byma_open_data"),  # provisional
]
ASSET_TYPES_PER_100_NOMINAL = ("BONO_SOBERANO", "BONO_SUBSOBERANO", "LETRA_LECAP_BONCAP", "ON")
PER_100_NOMINAL_FACTOR = 0.01  # valor inicial; la etapa 05 lo valida por símbolo
ASSET_TYPES = ("CEDEAR", "ACCION_LOCAL", "BONO_SOBERANO", "BONO_SUBSOBERANO", "LETRA_LECAP_BONCAP", "ON", "OPCION", "WARRANT", "OTRO")
SETTLEMENT_VARIANTS = ("ARS", "USD_MEP", "USD_CCL", "USD_OTHER", "UNKNOWN")
MAPPING_STATUSES = ("UNVERIFIED", "MAPPED", "UNMAPPED", "UNSUPPORTED")
VARIANT_FX_RANGE = (1000.0, 2000.0)  # [SUPUESTO] para el período: el MEP implícito medido da ≈1.447 (ADR-004)
DATA912_LIVE_URL = "https://data912.com/live/{name}"
DATA912_LIST_TYPES = {  # lista de data912 -> tipos de activo que le corresponden, para validar la clasificación
    "arg_cedears": ("CEDEAR",),
    "arg_bonds": ("BONO_SOBERANO", "BONO_SUBSOBERANO"),
    "arg_notes": ("LETRA_LECAP_BONCAP", "BONO_SOBERANO"),
    "arg_corp": ("ON",),
    "arg_stocks": ("ACCION_LOCAL",),
}
DATA912_NOT_INDEPENDENT_LISTS = ("arg_stocks",)  # es la misma lista que usa la regla de acciones
DISAGREEMENT_EXAMPLES = 10
BYMA_SYMBOL_SUFFIX = " 24HS"
YAHOO_SYMBOL_SUFFIX = ".BA"

# --- IA (ADR-011, spec 09) ---------------------------------------------------------
AI_MODEL = "databricks-qwen3-next-80b-a3b-instruct"  # el de mayor accuracy en la muestra (ADR-011)
AI_EVAL_MODELS = ("databricks-meta-llama-3-3-70b-instruct", "databricks-gpt-oss-120b", "databricks-qwen3-next-80b-a3b-instruct")
AI_PROMPT_VERSION = "instrument_enrichment_v1"
AI_BATCH_SIZE = 50
AI_MAX_TOKENS = 8000
AI_TEMPERATURE = 0.0
AI_MIN_CONFIDENCE = 0.6  # [SUPUESTO] por debajo no se usa el resultado
AI_MAX_ISSUER_CHARS = 120
AI_EXAMPLE_ROWS = 25
AI_SECTORS = ("FINANCIERO", "ENERGIA", "UTILITIES", "MATERIALES", "INDUSTRIAL", "CONSUMO", "TECNOLOGIA", "SALUD", "COMUNICACIONES",
              "INMOBILIARIO", "SOBERANO", "SUBSOBERANO", "ETF", "OTRO")

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

# --- Profiling (spec 01) ---------------------------------------------------------
CLIENT_PERCENTILES = (0.5, 0.75, 0.9, 0.95, 0.99)
PROFILE_TOP_N = 10
PROFILE_TOP_SYMBOLS = 20
PROFILE_PRICE_GROUPS = (("AL30", "AL30D", "AL30C"), ("GD30", "GD30D"), ("AAPL", "AAPLD", "AAPLC"))  # base primero

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
