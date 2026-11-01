"""Cotizaciones de mercado (spec 05, ADR-005, ADR-008, ADR-009)."""
from __future__ import annotations

import json
import math
import random
import re
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Protocol

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from iol_pipeline import calendar, config, io

HEADERS = {"User-Agent": "Mozilla/5.0"}
MARKET_TZ = timezone(timedelta(hours=-3))  # Argentina no tiene horario de verano
DATA912_URL = "https://data912.com/historical/{kind}/{symbol}"
BYMA_URL = "https://open.bymadata.com.ar/vanoms-be-core/rest/api/bymadata/free/chart/historical-series/history"
YAHOO_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"


class ProviderError(Exception):
    """Falla de un proveedor para un símbolo. `retryable` solo en 429, 5xx y errores de conexión: un 4xx no cambia reintentando."""

    def __init__(self, message: str, status: int | None = None, retryable: bool = False) -> None:
        super().__init__(message)
        self.status = status
        self.retryable = retryable


class QuoteProvider(Protocol):
    name: str

    def fetch_daily(self, provider_symbol: str, date_from: date, date_to: date) -> list[dict]:
        """Filas con date, open, high, low, close, volume. Sin datos: lista vacía. Falla: ProviderError."""
        ...


def _in_range(day: date, start: date, end: date) -> bool:
    return start <= day <= end


def _epoch(day: date) -> int:
    return int(datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp())


def parse_data912(payload: Any, start: date, end: date) -> list[dict]:
    """Lista de filas. Un símbolo que no es de ese tipo de instrumento devuelve otra cosa, con HTTP 200."""
    if not isinstance(payload, list):
        return []
    rows = [{"date": date.fromisoformat(r["date"]), "open": r.get("o"), "high": r.get("h"), "low": r.get("l"), "close": r.get("c"), "volume": r.get("v")}
            for r in payload]
    return [r for r in rows if _in_range(r["date"], start, end) and r["close"] is not None]


def parse_byma(payload: dict, start: date, end: date) -> list[dict]:
    """Series paralelas con epoch: {"s": "ok", "t": [...], "o": [...], ...}. Sin datos: {"s": "no_data"}."""
    if payload.get("s") != "ok":
        return []
    volumes = payload.get("v") or [None] * len(payload["t"])
    rows = [{"date": datetime.fromtimestamp(t, MARKET_TZ).date(), "open": o, "high": h, "low": l, "close": c, "volume": v}
            for t, o, h, l, c, v in zip(payload["t"], payload["o"], payload["h"], payload["l"], payload["c"], volumes)]
    return [r for r in rows if _in_range(r["date"], start, end) and r["close"] is not None]


def parse_yahoo(payload: dict, start: date, end: date) -> list[dict]:
    result = (payload.get("chart", {}).get("result") or [None])[0]
    if not result or "timestamp" not in result:
        return []
    quote = result["indicators"]["quote"][0]
    volumes = quote.get("volume") or [None] * len(result["timestamp"])
    rows = [{"date": datetime.fromtimestamp(t, MARKET_TZ).date(), "open": o, "high": h, "low": l, "close": c, "volume": v}
            for t, o, h, l, c, v in zip(result["timestamp"], quote["open"], quote["high"], quote["low"], quote["close"], volumes)]
    return [r for r in rows if _in_range(r["date"], start, end) and r["close"] is not None]


def _get_json(get: Callable[..., Any], url: str, params: dict | None = None) -> Any:
    """GET con timeout. Traduce cada falla a ProviderError, con si conviene reintentar."""
    try:
        response = get(url, params=params, timeout=config.HTTP_TIMEOUT_S, headers=HEADERS)
    except Exception as exc:  # el cliente HTTP es inyectable: sus excepciones no se conocen acá
        raise ProviderError(f"{type(exc).__name__}: {exc}", retryable=True) from exc
    status = response.status_code
    if status != 200:
        raise ProviderError(f"HTTP {status}", status=status, retryable=status == 429 or status >= 500)
    try:
        return response.json()
    except ValueError as exc:
        raise ProviderError("La respuesta no es JSON", status=status) from exc


class Data912Provider:
    """El histórico se separa por tipo de instrumento: `kind_of` dice dónde buscar cada símbolo. Un 404 es un símbolo que no tiene."""

    name = "data912"

    def __init__(self, get: Callable[..., Any], kind_of: dict[str, str]) -> None:
        self.get = get
        self.kind_of = kind_of

    def fetch_daily(self, provider_symbol: str, date_from: date, date_to: date) -> list[dict]:
        url = DATA912_URL.format(kind=self.kind_of[provider_symbol], symbol=provider_symbol)
        try:
            return parse_data912(_get_json(self.get, url), date_from, date_to)
        except ProviderError as exc:
            if exc.status == 404:
                return []
            raise


class BymaProvider:
    name = "byma_open_data"

    def __init__(self, get: Callable[..., Any]) -> None:
        self.get = get

    def fetch_daily(self, provider_symbol: str, date_from: date, date_to: date) -> list[dict]:
        params = {"symbol": provider_symbol, "resolution": "D", "from": _epoch(date_from), "to": _epoch(date_to + timedelta(days=1))}
        return parse_byma(_get_json(self.get, BYMA_URL, params), date_from, date_to)


class YahooProvider:
    name = "yahoo"

    def __init__(self, get: Callable[..., Any]) -> None:
        self.get = get

    def fetch_daily(self, provider_symbol: str, date_from: date, date_to: date) -> list[dict]:
        params = {"period1": _epoch(date_from), "period2": _epoch(date_to + timedelta(days=1)), "interval": "1d"}
        try:
            return parse_yahoo(_get_json(self.get, YAHOO_URL.format(symbol=provider_symbol), params), date_from, date_to)
        except ProviderError as exc:
            if exc.status == 404:
                return []
            raise


def _safe_name(provider_symbol: str) -> str:
    """Nombre de archivo sin caracteres que Windows o un Volume rechazan. `#` y el espacio de ` 24HS` no pasan."""
    return re.sub(r"[^A-Za-z0-9._-]", "_", provider_symbol)


def replay_path(root: Path, provider: str, provider_symbol: str) -> Path:
    return Path(root) / provider / f"{_safe_name(provider_symbol)}.json"


def save_replay(root: Path, provider: str, provider_symbol: str, rows: list[dict]) -> None:
    path = replay_path(root, provider, provider_symbol)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = [{**row, "date": row["date"].isoformat()} for row in rows]
    path.write_text(json.dumps(payload), encoding="utf-8")


class FileReplayProvider:
    """Lee lo que guardó `tools/quotes_fetcher`. Sin archivo para el símbolo se comporta como un proveedor caído para ese símbolo."""

    def __init__(self, name: str, root: Path) -> None:
        self.name = name
        self.root = Path(root)

    def fetch_daily(self, provider_symbol: str, date_from: date, date_to: date) -> list[dict]:
        path = replay_path(self.root, self.name, provider_symbol)
        if not path.is_file():
            raise ProviderError(f"Sin archivo para {provider_symbol}")
        rows = [{**row, "date": date.fromisoformat(row["date"])} for row in json.loads(path.read_text(encoding="utf-8"))]
        return [row for row in rows if _in_range(row["date"], date_from, date_to)]


def fetch_with_retries(provider: QuoteProvider, provider_symbol: str, date_from: date, date_to: date, sleep: Callable[[float], None] = time.sleep) -> tuple[list[dict], dict]:
    """Devuelve (filas, registro para `quote_requests`). Nunca lanza: el error de un símbolo no detiene el lote (R-20)."""
    attempts = 0
    status: int | None = None
    error: str | None = None
    rows: list[dict] = []
    while attempts < config.HTTP_MAX_RETRIES:
        attempts += 1
        try:
            rows = provider.fetch_daily(provider_symbol, date_from, date_to)
            status, error = 200, None
            break
        except ProviderError as exc:
            status, error = exc.status, str(exc)[:config.RUN_ERROR_MAX_CHARS]
            if not exc.retryable or attempts == config.HTTP_MAX_RETRIES:
                break
            sleep(config.HTTP_BACKOFF_BASE_S * 2 ** (attempts - 1) + random.uniform(0, config.HTTP_BACKOFF_JITTER_S))
        except Exception as exc:  # una respuesta con otra forma tampoco puede detener el lote; reintentar no la cambia
            status, error = None, f"{type(exc).__name__}: {exc}"[:config.RUN_ERROR_MAX_CHARS]
            break
    return rows, {"provider": provider.name, "provider_symbol": provider_symbol, "date_from": date_from, "date_to": date_to, "http_status": status,
                  "attempts": attempts, "rows_returned": len(rows), "error": error}


# --- Adquisición ------------------------------------------------------------------------
RAW_SCHEMA = ("provider string, provider_symbol string, symbol string, quote_date date, open double, high double, low double, close double, volume double, "
              "fetch_run_id string, fetched_at timestamp")
REQUESTS_SCHEMA = ("fetch_run_id string, source string, provider string, provider_symbol string, symbol string, date_from date, date_to date, http_status int, attempts int, "
                   "rows_returned int, error string, requested_at timestamp")
RAW_KEYS = ["provider", "provider_symbol", "quote_date"]
REQUEST_KEYS = ["provider", "provider_symbol", "date_from", "date_to"]


def provider_symbol(provider: str, symbol: str) -> str:
    return symbol + {"byma_open_data": config.BYMA_SYMBOL_SUFFIX, "yahoo": config.YAHOO_SYMBOL_SUFFIX}.get(provider, "")


def build_provider(name: str, symbols: list[dict], get: Callable[..., Any]) -> QuoteProvider:
    """`symbols` tiene `symbol` y `asset_type`: data912 guarda el histórico en un lugar distinto por tipo."""
    if name == "data912":
        return Data912Provider(get, {s["symbol"]: config.DATA912_KIND_BY_TYPE[s["asset_type"]] for s in symbols if s["asset_type"] in config.DATA912_KIND_BY_TYPE})
    return {"byma_open_data": BymaProvider, "yahoo": YahooProvider}[name](get)


def candidate_series(mapping_rows: list[dict]) -> list[dict]:
    """Una fila por símbolo y proveedor posible, en orden de prioridad. No depende del proveedor del mapeo: lo sobrescribe esta etapa."""
    return [{"symbol": row["symbol"], "asset_type": row["asset_type"], "provider": provider, "provider_symbol": provider_symbol(provider, row["symbol"]), "priority": priority}
            for row in mapping_rows
            for priority, provider in enumerate(config.QUOTE_PROVIDERS_BY_TYPE.get(row["asset_type"], ()))]


def _fetch_provider(provider: QuoteProvider, items: list[dict], date_from: date, date_to: date, pause_s: float) -> tuple[list[dict], list[dict]]:
    rows, records = [], []
    for item in items:
        fetched, record = fetch_with_retries(provider, item["provider_symbol"], date_from, date_to)
        rows += [{**row, "symbol": item["symbol"], "provider": provider.name, "provider_symbol": item["provider_symbol"]} for row in fetched]
        records.append({**record, "symbol": item["symbol"]})
        time.sleep(pause_s)
    return rows, records


def _as_float(value: Any) -> float | None:
    return float(value) if value is not None else None


def run_fetch(spark: SparkSession, candidates: list[dict], providers: dict[str, QuoteProvider], raw_table: str, requests_table: str, source: str, fetch_run_id: str = "",
              date_from: date = config.QUOTES_FROM, date_to: date = config.QUOTES_TO, pause_s: float = 0.0, batch_size: int = config.QUOTE_WRITE_BATCH) -> dict:
    """Pide lo que falta, un hilo por proveedor. Lo ya pedido sin error no se repite, y un error se reintenta en otra corrida (R-20).

    Escribe cada `batch_size` pedidos por proveedor: si la corrida se corta, la siguiente sigue desde ahí. Los hilos solo hacen HTTP; Spark queda en el hilo principal.
    """
    done: set[tuple[str, str]] = set()
    if spark.catalog.tableExists(requests_table):
        finished = spark.table(requests_table).where(F.col("error").isNull() & (F.col("date_from") == date_from) & (F.col("date_to") == date_to))
        done = {(r["provider"], r["provider_symbol"]) for r in finished.select("provider", "provider_symbol").collect()}
    pending: dict[str, list[dict]] = {}
    for item in candidates:
        if (item["provider"], item["provider_symbol"]) not in done:
            pending.setdefault(item["provider"], []).append(item)

    counts = {"requested": 0, "with_rows": 0, "errors": 0, "skipped": len(candidates) - sum(len(v) for v in pending.values())}
    while pending:
        batch = {name: items[:batch_size] for name, items in pending.items()}
        pending = {name: items[batch_size:] for name, items in pending.items() if items[batch_size:]}
        with ThreadPoolExecutor(max_workers=len(batch)) as pool:
            results = list(pool.map(lambda entry: _fetch_provider(providers[entry[0]], entry[1], date_from, date_to, pause_s), batch.items()))
        rows = [row for fetched, _ in results for row in fetched]
        records = [record for _, logged in results for record in logged]
        now = datetime.now(timezone.utc)
        if rows:
            raw = spark.createDataFrame([(r["provider"], r["provider_symbol"], r["symbol"], r["date"], _as_float(r["open"]), _as_float(r["high"]), _as_float(r["low"]), _as_float(r["close"]),
                                          _as_float(r.get("volume")), fetch_run_id, now) for r in rows], RAW_SCHEMA)
            io.write_delta_upsert(spark, raw, raw_table, RAW_KEYS, ["open", "high", "low", "close", "volume"])
        log = spark.createDataFrame([(fetch_run_id, source, r["provider"], r["provider_symbol"], r["symbol"], r["date_from"], r["date_to"], r["http_status"], r["attempts"], r["rows_returned"], r["error"], now)
                                     for r in records], REQUESTS_SCHEMA)
        io.write_delta_upsert(spark, log, requests_table, REQUEST_KEYS, ["http_status", "attempts", "rows_returned", "error"])
        counts["requested"] += len(records)
        counts["with_rows"] += sum(r["rows_returned"] > 0 for r in records)
        counts["errors"] += sum(r["error"] is not None for r in records)
    return counts


def reconcile_requests(requests: DataFrame, raw: DataFrame) -> DataFrame:
    """Por proveedor, lo que `quote_requests` dice que volvió contra lo que hay en `quotes_raw`. Tienen que coincidir."""
    logged = requests.groupBy("provider").agg(F.count("*").alias("requests"), F.sum(F.when(F.col("rows_returned") > 0, 1).otherwise(0)).alias("requests_with_rows"),
                                              F.sum(F.when(F.col("error").isNotNull(), 1).otherwise(0)).alias("requests_with_error"), F.sum("rows_returned").alias("rows_returned"))
    stored = raw.groupBy("provider").agg(F.count("*").alias("rows_stored"))
    return logged.join(stored, "provider", "left").fillna({"rows_stored": 0}).orderBy("provider")


# --- Validación por serie (ADR-005, ADR-008) -----------------------------------------------
OK, THIN, BIASED, SCALE_SUSPECT, NO_OVERLAP, NO_DATA = "OK", "THIN", "BIASED", "SCALE_SUSPECT", "NO_OVERLAP", "NO_DATA"
SERIES_SCHEMA = ("symbol string, provider string, provider_symbol string, priority int, quote_rows int, days_compared int, ratio_p10 double, ratio_p50 double, ratio_p90 double, "
                 "price_scale_factor double, bias_pct double, series_status string, is_selected boolean, is_active boolean")
SERIES_COLUMNS = [c.split()[0] for c in SERIES_SCHEMA.split(", ")]


def tolerance_for(asset_type: str) -> float:
    return config.ABOVE_MARKET_TOLERANCE_BY_TYPE.get(asset_type, config.ABOVE_MARKET_TOLERANCE)


def _eligible_trades(trades: DataFrame) -> DataFrame:
    return trades.where(F.col("quality_status").isin(*config.ANALYTICS_ELIGIBLE_STATUSES) & F.col("effective_trade_date").isNotNull())


def series_stats(raw: DataFrame, trades: DataFrame) -> DataFrame:
    """Por símbolo y proveedor, la relación diaria: mediana del precio operado / cierre crudo del mismo día (spec 05)."""
    daily = _eligible_trades(trades).groupBy("symbol", "effective_trade_date").agg(F.median(F.col("price").cast("double")).alias("trade_price"))
    ratios = (raw.where(F.col("close") > 0).alias("q")
              .join(daily.alias("t"), (F.col("q.symbol") == F.col("t.symbol")) & (F.col("q.quote_date") == F.col("t.effective_trade_date")))
              .select("q.provider", "q.symbol", (F.col("t.trade_price") / F.col("q.close")).alias("ratio")))
    stats = ratios.groupBy("provider", "symbol").agg(F.count("*").cast("int").alias("days_compared"), F.percentile("ratio", 0.1).alias("ratio_p10"),
                                                     F.median("ratio").alias("ratio_p50"), F.percentile("ratio", 0.9).alias("ratio_p90"))
    sizes = raw.groupBy("provider", "symbol").agg(F.count("*").cast("int").alias("quote_rows"))
    return sizes.join(stats, ["provider", "symbol"], "left")


def assess_series(asset_type: str, quote_rows: int, days_compared: int, ratio_p50: float | None) -> dict:
    """Factor por símbolo y proveedor, y si la serie sirve como benchmark. Sin Spark, para poder probar cada caso.

    El factor es el candidato más cercano a la relación medida: corrige la unidad, y no puede absorber un sobreprecio. Lo que queda después de aplicarlo es el sesgo.
    Un sesgo mayor a la tolerancia descarta la serie con cualquier cantidad de días; `THIN` es una serie que no se contradice, pero sin días para validarla.
    """
    if not quote_rows:
        return {"price_scale_factor": None, "bias_pct": None, "series_status": NO_DATA}
    if not days_compared or not ratio_p50:
        return {"price_scale_factor": None, "bias_pct": None, "series_status": NO_OVERLAP}
    factor = min(config.QUOTE_SCALE_CANDIDATES, key=lambda c: abs(math.log(ratio_p50 / c)))
    remainder = ratio_p50 / factor
    if not config.SCALE_SANITY_RANGE[0] <= remainder <= config.SCALE_SANITY_RANGE[1]:
        return {"price_scale_factor": None, "bias_pct": None, "series_status": SCALE_SUSPECT}
    bias = remainder - 1
    if abs(bias) > tolerance_for(asset_type):
        status = BIASED
    else:
        status = THIN if days_compared < config.QUOTE_MIN_DAYS else OK
    return {"price_scale_factor": factor, "bias_pct": round(bias, 6), "series_status": status}


def assess_all(candidates: list[dict], stats_rows: list[dict]) -> list[dict]:
    """Una fila por candidato. Se elige la primera `OK` de la prioridad fijada de antemano (ADR-008), y si no hay, la primera `THIN`.

    Nunca la que más se parece a las operaciones: elegir por parecido sesgaría la métrica de "por encima del mercado".
    """
    stats = {(r["provider"], r["symbol"]): r for r in stats_rows}
    selected: set[str] = set()
    rows = []
    for item in sorted(candidates, key=lambda c: (c["symbol"], c["priority"])):
        stat = stats.get((item["provider"], item["symbol"]), {})
        assessed = assess_series(item["asset_type"], stat.get("quote_rows") or 0, stat.get("days_compared") or 0, stat.get("ratio_p50"))
        is_selected = assessed["series_status"] == OK and item["symbol"] not in selected
        rows.append({**item, **assessed, "quote_rows": stat.get("quote_rows") or 0, "days_compared": stat.get("days_compared") or 0,
                     "ratio_p10": stat.get("ratio_p10"), "ratio_p50": stat.get("ratio_p50"), "ratio_p90": stat.get("ratio_p90"), "is_selected": is_selected, "is_active": True})
        if is_selected:
            selected.add(item["symbol"])
    for row in rows:
        if row["series_status"] == THIN and row["symbol"] not in selected:
            row["is_selected"] = True
            selected.add(row["symbol"])
    return rows


def run_series(spark: SparkSession, candidates: list[dict], raw_table: str, trades: DataFrame, series_table: str) -> dict:
    """`quote_series`. Un candidato que ya no corresponde, porque el símbolo cambió de tipo, queda con `is_active = false`."""
    raw = spark.table(raw_table)
    if not candidates:
        raise ValueError("Sin candidatos: el mapeo no tiene símbolos con proveedor posible")
    rows = assess_all(candidates, [r.asDict() for r in series_stats(raw, trades).collect()])
    df = spark.createDataFrame([tuple(r[c] for c in SERIES_COLUMNS) for r in rows], SERIES_SCHEMA)
    return io.write_delta_upsert(spark, df, series_table, ["symbol", "provider"], [c for c in SERIES_COLUMNS if c not in ("symbol", "provider")], deactivate_column="is_active")


# --- Benchmark y matching (ADR-009) -----------------------------------------------------
FACT_QUOTES_KEYS = ["instrument_sk", "quote_date", "provider"]
FACT_QUOTES_COMPARE = ["symbol", "open", "high", "low", "close", "close_raw", "currency_code", "price_scale_factor_applied", "volume", "is_active"]
FACT_TRADE_MARKET_COMPARE = ["instrument_sk", "symbol", "currency_code", "side", "match_method", "quote_date_used", "quote_age_business_days", "benchmark_close", "day_low", "day_high",
                             "deviation_pct", "position_vs_range", "above_market_tolerance", "is_above_market", "is_unfavorable_for_client", "is_comparable"]
COVERAGE_COMPARE = ["currency_code", "asset_type", "provider", "provider_symbol", "series_status", "price_scale_factor", "bias_pct", "trades_total", "trades_eligible", "matched_exact", "matched_imputed",
                    "unavailable", "excluded_by_validation", "unmapped", "coverage_pct", "last_error"]
PCT_DECIMALS = 4


def instrument_sk() -> Column:
    return F.xxhash64("symbol")


def market_quotes(raw: DataFrame, series: DataFrame, mapping: DataFrame) -> DataFrame:
    """Solo la serie elegida de cada símbolo, en la escala del trade. Se redondea para que 87.600 × 0,01 no dé 876,0000000001 y quede fuera del rango del día."""
    selected = series.where("is_active AND is_selected").select("provider", "symbol", "price_scale_factor")
    factor = F.col("price_scale_factor")

    def scaled(column: str) -> Column:
        return F.round(F.col(column) * factor, config.QUOTE_PRICE_DECIMALS).alias(column)

    return (raw.where(F.col("close") > 0).join(selected, ["provider", "symbol"]).join(mapping.select("symbol", "currency_code"), "symbol", "left")
            .withColumn("instrument_sk", instrument_sk())
            .select("instrument_sk", "symbol", "quote_date", "provider", scaled("open"), scaled("high"), scaled("low"), scaled("close"), F.col("close").alias("close_raw"),
                    "currency_code", factor.alias("price_scale_factor_applied"), "volume", "fetch_run_id", F.lit(True).alias("is_active")))


def business_day_index(spark: SparkSession, first: date, last: date) -> DataFrame:
    """Días hábiles acumulados por fecha: la resta da la antigüedad en días hábiles, con el mismo calendario que Silver."""
    rows, count, day = [], 0, first
    while day <= last:
        count += calendar.is_business_day(day)
        rows.append((day, count))
        day += timedelta(days=1)
    return spark.createDataFrame(rows, "calendar_date date, business_day_index int")


def best_quote(spark: SparkSession, trades: DataFrame, quotes: DataFrame) -> DataFrame:
    """Última cotización con `quote_date <= effective_trade_date` y antigüedad permitida. Nunca una posterior: no hay look-ahead."""
    days = trades.where(F.col("effective_trade_date").isNotNull()).select("symbol", "effective_trade_date").distinct()
    index = business_day_index(spark, config.QUOTES_FROM, config.QUOTES_TO + timedelta(days=config.QUOTE_INDEX_MARGIN_DAYS))
    candidates = (days.alias("d").join(quotes.alias("q"), (F.col("d.symbol") == F.col("q.symbol")) & (F.col("q.quote_date") <= F.col("d.effective_trade_date"))
                                       & (F.col("q.quote_date") >= F.date_sub(F.col("d.effective_trade_date"), config.QUOTE_LOOKBACK_CALENDAR_DAYS)))
                  .join(index.alias("i"), F.col("d.effective_trade_date") == F.col("i.calendar_date"))
                  .join(index.alias("j"), F.col("q.quote_date") == F.col("j.calendar_date"))
                  .withColumn("quote_age_business_days", F.col("i.business_day_index") - F.col("j.business_day_index"))
                  .where(F.col("quote_age_business_days") <= config.MAX_QUOTE_AGE_BDAYS))
    latest = Window.partitionBy("d.symbol", "d.effective_trade_date").orderBy(F.col("q.quote_date").desc())
    return (candidates.withColumn("_rank", F.row_number().over(latest)).where("_rank = 1")
            .select(F.col("d.symbol").alias("symbol"), "d.effective_trade_date", F.col("q.quote_date").alias("quote_date_used"), "quote_age_business_days",
                    F.col("q.close").alias("benchmark_close"), F.col("q.low").alias("quote_low"), F.col("q.high").alias("quote_high")))


def match_trades(spark: SparkSession, trades: DataFrame, mapping: DataFrame, series: DataFrame, quotes: DataFrame) -> DataFrame:
    """Una fila por operación, siempre: ninguna se descarta (R-16). `match_method` dice por qué no hay benchmark cuando no lo hay."""
    active = series.where("is_active")
    states = active.groupBy("symbol").agg(F.max(F.col("is_selected").cast("int")).alias("_has_selected"), F.max(F.when(F.col("series_status") == BIASED, 1).otherwise(0)).alias("_biased"),
                                          F.max(F.when(F.col("series_status") == SCALE_SUSPECT, 1).otherwise(0)).alias("_scale_suspect"))
    joined = (trades
              .join(mapping.select("symbol", F.col("currency_code").alias("_mapped_currency"), F.col("asset_type").alias("_asset_type")), "symbol", "left")
              .join(states, "symbol", "left")
              .join(best_quote(spark, trades, quotes), ["symbol", "effective_trade_date"], "left"))
    tolerances = F.create_map(*[F.lit(v) for pair in config.ABOVE_MARKET_TOLERANCE_BY_TYPE.items() for v in pair])
    tolerance = F.coalesce(F.try_element_at(tolerances, F.col("_asset_type")), F.lit(config.ABOVE_MARKET_TOLERANCE))
    price = F.col("price").cast("double")
    close = F.col("benchmark_close")
    method = (F.when(F.col("_mapped_currency").isNull(), "UNMAPPED")
              .when(F.col("currency_code") != F.col("_mapped_currency"), "CURRENCY_MISMATCH")
              .when(F.coalesce(F.col("_has_selected"), F.lit(0)) == 0,
                    F.when(F.col("_biased") == 1, "SOURCE_BIASED").when(F.col("_scale_suspect") == 1, "SCALE_SUSPECT").otherwise("UNAVAILABLE"))
              .when(close.isNull(), "UNAVAILABLE")
              .when(F.col("quote_date_used") == F.col("effective_trade_date"), "EXACT_DAY")
              .otherwise("PRIOR_DAY_IMPUTED"))
    same_day = F.col("match_method") == "EXACT_DAY"  # el rango de otro día no dice nada del día de la operación
    usable = F.col("match_method").isin("EXACT_DAY", "PRIOR_DAY_IMPUTED")
    has_range = same_day & F.col("quote_low").isNotNull() & F.col("quote_high").isNotNull()
    is_above = price > close * (1 + tolerance)
    is_below = price < close * (1 - tolerance)
    return (joined
            .withColumn("match_method", method)
            .withColumn("instrument_sk", instrument_sk())
            .select(
                "trade_id", "instrument_sk", "symbol", "currency_code", "side", "match_method",
                F.when(usable, F.col("quote_date_used")).alias("quote_date_used"),
                F.when(usable, F.col("quote_age_business_days")).alias("quote_age_business_days"),
                F.when(usable, close).alias("benchmark_close"),
                F.when(same_day, F.col("quote_low")).alias("day_low"),
                F.when(same_day, F.col("quote_high")).alias("day_high"),
                F.when(usable, (price - close) / close).alias("deviation_pct"),
                F.when(~has_range, "NO_RANGE").when(price > F.col("quote_high"), "ABOVE_HIGH").when(price < F.col("quote_low"), "BELOW_LOW").otherwise("WITHIN").alias("position_vs_range"),
                F.when(usable, tolerance).alias("above_market_tolerance"),
                F.when(usable, is_above).alias("is_above_market"),
                F.when(usable, ((F.col("side") == "BUY") & is_above) | ((F.col("side") == "SELL") & is_below)).alias("is_unfavorable_for_client"),
                (usable & F.col("quality_status").isin(*config.ANALYTICS_ELIGIBLE_STATUSES)).alias("is_comparable"),
            ))


# --- Cobertura (R-21) -------------------------------------------------------------------
def coverage(trades: DataFrame, matched: DataFrame, mapping: DataFrame, series: DataFrame, requests: DataFrame) -> DataFrame:
    """Por símbolo. El denominador son las operaciones elegibles, sin excluir los tipos sin fuente: su cobertura baja tiene que verse."""
    eligible = F.col("quality_status").isin(*config.ANALYTICS_ELIGIBLE_STATUSES)

    def count_if(condition: Column) -> Column:
        return F.sum(F.when(condition, 1).otherwise(0)).cast("int")

    method = F.col("match_method")
    per_symbol = (trades.select("trade_id", "symbol", "quality_status").join(matched.select("trade_id", "match_method"), "trade_id")
                  .groupBy("symbol")
                  .agg(F.count("*").cast("int").alias("trades_total"), count_if(eligible).alias("trades_eligible"),
                       count_if(eligible & (method == "EXACT_DAY")).alias("matched_exact"), count_if(eligible & (method == "PRIOR_DAY_IMPUTED")).alias("matched_imputed"),
                       count_if(eligible & (method == "UNAVAILABLE")).alias("unavailable"), count_if(eligible & method.isin("SOURCE_BIASED", "SCALE_SUSPECT")).alias("excluded_by_validation"),
                       count_if(eligible & method.isin("UNMAPPED", "CURRENCY_MISMATCH")).alias("unmapped")))
    chosen = series.where("is_active AND is_selected").select("symbol", "provider", "provider_symbol", "series_status", "price_scale_factor", "bias_pct")
    errors = requests.where(F.col("error").isNotNull()).groupBy("symbol").agg(F.max("error").alias("last_error"))
    return (per_symbol.join(mapping.select("symbol", "asset_type", "currency_code"), "symbol", "left")
            .join(chosen, "symbol", "left").join(errors, "symbol", "left")
            .withColumn("coverage_pct", F.when(F.col("trades_eligible") > 0, F.round((F.col("matched_exact") + F.col("matched_imputed")) / F.col("trades_eligible"), PCT_DECIMALS)))
            .select("symbol", "currency_code", "asset_type", "provider", "provider_symbol", "series_status", "price_scale_factor", "bias_pct", "trades_total", "trades_eligible",
                    "matched_exact", "matched_imputed", "unavailable", "excluded_by_validation", "unmapped", "coverage_pct", "last_error"))


def run_market(spark: SparkSession, trades_table: str, mapping_table: str, raw_table: str, requests_table: str, series_table: str, market_table: str, trade_market_table: str, coverage_table: str, gold_run_id: str = "") -> dict:
    """Benchmark, matching y cobertura, en ese orden. Reejecutar con el mismo input no cambia ninguna fila (regla 2)."""
    trades, mapping, series = spark.table(trades_table), spark.table(mapping_table), spark.table(series_table)
    quote_result = io.write_delta_upsert(spark, market_quotes(spark.table(raw_table), series, mapping), market_table, FACT_QUOTES_KEYS, FACT_QUOTES_COMPARE, deactivate_column="is_active")
    quotes = spark.table(market_table).where("is_active")
    matched = match_trades(spark, trades, mapping, series, quotes).withColumn("gold_run_id", F.lit(gold_run_id))
    trade_result = io.write_delta_upsert(spark, matched, trade_market_table, ["trade_id"], FACT_TRADE_MARKET_COMPARE)
    matched = spark.table(trade_market_table)
    coverage_result = io.write_delta_upsert(spark, coverage(trades, matched, mapping, series, spark.table(requests_table)), coverage_table, ["symbol"], COVERAGE_COMPARE)
    return {"quotes": quote_result, "trade_market": trade_result, "coverage": coverage_result}


# --- Salidas del notebook ------------------------------------------------------------
def coverage_by_type(coverage_df: DataFrame) -> DataFrame:
    return (coverage_df.groupBy("asset_type").agg(F.count("*").alias("symbols"), F.sum("trades_eligible").alias("eligible"), F.sum("matched_exact").alias("exact"), F.sum("matched_imputed").alias("imputed"),
                                                  F.sum("unavailable").alias("unavailable"), F.sum("excluded_by_validation").alias("excluded_by_validation"))
            .withColumn("coverage_pct", F.round((F.col("exact") + F.col("imputed")) / F.col("eligible"), PCT_DECIMALS)).orderBy(F.col("eligible").desc()))


def overall_coverage(coverage_df: DataFrame) -> dict:
    """Cobertura ponderada por operaciones (G08)."""
    row = coverage_df.agg(F.sum("trades_total").alias("total"), F.sum("trades_eligible").alias("eligible"), F.sum("matched_exact").alias("exact"), F.sum("matched_imputed").alias("imputed"),
                          F.sum("unavailable").alias("unavailable"), F.sum("excluded_by_validation").alias("excluded"), F.sum("unmapped").alias("unmapped")).first()
    return {"trades_total": row["total"], "trades_eligible": row["eligible"], "exact": row["exact"], "imputed": row["imputed"], "unavailable": row["unavailable"],
            "excluded_by_validation": row["excluded"], "unmapped": row["unmapped"], "coverage_pct": round((row["exact"] + row["imputed"]) / row["eligible"], PCT_DECIMALS)}


def series_summary(series: DataFrame) -> DataFrame:
    return (series.where("is_active").groupBy("provider", "series_status").agg(F.count("*").alias("series"), F.sum(F.col("is_selected").cast("int")).alias("selected"))
            .orderBy("provider", "series_status"))


def daily_range_by_type(quotes: DataFrame, mapping: DataFrame) -> DataFrame:
    """Mediana de (high - low) / close por tipo: la base para calibrar τ (ADR-009). Se publica para que la calibración se pueda revisar."""
    usable = quotes.where((F.col("close") > 0) & F.col("high").isNotNull() & F.col("low").isNotNull()).join(mapping.select("symbol", "asset_type"), "symbol")
    return (usable.withColumn("_range", (F.col("high") - F.col("low")) / F.col("close")).groupBy("asset_type")
            .agg(F.count("*").alias("quote_days"), F.round(F.median("_range"), PCT_DECIMALS).alias("median_range"), F.round(F.percentile("_range", 0.9), PCT_DECIMALS).alias("p90_range")).orderBy("asset_type"))


def deviation_summary(matched: DataFrame, mapping: DataFrame) -> DataFrame:
    """Desvío contra el benchmark y tasas por tipo. Es una foto de la comparación: la consulta por cliente es de la etapa 08."""
    usable = matched.where("is_comparable").join(mapping.select("symbol", "asset_type"), "symbol")
    return (usable.groupBy("asset_type").agg(F.count("*").alias("trades"), F.round(F.median("deviation_pct"), PCT_DECIMALS).alias("median_deviation"), F.round(F.median(F.abs("deviation_pct")), PCT_DECIMALS).alias("median_abs"),
                                              F.round(F.percentile(F.abs("deviation_pct"), 0.9), PCT_DECIMALS).alias("p90_abs"), F.round(F.avg(F.col("is_above_market").cast("int")), PCT_DECIMALS).alias("share_above"),
                                              F.round(F.avg((F.col("position_vs_range") == "ABOVE_HIGH").cast("int")), PCT_DECIMALS).alias("share_above_high"),
                                              F.round(F.avg((F.col("position_vs_range") == "BELOW_LOW").cast("int")), PCT_DECIMALS).alias("share_below_low"))
            .orderBy(F.col("trades").desc()))
