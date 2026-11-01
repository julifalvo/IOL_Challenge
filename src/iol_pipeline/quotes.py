"""Cotizaciones de mercado (spec 05, ADR-005, ADR-008, ADR-009)."""
from __future__ import annotations

import json
import random
import re
import time
from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Protocol

from iol_pipeline import config

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
    return rows, {"provider": provider.name, "provider_symbol": provider_symbol, "date_from": date_from, "date_to": date_to, "http_status": status,
                  "attempts": attempts, "rows_returned": len(rows), "error": error}
