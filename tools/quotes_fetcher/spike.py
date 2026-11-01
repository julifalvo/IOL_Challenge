"""Spike de proveedores de cotizaciones (spec 05, Paso 0; resultados en ADR-008).

Mide cobertura y escala en el rango de cotizaciones. No guarda datos crudos: solo conteos y medianas,
porque los términos de uso de los proveedores no están evaluados.

Uso local:
    python -m tools.quotes_fetcher.spike data/raw/data-set-challenge-5-.csv
Las columnas de relación salen del CSV; sin argumento se omiten. Desde Databricks: notebooks/00_quotes_spike.py.
"""
from __future__ import annotations

import statistics
import sys
import time
from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from iol_pipeline import config

SYMBOLS = [
    "AL30", "AL30D", "AL30C", "AAPL", "AAPLD", "YPFD", "T30J6", "YM34D",  # spec 05
    "MSFT", "MSFTD", "SPY", "SPYD", "NVDA", "MELI", "AMZN", "GOOGL",  # CEDEARs: con un solo símbolo no se ve el sesgo (ADR-008)
]
DATA912_KINDS = ("bonds", "cedears", "stocks")
HEADERS = {"User-Agent": "Mozilla/5.0"}
MARKET_TZ = timezone(timedelta(hours=-3))  # Argentina no tiene horario de verano
PAUSE_S = 1.5


def _in_range(day: date, start: date, end: date) -> bool:
    return start <= day <= end


def _epoch(day: date) -> int:
    return int(datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp())


def parse_data912(payload: Any, start: date, end: date) -> list[dict]:
    """Lista de filas. Un símbolo que no es de ese tipo de instrumento devuelve otra cosa, con HTTP 200."""
    if not isinstance(payload, list):
        return []
    rows = [{"date": date.fromisoformat(r["date"]), "open": r.get("o"), "high": r.get("h"), "low": r.get("l"), "close": r.get("c")}
            for r in payload]
    return [r for r in rows if _in_range(r["date"], start, end)]


def parse_byma(payload: dict, start: date, end: date) -> list[dict]:
    """Series paralelas con epoch: {"s": "ok", "t": [...], "o": [...], ...}. Sin datos: {"s": "no_data"}."""
    if payload.get("s") != "ok":
        return []
    rows = [{"date": datetime.fromtimestamp(t, MARKET_TZ).date(), "open": o, "high": h, "low": l, "close": c}
            for t, o, h, l, c in zip(payload["t"], payload["o"], payload["h"], payload["l"], payload["c"])]
    return [r for r in rows if _in_range(r["date"], start, end)]


def parse_yahoo(payload: dict, start: date, end: date) -> list[dict]:
    result = (payload.get("chart", {}).get("result") or [None])[0]
    if not result or "timestamp" not in result:
        return []
    quote = result["indicators"]["quote"][0]
    rows = [{"date": datetime.fromtimestamp(t, MARKET_TZ).date(), "open": o, "high": h, "low": l, "close": c}
            for t, o, h, l, c in zip(result["timestamp"], quote["open"], quote["high"], quote["low"], quote["close"])]
    return [r for r in rows if _in_range(r["date"], start, end) and r["close"] is not None]


def fetch_data912(symbol: str, get: Callable[..., Any], start: date, end: date) -> tuple[str, list[dict]]:
    """data912 separa el histórico por tipo de instrumento: prueba cada uno hasta encontrar filas."""
    statuses = []
    for kind in DATA912_KINDS:
        response = get(f"https://data912.com/historical/{kind}/{symbol}", timeout=config.HTTP_TIMEOUT_S, headers=HEADERS)
        statuses.append(f"{kind}:{response.status_code}")
        time.sleep(PAUSE_S)
        if response.status_code == 200:
            rows = parse_data912(response.json(), start, end)
            if rows:
                return f"{kind}:200", rows
    return " ".join(statuses), []


def fetch_byma(symbol: str, get: Callable[..., Any], start: date, end: date) -> tuple[str, list[dict]]:
    response = get("https://open.bymadata.com.ar/vanoms-be-core/rest/api/bymadata/free/chart/historical-series/history",
                   params={"symbol": f"{symbol} 24HS", "resolution": "D", "from": _epoch(start), "to": _epoch(end + timedelta(days=1))},
                   timeout=config.HTTP_TIMEOUT_S, headers=HEADERS)
    time.sleep(PAUSE_S)
    return str(response.status_code), parse_byma(response.json(), start, end) if response.status_code == 200 else []


def fetch_yahoo(symbol: str, get: Callable[..., Any], start: date, end: date) -> tuple[str, list[dict]]:
    response = get(f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}.BA",
                   params={"period1": _epoch(start), "period2": _epoch(end + timedelta(days=1)), "interval": "1d"},
                   timeout=config.HTTP_TIMEOUT_S, headers=HEADERS)
    time.sleep(PAUSE_S)
    return str(response.status_code), parse_yahoo(response.json(), start, end) if response.status_code == 200 else []


PROVIDERS = {"data912": fetch_data912, "byma_open_data": fetch_byma, "yahoo": fetch_yahoo}


def same_day_ratio(rows: list[dict], trade_medians: dict[date, float]) -> dict:
    """Mediana diaria del precio de operación / cierre del mismo día (spec 05). Con ventanas distintas, un activo
    que se movió en el período da una relación falsa."""
    ratios = [trade_medians[r["date"]] / r["close"] for r in rows if r["close"] and r["date"] in trade_medians]
    if len(ratios) < 2:
        return {"ratio_p50": None, "ratio_p10": None, "ratio_p90": None, "ratio_days": len(ratios)}
    deciles = statistics.quantiles(ratios, n=10)
    return {"ratio_p50": statistics.median(ratios), "ratio_p10": deciles[0], "ratio_p90": deciles[8], "ratio_days": len(ratios)}


def run_spike(get: Callable[..., Any], start: date = config.QUOTES_FROM, end: date = config.QUOTES_TO,
              trade_medians: dict[str, dict[date, float]] | None = None) -> list[dict]:
    """Una fila por proveedor y símbolo. Un error de un símbolo se registra y no corta el resto.
    `trade_medians`: símbolo -> fecha -> mediana del precio de operación."""
    results = []
    for provider, fetch in PROVIDERS.items():
        for symbol in SYMBOLS:
            try:
                http, rows = fetch(symbol, get, start, end)
                error = ""
            except Exception as exc:
                http, rows, error = "", [], f"{type(exc).__name__}: {str(exc)[:config.SMOKE_ERROR_MAX_CHARS]}"
            closes = [r["close"] for r in rows if r["close"] is not None]
            results.append({
                "provider": provider, "symbol": symbol, "http": http, "rows": len(rows),
                "first": min((r["date"] for r in rows), default=None), "last": max((r["date"] for r in rows), default=None),
                "has_ohlc": bool(rows) and all(r["open"] and r["high"] and r["low"] for r in rows),
                "median_close": statistics.median(closes) if closes else None, "error": error,
                **same_day_ratio(rows, (trade_medians or {}).get(symbol, {})),
            })
    return results


def main(csv_path: str | None) -> None:
    import requests

    medians: dict[str, dict[date, float]] = {}
    if csv_path:
        import pandas as pd

        trades = pd.read_csv(csv_path, usecols=["fecha", "simbolo_titulo", "precio"])
        trades = trades[trades["simbolo_titulo"].isin(SYMBOLS)]
        trades["day"] = pd.to_datetime(trades["fecha"], utc=True).dt.tz_convert(config.TZ_MARKET).dt.date
        for (symbol, day), median in trades.groupby(["simbolo_titulo", "day"])["precio"].median().items():
            medians.setdefault(symbol, {})[day] = median

    def fmt(value: float | None) -> str:
        return f"{value:.4f}" if value else "—"

    print("| Proveedor | Símbolo | HTTP | Filas | Desde | Hasta | OHLC | Mediana cierre | Operación/cierre p50 | p10 | p90 | Días | Error |")
    print("|---|---|---|---:|---|---|---|---:|---:|---:|---:|---:|---|")
    for r in run_spike(requests.get, trade_medians=medians):
        print(f"| {r['provider']} | {r['symbol']} | {r['http']} | {r['rows']} | {r['first'] or '—'} | {r['last'] or '—'} | "
              f"{'sí' if r['has_ohlc'] else 'no'} | {fmt(r['median_close'])} | {fmt(r['ratio_p50'])} | {fmt(r['ratio_p10'])} | "
              f"{fmt(r['ratio_p90'])} | {r['ratio_days']} | {r['error']} |")


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main(sys.argv[1] if len(sys.argv) > 1 else None)
