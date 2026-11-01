"""Descarga cotizaciones fuera de Databricks (ADR-008, plan B). Mismo contrato que el pipeline.

Guarda en `data/quotes_raw/<provider>/<símbolo>.json` y lleva `manifest.csv`, una fila por pedido. Un símbolo ya pedido sin error no se vuelve a pedir.

Uso:
    python -m tools.quotes_fetcher.fetch --provider byma_open_data --symbols-file <csv> --from 2025-12-15 --to 2026-03-13
`<csv>` tiene las columnas `symbol` y `asset_type`, sacadas de `iol_ops.instrument_mapping`.
"""
from __future__ import annotations

import argparse
import csv
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from iol_pipeline import config, quotes

DEFAULT_ROOT = Path("data/quotes_raw")
MANIFEST_COLUMNS = ["provider", "provider_symbol", "date_from", "date_to", "http_status", "attempts", "rows_returned", "error", "requested_at"]


def provider_symbol(provider: str, symbol: str) -> str:
    return symbol + {"byma_open_data": config.BYMA_SYMBOL_SUFFIX, "yahoo": config.YAHOO_SYMBOL_SUFFIX}.get(provider, "")


def read_symbols(path: str) -> list[dict]:
    with open(path, newline="", encoding="utf-8-sig") as fh:  # PowerShell escribe BOM
        return list(csv.DictReader(fh))


def already_fetched(manifest: Path, provider: str, date_from: date, date_to: date) -> set[str]:
    if not manifest.is_file():
        return set()
    with manifest.open(newline="", encoding="utf-8") as fh:
        return {r["provider_symbol"] for r in csv.DictReader(fh) if r["provider"] == provider and r["date_from"] == date_from.isoformat()
                and r["date_to"] == date_to.isoformat() and not r["error"]}


def build_provider(name: str, symbols: list[dict], get) -> quotes.QuoteProvider:
    if name == "data912":
        return quotes.Data912Provider(get, {s["symbol"]: config.DATA912_KIND_BY_TYPE[s["asset_type"]] for s in symbols if s["asset_type"] in config.DATA912_KIND_BY_TYPE})
    return {"byma_open_data": quotes.BymaProvider, "yahoo": quotes.YahooProvider}[name](get)


def run(provider: quotes.QuoteProvider, symbols: list[str], date_from: date, date_to: date, root: Path, pause_s: float = config.QUOTE_PAUSE_S) -> dict:
    """Pide lo que falta. Un error de un símbolo queda en el manifest y se reintenta en la próxima corrida."""
    manifest = root / f"manifest_{provider.name}.csv"
    done = already_fetched(manifest, provider.name, date_from, date_to)
    pending = [s for s in symbols if s not in done]
    root.mkdir(parents=True, exist_ok=True)
    is_new = not manifest.is_file()
    counts = {"requested": 0, "with_rows": 0, "errors": 0, "skipped": len(symbols) - len(pending)}
    with manifest.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, MANIFEST_COLUMNS, lineterminator="\n")
        if is_new:
            writer.writeheader()
        for symbol in pending:
            rows, record = quotes.fetch_with_retries(provider, symbol, date_from, date_to)
            if not record["error"]:
                quotes.save_replay(root, provider.name, symbol, rows)
            writer.writerow({**record, "requested_at": datetime.now(timezone.utc).isoformat()})
            fh.flush()
            counts["requested"] += 1
            counts["with_rows"] += bool(rows)
            counts["errors"] += bool(record["error"])
            time.sleep(pause_s)
    return counts


def main() -> None:
    import requests

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provider", required=True, choices=["data912", "byma_open_data", "yahoo"])
    parser.add_argument("--symbols-file", required=True)
    parser.add_argument("--from", dest="date_from", type=date.fromisoformat, default=config.QUOTES_FROM)
    parser.add_argument("--to", dest="date_to", type=date.fromisoformat, default=config.QUOTES_TO)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    args = parser.parse_args()

    rows = read_symbols(args.symbols_file)
    provider = build_provider(args.provider, rows, requests.get)
    wanted = [s["symbol"] for s in rows if args.provider != "data912" or s["asset_type"] in config.DATA912_KIND_BY_TYPE]
    print(run(provider, [provider_symbol(args.provider, s) for s in wanted], args.date_from, args.date_to, args.root))


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
