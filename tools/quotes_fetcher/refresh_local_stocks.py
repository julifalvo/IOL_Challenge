"""Actualiza src/iol_pipeline/mapping/local_stocks.csv con las acciones locales que publica data912 (ADR-004).

Solo guarda tickers. Es una foto: un símbolo dado de baja desde entonces no aparece, y sigue a la IA.

Uso:
    python -m tools.quotes_fetcher.refresh_local_stocks
"""
from __future__ import annotations

import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from iol_pipeline import config

URL = config.DATA912_LIVE_URL.format(name="arg_stocks")
TARGET = Path(__file__).resolve().parents[2] / "src" / "iol_pipeline" / "mapping" / "local_stocks.csv"


def main() -> None:
    response = requests.get(URL, timeout=config.HTTP_TIMEOUT_S)
    response.raise_for_status()
    symbols = sorted({row["symbol"] for row in response.json()})
    TARGET.write_text("\n".join(["symbol", *symbols]) + "\n", encoding="utf-8", newline="\n")
    print(f"{len(symbols)} símbolos en {TARGET}")


if __name__ == "__main__":
    main()
