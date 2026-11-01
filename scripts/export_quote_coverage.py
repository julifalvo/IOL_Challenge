"""Genera `docs/quote_coverage.md` desde un CSV con `SELECT * FROM iol_ops.quote_coverage` (spec 05, R-21).

Solo publica cobertura y agregados, nunca precios del proveedor (ADR-008).

Uso:
    python scripts/export_quote_coverage.py <coverage.csv> docs/quote_coverage.md --as-of 2026-11-01
"""
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path

HEADER = """# Cobertura de cotizaciones por símbolo

Generado desde `iol_ops.quote_coverage` con `scripts/export_quote_coverage.py`, corrida del {as_of} [MEDIDO].

- **Elegibles:** operaciones con `quality_status` OK o WARNING. Los tipos sin fuente siguen en el denominador.
- **Exactas:** cotización de la misma fecha efectiva. **Imputadas:** cierre previo, con hasta 3 días hábiles (ADR-009).
- **Sin dato:** no hay cotización en ese plazo. **Excluidas:** hay serie, pero sesgada o con otra escala (ADR-005).
- **Sesgo:** mediana de `precio operado / cierre / factor − 1` de la serie elegida. Se usa para validarla: no es una medida de sobreprecio.
"""


def _int(row: dict, column: str) -> int:
    return int(row[column] or 0)


def _count(value: int) -> str:
    return f"{value:,}".replace(",", ".")


def _pct(numerator: int, denominator: int) -> str:
    return f"{numerator / denominator:.1%}".replace(".", ",") if denominator else "—"


def _summary_rows(rows: list[dict]) -> list[str]:
    totals: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for row in rows:
        group = totals[row["asset_type"] or "SIN_MAPEO"]
        group["symbols"] += 1
        for column in ("trades_eligible", "matched_exact", "matched_imputed", "unavailable", "excluded_by_validation", "unmapped"):
            group[column] += _int(row, column)
    ordered = sorted(totals.items(), key=lambda item: -item[1]["trades_eligible"])
    overall: dict[str, int] = defaultdict(int)
    lines = []
    for name, group in ordered:
        lines.append(_summary_line(name, group))
        for column, value in group.items():
            overall[column] += value
    lines.append(_summary_line("**Total**", overall))
    return lines


def _summary_line(name: str, group: dict[str, int]) -> str:
    matched = group["matched_exact"] + group["matched_imputed"]
    counts = " | ".join(_count(group[c]) for c in ("symbols", "trades_eligible", "matched_exact", "matched_imputed", "unavailable", "excluded_by_validation", "unmapped"))
    return f"| {name} | {counts} | {_pct(matched, group['trades_eligible'])} |"


def render(rows: list[dict], as_of: str) -> str:
    """Resumen por tipo y tabla completa, de más a menos operaciones. Desempate por símbolo: dos corridas iguales dan el mismo archivo."""
    ordered = sorted(rows, key=lambda r: (-_int(r, "trades_eligible"), r["symbol"]))
    lines = [HEADER.format(as_of=as_of), "## Resumen por tipo de activo\n",
             "| Tipo | Símbolos | Elegibles | Exactas | Imputadas | Sin dato | Excluidas | Sin mapeo | Cobertura |", "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
             *_summary_rows(rows), "", "## Por símbolo\n",
             "| Símbolo | Moneda | Tipo | Proveedor | Estado | Factor | Sesgo | Elegibles | Exactas | Imputadas | Sin dato | Excluidas | Cobertura |", "|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for row in ordered:
        factor = f"{float(row['price_scale_factor']):.4g}".replace(".", ",") if row["price_scale_factor"] else "—"
        bias = f"{float(row['bias_pct']):+.2%}".replace(".", ",") if row["bias_pct"] else "—"
        symbol = row["symbol"].replace("|", "\\|")
        counts = " | ".join(_count(_int(row, c)) for c in ("trades_eligible", "matched_exact", "matched_imputed", "unavailable", "excluded_by_validation"))
        coverage = _pct(_int(row, "matched_exact") + _int(row, "matched_imputed"), _int(row, "trades_eligible"))
        lines.append(f"| {symbol} | {row['currency_code']} | {row['asset_type']} | {row['provider'] or '—'} | {row['series_status'] or '—'} | {factor} | {bias} | {counts} | {coverage} |")
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("coverage_csv")
    parser.add_argument("output")
    parser.add_argument("--as-of", required=True)
    args = parser.parse_args()
    with open(args.coverage_csv, newline="", encoding="utf-8-sig") as fh:
        rows = list(csv.DictReader(fh))
    Path(args.output).write_text(render(rows, args.as_of), encoding="utf-8", newline="\n")


if __name__ == "__main__":
    main()
