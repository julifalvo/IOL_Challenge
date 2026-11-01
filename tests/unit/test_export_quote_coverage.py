"""Tabla de cobertura publicada (spec 05, R-21)."""
from __future__ import annotations

import importlib.util
from pathlib import Path

SCRIPT = Path(__file__).parents[2] / "scripts" / "export_quote_coverage.py"
spec = importlib.util.spec_from_file_location("export_quote_coverage", SCRIPT)
export = importlib.util.module_from_spec(spec)
spec.loader.exec_module(export)


def _row(symbol, asset_type, eligible, exact=0, imputed=0, unavailable=0, excluded=0, provider="data912", status="OK", factor="1.0", bias="0.001"):
    return {"symbol": symbol, "currency_code": "ARS", "asset_type": asset_type, "provider": provider, "series_status": status, "price_scale_factor": factor, "bias_pct": bias,
            "trades_eligible": str(eligible), "matched_exact": str(exact), "matched_imputed": str(imputed), "unavailable": str(unavailable), "excluded_by_validation": str(excluded), "unmapped": "0"}


def _lines(rows):
    return export.render(rows, "2026-11-01").splitlines()


def test_summary_adds_up_and_keeps_types_without_a_source():
    rows = [_row("AAA", "CEDEAR", 90, exact=80, imputed=10), _row("OPT", "OPCION", 10, unavailable=10, provider="", status="", factor="", bias="")]
    lines = _lines(rows)
    assert "| CEDEAR | 1 | 90 | 80 | 10 | 0 | 0 | 0 | 100,0% |" in lines
    assert "| OPCION | 1 | 10 | 0 | 0 | 10 | 0 | 0 | 0,0% |" in lines
    assert "| **Total** | 2 | 100 | 80 | 10 | 10 | 0 | 0 | 90,0% |" in lines


def test_symbols_are_ordered_by_operations_with_a_stable_tie_break():
    rows = [_row("BBB", "CEDEAR", 5, exact=5), _row("AAA", "CEDEAR", 5, exact=5), _row("CCC", "CEDEAR", 9, exact=9)]
    table = [line.split(" | ")[0] for line in _lines(rows) if line.startswith(("| AAA", "| BBB", "| CCC"))]
    assert table == ["| CCC", "| AAA", "| BBB"]


def test_a_symbol_without_series_shows_dashes_and_without_eligible_does_not_divide_by_zero():
    lines = _lines([_row("OPT", "OPCION", 0, provider="", status="", factor="", bias="")])
    assert "| OPT | ARS | OPCION | — | — | — | — | 0 | 0 | 0 | 0 | 0 | — |" in lines


def test_an_empty_mapping_type_is_grouped_and_a_pipe_in_a_symbol_does_not_break_the_table():
    lines = _lines([_row("A|B", "", 1, unavailable=1)])
    assert any(line.startswith("| SIN_MAPEO | 1 |") for line in lines)
    assert any(line.startswith("| A\\|B |") for line in lines)
