"""Parseo de los proveedores del spike, sin red."""
from __future__ import annotations

from datetime import date
from types import SimpleNamespace

from tools.quotes_fetcher import spike

START, END = date(2026, 1, 1), date(2026, 1, 31)


def test_data912_keeps_only_the_range():
    payload = [{"date": "2025-12-31", "o": 1, "h": 1, "l": 1, "c": 1}, {"date": "2026-01-05", "o": 2, "h": 3, "l": 1, "c": 2}]
    rows = spike.parse_data912(payload, START, END)
    assert [r["date"] for r in rows] == [date(2026, 1, 5)]
    assert rows[0]["close"] == 2


def test_data912_ignores_a_response_that_is_not_a_list():
    assert spike.parse_data912({"detail": "no encontrado"}, START, END) == []


def test_byma_uses_local_date_and_handles_no_data():
    # 1767582000 = 2026-01-05 03:00 UTC = 00:00 en Buenos Aires
    payload = {"s": "ok", "t": [1767582000], "o": [1.0], "h": [2.0], "l": [0.5], "c": [1.5]}
    assert spike.parse_byma(payload, START, END)[0]["date"] == date(2026, 1, 5)
    assert spike.parse_byma({"s": "no_data", "t": []}, START, END) == []


def test_yahoo_skips_empty_closes_and_missing_result():
    quote = {"open": [1.0, None], "high": [2.0, None], "low": [0.5, None], "close": [1.5, None]}
    payload = {"chart": {"result": [{"timestamp": [1767582000, 1767668400], "indicators": {"quote": [quote]}}]}}
    assert len(spike.parse_yahoo(payload, START, END)) == 1
    assert spike.parse_yahoo({"chart": {"result": None}}, START, END) == []


def test_same_day_ratio_ignores_days_outside_the_other_series():
    rows = [{"date": date(2026, 1, 5), "close": 100.0}, {"date": date(2026, 1, 6), "close": 200.0}, {"date": date(2026, 1, 7), "close": 50.0}]
    # Sube el doble en el período: comparar medianas de ventanas distintas daría una relación falsa.
    trade = {date(2026, 1, 5): 100.0, date(2026, 1, 6): 200.0, date(2026, 1, 20): 999.0}
    result = spike.same_day_ratio(rows, trade)
    assert result["ratio_p50"] == 1.0
    assert result["ratio_days"] == 2


def test_same_day_ratio_without_enough_days_is_empty():
    assert spike.same_day_ratio([{"date": date(2026, 1, 5), "close": 100.0}], {date(2026, 1, 5): 100.0})["ratio_p50"] is None


def test_a_provider_error_does_not_stop_the_rest(monkeypatch):
    monkeypatch.setattr(spike, "PAUSE_S", 0)

    def get(url, **kwargs):
        if "yahoo" in url:
            raise ConnectionError("bloqueado")
        return SimpleNamespace(status_code=404, json=lambda: {})

    results = spike.run_spike(get, START, END)
    assert len(results) == len(spike.SYMBOLS) * len(spike.PROVIDERS)
    assert all("ConnectionError" in r["error"] for r in results if r["provider"] == "yahoo")
    assert all(r["rows"] == 0 and r["median_close"] is None for r in results)
