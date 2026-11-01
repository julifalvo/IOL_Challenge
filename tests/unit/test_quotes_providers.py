"""Proveedores, reintentos y fetcher: sin red ni Spark (spec 05)."""
from __future__ import annotations

import csv
from datetime import date
from types import SimpleNamespace

import pytest

from iol_pipeline import config, quotes
from tools.quotes_fetcher import fetch

START, END = date(2026, 1, 1), date(2026, 1, 31)
ROW = {"date": date(2026, 1, 5), "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 10}


def _response(status=200, payload=None):
    return SimpleNamespace(status_code=status, json=lambda: payload)


class Flaky:
    """Falla las primeras `failures` veces con el error indicado."""

    name = "flaky"

    def __init__(self, failures, error):
        self.failures, self.error, self.calls = failures, error, 0

    def fetch_daily(self, provider_symbol, date_from, date_to):
        self.calls += 1
        if self.calls <= self.failures:
            raise self.error
        return [ROW]


def test_parsers_keep_volume_and_drop_rows_without_close():
    payload = [{"date": "2026-01-05", "o": 1, "h": 2, "l": 1, "c": 1.5, "v": 9}, {"date": "2026-01-06", "o": 1, "h": 2, "l": 1, "c": None, "v": 9}, {"date": "2025-01-06", "c": 1}]
    rows = quotes.parse_data912(payload, START, END)
    assert [r["date"] for r in rows] == [date(2026, 1, 5)] and rows[0]["volume"] == 9


def test_byma_without_volume_still_parses():
    payload = {"s": "ok", "t": [1767582000], "o": [1], "h": [2], "l": [1], "c": [1.5]}
    assert quotes.parse_byma(payload, START, END)[0]["volume"] is None


@pytest.mark.parametrize(("status", "retryable"), [(429, True), (500, True), (503, True), (400, False), (401, False)])
def test_http_errors_say_if_a_retry_helps(status, retryable):
    with pytest.raises(quotes.ProviderError) as info:
        quotes.BymaProvider(lambda *a, **k: _response(status)).fetch_daily("AL30 24HS", START, END)
    assert info.value.status == status and info.value.retryable is retryable


def test_a_connection_error_is_retryable():
    def get(*args, **kwargs):
        raise TimeoutError("sin respuesta")

    with pytest.raises(quotes.ProviderError) as info:
        quotes.YahooProvider(get).fetch_daily("AAPL.BA", START, END)
    assert info.value.retryable


def test_a_response_that_is_not_json_is_an_error():
    def no_json():
        raise ValueError("no")

    with pytest.raises(quotes.ProviderError):
        quotes.BymaProvider(lambda *a, **k: SimpleNamespace(status_code=200, json=no_json)).fetch_daily("AL30 24HS", START, END)


@pytest.mark.parametrize("provider", [quotes.Data912Provider(lambda *a, **k: _response(404), {"X": "bonds"}), quotes.YahooProvider(lambda *a, **k: _response(404))])
def test_404_means_the_provider_does_not_have_the_symbol(provider):
    assert provider.fetch_daily("X", START, END) == []


def test_data912_asks_where_the_symbol_type_says():
    urls = []

    def get(url, **kwargs):
        urls.append(url)
        return _response(200, [])

    quotes.Data912Provider(get, {"AL30": "bonds"}).fetch_daily("AL30", START, END)
    assert urls == ["https://data912.com/historical/bonds/AL30"]


def test_retries_with_growing_wait_and_then_succeeds():
    provider, waits = Flaky(2, quotes.ProviderError("HTTP 429", 429, True)), []
    rows, record = quotes.fetch_with_retries(provider, "X", START, END, sleep=waits.append)
    assert rows == [ROW]
    assert record["attempts"] == 3 and record["http_status"] == 200 and record["error"] is None and record["rows_returned"] == 1
    assert waits[0] < waits[1]


def test_gives_up_after_the_limit_without_raising():
    provider = Flaky(99, quotes.ProviderError("HTTP 500", 500, True))
    rows, record = quotes.fetch_with_retries(provider, "X", START, END, sleep=lambda s: None)
    assert rows == [] and provider.calls == config.HTTP_MAX_RETRIES
    assert record["http_status"] == 500 and record["error"] == "HTTP 500"


def test_does_not_retry_what_will_not_change():
    provider = Flaky(99, quotes.ProviderError("HTTP 400", 400, False))
    _, record = quotes.fetch_with_retries(provider, "X", START, END, sleep=lambda s: pytest.fail("no tenía que esperar"))
    assert provider.calls == 1 and record["attempts"] == 1


def test_replay_returns_what_was_saved_in_range(tmp_path):
    quotes.save_replay(tmp_path, "byma_open_data", "AL30 24HS", [ROW, {**ROW, "date": date(2026, 3, 1)}])
    provider = quotes.FileReplayProvider("byma_open_data", tmp_path)
    assert provider.fetch_daily("AL30 24HS", START, END) == [ROW]


def test_replay_without_a_file_behaves_like_a_provider_down(tmp_path):
    rows, record = quotes.fetch_with_retries(quotes.FileReplayProvider("x", tmp_path), "#MAV1", START, END, sleep=lambda s: None)
    assert rows == [] and "Sin archivo" in record["error"] and record["attempts"] == 1


def test_file_names_are_valid_on_windows_and_volumes(tmp_path):
    path = quotes.replay_path(tmp_path, "byma_open_data", "#MAV/1 24HS")
    assert path.name == "_MAV_1_24HS.json"


def test_provider_symbol_follows_the_mapping_format():
    assert quotes.provider_symbol("byma_open_data", "AL30") == f"AL30{config.BYMA_SYMBOL_SUFFIX}"
    assert quotes.provider_symbol("yahoo", "AAPL") == f"AAPL{config.YAHOO_SYMBOL_SUFFIX}"
    assert quotes.provider_symbol("data912", "AL30") == "AL30"


def test_fetcher_keeps_going_after_an_error_and_only_asks_for_what_is_missing(tmp_path):
    class OneDown:
        name = "prov"

        def __init__(self):
            self.asked = []

        def fetch_daily(self, symbol, date_from, date_to):
            self.asked.append(symbol)
            if symbol == "MALO" and len(self.asked) < 4:
                raise quotes.ProviderError("HTTP 400", 400)
            return [ROW]

    provider = OneDown()
    first = fetch.run(provider, ["A", "MALO", "B"], START, END, tmp_path, pause_s=0)
    assert first == {"requested": 3, "with_rows": 2, "errors": 1, "skipped": 0}
    assert (tmp_path / "prov" / "B.json").is_file() and not (tmp_path / "prov" / "MALO.json").exists()

    second = fetch.run(provider, ["A", "MALO", "B"], START, END, tmp_path, pause_s=0)
    assert second == {"requested": 1, "with_rows": 1, "errors": 0, "skipped": 2}
    assert provider.asked == ["A", "MALO", "B", "MALO"]
    with (tmp_path / "manifest_prov.csv").open(newline="", encoding="utf-8") as fh:
        assert len(list(csv.DictReader(fh))) == 4


def test_an_unexpected_response_is_recorded_and_does_not_raise():
    """Un payload con otra forma no puede detener el lote, y reintentar no lo cambia."""

    class Broken:
        name = "x"
        calls = 0

        def fetch_daily(self, provider_symbol, date_from, date_to):
            Broken.calls += 1
            raise KeyError("close")

    rows, record = quotes.fetch_with_retries(Broken(), "AAA", date(2026, 1, 1), date(2026, 1, 2), sleep=lambda _: None)
    assert rows == []
    assert (record["attempts"], record["http_status"], Broken.calls) == (1, None, 1)
    assert record["error"].startswith("KeyError")
