"""Chequeo de egress del smoke test, sin red (requests.get inyectado)."""
from __future__ import annotations

from types import SimpleNamespace

from iol_pipeline import config, smoke


def test_any_http_response_counts_as_reachable():
    results = smoke.check_egress({"p": "https://x.test"}, lambda url, timeout: SimpleNamespace(status_code=403), 1)
    assert results[0]["check"] == "egress:p"
    assert results[0]["status"] == "OK"
    assert "HTTP 403" in results[0]["detail"]


def test_connection_error_is_recorded_not_raised():
    def blocked(url, timeout):
        raise ConnectionError("Name or service not known")

    results = smoke.check_egress({"a": "https://a.test", "b": "https://b.test"}, blocked, 1)
    assert [r["status"] for r in results] == ["FAIL", "FAIL"]
    assert "ConnectionError" in results[0]["detail"]


def test_timeout_is_passed_to_get():
    seen = {}

    def get(url, timeout):
        seen["timeout"] = timeout
        return SimpleNamespace(status_code=200)

    smoke.check_egress({"p": "https://x.test"}, get, 7)
    assert seen["timeout"] == 7


def test_candidate_providers_of_adr_008_are_checked():
    assert set(config.SMOKE_EGRESS_URLS) == {"iol_api", "byma_open_data", "data912", "yahoo_finance"}
    assert all(u.startswith("https://") for u in config.SMOKE_EGRESS_URLS.values())
