"""Chequeo de egress del smoke test, sin red (requests.get inyectado)."""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from iol_pipeline import config, smoke


def _answer(status_code, headers=None):
    return lambda url, timeout: SimpleNamespace(status_code=status_code, headers=headers or {})


def _check(get, urls=None):
    return smoke.check_egress(urls or {"p": "https://x.test"}, get, 1, config.SMOKE_EGRESS_INCONCLUSIVE_HTTP)


@pytest.mark.parametrize("code", [200, 404])
def test_other_http_responses_count_as_reachable(code):
    result = _check(_answer(code, {"Server": "nginx"}))[0]
    assert result["check"] == "egress:p"
    assert result["status"] == "OK"
    assert f"HTTP {code}, Server=nginx" in result["detail"]


@pytest.mark.parametrize("code", [403, 407, 429])
def test_codes_an_intermediary_could_return_are_inconclusive(code):
    result = _check(_answer(code))[0]
    assert result["status"] == "INCONCLUSO"
    assert f"HTTP {code}, Server=<sin header>" in result["detail"]


def test_connection_error_is_recorded_not_raised():
    def blocked(url, timeout):
        raise ConnectionError("Name or service not known")

    results = _check(blocked, {"a": "https://a.test", "b": "https://b.test"})
    assert [r["status"] for r in results] == ["FAIL", "FAIL"]
    assert "ConnectionError" in results[0]["detail"]


def test_timeout_is_passed_to_get():
    seen = {}

    def get(url, timeout):
        seen["timeout"] = timeout
        return SimpleNamespace(status_code=200, headers={})

    smoke.check_egress({"p": "https://x.test"}, get, 7, config.SMOKE_EGRESS_INCONCLUSIVE_HTTP)
    assert seen["timeout"] == 7


def test_candidate_providers_of_adr_008_are_checked():
    assert set(config.SMOKE_EGRESS_URLS) == {"iol_api", "byma_open_data", "data912", "yahoo_finance"}
    assert all(u.startswith("https://") for u in config.SMOKE_EGRESS_URLS.values())
