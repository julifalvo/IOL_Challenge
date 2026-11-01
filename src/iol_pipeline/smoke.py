"""Chequeos del smoke test de entorno (spec 00).

Solo lo que se puede probar sin Databricks vive acá; los chequeos que dependen de
dbutils o de SQL de Databricks están en notebooks/00_smoke_test.py.
"""
from __future__ import annotations

from collections.abc import Callable, Collection
from datetime import datetime, timezone
from typing import Any

from iol_pipeline import config


def check_egress(urls: dict[str, str], get: Callable[..., Any], timeout_s: float,
                 inconclusive_http: Collection[int]) -> list[dict[str, str]]:
    """Prueba si cada host es alcanzable. Un código de `inconclusive_http` = INCONCLUSO (pudo responder
    un intermediario); otra respuesta HTTP = OK; error de conexión o timeout = FAIL. Registra el header
    `Server` para ver quién respondió. `get` es `requests.get` (inyectable para tests)."""
    results = []
    for name, url in urls.items():
        checked_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        try:
            response = get(url, timeout=timeout_s)
            status = "INCONCLUSO" if response.status_code in inconclusive_http else "OK"
            detail = f"HTTP {response.status_code}, Server={response.headers.get('Server', '<sin header>')}"
        except Exception as exc:  # el smoke test registra, no corta
            status, detail = "FAIL", f"{type(exc).__name__}: {str(exc)[:config.SMOKE_ERROR_MAX_CHARS]}"
        results.append({"check": f"egress:{name}", "status": status, "detail": f"{url} -> {detail}",
                        "checked_at": checked_at})
    return results


def run_status(results: list[dict[str, str]]) -> str:
    """FAILED si falló algún chequeo. INCONCLUSO no cuenta como falla: queda a la vista para revisarlo."""
    return "FAILED" if any(r["status"] == "FAIL" for r in results) else "SUCCESS"


def setup_status(contract_ok: dict[str, bool]) -> str:
    """SUCCESS solo si hay al menos un CSV en landing y todos cumplen el contrato."""
    return "SUCCESS" if contract_ok and all(contract_ok.values()) else "FAILED"
