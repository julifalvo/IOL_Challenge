"""Chequeos del smoke test de entorno (spec 00).

Solo lo que se puede probar sin Databricks vive acá; los chequeos que dependen de
dbutils o de SQL de Databricks están en notebooks/00_smoke_test.py.
"""
from __future__ import annotations

from collections.abc import Callable, Collection
from datetime import datetime, timezone
from typing import Any


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
            status, detail = "FAIL", f"{type(exc).__name__}: {str(exc)[:200]}"
        results.append({"check": f"egress:{name}", "status": status, "detail": f"{url} -> {detail}",
                        "checked_at": checked_at})
    return results
