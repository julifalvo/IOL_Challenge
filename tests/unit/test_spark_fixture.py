"""La fixture `spark` no puede pasar por verde con el entorno roto.

Se prueba `require_spark` (la decisión skip/fail de la fixture) con un arranque
simulado, sin levantar Spark.
"""
from __future__ import annotations

import pytest

from tests.conftest import SKIP_SPARK_ENV, require_spark


def _broken_start():
    raise RuntimeError("Java gateway process exited (simulado)")


def _must_not_start():
    raise AssertionError("con el skip deliberado no se debe intentar arrancar Spark")


@pytest.mark.parametrize("value", [None, "0", "true", ""])
def test_startup_failure_fails_instead_of_skipping(monkeypatch, value):
    if value is None:
        monkeypatch.delenv(SKIP_SPARK_ENV, raising=False)
    else:
        monkeypatch.setenv(SKIP_SPARK_ENV, value)

    with pytest.raises(pytest.fail.Exception) as info:  # Failed no es Skipped: un skip no lo satisface
        require_spark(_broken_start)

    msg = str(info.value)
    for hint in ("JAVA_HOME", "HADOOP_HOME", "PYSPARK_PYTHON", "winutils", SKIP_SPARK_ENV):
        assert hint in msg
    assert "Java gateway process exited (simulado)" in msg


def test_skip_only_when_requested_and_says_deliberate(monkeypatch):
    monkeypatch.setenv(SKIP_SPARK_ENV, "1")

    with pytest.raises(pytest.skip.Exception, match="deliberadamente"):
        require_spark(_must_not_start)


def test_successful_start_is_returned(monkeypatch):
    monkeypatch.delenv(SKIP_SPARK_ENV, raising=False)
    sentinel = object()

    assert require_spark(lambda: sentinel) is sentinel
