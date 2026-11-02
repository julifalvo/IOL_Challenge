"""Fixtures compartidas: SparkSession local con Delta Lake.

Corre sin cluster. Requiere Java 17+ y `pip install -r requirements-dev.txt`.
Si Spark+Delta no arranca, los tests que usan `spark` FALLAN (no se saltean):
un entorno roto no puede pasar por verde. Para saltearlos a propósito:
IOL_SKIP_SPARK_TESTS=1.
"""
from __future__ import annotations

import os
import shutil
import tempfile
from collections.abc import Callable
from pathlib import Path

import pytest

SKIP_SPARK_ENV = "IOL_SKIP_SPARK_TESTS"
FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _startup_help(exc: BaseException) -> str:
    lines = [line.strip() for line in str(exc).splitlines() if line.strip()]
    # En Py4JJavaError la primera línea es genérica; la causa está en la primera "...Exception: ...".
    detail = next((line for line in lines if "Exception:" in line), lines[0] if lines else "")[:300]
    hadoop_home = os.environ.get("HADOOP_HOME")
    winutils = Path(hadoop_home or ".", "bin", "winutils.exe")
    winutils_state = "encontrado" if hadoop_home and winutils.is_file() else "NO encontrado"
    return (
        f"Spark+Delta local no arrancó: {type(exc).__name__}: {detail}\n"
        "Revisar (ver docs/entorno_local.md):\n"
        f"  - JAVA_HOME={os.environ.get('JAVA_HOME', '<sin definir>')} -> debe apuntar a un JDK 17\n"
        f"  - HADOOP_HOME={hadoop_home or '<sin definir>'} -> (Windows) debe contener bin\\winutils.exe "
        f"y bin\\hadoop.dll; winutils.exe: {winutils_state}\n"
        "  - PATH debe incluir %HADOOP_HOME%\\bin (para cargar hadoop.dll)\n"
        f"  - PYSPARK_PYTHON={os.environ.get('PYSPARK_PYTHON', '<sin definir>')} -> debe ser el python del .venv (3.11)\n"
        "  - Primera corrida: acceso a Maven Central para bajar los jars de Delta\n"
        f"Para saltear los tests de Spark a propósito: {SKIP_SPARK_ENV}=1"
    )


def require_spark(start: Callable[[], object]) -> object:
    """Arranca Spark con `start` o corta el test: skip solo si se pidió, si no fail."""
    if os.environ.get(SKIP_SPARK_ENV) == "1":
        pytest.skip(f"Tests de Spark salteados deliberadamente ({SKIP_SPARK_ENV}=1).")
    try:
        return start()
    except Exception as exc:
        pytest.fail(_startup_help(exc), pytrace=False)


def _start_spark(warehouse: Path):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[2]")
        .appName("iol-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
    )
    # La primera vez descarga los jars de Delta desde Maven Central (~/.ivy2).
    return configure_spark_with_delta_pip(builder).getOrCreate()


@pytest.fixture(scope="session")
def spark():
    warehouse = Path(tempfile.mkdtemp(prefix="iol_wh_"))
    try:
        session = require_spark(lambda: _start_spark(warehouse))
    except BaseException:
        shutil.rmtree(warehouse, ignore_errors=True)
        raise
    session.sparkContext.setLogLevel("ERROR")  # solo en tests locales; no usar sparkContext en src/
    yield session
    session.stop()
    shutil.rmtree(warehouse, ignore_errors=True)


@pytest.fixture(scope="session")
def trades_small_csv() -> Path:
    """CSV sintético con los casos de borde del dataset (ver tests/fixtures/README.md)."""
    return FIXTURES_DIR / "trades_small.csv"


@pytest.fixture()
def schema(spark, request):
    """Schema aislado por test (simula iol_bronze/silver/gold/ops)."""
    name = "t_" + request.node.name.lower().replace("[", "_").replace("]", "_")[:40]
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {name}")
    yield name
    spark.sql(f"DROP DATABASE IF EXISTS {name} CASCADE")
