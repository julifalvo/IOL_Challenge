"""Helpers de io: nombres de tabla, schemas y MERGE Delta idempotente (ADR-002)."""
from __future__ import annotations

import pytest

from iol_pipeline import config, io

SCHEMA = "trade_id string, record_hash string, payload string"


def test_table_name_local_has_no_catalog(monkeypatch):
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    assert io.table_name("bronze", "trades_raw") == "iol_bronze.trades_raw"


def test_table_name_databricks_uses_catalog(monkeypatch):
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "client.4.0")
    assert io.table_name("gold", "fact_trades") == f"{config.CATALOG}.iol_gold.fact_trades"
    assert io.table_name("ops", "gold_status", catalog="dev") == "dev.iol_ops.gold_status"


def test_landing_path():
    assert io.landing_path(catalog="workspace") == "/Volumes/workspace/iol_bronze/landing"
    assert io.landing_path("trades", catalog="workspace") == "/Volumes/workspace/iol_bronze/landing/trades"
    with pytest.raises(ValueError):
        io.landing_path("otra")


def test_ensure_schemas_is_idempotent_locally(spark, monkeypatch):
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    first = io.ensure_schemas(spark)
    second = io.ensure_schemas(spark)
    assert first == second == list(config.SCHEMAS.values())
    existing = {r[0] for r in spark.sql("SHOW DATABASES").collect()}
    assert set(config.SCHEMAS.values()) <= existing


def test_merge_creates_table_and_rerun_inserts_nothing(spark, schema):
    target = f"{schema}.trades_raw"
    df = spark.createDataFrame([("T1", "h1", "a"), ("T2", "h2", "b")], SCHEMA)

    first = io.write_delta_merge(spark, df, target, ["trade_id", "record_hash"])
    second = io.write_delta_merge(spark, df, target, ["trade_id", "record_hash"])

    assert first == {"rows_source": 2, "rows_inserted": 2, "rows_already_present": 0}
    assert second == {"rows_source": 2, "rows_inserted": 0, "rows_already_present": 2}
    assert spark.table(target).count() == 2
    assert spark.sql(f"DESCRIBE DETAIL {target}").first()["format"] == "delta"


def test_merge_preserves_conflicting_version(spark, schema):
    target = f"{schema}.trades_raw"
    keys = ["trade_id", "record_hash"]
    io.write_delta_merge(spark, spark.createDataFrame([("T1", "h1", "a")], SCHEMA), target, keys)

    result = io.write_delta_merge(spark, spark.createDataFrame([("T1", "h1_fix", "a2")], SCHEMA), target, keys)

    assert result["rows_inserted"] == 1  # mismo ID, distinto contenido: se agrega, no se pisa
    assert sorted(r.record_hash for r in spark.table(target).collect()) == ["h1", "h1_fix"]


def test_merge_null_key_is_not_reinserted(spark, schema):
    target = f"{schema}.with_null_key"
    df = spark.createDataFrame([(None, "h1", "a")], SCHEMA)

    io.write_delta_merge(spark, df, target, ["trade_id", "record_hash"])
    rerun = io.write_delta_merge(spark, df, target, ["trade_id", "record_hash"])

    assert rerun["rows_inserted"] == 0
    assert spark.table(target).count() == 1


def test_merge_rejects_duplicate_keys_in_source(spark, schema):
    target = f"{schema}.dupes"
    df = spark.createDataFrame([("T1", "h1", "a"), ("T1", "h1", "b")], SCHEMA)

    with pytest.raises(ValueError, match="repiten la clave"):
        io.write_delta_merge(spark, df, target, ["trade_id", "record_hash"])
    assert not spark.catalog.tableExists(target)


def test_merge_rejects_columns_the_target_does_not_have(spark, schema):
    target = f"{schema}.narrow"
    io.write_delta_merge(spark, spark.createDataFrame([("T1", "h1", "a")], SCHEMA), target, ["trade_id"])
    wider = spark.createDataFrame([("T2", "h2", "b", "nueva")], SCHEMA + ", extra string")

    with pytest.raises(ValueError, match="extra"):
        io.write_delta_merge(spark, wider, target, ["trade_id"])

    assert spark.table(target).count() == 1
    assert "extra" not in spark.table(target).columns


def test_merge_validates_keys(spark, schema):
    df = spark.createDataFrame([("T1", "h1", "a")], SCHEMA)
    with pytest.raises(ValueError, match="ausentes"):
        io.write_delta_merge(spark, df, f"{schema}.x", ["no_existe"])
    with pytest.raises(ValueError, match="al menos una clave"):
        io.write_delta_merge(spark, df, f"{schema}.x", [])
