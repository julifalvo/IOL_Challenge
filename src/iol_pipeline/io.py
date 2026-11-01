"""Nombres de tabla, schemas y escritura Delta idempotente.

El mismo código corre en Databricks (Unity Catalog: catalog.schema.tabla) y en
Spark local con delta-spark (spark_catalog: schema.tabla).
"""
from __future__ import annotations

import uuid

from pyspark.sql import DataFrame, SparkSession

from iol_pipeline import config


def _catalog_prefix(catalog: str | None) -> str:
    return f"{catalog or config.CATALOG}." if config.is_databricks() else ""


def table_name(layer: str, name: str, catalog: str | None = None) -> str:
    """`iol_<layer>.<name>`, con catálogo adelante solo en Databricks."""
    return f"{_catalog_prefix(catalog)}{config.SCHEMAS[layer]}.{name}"


def landing_path(subdir: str = "", catalog: str | None = None) -> str:
    """Ruta del Volume de landing en Databricks: /Volumes/<catalog>/iol_bronze/landing[/<subdir>]."""
    if subdir and subdir not in config.LANDING_SUBDIRS:
        raise ValueError(f"Subcarpeta de landing desconocida: {subdir!r} (válidas: {config.LANDING_SUBDIRS})")
    base = f"/Volumes/{catalog or config.CATALOG}/{config.SCHEMAS['bronze']}/{config.LANDING_VOLUME_NAME}"
    return f"{base}/{subdir}" if subdir else base


def ensure_schemas(spark: SparkSession, catalog: str | None = None) -> list[str]:
    """Crea los schemas del pipeline (y el Volume de landing en Databricks). Idempotente."""
    prefix = _catalog_prefix(catalog)
    created = []
    for schema in config.SCHEMAS.values():
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {prefix}{schema}")
        created.append(f"{prefix}{schema}")
    if config.is_databricks():
        volume = f"{prefix}{config.SCHEMAS['bronze']}.{config.LANDING_VOLUME_NAME}"
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {volume}")
        created.append(volume)
    return created


def write_delta_merge(spark: SparkSession, df: DataFrame, target: str, keys: list[str]) -> dict[str, int]:
    """Inserta en `target` las filas de `df` cuya clave no existe todavía (ADR-002).

    - Nunca actualiza ni borra: reejecutar con el mismo input inserta 0 filas.
    - Claves comparadas con `<=>` (null-safe), para que una clave nula no se reinserte en cada run.
    - Si `df` trae la misma clave más de una vez, falla: elegir una fila sería sobrescribir
      en silencio. Quien llama decide qué hacer con esos duplicados y los registra.
    - Si `target` no existe, lo crea vacío con el esquema de `df` (Delta) y después hace el MERGE.
    - Si `df` trae columnas que `target` no tiene, falla: `INSERT *` las ignoraría sin error.
    """
    if not keys:
        raise ValueError("write_delta_merge necesita al menos una clave")
    missing = [k for k in keys if k not in df.columns]
    if missing:
        raise ValueError(f"Claves ausentes en el DataFrame: {missing}")

    rows_source = df.count()
    duplicated = rows_source - df.select(*keys).distinct().count()
    if duplicated:
        raise ValueError(f"{duplicated} filas repiten la clave {keys} en el origen; no se escribe {target}")

    if not spark.catalog.tableExists(target):
        df.limit(0).write.format("delta").saveAsTable(target)

    target_columns = {c.lower() for c in spark.table(target).columns}
    extra = [c for c in df.columns if c.lower() not in target_columns]
    if extra:
        raise ValueError(f"Columnas que {target} no tiene: {extra}; no se escribe")

    view = f"_merge_src_{uuid.uuid4().hex}"
    df.createOrReplaceTempView(view)
    try:
        condition = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in keys)
        result = spark.sql(f"MERGE INTO {target} t USING {view} s ON {condition} WHEN NOT MATCHED THEN INSERT *")
        rows_inserted = int(result.first()["num_inserted_rows"])
    finally:
        spark.catalog.dropTempView(view)

    return {"rows_source": rows_source, "rows_inserted": rows_inserted,
            "rows_already_present": rows_source - rows_inserted}
