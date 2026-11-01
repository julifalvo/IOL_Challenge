"""Silver: 1:1 con Bronze, tipado, normalizado y con flags de calidad. Ninguna fila se descarta (ADR-003, ADR-005)."""
from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from iol_pipeline import calendar, config, instruments, io, quality, segmentation

SILVER_COLUMNS = [
    "trade_id", "trade_ts_utc", "trade_hour_utc", "trade_date_utc", "trade_ts_local", "trade_date_local", "effective_trade_date",
    "side", "client_id", "symbol", "description_raw", "currency_code", "channel_code", "qty", "price", "gross_amount",
    "asset_type_rule", "maturity_date", "quality_flags", "quality_status", "rules_version", "record_hash", "ingestion_run_id",
    "silver_run_id", "processed_at",
]
QUALITY_COLUMNS = ["quality_flags", "quality_status", "rules_version"]


def _text(name: str) -> F.Column:
    return F.nullif(F.trim(F.col(name)), F.lit(""))


def _whole_number(text: F.Column) -> F.Column:
    """DECIMAL(20,0), o null si tiene decimales: redondear cambiaría el dato sin aviso."""
    parsed = text.try_cast(config.DECIMAL_PARSE_TYPE)
    whole = parsed.try_cast(config.QTY_TYPE)
    return F.when(parsed == whole, whole)


def type_trades(bronze: DataFrame) -> DataFrame:
    """Tipado seguro y normalización. Lo que no parsea queda en null y lo marca una regla, nunca lanza excepción."""
    side_of = F.create_map(*[F.lit(v) for pair in config.SIDES.items() for v in pair])
    qty = _whole_number(F.trim("cantidad"))
    price = F.trim("precio").try_cast(config.PRICE_TYPE)
    typed = (calendar.with_trade_dates(bronze)
             .withColumn("trade_id", _text("id_transaccion"))
             .withColumn("side", F.try_element_at(side_of, F.trim("tipoTran")))
             .withColumn("client_id", _text("id_cliente"))
             .withColumn("symbol", F.upper(_text("simbolo_titulo")))
             .withColumn("description_raw", _text("descripcion_titulo"))
             .withColumn("currency_code", F.upper(_text("moneda")))
             .withColumn("channel_code", segmentation.channel_code(F.trim("origen")))
             .withColumn("qty", qty)
             .withColumn("price", price)
             .withColumn("gross_amount", F.try_multiply(qty, price).try_cast(config.AMOUNT_TYPE))
             .withColumn("asset_type_rule", instruments.asset_type_by_rules(F.col("symbol"), F.col("description_raw")))
             .withColumn("maturity_date", instruments.maturity_date(F.col("description_raw"))))

    versions = (typed.where(F.col("trade_id").isNotNull()).groupBy("trade_id")
                .agg(F.countDistinct("record_hash").alias("_id_versions")))
    descriptions = typed.groupBy("symbol").agg(F.countDistinct("description_raw").alias("_symbol_descriptions"))
    return (typed.join(versions, "trade_id", "left").join(descriptions, "symbol", "left")
            .fillna(1, ["_id_versions"]))


def build_silver(typed: DataFrame, baselines: DataFrame, silver_run_id: str, processed_at: datetime) -> DataFrame:
    """Fecha y corrida como literales: el origen se puede evaluar más de una vez y tiene que dar lo mismo."""
    return (quality.apply_rules(typed, baselines)
            .withColumn("rules_version", F.lit(config.RULES_VERSION))
            .withColumn("silver_run_id", F.lit(silver_run_id))
            .withColumn("processed_at", F.lit(processed_at))
            .select(*SILVER_COLUMNS))


def run_silver(spark: SparkSession, bronze_table: str, silver_table: str, baselines_table: str, events_table: str, pipeline_run_id: str = "") -> dict:
    """Reaplica las reglas sobre Bronze completo. Con el mismo input deja las tres tablas idénticas.

    Actualiza una fila si cambian sus flags, no solo la versión de reglas: una versión nueva de una operación
    tiene que marcar `DQ004` también en la fila que ya estaba.
    """
    bronze = spark.table(bronze_table)
    typed = type_trades(bronze)
    baselines_result = io.write_delta_upsert(spark, quality.compute_baselines(typed), baselines_table,
                                             ["rules_version", "rule_code", "group_key"], ["median_value", "mad_value", "limit_value", "n"])
    baselines = spark.table(baselines_table).where(F.col("rules_version") == config.RULES_VERSION)
    silver = build_silver(typed, baselines, pipeline_run_id, datetime.now(timezone.utc))
    silver_result = io.write_delta_upsert(spark, silver, silver_table, ["trade_id", "record_hash"], QUALITY_COLUMNS)
    events = quality.build_events(spark.table(silver_table), bronze, baselines, pipeline_run_id)
    events_result = io.write_delta_merge(spark, events, events_table, ["event_id"])

    bronze_rows, silver_rows = bronze.count(), spark.table(silver_table).count()
    if bronze_rows != silver_rows:
        raise ValueError(f"Silver no es 1:1 con Bronze: {bronze_rows} filas en Bronze, {silver_rows} en Silver")
    return {"bronze_rows": bronze_rows, "silver_rows": silver_rows, "silver_inserted": silver_result["rows_inserted"],
            "silver_updated": silver_result["rows_updated"], "baselines_updated": baselines_result["rows_updated"],
            "baselines_inserted": baselines_result["rows_inserted"], "events_inserted": events_result["rows_inserted"]}


# --- Salidas del notebook: sin IDs de cliente ni de operación ----------------------
def rule_summary(silver: DataFrame) -> DataFrame:
    """Regla, severidad, filas, % y un ejemplo: el prefijo de `record_hash`, trazable a Bronze sin publicar IDs."""
    total = silver.count()
    exploded = silver.withColumn("rule_code", F.explode("quality_flags"))
    severity = F.lit(None).cast("string")
    for code in config.RULE_SEVERITY:
        severity = F.when(F.col("rule_code") == code, quality.rule_severity(code)).otherwise(severity)
    counts = (exploded.withColumn("severity", severity).groupBy("rule_code", "severity")
              .agg(F.count("*").alias("rows"), F.substring(F.min("record_hash"), 1, config.EXAMPLE_HASH_CHARS).alias("example_record_hash")))
    spark = silver.sparkSession
    catalog = spark.createDataFrame(list(config.RULE_SEVERITY.items()), "rule_code string, default_severity string")
    missing = catalog.join(counts, "rule_code", "left_anti").select(
        "rule_code", F.col("default_severity").alias("severity"), F.lit(0).cast("long").alias("rows"), F.lit(None).cast("string").alias("example_record_hash"))
    return (counts.unionByName(missing).withColumn("pct_of_rows", F.round(F.col("rows") / F.lit(total) * 100, 3)).orderBy("rule_code", "severity"))


def status_distribution(silver: DataFrame) -> DataFrame:
    total = silver.count()
    return (silver.groupBy("quality_status").agg(F.count("*").alias("rows"))
            .withColumn("pct_of_rows", F.round(F.col("rows") / F.lit(total) * 100, 3)).orderBy("quality_status"))


def reconciliation(bronze: DataFrame, silver: DataFrame, events: DataFrame) -> dict[str, int]:
    """Invariantes de docs/quality_rules.md. Todas las diferencias tienen que dar 0."""
    keys = ["record_hash"]
    severe_flags = [code for code, severity in config.RULE_SEVERITY.items() if severity != "INFO"]
    flagged = silver.withColumn("rule_code", F.explode("quality_flags")).where(F.col("rule_code").isin(severe_flags))
    return {
        "bronze_rows": bronze.count(),
        "silver_rows": silver.count(),
        "bronze_not_in_silver": bronze.join(silver, keys, "left_anti").count(),
        "silver_not_in_bronze": silver.join(bronze, keys, "left_anti").count(),
        "not_ok_without_flag": silver.where((F.col("quality_status") != "OK") & (F.size("quality_flags") == 0)).count(),
        "severe_flags_without_event": flagged.join(events, ["record_hash", "rule_code"], "left_anti").count(),
    }
