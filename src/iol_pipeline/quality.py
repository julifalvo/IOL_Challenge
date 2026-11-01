"""Reglas de calidad: docs/quality_rules.md. Marcan filas y no descartan ninguna."""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from iol_pipeline import calendar, config

SCALE = "DQ021_POSSIBLE_SCALE_ERROR"
AMOUNT = "DQ020_EXTREME_AMOUNT"
QUANTITY = "DQ022_EXTREME_QTY"
OPTION = "OPCION"


def _blank(name: str) -> Column:
    return F.col(name).isNull() | (F.trim(F.col(name)) == "")


def eligible_for_baselines() -> Column:
    """Filas que entran a las estadísticas: interpretables, con dominio válido, y sin versiones en conflicto."""
    required = F.lit(True)
    for name in config.TRADES_CONTRACT:
        required = required & ~_blank(name)
    return (required & F.col("trade_ts_utc").isNotNull() & F.col("extra_fields").isNull() & (F.col("_id_versions") <= 1)
            & (F.col("qty") > 0) & (F.col("price") > 0) & F.col("gross_amount").isNotNull()
            & F.col("currency_code").isin(list(config.CURRENCIES)))


def compute_baselines(typed: DataFrame) -> DataFrame:
    """Estadísticas de DQ020, DQ021 y DQ022: una fila por (regla, grupo). Se persisten para poder explicar cada marca."""
    eligible = typed.where(eligible_for_baselines()).select(
        "symbol", "asset_type_rule", "currency_code", F.col("price").cast("double").alias("price"),
        F.col("qty").cast("double").alias("qty"), F.log(F.col("qty").cast("double") * F.col("price").cast("double")).alias("log_amount"))

    scale = (eligible.withColumn("group_key", F.concat_ws("|", "symbol", "currency_code")).groupBy("group_key")
             .agg(F.median("price").alias("median_value"), F.count("*").alias("n"))
             .select(F.lit(SCALE).alias("rule_code"), "group_key", "median_value", F.lit(None).cast("double").alias("mad_value"),
                     F.lit(None).cast("double").alias("limit_value"), "n"))

    by_group = eligible.withColumn("group_key", F.concat_ws("|", "asset_type_rule", "currency_code"))
    medians = by_group.groupBy("group_key").agg(F.median("log_amount").alias("median_value"), F.count("*").alias("n"))
    amount = (by_group.join(medians, "group_key")
              .groupBy("group_key", "median_value", "n").agg(F.median(F.abs(F.col("log_amount") - F.col("median_value"))).alias("mad_value"))
              .select(F.lit(AMOUNT).alias("rule_code"), "group_key", "median_value", "mad_value", F.lit(None).cast("double").alias("limit_value"), "n"))

    quantity = (eligible.groupBy(F.col("asset_type_rule").alias("group_key"))
                .agg(F.percentile("qty", config.QUALITY_PARAMS["extreme_qty_quantile"]).alias("limit_value"), F.count("*").alias("n"))
                .select(F.lit(QUANTITY).alias("rule_code"), "group_key", F.lit(None).cast("double").alias("median_value"),
                        F.lit(None).cast("double").alias("mad_value"), "limit_value", "n"))

    return scale.unionByName(amount).unionByName(quantity).withColumn("rules_version", F.lit(config.RULES_VERSION))


def with_baseline_values(df: DataFrame, baselines: DataFrame) -> DataFrame:
    """Agrega symbol_median, price_ratio, robust_z y qty_limit. Con MAD = 0 el z robusto no está definido."""
    def only(code: str) -> DataFrame:
        return baselines.where(F.col("rule_code") == code)

    scale = only(SCALE).select(F.col("group_key").alias("_scale_group"), F.col("median_value").alias("symbol_median"))
    amount = only(AMOUNT).select(F.col("group_key").alias("_amount_group"), F.col("median_value").alias("_log_median"), F.col("mad_value").alias("_log_mad"))
    quantity = only(QUANTITY).select(F.col("group_key").alias("asset_type_rule"), F.col("limit_value").alias("qty_limit"))
    log_amount = F.log(F.col("qty").cast("double") * F.col("price").cast("double"))
    return (df.withColumn("_amount_group", F.concat_ws("|", "asset_type_rule", "currency_code"))
            .withColumn("_scale_group", F.concat_ws("|", "symbol", "currency_code"))
            .join(scale, "_scale_group", "left").join(amount, "_amount_group", "left").join(quantity, "asset_type_rule", "left")
            .withColumn("price_ratio", F.when(F.col("price") > 0, F.col("price").cast("double") / F.col("symbol_median")))
            .withColumn("robust_z", F.when((F.col("_log_mad") > 0) & (F.col("qty") > 0) & (F.col("price") > 0),
                                           config.QUALITY_PARAMS["robust_z_constant"] * (log_amount - F.col("_log_median")) / F.col("_log_mad")))
            .drop("_amount_group", "_scale_group", "_log_median", "_log_mad"))


def row_rules() -> dict[str, Column]:
    """Condición de cada regla, por código. Una condición null no marca."""
    params = config.QUALITY_PARAMS
    required_blank = F.lit(False)
    for name in config.TRADES_CONTRACT:
        required_blank = required_blank | _blank(name)
    day_of_week = F.dayofweek("trade_date_local")
    return {
        "DQ002_REQUIRED_NULL": required_blank,
        "DQ003_TS_UNPARSEABLE": ~_blank("fecha") & F.col("trade_ts_utc").isNull(),
        "DQ004_ID_CONFLICT": F.col("_id_versions") > 1,
        "DQ005_NUMERIC_UNPARSEABLE": ((~_blank("cantidad") & F.col("qty").isNull()) | (~_blank("precio") & F.col("price").isNull())
                                      | (F.col("qty").isNotNull() & F.col("price").isNotNull() & F.col("gross_amount").isNull())),
        "DQ006_NON_POSITIVE": (F.col("qty") <= 0) | (F.col("price") <= 0),
        "DQ007_DOMAIN_INVALID": ((~_blank("tipoTran") & ~F.trim("tipoTran").isin(list(config.SIDES)))
                                 | (~_blank("moneda") & ~F.upper(F.trim("moneda")).isin(list(config.CURRENCIES)))
                                 | (~_blank("origen") & ~F.trim("origen").isin(list(config.CHANNELS)))),
        "DQ008_MALFORMED_ROW": F.col("extra_fields").isNotNull(),
        "DQ010_SATURDAY": day_of_week == calendar.SPARK_SATURDAY,
        "DQ011_SUNDAY": day_of_week == calendar.SPARK_SUNDAY,
        "DQ012_MARKET_HOLIDAY": F.col("trade_date_local").isin(list(config.MARKET_HOLIDAYS)),
        "DQ013_OUTSIDE_SESSION": F.col("trade_ts_local").isNotNull() & ~calendar.in_session("trade_ts_local"),
        "DQ014_OUTSIDE_CALENDAR": F.col("trade_date_local").isNotNull() & F.col("effective_trade_date").isNull(),
        AMOUNT: F.abs(F.col("robust_z")) > params["robust_z_threshold"],
        SCALE: (F.col("price_ratio") < params["scale_ratio_low"]) | (F.col("price_ratio") > params["scale_ratio_high"]),
        QUANTITY: F.col("qty").cast("double") > F.col("qty_limit"),
        "DQ031_SYMBOL_MULTI_DESC": F.col("_symbol_descriptions") > 1,
        "DQ032_AFTER_MATURITY": F.col("effective_trade_date") > F.col("maturity_date"),
    }


def rule_severity(code: str) -> Column:
    """DQ021 es WARNING en opciones: la prima varía legítimamente ×50."""
    severity = F.lit(config.RULE_SEVERITY[code])
    return F.when(F.col("asset_type_rule") == OPTION, F.lit("WARNING")).otherwise(severity) if code == SCALE else severity


def apply_rules(typed: DataFrame, baselines: DataFrame) -> DataFrame:
    """Agrega quality_flags y quality_status (la máxima severidad; INFO solo cuenta como OK)."""
    with_values = with_baseline_values(typed, baselines)
    rules = row_rules()
    flags = F.array(*[F.when(condition, F.lit(code)) for code, condition in rules.items()])
    rank_of = F.create_map(*[F.lit(v) for pair in config.SEVERITY_RANK.items() for v in pair])
    ranks = [F.when(condition, rank_of[rule_severity(code)]).otherwise(0) for code, condition in rules.items()]
    status_by_rank = {rank: name for name, rank in config.SEVERITY_RANK.items() if name != "INFO"}  # INFO solo cuenta como OK
    status_of = F.create_map(*[F.lit(v) for pair in status_by_rank.items() for v in pair])
    return (with_values
            .withColumn("quality_flags", F.filter(flags, lambda flag: flag.isNotNull()))
            .withColumn("quality_status", status_of[F.greatest(*ranks)]))


# Valores que disparan cada regla. Ninguno identifica al cliente.
EVENT_DETAILS = {
    "DQ002_REQUIRED_NULL": ["fecha", "tipoTran", "descripcion_titulo", "moneda", "simbolo_titulo", "cantidad", "precio", "origen"],
    "DQ003_TS_UNPARSEABLE": ["fecha"],
    "DQ004_ID_CONFLICT": ["record_hash"],
    "DQ005_NUMERIC_UNPARSEABLE": ["cantidad", "precio"],
    "DQ006_NON_POSITIVE": ["qty", "price"],
    "DQ007_DOMAIN_INVALID": ["tipoTran", "moneda", "origen"],
    "DQ008_MALFORMED_ROW": ["extra_fields"],
    "DQ010_SATURDAY": ["trade_date_local", "effective_trade_date"],
    "DQ011_SUNDAY": ["trade_date_local"],
    "DQ012_MARKET_HOLIDAY": ["trade_date_local"],
    "DQ014_OUTSIDE_CALENDAR": ["trade_date_local"],
    AMOUNT: ["gross_amount", "currency_code", "asset_type_rule", "robust_z"],
    SCALE: ["price", "symbol", "symbol_median", "price_ratio"],
    QUANTITY: ["qty", "asset_type_rule", "qty_limit"],
    "DQ032_AFTER_MATURITY": ["effective_trade_date", "maturity_date"],
}


def build_events(silver: DataFrame, bronze: DataFrame, baselines: DataFrame, pipeline_run_id: str) -> DataFrame:
    """Una fila por (operación, versión, regla) con severidad WARNING o mayor. `event_id` es determinista."""
    raw = bronze.select("record_hash", "fecha", "tipoTran", "moneda", "origen", "cantidad", "precio", "descripcion_titulo", "simbolo_titulo", "extra_fields")
    source = with_baseline_values(silver, baselines).join(raw, "record_hash")
    severe = [code for code, severity in config.RULE_SEVERITY.items() if severity != "INFO" and code in EVENT_DETAILS]
    exploded = source.withColumn("rule_code", F.explode("quality_flags")).where(F.col("rule_code").isin(severe))

    details = F.lit(None).cast("string")
    severity = F.lit(None).cast("string")
    for code in severe:
        details = F.when(F.col("rule_code") == code, F.to_json(F.struct(*EVENT_DETAILS[code]))).otherwise(details)
        severity = F.when(F.col("rule_code") == code, rule_severity(code)).otherwise(severity)

    return exploded.select(
        F.sha2(F.concat_ws("|", F.coalesce("trade_id", F.lit("")), "record_hash", "rule_code", F.lit(config.RULES_VERSION)), 256).alias("event_id"),
        F.lit(pipeline_run_id).alias("pipeline_run_id"), "trade_id", "record_hash", "rule_code", severity.alias("severity"),
        details.alias("details_json"), F.lit(config.RULES_VERSION).alias("rules_version"), F.lit(True).alias("is_active"))
