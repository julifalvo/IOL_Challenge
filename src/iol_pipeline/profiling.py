"""Profiling del CSV en Spark (spec 01, notebook 01_data_profiling).

Solo describe el dato. Los resultados no llevan `id_cliente` ni `id_transaccion`, porque
los outputs del notebook van a un repo público.
"""
from __future__ import annotations

from datetime import timedelta

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from iol_pipeline import calendar, config, instruments

EXTREME_COLUMNS = ["simbolo_titulo", "origen", "asset_type_rule", "moneda", "cantidad", "precio", "amount"]


def read_trades_csv(spark: SparkSession, path: str) -> DataFrame:
    """Todo como texto: el profiling no asume tipos."""
    return spark.read.option("header", True).option("inferSchema", False).csv(path)


def prepare(raw: DataFrame) -> DataFrame:
    """Agrega fechas, cantidad y precio numéricos, importe y tipo de activo. Lo que no parsea queda en null.

    `double` alcanza para describir; Silver usa DECIMAL (data_model.md).
    """
    return (calendar.with_trade_dates(raw)
            .withColumn("cantidad", F.col("cantidad").try_cast("double"))
            .withColumn("precio", F.col("precio").try_cast("double"))
            .withColumn("amount", F.col("cantidad") * F.col("precio"))
            .withColumn("asset_type_rule", instruments.asset_type_by_rules(F.col("simbolo_titulo"), F.col("descripcion_titulo"))))


# --- Celda 1 -------------------------------------------------------------------
def general_summary(raw: DataFrame, trades: DataFrame) -> dict:
    row = trades.agg(
        F.count("*").alias("rows"),
        F.countDistinct("id_cliente").alias("clients"),
        F.countDistinct("simbolo_titulo").alias("symbols"),
        F.countDistinct("id_transaccion").alias("distinct_trade_ids"),
        F.min("trade_ts_local").alias("first_local"),
        F.max("trade_ts_local").alias("last_local"),
        F.sum(F.col("trade_ts_utc").isNull().cast("int")).alias("unparseable_ts"),
    ).first().asDict()
    nulls = raw.agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in raw.columns]).first().asDict()
    return {**row, "duplicate_trade_ids": row["rows"] - row["distinct_trade_ids"], "nulls_by_column": nulls}


# --- Celda 2 -------------------------------------------------------------------
def weekday_distribution(trades: DataFrame) -> DataFrame:
    """weekday (1 = lunes), name, ops_utc, ops_local."""
    def per_day(date_col: str, alias: str) -> DataFrame:
        return (trades.where(F.col(date_col).isNotNull())
                .withColumn("weekday", (F.dayofweek(date_col) + 5) % 7 + 1)
                .groupBy("weekday").agg(F.count("*").alias(alias)))

    days = trades.sparkSession.createDataFrame(
        list(enumerate(["lun", "mar", "mie", "jue", "vie", "sab", "dom"], start=1)), "weekday int, name string")
    return (days.join(per_day("trade_date_utc", "ops_utc"), "weekday", "left")
            .join(per_day("trade_date_local", "ops_local"), "weekday", "left")
            .fillna(0, ["ops_utc", "ops_local"]).orderBy("weekday"))


def weekdays_without_trades(trades: DataFrame) -> list[dict]:
    """Días de lunes a viernes sin operaciones dentro del rango observado, y si están en el calendario de feriados."""
    present = {r[0] for r in trades.select("trade_date_local").distinct().collect() if r[0] is not None}
    first, last = min(present), max(present)
    days = (first + timedelta(days=i) for i in range((last - first).days + 1))
    return [{"date": d.isoformat(), "market_holiday": config.MARKET_HOLIDAYS.get(d)}
            for d in days if d.weekday() < 5 and d not in present]


def trades_on_market_holidays(trades: DataFrame) -> int:
    return trades.where(F.col("trade_date_local").isin(list(config.MARKET_HOLIDAYS))).count()


# --- Celda 3 -------------------------------------------------------------------
def hourly_by_weekday(trades: DataFrame) -> DataFrame:
    """Operaciones por hora UTC y día de semana UTC."""
    return (trades.where(F.col("trade_date_utc").isNotNull())
            .withColumn("weekday", F.date_format("trade_date_utc", "E"))
            .groupBy("trade_hour_utc").pivot("weekday", ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"])
            .count().fillna(0).orderBy("trade_hour_utc"))


def hour_bounds_by_weekday(trades: DataFrame) -> DataFrame:
    return (trades.where(F.col("trade_date_utc").isNotNull())
            .withColumn("weekday", F.date_format("trade_date_utc", "E"))
            .groupBy("weekday").agg(F.min("trade_hour_utc").alias("first_hour_utc"),
                                    F.max("trade_hour_utc").alias("last_hour_utc"), F.count("*").alias("ops")))


def share_in_session(trades: DataFrame) -> float:
    """Proporción de operaciones con hora local dentro de la rueda."""
    row = trades.where(F.col("trade_ts_local").isNotNull()).agg(
        F.avg(calendar.in_session("trade_ts_local").cast("int")).alias("share")).first()
    return row["share"]


# --- Celda 4 -------------------------------------------------------------------
def channel_counts(trades: DataFrame) -> DataFrame:
    return trades.groupBy("origen").agg(F.count("*").alias("ops")).orderBy(F.col("ops").desc())


def multichannel_clients(trades: DataFrame) -> int:
    return (trades.where(F.col("id_cliente").isNotNull()).groupBy("id_cliente")
            .agg(F.countDistinct("origen").alias("n")).where("n > 1").count())


# --- Celda 5 -------------------------------------------------------------------
def client_activity(trades: DataFrame) -> dict:
    per_client = trades.where(F.col("id_cliente").isNotNull()).groupBy("id_cliente").agg(F.count("*").alias("ops"))
    row = per_client.agg(
        F.percentile("ops", list(config.CLIENT_PERCENTILES)).alias("percentiles"),
        F.max("ops").alias("max_ops"),
        F.sum((F.col("ops") == 1).cast("int")).alias("one_trade"),
        F.count("*").alias("clients"),
    ).first()
    return {"percentiles": dict(zip(config.CLIENT_PERCENTILES, row["percentiles"])), "max_ops": row["max_ops"],
            "clients": row["clients"], "one_trade": row["one_trade"], "one_trade_share": row["one_trade"] / row["clients"]}


# --- Celda 6 -------------------------------------------------------------------
def asset_type_by_currency(trades: DataFrame) -> DataFrame:
    return trades.groupBy("asset_type_rule").pivot("moneda").count().fillna(0).orderBy("asset_type_rule")


def instrument_summary(trades: DataFrame) -> dict:
    symbols = trades.select("simbolo_titulo", "moneda").distinct()
    ars = symbols.where("moneda = 'ARS'").select(F.col("simbolo_titulo").alias("base"))
    usd = symbols.where("moneda = 'USD'").withColumn("base", F.expr("substring(simbolo_titulo, 1, length(simbolo_titulo) - 1)"))
    ars_with_suffix = [r[0] for r in (symbols.where("moneda = 'ARS' AND simbolo_titulo RLIKE '[DC]$'")
                                      .select("simbolo_titulo").orderBy("simbolo_titulo").collect())]
    multi_desc = [r[0] for r in (trades.groupBy("simbolo_titulo").agg(F.countDistinct("descripcion_titulo").alias("n"))
                                 .where("n > 1").select("simbolo_titulo").orderBy("simbolo_titulo").collect())]
    return {"usd_symbols": usd.count(), "usd_with_ars_base": usd.join(ars, "base").count(),
            "ars_ending_in_d_or_c": ars_with_suffix, "symbols_with_many_descriptions": multi_desc}


def top_symbols_share(trades: DataFrame, top_n: int) -> float:
    total = trades.count()
    top = trades.groupBy("simbolo_titulo").count().orderBy(F.col("count").desc(), "simbolo_titulo").limit(top_n)
    return top.agg(F.sum("count")).first()[0] / total


# --- Celda 7 -------------------------------------------------------------------
def settlement_price_medians(trades: DataFrame) -> DataFrame:
    """Mediana por variante de cada grupo y su relación con la base: el MEP o CCL implícito."""
    rows = [(group[0], symbol) for group in config.PROFILE_PRICE_GROUPS for symbol in group]
    groups = trades.sparkSession.createDataFrame(rows, "base string, simbolo_titulo string")
    medians = (trades.where(F.col("precio") > 0).join(groups, "simbolo_titulo")
               .groupBy("base", "simbolo_titulo", "moneda").agg(F.median("precio").alias("median_price"), F.count("*").alias("ops")))
    base_median = (medians.where(F.col("base") == F.col("simbolo_titulo"))
                   .select("base", F.col("median_price").alias("base_median")))
    return (medians.join(base_median, "base", "left")
            .withColumn("implied_fx", F.when(F.col("base") != F.col("simbolo_titulo"), F.col("base_median") / F.col("median_price")))
            .drop("base_median").orderBy("base", "simbolo_titulo"))


# --- Celda 8 -------------------------------------------------------------------
def top_by_quantity(trades: DataFrame, top_n: int) -> DataFrame:
    return trades.orderBy(F.col("cantidad").desc_nulls_last()).limit(top_n).select(*EXTREME_COLUMNS)


def top_by_amount(trades: DataFrame, top_n: int) -> DataFrame:
    """Top por importe dentro de cada moneda: ARS y USD no se comparan entre sí."""
    order = Window.partitionBy("moneda").orderBy(F.col("amount").desc_nulls_last())
    return (trades.withColumn("rank", F.row_number().over(order)).where(F.col("rank") <= top_n)
            .select("rank", *EXTREME_COLUMNS).orderBy("moneda", "rank"))


def with_symbol_median_ratio(trades: DataFrame) -> DataFrame:
    medians = (trades.where(F.col("precio") > 0).groupBy("simbolo_titulo")
               .agg(F.median("precio").alias("symbol_median")))
    return trades.join(medians, "simbolo_titulo", "left").withColumn("price_ratio", F.col("precio") / F.col("symbol_median"))


def scale_anomalies(trades: DataFrame) -> DataFrame:
    """Filas con precio fuera del rango de DQ021 respecto de la mediana de su símbolo."""
    low, high = config.QUALITY_PARAMS["scale_ratio_low"], config.QUALITY_PARAMS["scale_ratio_high"]
    return (with_symbol_median_ratio(trades)
            .where((F.col("price_ratio") < low) | (F.col("price_ratio") > high))
            .select(*EXTREME_COLUMNS, "symbol_median", "price_ratio").orderBy("origen", "simbolo_titulo"))


def treasury_capitalizable_prices(trades: DataFrame) -> DataFrame:
    """T30A7 frente a otras letras y bonos capitalizables: ¿su mínimo es propio o es de toda la familia?"""
    desc = F.lower(F.col("descripcion_titulo"))
    family = trades.where(desc.rlike(r"boncap|tesoro nac.*cap") & (F.col("precio") > 0))
    return (family.groupBy("simbolo_titulo").agg(
                F.count("*").alias("ops"), F.min("precio").alias("min_price"), F.median("precio").alias("median_price"),
                F.max("precio").alias("max_price"))
            .withColumn("min_over_median", F.col("min_price") / F.col("median_price"))
            .orderBy("min_over_median"))


# --- Celda 9 -------------------------------------------------------------------
def monthly_activity(trades: DataFrame) -> DataFrame:
    """Operaciones por mes y por día hábil observado, por fecha efectiva."""
    return (trades.where(F.col("effective_trade_date").isNotNull())
            .withColumn("month", F.date_format("effective_trade_date", "yyyy-MM"))
            .groupBy("month").agg(F.count("*").alias("ops"), F.countDistinct("effective_trade_date").alias("business_days_observed"))
            .withColumn("ops_per_business_day", F.col("ops") / F.col("business_days_observed"))
            .orderBy("month"))

