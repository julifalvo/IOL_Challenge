"""Atributos derivados de cliente para el SCD2 y su viabilidad sobre el dataset (ADR-007)."""
from __future__ import annotations

from datetime import date

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from iol_pipeline import config

INITIAL_POINT = "inicial"


def activity_segment(cum_trades: Column) -> Column:
    """Segmento por operaciones acumuladas, con los cortes congelados de `SEGMENT_RULE_VERSION`."""
    result = None
    for minimum, segment in config.ACTIVITY_SEGMENTS:
        result = F.when(cum_trades >= minimum, segment) if result is None else result.when(cum_trades >= minimum, segment)
    return result


def channel_code(origin: Column) -> Column:
    mapping = F.create_map(*[F.lit(v) for pair in config.CHANNELS.items() for v in pair])
    return F.coalesce(F.try_element_at(mapping, origin), F.lit(config.CHANNEL_UNKNOWN))


def preferred_channel(trades: DataFrame) -> DataFrame:
    """client_id, preferred_channel: más operaciones; desempate por operación más reciente, después alfabético."""
    per_channel = (trades.groupBy("client_id", "channel_code")
                   .agg(F.count("*").alias("n"), F.max("trade_ts_utc").alias("last_ts")))
    order = Window.partitionBy("client_id").orderBy(F.col("n").desc(), F.col("last_ts").desc(), F.col("channel_code"))
    return (per_channel.withColumn("rank", F.row_number().over(order)).where("rank = 1")
            .select("client_id", F.col("channel_code").alias("preferred_channel")))


def attributes_at_cut(trades: DataFrame, cut: date) -> DataFrame:
    """Atributos de cada cliente con la información hasta `cut` (por fecha efectiva): sin leakage."""
    until_cut = trades.where(F.col("effective_trade_date") <= F.lit(cut))
    counts = until_cut.groupBy("client_id").agg(F.count("*").alias("cum_trades"))
    return (counts.join(preferred_channel(until_cut), "client_id")
            .select("client_id", "cum_trades", activity_segment(F.col("cum_trades")).alias("activity_segment"),
                    "preferred_channel"))


def initial_attributes(trades: DataFrame) -> DataFrame:
    """Versión inicial: atributos de la primera operación del cliente, y solo de esa."""
    order = Window.partitionBy("client_id").orderBy(F.col("trade_ts_utc"), F.col("channel_code"))
    return (trades.withColumn("rank", F.row_number().over(order)).where("rank = 1")
            .select("client_id", F.lit(1).alias("cum_trades"), activity_segment(F.lit(1)).alias("activity_segment"),
                    F.col("channel_code").alias("preferred_channel")))


def usable_trades(df: DataFrame) -> DataFrame:
    """Filas con cliente y fecha, listas para calcular atributos. Espera las columnas de `calendar.with_trade_dates`."""
    return (df.where(F.col("id_cliente").isNotNull() & F.col("trade_ts_utc").isNotNull())
            .select(F.col("id_cliente").alias("client_id"), channel_code(F.col("origen")).alias("channel_code"),
                    "trade_ts_utc", "effective_trade_date"))


def client_points(trades: DataFrame, cuts: list[date] | None = None) -> DataFrame:
    """Una fila por cliente y punto (versión inicial + cada corte en el que el cliente ya existe)."""
    cuts = config.SCD2_SNAPSHOTS if cuts is None else cuts
    points = initial_attributes(trades).select(F.lit(0).alias("point_order"), F.lit(INITIAL_POINT).alias("point"), "*")
    for order, cut in enumerate(cuts, start=1):
        points = points.unionByName(
            attributes_at_cut(trades, cut).select(F.lit(order).alias("point_order"), F.lit(cut.isoformat()).alias("point"), "*"))
    return points


def _with_previous(points: DataFrame) -> DataFrame:
    order = Window.partitionBy("client_id").orderBy("point_order")
    return (points
            .withColumn("from_point", F.lag("point").over(order))
            .withColumn("from_segment", F.lag("activity_segment").over(order))
            .withColumn("from_channel", F.lag("preferred_channel").over(order)))


def versions_per_client(points: DataFrame) -> DataFrame:
    """client_id, versions: 1 + cantidad de puntos donde cambia segmento o canal respecto del punto anterior."""
    changed = F.col("from_point").isNotNull() & (
        (F.col("activity_segment") != F.col("from_segment")) | (F.col("preferred_channel") != F.col("from_channel")))
    return (_with_previous(points).groupBy("client_id")
            .agg((F.lit(1) + F.sum(F.when(changed, 1).otherwise(0))).alias("versions")))


def versions_distribution(points: DataFrame) -> DataFrame:
    """(a) versions, clients."""
    return versions_per_client(points).groupBy("versions").agg(F.count("*").alias("clients")).orderBy("versions")


def transitions(points: DataFrame) -> DataFrame:
    """(b) Por par de puntos consecutivos: clientes presentes en ambos y cuántos cambian de canal o de segmento."""
    return (_with_previous(points).where(F.col("from_point").isNotNull())
            .groupBy("from_point", "point")
            .agg(F.count("*").alias("clients"),
                 F.sum(F.when(F.col("preferred_channel") != F.col("from_channel"), 1).otherwise(0)).alias("channel_changes"),
                 F.sum(F.when(F.col("activity_segment") != F.col("from_segment"), 1).otherwise(0)).alias("segment_changes"))
            .orderBy("point", "from_point"))


def segment_transition_matrix(points: DataFrame) -> DataFrame:
    """(c) Clientes por (punto de origen, punto de destino, segmento de origen, segmento de destino)."""
    return (_with_previous(points).where(F.col("from_point").isNotNull())
            .groupBy("from_point", "point", "from_segment", "activity_segment")
            .agg(F.count("*").alias("clients"))
            .orderBy("point", "from_point", "from_segment", "activity_segment"))
