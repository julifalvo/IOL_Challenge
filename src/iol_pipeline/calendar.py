"""Calendario de mercado y fecha efectiva de una operación (ADR-003)."""
from __future__ import annotations

from datetime import date, timedelta

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from iol_pipeline import config

SATURDAY = 5
SPARK_SATURDAY = 7  # F.dayofweek: 1 = domingo
SPARK_SUNDAY = 1


def is_business_day(day: date) -> bool:
    return day.weekday() < SATURDAY and day not in config.MARKET_HOLIDAYS


def effective_date(day: date) -> date:
    """Sábado → último día hábil previo. Cualquier otro día queda igual: domingo y feriado no se reasignan."""
    if day.weekday() != SATURDAY:
        return day
    previous = day - timedelta(days=1)
    while not is_business_day(previous):
        previous -= timedelta(days=1)
    return previous


def _wall_clock(ts: Column, tz: str) -> Column:
    """Hora de pared de un instante en `tz`, sin depender de la zona horaria de la sesión de Spark."""
    return F.convert_timezone(None, F.lit(tz), ts.cast("timestamp_ntz"))


def in_session(ts_local: str) -> Column:
    """Hora local dentro de la rueda: incluye el inicio y excluye el cierre."""
    start, end = config.SESSION_HOURS_LOCAL
    local_time = F.date_format(ts_local, "HH:mm")
    return (local_time >= start) & (local_time < end)


def _saturday_to_effective() -> Column:
    days = (config.CALENDAR_TO - config.CALENDAR_FROM).days + 1
    saturdays = [d for d in (config.CALENDAR_FROM + timedelta(days=i) for i in range(days)) if d.weekday() == SATURDAY]
    return F.create_map(*[F.lit(v) for d in saturdays for v in (d, effective_date(d))])


def with_trade_dates(df: DataFrame, ts_col: str = "fecha") -> DataFrame:
    """Agrega trade_ts_utc, trade_hour_utc, trade_date_utc, trade_ts_local, trade_date_local y effective_trade_date.

    Una fecha que no parsea queda en null en todas: se marca aguas abajo, nunca lanza excepción.
    Una fecha fuera del calendario deja `effective_trade_date` en null (ADR-003).
    """
    ts = F.try_to_timestamp(F.col(ts_col), F.lit(config.TRADES_TS_FORMAT))
    utc = _wall_clock(ts, "UTC")
    local = _wall_clock(ts, config.TZ_MARKET)
    in_calendar = F.to_date(local).between(F.lit(config.CALENDAR_FROM), F.lit(config.CALENDAR_TO))
    return (df
            .withColumn("trade_ts_utc", ts)
            .withColumn("trade_hour_utc", F.hour(utc))
            .withColumn("trade_date_utc", F.to_date(utc))
            .withColumn("trade_ts_local", local)
            .withColumn("trade_date_local", F.to_date(local))
            .withColumn("effective_trade_date",
                        F.when(in_calendar, F.coalesce(F.try_element_at(_saturday_to_effective(), F.col("trade_date_local")),
                                                       F.col("trade_date_local")))))
