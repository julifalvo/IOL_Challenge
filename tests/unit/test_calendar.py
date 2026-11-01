"""Fecha efectiva y columnas de fecha de una operación (ADR-003)."""
from __future__ import annotations

from datetime import date

from iol_pipeline import calendar, config


def test_saturday_moves_to_previous_friday():
    assert calendar.effective_date(date(2026, 1, 10)) == date(2026, 1, 9)


def test_saturday_skips_a_friday_holiday(monkeypatch):
    monkeypatch.setattr(config, "MARKET_HOLIDAYS", {date(2026, 1, 9): "feriado de prueba"})
    assert calendar.effective_date(date(2026, 1, 10)) == date(2026, 1, 8)


def test_other_days_are_not_reassigned():
    assert calendar.effective_date(date(2026, 1, 9)) == date(2026, 1, 9)    # viernes
    assert calendar.effective_date(date(2026, 2, 22)) == date(2026, 2, 22)  # domingo: es anomalía, no se mueve
    assert calendar.effective_date(date(2026, 2, 16)) == date(2026, 2, 16)  # feriado: es anomalía, no se mueve


def _row(spark, fecha):
    return calendar.with_trade_dates(spark.createDataFrame([(fecha,)], "fecha string")).first()


def test_local_date_uses_market_timezone(spark):
    row = _row(spark, "2026-01-10T01:30:00.000Z")  # sábado UTC, viernes 22:30 local
    assert row.trade_date_utc == date(2026, 1, 10)
    assert row.trade_hour_utc == 1
    assert row.trade_date_local == date(2026, 1, 9)
    assert row.effective_trade_date == date(2026, 1, 9)


def test_local_saturday_gets_effective_friday(spark):
    row = _row(spark, "2026-01-10T15:00:00.000Z")
    assert row.trade_date_local == date(2026, 1, 10)
    assert row.effective_trade_date == date(2026, 1, 9)


def test_date_outside_the_calendar_has_no_effective_date(spark):
    after_range = _row(spark, "2026-04-04T15:00:00.000Z")  # sábado después de CALENDAR_TO
    assert after_range.trade_date_local == date(2026, 4, 4)
    assert after_range.effective_trade_date is None
    assert _row(spark, "2025-11-28T15:00:00.000Z").effective_trade_date is None  # viernes antes de CALENDAR_FROM


def test_calendar_limits_are_inside_the_calendar(spark):
    assert _row(spark, "2026-03-31T15:00:00.000Z").effective_trade_date == date(2026, 3, 31)
    assert _row(spark, "2025-12-01T15:00:00.000Z").effective_trade_date == date(2025, 12, 1)


def test_unparseable_date_is_null_and_does_not_raise(spark):
    row = _row(spark, "2026-02-30T15:00:00.000Z")
    assert row.trade_ts_utc is None and row.trade_date_local is None and row.effective_trade_date is None


def test_dates_do_not_depend_on_the_session_timezone(spark):
    previous = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", "Asia/Tokyo")
    try:
        row = _row(spark, "2026-01-10T01:30:00.000Z")
    finally:
        spark.conf.set("spark.sql.session.timeZone", previous)
    assert row.trade_date_utc == date(2026, 1, 10)
    assert row.trade_hour_utc == 1
    assert row.trade_date_local == date(2026, 1, 9)
