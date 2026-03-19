import datetime as dt
from zoneinfo import ZoneInfo

import pytest

from utils import time_utils as tu


pytestmark = pytest.mark.unit


def test_parse_hhmm_accepts_valid_values():
    # Given: a valid HH:MM string.
    # When: parsing it with `parse_hhmm`.
    # Then: a `datetime.time` object with matching hour/minute is returned.

    parsed = tu.parse_hhmm("04:30")
    assert parsed == dt.time(hour=4, minute=30)


def test_parse_hhmm_rejects_invalid_format_and_ranges():
    # Given: malformed and out-of-range time strings.
    # When: parsing each value with `parse_hhmm`.
    # Then: each invalid input raises `ValueError`.

    with pytest.raises(ValueError):
        tu.parse_hhmm("0430")

    with pytest.raises(ValueError):
        tu.parse_hhmm("24:00")

    with pytest.raises(ValueError):
        tu.parse_hhmm("09:60")


def test_current_hour_truncates_minutes_seconds_and_microseconds():
    # Given: a timezone-aware datetime with non-zero minute/second/microsecond values.
    # When: normalizing it with `current_hour`.
    # Then: the returned datetime is the top of the same hour in the same timezone.

    tz = ZoneInfo("America/New_York")
    now = dt.datetime(2026, 3, 19, 13, 47, 59, 123456, tzinfo=tz)

    result = tu.current_hour(now)

    assert result == dt.datetime(2026, 3, 19, 13, 0, 0, 0, tzinfo=tz)


def test_is_market_open_handles_weekday_and_weekend_boundaries():
    # Given: timestamps spanning weekday open hours, weekend time, pre-open, and closing edge.
    # When: evaluating each timestamp with `is_market_open`.
    # Then: only valid in-session weekday times are reported as open.

    tz = ZoneInfo("America/New_York")
    open_time = dt.time(9, 30)
    close_time = dt.time(16, 0)

    weekday_open = dt.datetime(2026, 3, 18, 10, 0, tzinfo=tz)
    weekend_same_time = dt.datetime(2026, 3, 21, 10, 0, tzinfo=tz)
    before_open = dt.datetime(2026, 3, 18, 9, 0, tzinfo=tz)
    at_close = dt.datetime(2026, 3, 18, 16, 0, tzinfo=tz)

    assert tu.is_market_open(weekday_open, open_time, close_time) is True
    assert tu.is_market_open(weekend_same_time, open_time, close_time) is False
    assert tu.is_market_open(before_open, open_time, close_time) is False
    assert tu.is_market_open(at_close, open_time, close_time) is True


def test_infer_timestamp_uses_wall_clock_when_last_ts_missing(monkeypatch):
    # Given: no previous timestamp and a fixed mocked "now" value.
    # When: inferring a timestamp with `infer_timestamp`.
    # Then: the function rounds to the latest 5-minute wall-clock bucket and reports wall-clock reason.

    tz = ZoneInfo("America/New_York")
    fixed_now = dt.datetime(2026, 3, 19, 10, 7, 12, tzinfo=tz)

    class FixedDateTime(dt.datetime):
        @classmethod
        def now(cls, tzinfo=None):
            if tzinfo is None:
                return fixed_now
            return fixed_now.astimezone(tzinfo)

    monkeypatch.setattr(tu.dt, "datetime", FixedDateTime)

    inferred, reason = tu.infer_timestamp(
        last_ts=None,
        now_local=None,
        reason_prefix="date_missing",
        tz=tz,
    )

    assert inferred == dt.datetime(2026, 3, 19, 10, 5, 0, tzinfo=tz)
    assert reason == "date_missing_wall_clock"


def test_infer_timestamp_prefers_last_ts_increment_when_available():
    # Given: a known previous bar timestamp.
    # When: inferring the next timestamp with `infer_timestamp`.
    # Then: the function advances exactly 5 minutes from `last_ts` and reports previous-row reason.

    tz = ZoneInfo("America/New_York")
    last_ts = dt.datetime(2026, 3, 19, 10, 0, 0, tzinfo=tz)

    inferred, reason = tu.infer_timestamp(
        last_ts=last_ts,
        now_local=dt.datetime(2026, 3, 19, 12, 0, 0, tzinfo=tz),
        reason_prefix="date_missing",
        tz=tz,
    )

    assert inferred == dt.datetime(2026, 3, 19, 10, 5, 0, tzinfo=tz)
    assert reason == "date_missing_prev_row"