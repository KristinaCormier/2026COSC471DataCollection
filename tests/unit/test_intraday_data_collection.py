import datetime as dt
from zoneinfo import ZoneInfo

import pytest

import intraday_data_collection as collector
from src.model.models import MarketData
from utils import time_utils as tu


def _run_batch(
    api_payload,
    start,
    end,
    tz,
    symbol="AAPL",
    source_url="FMP_intraday",
    now_local=None,
):
    if now_local is None:
        now_local = dt.datetime(2026, 1, 26, 10, 30, tzinfo=tz)
    return collector._process_data_batch(
        api_payload,
        symbol,
        collector.STAGING_TABLE_NAME,
        source_url,
        start,
        end,
        now_local,
        tz,
    )


def test_ymd_format():
    # Given: a concrete `datetime.date` value.
    # When: formatting it with `ymd`.
    # Then: the result uses `YYYY-MM-DD` string format.

    date = dt.date(2026, 1, 26)
    result = tu.ymd(date)
    assert result == "2026-01-26"


def test_compute_window_aligns_to_5_minute_boundary_and_is_valid():
    # Given: a timezone-aware current time not already aligned to a 5-minute boundary.
    # When: computing the collection window with `compute_window`.
    # Then: start/end align to 5-minute boundaries and span exactly 5 minutes.

    tz = ZoneInfo("America/New_York")
    now = dt.datetime(2026, 1, 26, 15, 27, 42, tzinfo=tz)
    window_min = 60

    start, end = tu.compute_window(now, window_min=window_min)

    assert end.second == 0 and end.microsecond == 0
    assert end.minute % 5 == 0

    assert (end - start) == dt.timedelta(minutes=5)
    assert start.second == 0 and start.microsecond == 0
    assert start.minute % 5 == 0

    assert end > start


def test_parse_api_time_uses_market_tz():
    # Given: an API timestamp string and target market timezone.
    # When: parsing with `parse_api_time`.
    # Then: resulting datetime keeps the market timezone and expected clock time.

    tz = ZoneInfo("America/New_York")
    ts_str = "2026-01-26 10:05:00"

    parsed = tu.parse_api_time(ts_str, tz)

    assert parsed.tzinfo == tz
    assert parsed.hour == 10
    assert parsed.minute == 5


def test_process_data_batch_excludes_out_of_range_rows():
    # Given: payload rows whose timestamp falls outside the requested window.
    # When: processing with `_process_data_batch`.
    # Then: no rows are returned for insertion.

    tz = ZoneInfo("America/New_York")

    start = dt.datetime(2026, 1, 26, 10, 0, tzinfo=tz)
    end = dt.datetime(2026, 1, 26, 10, 30, tzinfo=tz)

    api_payload = [
        {"date": "2026-01-26 09:55:00", "open": 1, "high": 2, "low": 1, "close": 1.5, "volume": 10},
    ]

    rows = _run_batch(api_payload, start, end, tz)

    assert len(rows) == 0


def test_process_data_batch_allows_missing_close(mock_error_log_dir):
    # Given: a valid in-range payload row where `close` is missing.
    # When: processing the batch.
    # Then: the row is still accepted into staging with `close=None`.

    tz = ZoneInfo("America/New_York")
    start = dt.datetime(2026, 1, 26, 10, 0, tzinfo=tz)
    end = dt.datetime(2026, 1, 26, 10, 30, tzinfo=tz)

    api_payload = [
        {"date": "2026-01-26 10:05:00", "open": 1, "high": 2, "low": 1, "close": None, "volume": 10},
    ]

    rows = _run_batch(api_payload, start, end, tz)

    assert len(rows) == 1
    assert isinstance(rows[0], MarketData)
    assert rows[0].symbol == "AAPL"
    assert rows[0].close is None


def test_process_data_batch_sorts_by_ts():
    # Given: two in-range payload rows provided out of chronological order.
    # When: processing the batch.
    # Then: output rows are sorted ascending by timestamp.

    tz = ZoneInfo("America/New_York")
    start = dt.datetime(2026, 1, 26, 10, 0, tzinfo=tz)
    end = dt.datetime(2026, 1, 26, 10, 30, tzinfo=tz)

    api_payload = [
        {"date": "2026-01-26 10:25:00", "open": 1, "high": 2, "low": 1, "close": 1.7, "volume": 10},
        {"date": "2026-01-26 10:05:00", "open": 1, "high": 2, "low": 1, "close": 1.5, "volume": 10},
    ]

    rows = _run_batch(api_payload, start, end, tz)

    assert len(rows) == 2
    assert rows[0].symbol == "AAPL"
    assert rows[0].ts.minute == 5
    assert rows[1].symbol == "AAPL"
    assert rows[1].ts.minute == 25


def test_process_data_batch_sorts_newest_first_when_enabled():
    # Given: two in-range payload rows provided out of chronological order.
    # When: processing the batch with `newest_first=True`.
    # Then: output rows are sorted descending by timestamp.

    tz = ZoneInfo("America/New_York")
    start = dt.datetime(2026, 1, 26, 10, 0, tzinfo=tz)
    end = dt.datetime(2026, 1, 26, 10, 30, tzinfo=tz)

    api_payload = [
        {"date": "2026-01-26 10:05:00", "open": 1, "high": 2, "low": 1, "close": 1.5, "volume": 10},
        {"date": "2026-01-26 10:25:00", "open": 1, "high": 2, "low": 1, "close": 1.7, "volume": 10},
    ]

    rows = collector._process_data_batch(
        api_payload,
        "AAPL",
        collector.STAGING_TABLE_NAME,
        "FMP_intraday",
        start,
        end,
        dt.datetime(2026, 1, 26, 10, 30, tzinfo=tz),
        tz,
        newest_first=True,
    )

    assert len(rows) == 2
    assert rows[0].ts.minute == 25
    assert rows[1].ts.minute == 5


def test_process_data_batch_infers_missing_date_field(mock_error_log_dir):
    # Given: a payload row without `date` plus a deterministic `now_local` reference.
    # When: running `_process_data_batch`.
    # Then: the timestamp is inferred and a valid timezone-aware `MarketData` row is produced.

    tz = ZoneInfo("America/New_York")
    start = dt.datetime(2026, 1, 26, 10, 0, tzinfo=tz)
    end = dt.datetime(2026, 1, 26, 10, 35, tzinfo=tz)

    api_payload = [
        {"open": 1, "high": 2, "low": 1, "close": 1.5, "volume": 10},
    ]

    rows = _run_batch(
        api_payload,
        start,
        end,
        tz,
        now_local=dt.datetime(2026, 1, 26, 10, 30, tzinfo=tz),
    )

    assert len(rows) == 1
    assert rows[0].symbol == "AAPL"
    inferred_ts = rows[0].ts
    assert isinstance(inferred_ts, dt.datetime)
    assert inferred_ts.tzinfo == tz


def test_process_data_batch_handles_empty_api_response():
    # Given: an empty API response payload.
    # When: processing through `_process_data_batch`.
    # Then: no staging rows are created.

    tz = ZoneInfo("America/New_York")
    start = dt.datetime(2026, 1, 26, 10, 0, tzinfo=tz)
    end = dt.datetime(2026, 1, 26, 10, 30, tzinfo=tz)

    api_payload = []

    rows = _run_batch(api_payload, start, end, tz)

    assert len(rows) == 0


def test_process_data_batch_inserts_rows_with_missing_fields(mock_error_log_dir):
    # Given: mixed payload rows (out-of-window, missing close, missing date).
    # When: processing the batch with normal source metadata.
    # Then: valid/inferable rows are returned as `MarketData` objects with expected defaults.

    tz = ZoneInfo("America/New_York")
    start = dt.datetime(2026, 1, 26, 10, 0, tzinfo=tz)
    end = dt.datetime(2026, 1, 26, 10, 30, tzinfo=tz)

    api_payload = [
        {"date": "2026-01-26 09:55:00", "open": 1, "high": 2, "low": 1, "close": 1.5, "volume": 10},
        {"date": "2026-01-26 10:05:00", "open": 1, "high": 2, "low": 1, "close": None, "volume": 10},
        {"open": 1, "high": 2, "low": 1, "close": 1.5, "volume": 10},
    ]

    rows = _run_batch(api_payload, start, end, tz, source_url="FMP_intraday")

    assert len(rows) >= 1
    assert len(rows) <= 2
    for row in rows:
        assert isinstance(row, MarketData)
        assert row.symbol == "AAPL"
        assert row.source == "FMP_intraday"
        assert row.asset_type == "stock"


def test_process_data_batch_rejects_invalid_and_logs_load_errors(mock_error_log_dir):
    # Given: payload rows containing type mismatch, duplicate timestamp, and non-standard timestamp format.
    # When: processing the batch.
    # Then: only one clean row is accepted and invalid cases are written to DB error logs.

    tz = ZoneInfo("America/New_York")
    start = dt.datetime(2026, 1, 26, 10, 0, tzinfo=tz)
    end = dt.datetime(2026, 1, 26, 10, 30, tzinfo=tz)

    api_payload = [
        {"date": "2026-01-26 10:05:00", "open": 1.0, "high": 2.0, "low": 1.0, "close": 1.5, "volume": 10},
        {"date": "2026-01-26 10:10:00", "open": "bad", "high": 2.0, "low": 1.0, "close": 1.5, "volume": 10},
        {"date": "2026-01-26T10:15:00", "open": 1.1, "high": 2.1, "low": 1.1, "close": 1.6, "volume": 11},
        {"date": "2026-01-26 10:05:00", "open": 1.2, "high": 2.2, "low": 1.2, "close": 1.7, "volume": 12},
    ]

    rows = _run_batch(api_payload, start, end, tz)

    assert len(rows) == 1
    assert rows[0].symbol == "AAPL"

    log_file = mock_error_log_dir / "db_insert_errors.csv"
    assert log_file.exists()
    lines = log_file.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 4
    assert all("AAPL" in line for line in lines[1:])


def test_process_data_batch_logs_duplicate_timestamps(mock_error_log_dir):
    # Given: two rows with the same symbol and timestamp in one batch.
    # When: processing for staging insertion.
    # Then: one row survives dedupe and one duplicate conflict is logged.

    tz = ZoneInfo("America/New_York")
    start = dt.datetime(2026, 1, 26, 10, 0, tzinfo=tz)
    end = dt.datetime(2026, 1, 26, 10, 30, tzinfo=tz)

    api_payload = [
        {"date": "2026-01-26 10:05:00", "open": 1.0, "high": 2.0, "low": 1.0, "close": 1.5, "volume": 10},
        {"date": "2026-01-26 10:05:00", "open": 1.2, "high": 2.2, "low": 1.2, "close": 1.7, "volume": 12},
    ]

    rows = _run_batch(api_payload, start, end, tz)

    assert len(rows) == 1
    assert rows[0].symbol == "AAPL"

    log_file = mock_error_log_dir / "db_insert_errors.csv"
    assert log_file.exists()
    lines = log_file.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    assert "AAPL" in lines[1]


def test_process_data_batch_logs_schema_type_mismatch(mock_error_log_dir):
    # Given: payload rows with invalid numeric types and missing timestamp data.
    # When: processing the batch.
    # Then: all rows are rejected and schema/data issues are captured in error logs.

    tz = ZoneInfo("America/New_York")
    start = dt.datetime(2026, 1, 26, 10, 0, tzinfo=tz)
    end = dt.datetime(2026, 1, 26, 10, 30, tzinfo=tz)

    api_payload = [
        {"date": "2026-01-26 10:05:00", "open": "oops", "high": 2.0, "low": 1.0, "close": 1.5, "volume": 10},
        {"date": "2026-01-26 10:10:00", "open": 1.0, "high": 2.0, "low": 1.0, "close": 1.5, "volume": "bad"},
        {"open": 1.0, "high": 2.0, "low": 1.0, "close": 1.5, "volume": 10},
    ]

    rows = _run_batch(api_payload, start, end, tz)

    assert len(rows) == 0

    log_file = mock_error_log_dir / "db_insert_errors.csv"
    assert log_file.exists()
    lines = log_file.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 4
    assert all("AAPL" in line for line in lines[1:])
