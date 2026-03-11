import datetime as dt
from zoneinfo import ZoneInfo

import pytest

from src import intraday_data_collection as collector
from src import time_utils as tu
from models import MarketData


def test_ymd_format():
    date = dt.date(2026, 1, 26)
    result = tu.ymd(date)
    assert result == "2026-01-26"


def test_compute_window_aligns_to_5_minute_boundary_and_is_valid():
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
    tz = ZoneInfo("America/New_York")
    ts_str = "2026-01-26 10:05:00"

    parsed = tu.parse_api_time(ts_str, tz)

    assert parsed.tzinfo == tz
    assert parsed.hour == 10
    assert parsed.minute == 5


def test_process_data_batch_excludes_out_of_range_rows():
    tz = ZoneInfo("America/New_York")

    start = dt.datetime(2026, 1, 26, 10, 0, tzinfo=tz)
    end = dt.datetime(2026, 1, 26, 10, 30, tzinfo=tz)

    api_payload = [
        {"date": "2026-01-26 09:55:00", "open": 1, "high": 2, "low": 1, "close": 1.5, "volume": 10},
    ]

    collector.TZ = tz

    rows = collector._process_data_batch(
        api_payload,
        "AAPL",
        start,
        end,
        dt.datetime(2026, 1, 26, 10, 30, tzinfo=tz),
    )

    assert len(rows) == 0


def test_process_data_batch_allows_missing_close(mock_error_log_dir):
    tz = ZoneInfo("America/New_York")
    start = dt.datetime(2026, 1, 26, 10, 0, tzinfo=tz)
    end = dt.datetime(2026, 1, 26, 10, 30, tzinfo=tz)

    api_payload = [
        {"date": "2026-01-26 10:05:00", "open": 1, "high": 2, "low": 1, "close": None, "volume": 10},
    ]

    collector.TZ = tz

    rows = collector._process_data_batch(
        api_payload,
        "AAPL",
        start,
        end,
        dt.datetime(2026, 1, 26, 10, 30, tzinfo=tz),
    )

    assert len(rows) == 1
    assert isinstance(rows[0], MarketData)
    assert rows[0].symbol == "AAPL"
    assert rows[0].close is None


def test_process_data_batch_sorts_by_ts():
    tz = ZoneInfo("America/New_York")
    start = dt.datetime(2026, 1, 26, 10, 0, tzinfo=tz)
    end = dt.datetime(2026, 1, 26, 10, 30, tzinfo=tz)

    api_payload = [
        {"date": "2026-01-26 10:25:00", "open": 1, "high": 2, "low": 1, "close": 1.7, "volume": 10},
        {"date": "2026-01-26 10:05:00", "open": 1, "high": 2, "low": 1, "close": 1.5, "volume": 10},
    ]

    collector.TZ = tz

    rows = collector._process_data_batch(
        api_payload,
        "AAPL",
        start,
        end,
        dt.datetime(2026, 1, 26, 10, 30, tzinfo=tz),
    )

    assert len(rows) == 2
    assert rows[0].symbol == "AAPL"
    assert rows[0].ts.minute == 5
    assert rows[1].symbol == "AAPL"
    assert rows[1].ts.minute == 25


def test_process_data_batch_infers_missing_date_field(mock_error_log_dir):
    tz = ZoneInfo("America/New_York")
    start = dt.datetime(2026, 1, 26, 10, 0, tzinfo=tz)
    end = dt.datetime(2026, 1, 26, 10, 35, tzinfo=tz)

    api_payload = [
        {"open": 1, "high": 2, "low": 1, "close": 1.5, "volume": 10},
    ]

    collector.TZ = tz

    rows = collector._process_data_batch(
        api_payload,
        "AAPL",
        start,
        end,
        dt.datetime(2026, 1, 26, 10, 30, tzinfo=tz),
    )

    assert len(rows) == 1
    assert rows[0].symbol == "AAPL"
    inferred_ts = rows[0].ts
    assert isinstance(inferred_ts, dt.datetime)
    assert inferred_ts.tzinfo == tz


def test_process_data_batch_handles_empty_api_response():
    tz = ZoneInfo("America/New_York")
    start = dt.datetime(2026, 1, 26, 10, 0, tzinfo=tz)
    end = dt.datetime(2026, 1, 26, 10, 30, tzinfo=tz)

    api_payload = []

    collector.TZ = tz

    rows = collector._process_data_batch(
        api_payload,
        "AAPL",
        start,
        end,
        dt.datetime(2026, 1, 26, 10, 30, tzinfo=tz),
    )

    assert len(rows) == 0


def test_process_data_batch_inserts_rows_with_missing_fields(mock_error_log_dir):
    tz = ZoneInfo("America/New_York")
    start = dt.datetime(2026, 1, 26, 10, 0, tzinfo=tz)
    end = dt.datetime(2026, 1, 26, 10, 30, tzinfo=tz)

    api_payload = [
        {"date": "2026-01-26 09:55:00", "open": 1, "high": 2, "low": 1, "close": 1.5, "volume": 10},
        {"date": "2026-01-26 10:05:00", "open": 1, "high": 2, "low": 1, "close": None, "volume": 10},
        {"open": 1, "high": 2, "low": 1, "close": 1.5, "volume": 10},
    ]

    collector.TZ = tz

    rows = collector._process_data_batch(
        api_payload,
        "AAPL",
        start,
        end,
        dt.datetime(2026, 1, 26, 10, 30, tzinfo=tz),
    )

    assert len(rows) >= 1
    assert len(rows) <= 2
    for row in rows:
        assert isinstance(row, MarketData)
        assert row.symbol == "AAPL"
        assert row.source == "FMP_intraday"
        assert row.asset_type == collector.ASSET_TYPE


def test_process_data_batch_rejects_invalid_and_logs_load_errors(mock_error_log_dir):
    tz = ZoneInfo("America/New_York")
    start = dt.datetime(2026, 1, 26, 10, 0, tzinfo=tz)
    end = dt.datetime(2026, 1, 26, 10, 30, tzinfo=tz)

    api_payload = [
        {"date": "2026-01-26 10:05:00", "open": 1.0, "high": 2.0, "low": 1.0, "close": 1.5, "volume": 10},
        {"date": "2026-01-26 10:10:00", "open": "bad", "high": 2.0, "low": 1.0, "close": 1.5, "volume": 10},
        {"date": "2026-01-26T10:15:00", "open": 1.1, "high": 2.1, "low": 1.1, "close": 1.6, "volume": 11},
        {"date": "2026-01-26 10:05:00", "open": 1.2, "high": 2.2, "low": 1.2, "close": 1.7, "volume": 12},
    ]

    collector.TZ = tz

    rows = collector._process_data_batch(
        api_payload,
        "AAPL",
        start,
        end,
        dt.datetime(2026, 1, 26, 10, 30, tzinfo=tz),
    )

    assert len(rows) == 1
    assert rows[0].symbol == "AAPL"

    log_file = mock_error_log_dir / "db_insert_errors.csv"
    assert log_file.exists()
    lines = log_file.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 4
    assert all("AAPL" in line for line in lines[1:])


def test_process_data_batch_logs_duplicate_timestamps(mock_error_log_dir):
    tz = ZoneInfo("America/New_York")
    start = dt.datetime(2026, 1, 26, 10, 0, tzinfo=tz)
    end = dt.datetime(2026, 1, 26, 10, 30, tzinfo=tz)

    api_payload = [
        {"date": "2026-01-26 10:05:00", "open": 1.0, "high": 2.0, "low": 1.0, "close": 1.5, "volume": 10},
        {"date": "2026-01-26 10:05:00", "open": 1.2, "high": 2.2, "low": 1.2, "close": 1.7, "volume": 12},
    ]

    collector.TZ = tz

    rows = collector._process_data_batch(
        api_payload,
        "AAPL",
        start,
        end,
        dt.datetime(2026, 1, 26, 10, 30, tzinfo=tz),
    )

    assert len(rows) == 1
    assert rows[0].symbol == "AAPL"

    log_file = mock_error_log_dir / "db_insert_errors.csv"
    assert log_file.exists()
    lines = log_file.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    assert "AAPL" in lines[1]


def test_process_data_batch_logs_schema_type_mismatch(mock_error_log_dir):
    tz = ZoneInfo("America/New_York")
    start = dt.datetime(2026, 1, 26, 10, 0, tzinfo=tz)
    end = dt.datetime(2026, 1, 26, 10, 30, tzinfo=tz)

    api_payload = [
        {"date": "2026-01-26 10:05:00", "open": "oops", "high": 2.0, "low": 1.0, "close": 1.5, "volume": 10},
        {"date": "2026-01-26 10:10:00", "open": 1.0, "high": 2.0, "low": 1.0, "close": 1.5, "volume": "bad"},
        {"open": 1.0, "high": 2.0, "low": 1.0, "close": 1.5, "volume": 10},
    ]

    collector.TZ = tz

    rows = collector._process_data_batch(
        api_payload,
        "AAPL",
        start,
        end,
        dt.datetime(2026, 1, 26, 10, 30, tzinfo=tz),
    )

    assert len(rows) == 0

    log_file = mock_error_log_dir / "db_insert_errors.csv"
    assert log_file.exists()
    lines = log_file.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 4
    assert all("AAPL" in line for line in lines[1:])


@pytest.fixture(autouse=True)
def restore_collector_globals():
    original_api_key = collector.API_KEY
    original_base_url = collector.BASE_URL
    original_tz = collector.TZ
    yield
    collector.API_KEY = original_api_key
    collector.BASE_URL = original_base_url
    collector.TZ = original_tz