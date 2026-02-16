import datetime as dt
import pytest
from zoneinfo import ZoneInfo

from src import intraday_data_collection as collector
from src import time_utils as tu
from src import db_utils as dbu
from tests.conftest import FakeResponse, FakeConnection


# Execute

# Test 1. Test for safe_table_name_for_symbol()
def test_safe_table_name_for_symbol_basic():
    # Given: A valid stock symbol in uppercase
    symbol = "AAPL"

    # When: Converting the symbol to a safe table name
    result = dbu.safe_table_name_for_symbol(symbol)

    # Then: The result should be lowercase with market schema prefix
    assert result == "market.aapl"


# Test 2. Test for safe_table_name_for_symbol()
def test_safe_table_name_for_symbol_strips_non_alnum():
    # Given: A symbol with non-alphanumeric characters
    symbol = "^TNX"

    # When: Converting the symbol to a safe table name
    result = dbu.safe_table_name_for_symbol(symbol)

    # Then: Non-alphanumeric characters should be stripped
    assert result == "market.tnx"


# Test 3. Test for safe_table_name_for_symbol()
def test_safe_table_name_for_symbol_invalid_raises():
    # Given: A symbol containing only invalid characters
    invalid_symbol = "$$$"

    # When/Then: Converting the invalid symbol should raise ValueError
    with pytest.raises(ValueError):
        dbu.safe_table_name_for_symbol(invalid_symbol)


# Test 4. Test for ymd()
def test_ymd_format():
    # Given: A date object
    date = dt.date(2026, 1, 26)

    # When: Formatting the date using ymd()
    result = tu.ymd(date)

    # Then: The result should be in ISO format (YYYY-MM-DD)
    assert result == "2026-01-26"


# Test 5. Test for compute_window()
def test_compute_window_aligns_to_5_minute_boundary_and_is_valid():
    # Given: A specific timestamp (15:27:42) and a 60-minute window
    tz = ZoneInfo("America/New_York")
    now = dt.datetime(2026, 1, 26, 15, 27, 42, tzinfo=tz)
    window_min = 60

    # When: Computing the time window
    start, end = tu.compute_window(now, window_min=window_min)

    # Then: Start should be at top of hour with zero seconds/microseconds
    assert start.minute == 0 and start.second == 0 and start.microsecond == 0

    # Then: End should be aligned to 5-minute boundary
    assert end.second == 0 and end.microsecond == 0
    assert end.minute % 5 == 0

    # Then: End should be after start and within window limit
    assert end > start
    assert (end - start) <= dt.timedelta(minutes=window_min)


# Test 6. Test for parse_api_time()
def test_parse_api_time_uses_market_tz():
    # Given: A timestamp string and a known timezone
    tz = ZoneInfo("America/New_York")
    ts_str = "2026-01-26 10:05:00"

    # When: Parsing the API time
    parsed = tu.parse_api_time(ts_str, tz)

    # Then: Parsed datetime should include the configured timezone
    assert parsed.tzinfo == tz
    assert parsed.hour == 10 and parsed.minute == 5


# Test 7. Test for _process_data_batch(): out-of-range data is still processed
def test_process_data_batch_includes_out_of_range_rows(monkeypatch):
    # Given: A time window and API data outside that window
    tz = ZoneInfo("America/New_York")

    start = dt.datetime(2026, 1, 26, 10, 0, tzinfo=tz)
    end = dt.datetime(2026, 1, 26, 10, 30, tzinfo=tz)

    api_payload = [
        {"date": "2026-01-26 09:55:00", "open": 1, "high": 2, "low": 1, "close": 1.5, "volume": 10},
    ]

    collector.TZ = tz
    monkeypatch.setattr(dbu, "check_table_exists", lambda *args, **kwargs: None)

    # When: Processing the data batch
    rows = collector._process_data_batch(api_payload, "AAPL", "market.aapl", None)

    # Then: Row should be processed despite being out of range
    assert len(rows) == 1
    assert rows[0][0].minute == 55


# Test 8. Test for _process_data_batch(): missing close is allowed
def test_process_data_batch_allows_missing_close(monkeypatch, mock_error_log_dir):
    # Given: A valid window and API data with missing close
    tz = ZoneInfo("America/New_York")

    api_payload = [
        {"date": "2026-01-26 10:05:00", "open": 1, "high": 2, "low": 1, "close": None, "volume": 10},
    ]

    collector.TZ = tz
    monkeypatch.setattr(dbu, "check_table_exists", lambda *args, **kwargs: None)

    # When: Processing the data batch
    rows = collector._process_data_batch(api_payload, "AAPL", "market.aapl", None)

    # Then: Row should be processed with a null close
    assert len(rows) == 1
    assert rows[0][4] is None


# Test 9. Test for _process_data_batch(): inserts valid rows in ascending order
def test_process_data_batch_inserts_sorted_rows(monkeypatch):
    # Given: A valid window and two out-of-order rows
    tz = ZoneInfo("America/New_York")

    api_payload = [
        {"date": "2026-01-26 10:25:00", "open": 1, "high": 2, "low": 1, "close": 1.7, "volume": 10},
        {"date": "2026-01-26 10:05:00", "open": 1, "high": 2, "low": 1, "close": 1.5, "volume": 10},
    ]

    collector.TZ = tz
    monkeypatch.setattr(dbu, "check_table_exists", lambda *args, **kwargs: None)

    # When: Processing the data batch
    rows = collector._process_data_batch(api_payload, "AAPL", "market.aapl", None)

    # Then: Rows should be sorted ascending by timestamp
    assert len(rows) == 2
    assert rows[0][0].minute == 5
    assert rows[1][0].minute == 25


# Test 10. Test for _process_data_batch(): missing date field is inferred
def test_process_data_batch_infers_missing_date_field(monkeypatch, mock_error_log_dir):
    # Given: API data with missing date field
    tz = ZoneInfo("America/New_York")

    api_payload = [
        {"open": 1, "high": 2, "low": 1, "close": 1.5, "volume": 10},  # Missing 'date' field
    ]

    collector.TZ = tz
    monkeypatch.setattr(dbu, "check_table_exists", lambda *args, **kwargs: None)

    # When: Processing the data batch
    rows = collector._process_data_batch(api_payload, "AAPL", "market.aapl", None)

    # Then: Row should be processed with inferred timestamp
    assert len(rows) == 1
    inferred_ts = rows[0][0]
    assert isinstance(inferred_ts, dt.datetime)
    assert inferred_ts.tzinfo == tz


# Test 11. Test for _process_data_batch(): empty API response results in no batch
# Modus Tollens Logic:
#   P → Q: If API returns valid data, then rows should be processed
#   ¬Q: No rows were processed
#   ∴ ¬P: Therefore, API did NOT return valid data (empty response handling working)
def test_process_data_batch_handles_empty_api_response(monkeypatch):
    # Given: Empty API response
    tz = ZoneInfo("America/New_York")

    api_payload = []  # Empty response

    collector.TZ = tz
    monkeypatch.setattr(dbu, "check_table_exists", lambda *args, **kwargs: None)

    # When: Processing the data batch
    rows = collector._process_data_batch(api_payload, "AAPL", "market.aapl", None)

    # Then: No rows should be processed (¬Q observed, proving ¬P)
    assert len(rows) == 0


# Test 12. Test for _process_data_batch(): rows with missing fields are still processed
def test_process_data_batch_inserts_rows_with_missing_fields(monkeypatch, mock_error_log_dir):
    # Given: Multiple rows with missing fields
    tz = ZoneInfo("America/New_York")

    api_payload = [
        {"date": "2026-01-26 09:55:00", "open": 1, "high": 2, "low": 1, "close": 1.5, "volume": 10},  # Out of range
        {"date": "2026-01-26 10:05:00", "open": 1, "high": 2, "low": 1, "close": None, "volume": 10},  # Missing close
        {"open": 1, "high": 2, "low": 1, "close": 1.5, "volume": 10},  # Missing date
    ]

    collector.TZ = tz
    monkeypatch.setattr(dbu, "check_table_exists", lambda *args, **kwargs: None)

    # When: Processing the data batch
    rows = collector._process_data_batch(api_payload, "AAPL", "market.aapl", None)

    # Then: Valid rows should be processed
    assert len(rows) >= 1
    assert len(rows) <= 3


# Test 13. Test for _process_data_batch(): invalid/duplicate rows are rejected and logged
def test_process_data_batch_rejects_invalid_and_logs_load_errors(monkeypatch, mock_error_log_dir):
    # Given: API data with invalid types, invalid timestamps, and duplicate timestamps
    tz = ZoneInfo("America/New_York")

    api_payload = [
        {"date": "2026-01-26 10:05:00", "open": 1.0, "high": 2.0, "low": 1.0, "close": 1.5, "volume": 10},
        {"date": "2026-01-26 10:10:00", "open": "bad", "high": 2.0, "low": 1.0, "close": 1.5, "volume": 10},
        {"date": "2026-01-26T10:15:00", "open": 1.1, "high": 2.1, "low": 1.1, "close": 1.6, "volume": 11},
        {"date": "2026-01-26 10:05:00", "open": 1.2, "high": 2.2, "low": 1.2, "close": 1.7, "volume": 12},
    ]

    collector.TZ = tz
    monkeypatch.setattr(dbu, "check_table_exists", lambda *args, **kwargs: None)

    # When: Processing the data batch
    rows = collector._process_data_batch(api_payload, "AAPL", "market.aapl", None)

    # Then: Only valid, non-duplicate rows should be processed
    assert len(rows) == 1

    # Then: Invalid and duplicate rows should be logged as load errors
    log_file = mock_error_log_dir / "db_insert_errors.csv"
    assert log_file.exists()
    lines = log_file.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 4  # header + 3 error rows
    assert all("AAPL" in line for line in lines[1:])


# Test 14. Test for _process_data_batch(): duplicate timestamps are logged to db_insert_errors.csv
def test_process_data_batch_logs_duplicate_timestamps(monkeypatch, mock_error_log_dir):
    # Given: API data with duplicate timestamps
    tz = ZoneInfo("America/New_York")

    api_payload = [
        {"date": "2026-01-26 10:05:00", "open": 1.0, "high": 2.0, "low": 1.0, "close": 1.5, "volume": 10},
        {"date": "2026-01-26 10:05:00", "open": 1.2, "high": 2.2, "low": 1.2, "close": 1.7, "volume": 12},
    ]

    collector.TZ = tz
    monkeypatch.setattr(dbu, "check_table_exists", lambda *args, **kwargs: None)

    # When: Processing the data batch
    rows = collector._process_data_batch(api_payload, "AAPL", "market.aapl", None)

    # Then: Only one row should be processed
    assert len(rows) == 1

    # Then: Duplicate should be logged as a load error
    log_file = mock_error_log_dir / "db_insert_errors.csv"
    assert log_file.exists()
    lines = log_file.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2  # header + 1 duplicate error row
    assert "AAPL" in lines[1]


# Test 15. Test for _process_data_batch(): schema/type mismatch logs load errors and skips processing
def test_process_data_batch_logs_schema_type_mismatch(monkeypatch, mock_error_log_dir):
    # Given: API data with schema/type mismatches
    tz = ZoneInfo("America/New_York")

    api_payload = [
        {"date": "2026-01-26 10:05:00", "open": "oops", "high": 2.0, "low": 1.0, "close": 1.5, "volume": 10},
        {"date": "2026-01-26 10:10:00", "open": 1.0, "high": 2.0, "low": 1.0, "close": 1.5, "volume": "bad"},
        {"open": 1.0, "high": 2.0, "low": 1.0, "close": 1.5, "volume": 10},
    ]

    collector.TZ = tz
    monkeypatch.setattr(dbu, "check_table_exists", lambda *args, **kwargs: None)

    # When: Processing the data batch
    rows = collector._process_data_batch(api_payload, "AAPL", "market.aapl", None)

    # Then: No rows should be processed
    assert len(rows) == 0

    # Then: Each invalid row should be logged as a load error
    log_file = mock_error_log_dir / "db_insert_errors.csv"
    assert log_file.exists()
    lines = log_file.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 4  # header + 3 error rows
    assert all("AAPL" in line for line in lines[1:])

# Teardown


@pytest.fixture(autouse=True)
def restore_collector_globals():
    original_api_key = collector.API_KEY
    original_base_url = collector.BASE_URL
    yield
    collector.API_KEY = original_api_key
    collector.BASE_URL = original_base_url