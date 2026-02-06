import datetime as dt
import pytest
from zoneinfo import ZoneInfo

from src import auto_data_collection as collector
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


# Test 7. Test for fetch_and_insert(): out-of-range data is still inserted
def test_fetch_and_insert_inserts_out_of_range_rows(monkeypatch):
    # Given: A time window and API data outside that window
    tz = ZoneInfo("America/New_York")
    collector.API_KEY = "test"
    collector.BASE_URL = "https://example.test/{symbol}"

    start = dt.datetime(2026, 1, 26, 10, 0, tzinfo=tz)
    end = dt.datetime(2026, 1, 26, 10, 30, tzinfo=tz)

    api_payload = [
        {"date": "2026-01-26 09:55:00", "open": 1, "high": 2, "low": 1, "close": 1.5, "volume": 10},
    ]

    monkeypatch.setattr(collector.requests, "get", lambda *args, **kwargs: FakeResponse(api_payload))
    monkeypatch.setattr(dbu, "check_table_exists", lambda *args, **kwargs: None)

    conn = FakeConnection()

    # When: Fetching and inserting
    inserted = collector.fetch_and_insert(conn, "AAPL", start, end)

    # Then: Row should be inserted despite being out of range
    assert inserted == 1
    assert conn.commits == 1


# Test 8. Test for fetch_and_insert(): missing close is allowed
def test_fetch_and_insert_allows_missing_close(monkeypatch):
    # Given: A valid window and API data with missing close
    tz = ZoneInfo("America/New_York")
    collector.API_KEY = "test"
    collector.BASE_URL = "https://example.test/{symbol}"

    start = dt.datetime(2026, 1, 26, 10, 0, tzinfo=tz)
    end = dt.datetime(2026, 1, 26, 10, 30, tzinfo=tz)

    api_payload = [
        {"date": "2026-01-26 10:05:00", "open": 1, "high": 2, "low": 1, "close": None, "volume": 10},
    ]

    monkeypatch.setattr(collector.requests, "get", lambda *args, **kwargs: FakeResponse(api_payload))
    monkeypatch.setattr(dbu, "check_table_exists", lambda *args, **kwargs: None)

    conn = FakeConnection()

    # When: Fetching and inserting
    inserted = collector.fetch_and_insert(conn, "AAPL", start, end)

    # Then: Row should be inserted with a null close
    assert inserted == 1
    insert_cursor = conn.cursors[-1]
    assert insert_cursor.executed_values[0][4] is None
    assert conn.commits == 1


# Test 9. Test for fetch_and_insert(): inserts valid rows in ascending order
def test_fetch_and_insert_inserts_sorted_rows(monkeypatch):
    # Given: A valid window and two out-of-order rows
    tz = ZoneInfo("America/New_York")
    collector.API_KEY = "test"
    collector.BASE_URL = "https://example.test/{symbol}"

    start = dt.datetime(2026, 1, 26, 10, 0, tzinfo=tz)
    end = dt.datetime(2026, 1, 26, 10, 30, tzinfo=tz)

    api_payload = [
        {"date": "2026-01-26 10:25:00", "open": 1, "high": 2, "low": 1, "close": 1.7, "volume": 10},
        {"date": "2026-01-26 10:05:00", "open": 1, "high": 2, "low": 1, "close": 1.5, "volume": 10},
    ]

    monkeypatch.setattr(collector.requests, "get", lambda *args, **kwargs: FakeResponse(api_payload))
    monkeypatch.setattr(dbu, "check_table_exists", lambda *args, **kwargs: None)

    conn = FakeConnection()

    # When: Fetching and inserting
    inserted = collector.fetch_and_insert(conn, "AAPL", start, end)

    # Then: Rows should be sorted ascending and inserted
    assert inserted == 2
    # Check the cursor that executed the insert
    insert_cursor = conn.cursors[-1]
    assert [r[0].minute for r in insert_cursor.executed_values] == [5, 25]
    assert "INSERT INTO market.aapl" in insert_cursor.queries[-1]


# Test 10. Test for fetch_and_insert(): missing date field is inferred
def test_fetch_and_insert_infers_missing_date_field(monkeypatch):
    # Given: API data with missing date field
    tz = ZoneInfo("America/New_York")
    collector.API_KEY = "test"
    collector.BASE_URL = "https://example.test/{symbol}"

    start = dt.datetime(2026, 1, 26, 10, 0, tzinfo=tz)
    end = dt.datetime(2026, 1, 26, 10, 30, tzinfo=tz)

    api_payload = [
        {"open": 1, "high": 2, "low": 1, "close": 1.5, "volume": 10},  # Missing 'date' field
    ]

    monkeypatch.setattr(collector.requests, "get", lambda *args, **kwargs: FakeResponse(api_payload))
    monkeypatch.setattr(dbu, "check_table_exists", lambda *args, **kwargs: None)

    conn = FakeConnection()

    # When: Fetching and inserting
    inserted = collector.fetch_and_insert(conn, "AAPL", start, end)

    # Then: Row should be inserted with inferred timestamp
    assert inserted == 1
    insert_cursor = conn.cursors[-1]
    inferred_ts = insert_cursor.executed_values[0][0]
    assert isinstance(inferred_ts, dt.datetime)
    assert inferred_ts.tzinfo == tz
    assert conn.commits == 1


# Test 11. Test for fetch_and_insert(): empty API response results in no inserts (Modus Tollens)
# Modus Tollens Logic:
#   P → Q: If API returns valid data, then rows should be inserted
#   ¬Q: No rows were inserted
#   ∴ ¬P: Therefore, API did NOT return valid data (empty response handling working)
def test_fetch_and_insert_handles_empty_api_response(monkeypatch):
    # Given: Empty API response
    tz = ZoneInfo("America/New_York")
    collector.API_KEY = "test"
    collector.BASE_URL = "https://example.test/{symbol}"

    start = dt.datetime(2026, 1, 26, 10, 0, tzinfo=tz)
    end = dt.datetime(2026, 1, 26, 10, 30, tzinfo=tz)

    api_payload = []  # Empty response

    monkeypatch.setattr(collector.requests, "get", lambda *args, **kwargs: FakeResponse(api_payload))
    monkeypatch.setattr(dbu, "check_table_exists", lambda *args, **kwargs: None)

    conn = FakeConnection()

    # When: Fetching and inserting
    inserted = collector.fetch_and_insert(conn, "AAPL", start, end)

    # Then: No rows should be inserted (¬Q observed, proving ¬P)
    assert inserted == 0
    # No cursor should be created when no rows to insert
    assert conn.commits == 0


# Test 12. Test for fetch_and_insert(): rows with missing fields are still inserted
def test_fetch_and_insert_inserts_rows_with_missing_fields(monkeypatch):
    # Given: Multiple rows with missing fields
    tz = ZoneInfo("America/New_York")
    collector.API_KEY = "test"
    collector.BASE_URL = "https://example.test/{symbol}"

    start = dt.datetime(2026, 1, 26, 10, 0, tzinfo=tz)
    end = dt.datetime(2026, 1, 26, 10, 30, tzinfo=tz)

    api_payload = [
        {"date": "2026-01-26 09:55:00", "open": 1, "high": 2, "low": 1, "close": 1.5, "volume": 10},  # Out of range
        {"date": "2026-01-26 10:05:00", "open": 1, "high": 2, "low": 1, "close": None, "volume": 10},  # Missing close
        {"open": 1, "high": 2, "low": 1, "close": 1.5, "volume": 10},  # Missing date
    ]

    monkeypatch.setattr(collector.requests, "get", lambda *args, **kwargs: FakeResponse(api_payload))
    monkeypatch.setattr(dbu, "check_table_exists", lambda *args, **kwargs: None)

    conn = FakeConnection()

    # When: Fetching and inserting
    inserted = collector.fetch_and_insert(conn, "AAPL", start, end)

    # Then: Rows should be inserted even with missing fields
    assert inserted == 3
    assert conn.commits == 1

# Teardown


@pytest.fixture(autouse=True)
def restore_collector_globals():
    original_api_key = collector.API_KEY
    original_base_url = collector.BASE_URL
    yield
    collector.API_KEY = original_api_key
    collector.BASE_URL = original_base_url