import datetime as dt
import pytest
from pathlib import Path
from zoneinfo import ZoneInfo

from src import load_historical_data
from tests.conftest import FakeConnection


# Test 1: Test safe_table_name_for_symbol (same as auto_data_collection)
def test_safe_table_name_for_symbol():
    # Given: A valid stock symbol
    symbol = "AAPL"
    
    # When: Converting the symbol to a safe table name
    result = load_historical_data.safe_table_name_for_symbol(symbol)
    
    # Then: The result should be lowercase with market schema prefix
    assert result == "market.aapl"


# Test 2: Test parse_timestamp with different formats
def test_parse_timestamp_basic():
    # Given: A timestamp string in standard format
    ts_str = "2024-01-15 09:30:00"
    
    # When: Parsing the timestamp
    result = load_historical_data.parse_timestamp(ts_str)
    
    # Then: Should return a datetime with timezone
    assert result.year == 2024
    assert result.month == 1
    assert result.day == 15
    assert result.hour == 9
    assert result.minute == 30
    assert result.tzinfo is not None


# Test 3: Test parse_timestamp with microseconds
def test_parse_timestamp_with_microseconds():
    # Given: A timestamp string with microseconds
    ts_str = "2024-01-15 09:30:00.123456"
    
    # When: Parsing the timestamp
    result = load_historical_data.parse_timestamp(ts_str)
    
    # Then: Should handle microseconds correctly
    assert result.microsecond == 123456


# Test 4: Test parse_timestamp with ISO format
def test_parse_timestamp_iso_format():
    # Given: A timestamp in ISO format
    ts_str = "2024-01-15T09:30:00"
    
    # When: Parsing the timestamp
    result = load_historical_data.parse_timestamp(ts_str)
    
    # Then: Should parse correctly
    assert result.hour == 9
    assert result.minute == 30


# Test 5: Test parse_timestamp with invalid format
def test_parse_timestamp_invalid_raises():
    # Given: An invalid timestamp string
    ts_str = "invalid-date"
    
    # When/Then: Should raise ValueError
    with pytest.raises(ValueError, match="Could not parse timestamp"):
        load_historical_data.parse_timestamp(ts_str)


# Test 6: Test CSV loading with mock connection
def test_load_csv_basic(tmp_path, monkeypatch):
    # Given: A temporary CSV file with sample data
    csv_file = tmp_path / "test.csv"
    csv_file.write_text(
        "date,symbol,open,high,low,close,volume\n"
        "2024-01-15 09:30:00,AAPL,185.50,186.00,185.25,185.75,1000000\n"
        "2024-01-15 09:35:00,AAPL,185.75,186.25,185.50,186.00,950000\n"
    )
    
    # Mock database connection
    fake_conn = FakeConnection()
    monkeypatch.setattr("psycopg.connect", lambda **kwargs: fake_conn)
    
    # When: Loading the CSV
    total = load_historical_data.load_csv_to_staging(str(csv_file))
    
    # Then: Should load 2 rows
    assert total == 2
    assert fake_conn.commits > 0


# Test 7: Test CSV loading with date filter
def test_load_csv_with_date_filter(tmp_path, monkeypatch):
    # Given: A CSV with data from multiple dates
    csv_file = tmp_path / "test.csv"
    csv_file.write_text(
        "date,symbol,open,high,low,close,volume\n"
        "2024-01-10 09:30:00,AAPL,180.00,181.00,179.50,180.50,1000000\n"
        "2024-01-15 09:30:00,AAPL,185.50,186.00,185.25,185.75,1000000\n"
        "2024-01-20 09:30:00,AAPL,190.00,191.00,189.50,190.50,1000000\n"
    )
    
    # Mock database connection
    fake_conn = FakeConnection()
    monkeypatch.setattr("psycopg.connect", lambda **kwargs: fake_conn)
    
    # When: Loading with date filter (from 2024-01-15)
    from_date = dt.date(2024, 1, 15)
    total = load_historical_data.load_csv_to_staging(str(csv_file), from_date)
    
    # Then: Should only load 2 rows (Jan 15 and Jan 20)
    assert total == 2


# Test 8: Test CSV with multiple symbols
def test_load_csv_multiple_symbols(tmp_path, monkeypatch):
    # Given: A CSV with multiple symbols
    csv_file = tmp_path / "test.csv"
    csv_file.write_text(
        "date,symbol,open,high,low,close,volume\n"
        "2024-01-15 09:30:00,AAPL,185.50,186.00,185.25,185.75,1000000\n"
        "2024-01-15 09:30:00,MSFT,375.50,376.00,375.25,375.75,500000\n"
        "2024-01-15 09:35:00,AAPL,185.75,186.25,185.50,186.00,950000\n"
    )
    
    # Mock database connection
    fake_conn = FakeConnection()
    monkeypatch.setattr("psycopg.connect", lambda **kwargs: fake_conn)
    
    # When: Loading the CSV
    total = load_historical_data.load_csv_to_staging(str(csv_file))
    
    # Then: Should load 3 rows total
    assert total == 3


# Test 9: Test idempotency - loading same data twice
def test_load_csv_idempotent(tmp_path, monkeypatch):
    # Given: A CSV file with sample data
    csv_file = tmp_path / "test.csv"
    csv_file.write_text(
        "date,symbol,open,high,low,close,volume\n"
        "2024-01-15 09:30:00,AAPL,185.50,186.00,185.25,185.75,1000000\n"
    )
    
    # Mock database connection that persists across calls
    call_count = {"count": 0}
    
    def mock_connect(**kwargs):
        call_count["count"] += 1
        return FakeConnection()
    
    monkeypatch.setattr("psycopg.connect", mock_connect)
    
    # When: Loading the CSV twice
    total1 = load_historical_data.load_csv_to_staging(str(csv_file))
    total2 = load_historical_data.load_csv_to_staging(str(csv_file))
    
    # Then: Should load same number of rows (UPSERT handles duplicates)
    assert total1 == 1
    assert total2 == 1
    assert call_count["count"] == 2  # Two separate connections


# Test 10: Test CSV with missing close price (should skip)
def test_load_csv_skips_missing_close(tmp_path, monkeypatch):
    # Given: A CSV with a row missing close price
    csv_file = tmp_path / "test.csv"
    csv_file.write_text(
        "date,symbol,open,high,low,close,volume\n"
        "2024-01-15 09:30:00,AAPL,185.50,186.00,185.25,,1000000\n"
        "2024-01-15 09:35:00,AAPL,185.75,186.25,185.50,186.00,950000\n"
    )
    
    # Mock database connection
    fake_conn = FakeConnection()
    monkeypatch.setattr("psycopg.connect", lambda **kwargs: fake_conn)
    
    # When: Loading the CSV
    total = load_historical_data.load_csv_to_staging(str(csv_file))
    
    # Then: Should only load 1 row (skipping the one without close price)
    assert total == 1


# Test 11: Test CSV with missing required columns
def test_load_csv_missing_columns_raises(tmp_path, monkeypatch):
    # Given: A CSV missing required columns
    csv_file = tmp_path / "test.csv"
    csv_file.write_text(
        "date,symbol,close\n"
        "2024-01-15 09:30:00,AAPL,185.75\n"
    )
    
    # Mock database connection
    monkeypatch.setattr("psycopg.connect", lambda **kwargs: FakeConnection())
    
    # When/Then: Should raise ValueError
    with pytest.raises(ValueError, match="CSV missing required columns"):
        load_historical_data.load_csv_to_staging(str(csv_file))


# Test 12: Test nonexistent CSV file
def test_load_csv_file_not_found():
    # Given: A nonexistent file path
    csv_path = "/nonexistent/file.csv"
    
    # When/Then: Should raise FileNotFoundError
    with pytest.raises(FileNotFoundError, match="CSV file not found"):
        load_historical_data.load_csv_to_staging(csv_path)


# Test 13: Test check_table_exists passes when table exists
def test_check_table_exists_success(monkeypatch):
    # Given: A fake connection where table exists
    fake_conn = FakeConnection(table_exists=True)
    
    # When: Checking if table exists
    # Then: Should not raise (returns None on success)
    load_historical_data.check_table_exists(fake_conn, "market.aapl")


# Test 14: Test check_table_exists raises when table doesn't exist
def test_check_table_exists_failure(monkeypatch):
    # Given: A fake connection where table doesn't exist
    fake_conn = FakeConnection(table_exists=False)
    
    # When/Then: Should raise RuntimeError
    with pytest.raises(RuntimeError, match="does not exist"):
        load_historical_data.check_table_exists(fake_conn, "market.aapl")
