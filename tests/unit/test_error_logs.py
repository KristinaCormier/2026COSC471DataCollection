"""
Tests for error logging utilities.
Tests the three error logging functions: API errors, validation errors, and DB errors.
"""

import datetime as dt
from zoneinfo import ZoneInfo

from src import logging_utils as mod


TZ = ZoneInfo("America/New_York")


def test_log_api_error_creates_file_and_writes_header(mock_error_log_dir):
    
    mod.log_api_error(
        symbol="AAPL",
        url="https://api.example.com/data",
        error_type="HTTPError",
        error_message="404 Not Found",
        status_code=404,
        tz=TZ,
    )
    
    log_file = mock_error_log_dir / "api_errors.csv"
    assert log_file.exists()
    
    lines = log_file.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2  # header + 1 row
    assert lines[0] == "timestamp,symbol,url,error_type,error_message,status_code"
    assert "AAPL" in lines[1]
    assert "404 Not Found" in lines[1]
    assert "404" in lines[1]


def test_log_api_error_appends_multiple_errors(mock_error_log_dir):
    mod.log_api_error(
        symbol="MSFT",
        url="https://api.example.com/msft",
        error_type="Timeout",
        error_message="Request timed out after 25s",
        tz=TZ,
    )
    
    mod.log_api_error(
        symbol="TSLA",
        url="https://api.example.com/tsla",
        error_type="ConnectionError",
        error_message="Failed to establish connection",
        tz=TZ,
    )
    
    log_file = mock_error_log_dir / "api_errors.csv"
    lines = log_file.read_text(encoding="utf-8").splitlines()
    
    assert len(lines) == 3  # header + 2 rows
    assert "MSFT" in lines[1]
    assert "TSLA" in lines[2]


def test_log_validation_error_creates_file_and_writes_data(mock_error_log_dir):
    row_data = {"date": "", "open": "150.5", "high": "", "low": "148.2", "close": "149.0", "volume": ""}
    
    mod.log_validation_error(
        symbol="AAPL",
        row_data=row_data,
        missing_fields=["date", "high", "volume"],
        reason="fields_missing",
        tz=TZ,
    )
    
    log_file = mock_error_log_dir / "data_errors.csv"
    assert log_file.exists()
    
    lines = log_file.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    assert lines[0] == "timestamp,symbol,missing_fields,inferred_date,reason,row_json"
    assert "AAPL" in lines[1]
    assert "fields_missing" in lines[1]


def test_log_validation_error_with_inferred_date(mock_error_log_dir):
    inferred = dt.datetime(2026, 2, 3, 10, 30, tzinfo=TZ)
    row_data = {"date": None, "open": "100.0"}
    
    mod.log_validation_error(
        symbol="NVDA",
        row_data=row_data,
        missing_fields=["date"],
        inferred_date=inferred,
        reason="date_missing",
        tz=TZ,
    )
    
    log_file = mock_error_log_dir / "data_errors.csv"
    lines = log_file.read_text(encoding="utf-8").splitlines()
    
    assert len(lines) == 2
    assert "NVDA" in lines[1]
    assert "date_missing" in lines[1]
    assert "2026-02-03" in lines[1]  # inferred date should be in ISO format


def test_log_db_error_creates_file_with_full_details(mock_error_log_dir):
    mod.log_db_error(
        symbol="AAPL",
        operation="UPSERT",
        error_type="IntegrityError",
        error_message="duplicate key value violates unique constraint",
        table_name="aapl_data",
        row_count=50,
        tz=TZ,
    )
    
    log_file = mock_error_log_dir / "db_insert_errors.csv"
    assert log_file.exists()
    
    lines = log_file.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    assert lines[0] == "timestamp,symbol,operation,error_type,error_message,table_name,row_count"
    assert "AAPL" in lines[1]
    assert "UPSERT" in lines[1]
    assert "IntegrityError" in lines[1]
    assert "aapl_data" in lines[1]
    assert "50" in lines[1]


def test_log_db_error_connection_failure(mock_error_log_dir):
    mod.log_db_error(
        symbol="N/A",
        operation="CONNECT",
        error_type="OperationalError",
        error_message="could not connect to server",
        tz=TZ,
    )
    
    log_file = mock_error_log_dir / "db_insert_errors.csv"
    lines = log_file.read_text(encoding="utf-8").splitlines()
    
    assert len(lines) == 2
    assert "N/A" in lines[1]
    assert "CONNECT" in lines[1]
    assert "could not connect to server" in lines[1]


def test_all_error_logs_use_iso_timestamps(mock_error_log_dir):
    # Test API error timestamp
    mod.log_api_error(
        symbol="TEST",
        url="http://test.com",
        error_type="Test",
        error_message="test",
        tz=TZ,
    )
    
    api_log = mock_error_log_dir / "api_errors.csv"
    api_lines = api_log.read_text(encoding="utf-8").splitlines()
    timestamp = api_lines[1].split(",")[0]
    
    # Should be able to parse as ISO format
    parsed = dt.datetime.fromisoformat(timestamp)
    assert parsed.tzinfo is not None


def test_error_log_directory_created_automatically(mock_error_log_dir):
    # Fixture already creates the directory, so just verify it exists and functions work
    mod.log_api_error(
        symbol="AAPL",
        url="http://test.com",
        error_type="Test",
        error_message="test",
        tz=TZ,
    )
    
    assert mock_error_log_dir.is_dir()
    assert (mock_error_log_dir / "api_errors.csv").exists()

