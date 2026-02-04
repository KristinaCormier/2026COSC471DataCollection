"""
Pipeline tests for end-to-end execution of auto_data_collection.
These tests validate the main() function and complete execution flow.
"""

import datetime as dt
import os
import sys
import pytest
from unittest.mock import MagicMock

from src import auto_data_collection as collector
from tests.conftest import FakeResponse, FakeConnection, FakeCursor


# Setup

@pytest.fixture
def mock_env_complete(monkeypatch):
    """Set up complete environment for main() execution."""
    monkeypatch.setenv("FMP_API_KEY", "test_api_key")
    monkeypatch.setenv("SYMBOLS", "AAPL,MSFT")
    monkeypatch.setenv("MARKET_TZ", "America/New_York")
    monkeypatch.setenv("WINDOW_MINUTES", "60")
    monkeypatch.setenv("PGHOST", "localhost")
    monkeypatch.setenv("PGPORT", "5432")
    monkeypatch.setenv("PGDATABASE", "test_db")
    monkeypatch.setenv("PGUSER", "test_user")
    monkeypatch.setenv("PGPASSWORD", "test_pass")


@pytest.fixture
def mock_successful_api(monkeypatch):
    """Mock successful API responses."""
    def mock_get(url, params=None, timeout=None):
        return FakeResponse([
            {
                "date": "2026-02-02 10:05:00",
                "open": 150.0,
                "high": 151.0,
                "low": 149.5,
                "close": 150.5,
                "volume": 1000000
            }
        ])
    monkeypatch.setattr("requests.get", mock_get)


# Execute

# Test 1: main() exits when API_KEY is missing (Modus Tollens)
# Modus Tollens Logic:
#   P → Q: If API_KEY is set, then main() should proceed to data collection
#   ¬Q: main() exits with code 1 before data collection
#   ∴ ¬P: Therefore, API_KEY was NOT set (validation working)
@pytest.mark.pipeline
def test_main_exits_when_api_key_missing(monkeypatch, capsys):
    # Given: Environment without API_KEY
    monkeypatch.setenv("FMP_API_KEY", "")
    monkeypatch.setenv("SYMBOLS", "AAPL")
    monkeypatch.setenv("PGPORT", "5432")
    
    # When: Running main() and expecting system exit (¬Q)
    with pytest.raises(SystemExit) as exc_info:
        collector.main()
    
    # Then: Should exit with code 1 (¬Q observed, proving ¬P)
    assert exc_info.value.code == 1
    
    captured = capsys.readouterr()
    assert "API key is missing" in captured.err


# Test 2: main() exits when database connection fails (Modus Tollens)
# Modus Tollens Logic:
#   P → Q: If DB credentials are valid, then main() should connect successfully
#   ¬Q: main() exits with code 2 due to connection failure
#   ∴ ¬P: Therefore, DB credentials were NOT valid (error handling working)
@pytest.mark.pipeline
def test_main_exits_when_db_connection_fails(mock_env_complete, monkeypatch, capsys):
    # Given: Valid API key but failing DB connection
    def mock_db_connect_fail():
        raise Exception("Connection refused")
    
    monkeypatch.setattr(collector, "db_connect", mock_db_connect_fail)
    
    # When: Running main() and expecting system exit (¬Q)
    with pytest.raises(SystemExit) as exc_info:
        collector.main()
    
    # Then: Should exit with code 2 (¬Q observed, proving ¬P)
    assert exc_info.value.code == 2
    
    captured = capsys.readouterr()
    assert "cannot connect to Postgres" in captured.err


# Test 3: main() processes symbols and returns successfully
@pytest.mark.pipeline
def test_main_successful_execution(mock_env_complete, mock_successful_api, monkeypatch, capsys):
    # Given: Complete valid environment and mocked successful responses
    mock_conn = FakeConnection()
    
    def mock_db_connect():
        return mock_conn
    
    def mock_execute_values(cur, sql, rows, template=None):
        pass
    
    monkeypatch.setattr(collector, "db_connect", mock_db_connect)
    monkeypatch.setattr(collector, "execute_values", mock_execute_values)
    
    # When: Running main()
    collector.main()
    
    # Then: Should complete successfully
    captured = capsys.readouterr()
    assert "[Collector Startup]" in captured.out
    assert "AAPL" in captured.out or "MSFT" in captured.out
    assert "[done]" in captured.out
    assert mock_conn.closed


# Test 4: main() continues processing after individual symbol error (Modus Tollens)
# Modus Tollens Logic:
#   P → Q: If all symbols process successfully, then no error messages appear
#   ¬Q: Error message appears for one symbol
#   ∴ ¬P: Therefore, NOT all symbols processed successfully (error handling working)
@pytest.mark.pipeline
def test_main_continues_after_symbol_error(mock_env_complete, monkeypatch, capsys):
    # Given: Environment with multiple symbols, one will fail
    mock_conn = FakeConnection()
    call_count = {"count": 0}
    
    def mock_db_connect():
        return mock_conn
    
    def mock_fetch_and_insert(conn, symbol, start, end):
        call_count["count"] += 1
        if symbol == "AAPL":
            raise Exception("API rate limit exceeded")
        return 5  # Successful for other symbols
    
    monkeypatch.setattr(collector, "db_connect", mock_db_connect)
    monkeypatch.setattr(collector, "fetch_and_insert", mock_fetch_and_insert)
    
    # When: Running main()
    collector.main()
    
    # Then: Should process both symbols (¬Q observed for AAPL, proving ¬P)
    captured = capsys.readouterr()
    assert "[error] AAPL:" in captured.err
    assert "API rate limit exceeded" in captured.err
    assert call_count["count"] == 2  # Both symbols attempted
    assert mock_conn.closed


# Test 5: main() loads environment variables at runtime
@pytest.mark.pipeline
def test_main_loads_env_vars_at_runtime(monkeypatch, capsys):
    # Given: Environment variables set after import
    monkeypatch.setenv("FMP_API_KEY", "runtime_key")
    monkeypatch.setenv("SYMBOLS", "GOOGL")
    monkeypatch.setenv("MARKET_TZ", "America/Chicago")
    monkeypatch.setenv("WINDOW_MINUTES", "30")
    monkeypatch.setenv("PGHOST", "testhost")
    monkeypatch.setenv("PGPORT", "5433")
    monkeypatch.setenv("PGDATABASE", "runtime_db")
    monkeypatch.setenv("PGUSER", "runtime_user")
    monkeypatch.setenv("PGPASSWORD", "runtime_pass")
    
    mock_conn = FakeConnection()
    
    def mock_db_connect():
        # Verify runtime values are loaded
        assert collector.API_KEY == "runtime_key"
        assert collector.SYMBOLS == ["GOOGL"]
        assert collector.MARKET_TZ == "America/Chicago"
        assert collector.WINDOW_MIN == 30
        assert collector.PGHOST == "testhost"
        assert collector.PGPORT == 5433
        return mock_conn
    
    def mock_fetch_and_insert(conn, symbol, start, end):
        return 0
    
    monkeypatch.setattr(collector, "db_connect", mock_db_connect)
    monkeypatch.setattr(collector, "fetch_and_insert", mock_fetch_and_insert)
    
    # When: Running main()
    collector.main()
    
    # Then: Runtime environment variables should be loaded and used
    captured = capsys.readouterr()
    assert "GOOGL" in captured.out


# Test 6: main() computes correct time window
@pytest.mark.pipeline
def test_main_computes_time_window(mock_env_complete, monkeypatch, capsys):
    # Given: Complete environment
    mock_conn = FakeConnection()
    captured_window = {"start": None, "end": None}
    
    def mock_db_connect():
        return mock_conn
    
    def mock_fetch_and_insert(conn, symbol, start, end):
        captured_window["start"] = start
        captured_window["end"] = end
        return 0
    
    monkeypatch.setattr(collector, "db_connect", mock_db_connect)
    monkeypatch.setattr(collector, "fetch_and_insert", mock_fetch_and_insert)
    
    # When: Running main()
    collector.main()
    
    # Then: Time window should be computed correctly
    assert captured_window["start"] is not None
    assert captured_window["end"] is not None
    assert captured_window["start"] < captured_window["end"]
    assert captured_window["start"].minute == 0  # Top of hour
    assert captured_window["end"].minute % 5 == 0  # 5-minute aligned


# Teardown

# (No additional teardown needed - pytest fixtures handle cleanup)
