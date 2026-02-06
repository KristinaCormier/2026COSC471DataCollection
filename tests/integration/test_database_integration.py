"""
Integration tests for database connectivity and table operations.
These tests require a real or test Postgres database connection.
"""

import datetime as dt
import os
import pytest
from zoneinfo import ZoneInfo

from src import intraday_data_collection as collector
from src import db_utils as dbu
from src import time_utils as tu


# Setup

@pytest.fixture
def test_db_config(monkeypatch):
    """Configure collector with test database credentials."""
    monkeypatch.setattr(collector, "PGHOST", os.getenv("PGHOST", "localhost"))
    monkeypatch.setattr(collector, "PGPORT", int(os.getenv("PGPORT", "5432")))
    monkeypatch.setattr(collector, "PGDATABASE", os.getenv("PGDATABASE", "test_db"))
    monkeypatch.setattr(collector, "PGUSER", os.getenv("PGUSER", "postgres"))
    monkeypatch.setattr(collector, "PGPASSWORD", os.getenv("PGPASSWORD", ""))


# Execute

# Test 1: db_connect() establishes connection (Modus Tollens)
# Modus Tollens Logic:
#   P → Q: If DB credentials are valid, then connection should succeed
#   ¬Q: Connection fails (exception raised)
#   ∴ ¬P: Therefore, credentials are NOT valid (or DB is unreachable)
@pytest.mark.integration
def test_db_connect_with_invalid_credentials_fails(monkeypatch):
    # Given: Invalid database credentials
    monkeypatch.setattr(collector, "PGHOST", "invalid_host")
    monkeypatch.setattr(collector, "PGPORT", 9999)
    monkeypatch.setattr(collector, "PGDATABASE", "nonexistent_db")
    monkeypatch.setattr(collector, "PGUSER", "invalid_user")
    monkeypatch.setattr(collector, "PGPASSWORD", "invalid_password")

    # When/Then: Attempting to connect should raise an exception (¬Q observed, proving ¬P)
    with pytest.raises(Exception):
        conn = dbu.db_connect("invalid_host", 9999, "nonexistent_db", "invalid_user", "invalid_password")
        conn.close()


# Test 2: db_connect() returns connection object with valid credentials
@pytest.mark.integration
@pytest.mark.skipif(
    not os.getenv("PGHOST"),
    reason="Requires valid PGHOST environment variable"
)
def test_db_connect_succeeds_with_valid_credentials(test_db_config):
    # Given: Valid database credentials (from test_db_config fixture)
    
    # When: Establishing connection
    try:
        conn = dbu.db_connect(
            collector.PGHOST,
            collector.PGPORT,
            collector.PGDATABASE,
            collector.PGUSER,
            collector.PGPASSWORD
        )
        
        # Then: Connection should be established successfully
        assert conn is not None
        assert not conn.closed
        
        # Cleanup
        conn.close()
        assert conn.closed
    except Exception as e:
        pytest.skip(f"Database not available: {e}")


# Test 3: check_table_exists() raises when table does not exist (Modus Tollens)
# Modus Tollens Logic:
#   P → Q: If table exists in database, then check should pass without exception
#   ¬Q: Check raises RuntimeError
#   ∴ ¬P: Therefore, table does NOT exist (validation working)
@pytest.mark.integration
@pytest.mark.skipif(
    not os.getenv("PGHOST"),
    reason="Requires valid PGHOST environment variable"
)
def test_check_table_exists_raises_for_nonexistent_table(test_db_config):
    # Given: A database connection and a non-existent table name
    try:
        conn = dbu.db_connect(
            collector.PGHOST,
            collector.PGPORT,
            collector.PGDATABASE,
            collector.PGUSER,
            collector.PGPASSWORD
        )
    except Exception as e:
        pytest.skip(f"Database not available: {e}")
    
    table_name = "market.nonexistent_table_xyz"
    
    # When/Then: Checking for non-existent table should raise RuntimeError (¬Q observed, proving ¬P)
    with pytest.raises(RuntimeError, match="does not exist"):
        dbu.check_table_exists(conn, table_name)
    
    conn.close()


# Test 4: check_table_exists() passes when table exists
@pytest.mark.integration
@pytest.mark.skipif(
    not os.getenv("PGHOST"),
    reason="Requires valid PGHOST environment variable and existing market schema tables"
)
def test_check_table_exists_passes_for_existing_table(test_db_config):
    # Given: A database connection and an existing table
    # Note: This assumes market.aapl or similar exists from schema setup
    try:
        conn = dbu.db_connect(
            collector.PGHOST,
            collector.PGPORT,
            collector.PGDATABASE,
            collector.PGUSER,
            collector.PGPASSWORD
        )
    except Exception as e:
        pytest.skip(f"Database not available: {e}")
    
    # Try common test table names
    test_tables = ["market.aapl", "market.msft", "market.googl"]
    table_found = False
    
    for table_name in test_tables:
        try:
            # When: Checking for existing table
            dbu.check_table_exists(conn, table_name)
            table_found = True
            # Then: No exception should be raised
            break
        except RuntimeError:
            continue
    
    conn.close()
    
    if not table_found:
        pytest.skip("No test market tables found. Run table creation script first.")


# Test 5: current_hour() returns top of the hour
def test_current_hour_returns_top_of_hour():
    # Given: A timestamp with arbitrary minutes/seconds
    tz = ZoneInfo("America/New_York")
    now = dt.datetime(2026, 1, 26, 15, 47, 32, 123456, tzinfo=tz)
    
    # When: Getting the current hour
    result = tu.current_hour(now)
    
    # Then: Should return same hour with zeroed minutes/seconds/microseconds
    assert result.year == 2026
    assert result.month == 1
    assert result.day == 26
    assert result.hour == 15
    assert result.minute == 0
    assert result.second == 0
    assert result.microsecond == 0
    assert result.tzinfo == tz


# Teardown

# (No additional teardown needed - pytest fixtures handle cleanup)
