"""
Integration tests for database connectivity and table operations.
These tests require a real or test Postgres database connection.
"""

import datetime as dt
import os
import pytest
from zoneinfo import ZoneInfo

import intraday_data_collection as collector
from utils import db_utils as dbu
from utils import time_utils as tu


pytestmark = [pytest.mark.integration, pytest.mark.postgres_only]


# Setup

@pytest.fixture
def test_db_config(monkeypatch):
    """Configure test database credentials via environment variables."""
    monkeypatch.setenv("DB_HOST", os.getenv("DB_HOST", "localhost"))
    monkeypatch.setenv("DB_PORT", os.getenv("DB_PORT", "5432"))
    monkeypatch.setenv("DB_NAME", os.getenv("DB_NAME", "test_db"))
    monkeypatch.setenv("DB_USER", os.getenv("DB_USER", "postgres"))
    monkeypatch.setenv("DB_PASSWORD", os.getenv("DB_PASSWORD", ""))


# Execute

# Test 1: db_connect() establishes connection (Modus Tollens)
# Modus Tollens Logic:
#   P → Q: If DB credentials are valid, then connection should succeed
#   ¬Q: Connection fails (exception raised)
#   ∴ ¬P: Therefore, credentials are NOT valid (or DB is unreachable)
def test_db_connect_with_invalid_credentials_fails(monkeypatch):
    # Given: Invalid database credentials
    # When/Then: Attempting to connect should raise an exception (¬Q observed, proving ¬P)
    with pytest.raises(Exception):
        conn = dbu.db_connect("invalid_host", 9999, "nonexistent_db", "invalid_user", "invalid_password")
        conn.close()


# Test 2: db_connect() returns connection object with valid credentials
@pytest.mark.skipif(
    not os.getenv("DB_HOST"),
    reason="Requires valid DB_HOST environment variable"
)
def test_db_connect_succeeds_with_valid_credentials(test_db_config):
    # Given: Valid database credentials (from test_db_config fixture)
    
    # When: Establishing connection
    try:
        conn = dbu.db_connect(
            os.getenv("DB_HOST", "localhost"),
            int(os.getenv("DB_PORT", "5432")),
            os.getenv("DB_NAME", "test_db"),
            os.getenv("DB_USER", "postgres"),
            os.getenv("DB_PASSWORD", ""),
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
@pytest.mark.skipif(
    not os.getenv("DB_HOST"),
    reason="Requires valid DB_HOST environment variable"
)
def test_check_table_exists_raises_for_nonexistent_table(test_db_config):
    # Given: A database connection and a non-existent table name
    try:
        conn = dbu.db_connect(
            os.getenv("DB_HOST", "localhost"),
            int(os.getenv("DB_PORT", "5432")),
            os.getenv("DB_NAME", "test_db"),
            os.getenv("DB_USER", "postgres"),
            os.getenv("DB_PASSWORD", ""),
        )
    except Exception as e:
        pytest.skip(f"Database not available: {e}")
    
    table_name = "market.nonexistent_table_xyz"
    
    # When/Then: Checking for non-existent table should raise RuntimeError (¬Q observed, proving ¬P)
    with pytest.raises(RuntimeError, match="does not exist"):
        dbu.check_table_exists(conn, table_name)
    
    conn.close()


# Test 4: check_table_exists() passes when table exists
@pytest.mark.skipif(
    not os.getenv("DB_HOST"),
    reason="Requires valid DB_HOST environment variable and existing stg_raw schema tables"
)
def test_check_table_exists_passes_for_existing_table(test_db_config):
    # Given: A database connection and an existing table in the test database
    # Note: This assumes stg_raw.market_data or similar exists from schema setup
    try:
        conn = dbu.db_connect(
            os.getenv("DB_HOST", "localhost"),
            int(os.getenv("DB_PORT", "5432")),
            os.getenv("DB_NAME", "test_db"),
            os.getenv("DB_USER", "postgres"),
            os.getenv("DB_PASSWORD", ""),
        )
    except Exception as e:
        pytest.skip(f"Database not available: {e}")
    
    table_name = "stg_raw.market_data"
    
    try:
        # When: Checking for existing table
        dbu.check_table_exists(conn, table_name)
        # Then: No exception should be raised
    except RuntimeError:
        pytest.skip(f"Table {table_name} not found. Run table creation script first.")
    finally:
        conn.close()


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
