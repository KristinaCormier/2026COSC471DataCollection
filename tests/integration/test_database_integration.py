"""
Integration tests for database connectivity and table operations.
These tests require a real or test Postgres database connection.
"""

import datetime as dt
import pytest
from zoneinfo import ZoneInfo

import intraday_data_collection as collector
from utils import db_utils as dbu
from utils import time_utils as tu


pytestmark = [pytest.mark.integration, pytest.mark.postgres_only]


def _connect_from_engine_url(db_engine):
    """Create a psycopg connection using the active SQLAlchemy test engine URL."""
    url = db_engine.url
    conn = dbu.db_connect(
        url.host or "localhost",
        int(url.port or 5432),
        url.database or "",
        url.username or "",
        url.password or "",
    )
    return conn


# Execute

# Test 1: db_connect() establishes connection (Modus Tollens)
# Modus Tollens Logic:
#   P → Q: If DB credentials are valid, then connection should succeed
#   ¬Q: Connection fails (exception raised)
#   ∴ ¬P: Therefore, credentials are NOT valid (or DB is unreachable)
def test_db_connect_with_invalid_credentials_fails(monkeypatch):
    # Given: obviously invalid host, port, database, and credentials.
    # When: attempting to open a psycopg connection via `db_connect`.
    # Then: connection establishment fails and raises an exception.

    with pytest.raises(Exception):
        conn = dbu.db_connect("invalid_host", 9999, "nonexistent_db", "invalid_user", "invalid_password")
        conn.close()


# Test 2: db_connect() returns connection object with valid credentials
def test_db_connect_succeeds_with_valid_credentials(db_engine):
    # Given: a live test-engine URL produced by the DB fixture.
    # When: converting that URL into a raw psycopg connection.
    # Then: the connection opens successfully and closes cleanly.

    conn = _connect_from_engine_url(db_engine)

    # Then: Connection should be established successfully
    assert conn is not None
    assert not conn.closed

    # Cleanup
    conn.close()
    assert conn.closed


# Test 3: check_table_exists() raises when table does not exist (Modus Tollens)
# Modus Tollens Logic:
#   P → Q: If table exists in database, then check should pass without exception
#   ¬Q: Check raises RuntimeError
#   ∴ ¬P: Therefore, table does NOT exist (validation working)
def test_check_table_exists_raises_for_nonexistent_table(db_engine):
    # Given: a valid database connection and a table name that should not exist.
    # When: calling `check_table_exists` for that table.
    # Then: a RuntimeError is raised with a "does not exist" message.

    conn = _connect_from_engine_url(db_engine)

    table_name = "market.nonexistent_table_xyz"
    
    # When/Then: Checking for non-existent table should raise RuntimeError (¬Q observed, proving ¬P)
    with pytest.raises(RuntimeError, match="does not exist"):
        dbu.check_table_exists(conn, table_name)
    
    conn.close()


# Test 4: check_table_exists() passes when table exists
def test_check_table_exists_passes_for_existing_table(db_engine):
    # Given: a valid database connection and an expected staging table name.
    # When: calling `check_table_exists` for `stg_raw.market_data`.
    # Then: the call succeeds without raising (or test is skipped if schema is absent).

    conn = _connect_from_engine_url(db_engine)

    table_name = "stg_raw.market_data"
    
    try:

        dbu.check_table_exists(conn, table_name)

    except RuntimeError:
        pytest.skip(f"Table {table_name} not found. Run table creation script first.")
    finally:
        conn.close()


# Test 5: current_hour() returns top of the hour
def test_current_hour_returns_top_of_hour():
    # Given: a timezone-aware timestamp with non-zero minute/second/microsecond values.
    # When: normalizing it with `current_hour`.
    # Then: the same hour is returned with minute/second/microsecond zeroed.

    tz = ZoneInfo("America/New_York")
    now = dt.datetime(2026, 1, 26, 15, 47, 32, 123456, tzinfo=tz)
    result = tu.current_hour(now)
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
