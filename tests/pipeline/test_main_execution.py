"""
Pipeline tests for end-to-end execution of intraday_data_collection.
These tests validate the main() function and complete execution flow.
"""

import datetime as dt
import pytest
from unittest.mock import patch
from zoneinfo import ZoneInfo

import intraday_data_collection as collector
from tests.conftest import FakeResponse


@pytest.fixture
def mock_env_complete(monkeypatch):
    """Set up complete environment for main() execution."""
    monkeypatch.setenv("FMP_API_KEY", "test_api_key")
    monkeypatch.setenv("SYMBOLS", "AAPL,MSFT")
    monkeypatch.setenv("MARKET_TZ", "America/New_York")


@pytest.fixture
def mock_market_hours_time():
    """Mock datetime.now() to return a time during market hours."""
    market_time = dt.datetime(2026, 2, 2, 10, 0, 0, tzinfo=ZoneInfo("America/New_York"))

    real_datetime = dt.datetime

    def mock_now(tz=None):
        if tz is not None:
            return market_time.astimezone(tz)
        return market_time

    with patch("datetime.datetime") as mock_dt:
        mock_dt.now = mock_now
        mock_dt.side_effect = lambda *args, **kw: real_datetime(*args, **kw)
        yield


@pytest.fixture
def mock_successful_api(monkeypatch):
    """Mock successful API responses."""
    def mock_get(url, params=None, timeout=None):
        return FakeResponse([
            {
                "date": "2026-02-02 09:55:00",
                "open": 150.0,
                "high": 151.0,
                "low": 149.5,
                "close": 150.5,
                "volume": 1000000
            }
        ])
    monkeypatch.setattr("requests.get", mock_get)


class FakeSession:
    def __init__(self):
        self.closed = False
        self.executed = []
        self.commits = 0
        self.rollbacks = 0

    def execute(self, *args, **kwargs):
        self.executed.append((args, kwargs))
        result = type("Result", (), {})()
        result.rowcount = 1
        return result

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


@pytest.mark.pipeline
def test_main_exits_when_api_key_missing(mock_market_hours_time, monkeypatch, capsys, mock_error_log_dir):
    # Given: market-hour context with an empty `FMP_API_KEY`.
    # When: running collector `main`.
    # Then: execution aborts with `SystemExit(1)` and API-key error text.

    monkeypatch.setenv("FMP_API_KEY", "")
    monkeypatch.setenv("SYMBOLS", "AAPL")

    with pytest.raises(SystemExit) as exc_info:
        collector.main()

    assert exc_info.value.code == 1

    captured = capsys.readouterr()
    assert "API key is missing" in captured.err


@pytest.mark.pipeline
def test_main_exits_when_db_connection_fails(mock_market_hours_time, mock_env_complete, monkeypatch, capsys, mock_error_log_dir):
    # Given: DB engine construction patched to raise a connection exception.
    # When: running collector `main`.
    # Then: program exits with code 2 and reports database connectivity failure.

    def mock_get_engine_fail(*args, **kwargs):
        raise Exception("Connection refused")

    monkeypatch.setattr(collector, "get_engine", mock_get_engine_fail)

    with pytest.raises(SystemExit) as exc_info:
        collector.main()

    assert exc_info.value.code == 2

    captured = capsys.readouterr()
    assert "cannot connect to database" in captured.err


@pytest.mark.pipeline
def test_main_successful_execution(mock_market_hours_time, mock_env_complete, mock_successful_api, monkeypatch, capsys, mock_error_log_dir):
    # Given: complete environment, successful API response, and working fake session.
    # When: executing collector `main` end-to-end.
    # Then: startup/processing output appears and session closes after successful run.

    fake_session = FakeSession()

    monkeypatch.setattr(collector, "get_engine", lambda *args, **kwargs: object())
    monkeypatch.setattr(collector, "init_db", lambda engine: None)
    monkeypatch.setattr(collector, "get_session_factory", lambda engine: lambda: fake_session)

    collector.main()

    captured = capsys.readouterr()
    assert "[Collector Startup]" in captured.out
    assert "AAPL" in captured.out or "MSFT" in captured.out
    assert "[done]" in captured.out
    assert fake_session.closed


@pytest.mark.pipeline
def test_main_continues_after_symbol_error(mock_market_hours_time, mock_env_complete, monkeypatch, capsys, mock_error_log_dir):
    # Given: two symbols where the first API fetch fails and the second succeeds.
    # When: running collector `main`.
    # Then: per-symbol error is logged, processing continues, and session closes cleanly.

    fake_session = FakeSession()
    call_count = {"count": 0}

    def mock_fetch_api_data(symbol, start, end, api_key, tz):
        call_count["count"] += 1
        if symbol == "AAPL":
            raise Exception("API rate limit exceeded")
        return [{"date": "2026-02-02 09:55:00", "open": 150.0, "high": 151.0, "low": 149.5, "close": 150.5, "volume": 1000000}]

    monkeypatch.setattr(collector, "get_engine", lambda *args, **kwargs: object())
    monkeypatch.setattr(collector, "init_db", lambda engine: None)
    monkeypatch.setattr(collector, "get_session_factory", lambda engine: lambda: fake_session)
    monkeypatch.setattr(collector, "_fetch_api_data", mock_fetch_api_data)

    collector.main()

    captured = capsys.readouterr()
    assert "[error] AAPL:" in captured.err
    assert "API rate limit exceeded" in captured.err
    assert call_count["count"] == 2
    assert fake_session.closed


@pytest.mark.pipeline
def test_main_loads_env_vars_at_runtime(mock_market_hours_time, monkeypatch, capsys, mock_error_log_dir):
    # Given: runtime environment overrides for API key, symbols, timezone, and window.
    # When: collector `main` initializes dependencies and requests data.
    # Then: runtime values are used and DB engine creation is invoked with resolved settings.

    monkeypatch.setenv("FMP_API_KEY", "runtime_key")
    monkeypatch.setenv("SYMBOLS", "GOOGL")
    monkeypatch.setenv("MARKET_TZ", "America/Chicago")

    fake_session = FakeSession()
    captured_db_call = {}

    def mock_get_engine(host, port, dbname, user, password):
        captured_db_call["host"] = host
        captured_db_call["port"] = port
        captured_db_call["dbname"] = dbname
        captured_db_call["user"] = user
        captured_db_call["password"] = password
        return object()

    def mock_fetch_api_data(symbol, start, end, api_key, tz):
        assert api_key == "runtime_key"
        return []

    monkeypatch.setattr(collector, "get_engine", mock_get_engine)
    monkeypatch.setattr(collector, "init_db", lambda engine: None)
    monkeypatch.setattr(collector, "get_session_factory", lambda engine: lambda: fake_session)
    monkeypatch.setattr(collector, "_fetch_api_data", mock_fetch_api_data)

    collector.main()

    captured = capsys.readouterr()
    assert "GOOGL" in captured.out
    assert captured_db_call


@pytest.mark.pipeline
def test_main_computes_time_window(mock_market_hours_time, mock_env_complete, monkeypatch, capsys, mock_error_log_dir):
    # Given: a mocked current market time and standard environment.
    # When: `main` computes fetch window and calls API fetch.
    # Then: captured window spans market-open to aligned-now.

    fake_session = FakeSession()
    captured_window = {"start": None, "end": None}

    def mock_fetch_api_data(symbol, start, end, api_key, tz):
        captured_window["start"] = start
        captured_window["end"] = end
        return []

    monkeypatch.setattr(collector, "get_engine", lambda *args, **kwargs: object())
    monkeypatch.setattr(collector, "init_db", lambda engine: None)
    monkeypatch.setattr(collector, "get_session_factory", lambda engine: lambda: fake_session)
    monkeypatch.setattr(collector, "_fetch_api_data", mock_fetch_api_data)

    collector.main()

    assert captured_window["start"] is not None
    assert captured_window["end"] is not None
    assert captured_window["start"] < captured_window["end"]
    assert (captured_window["end"] - captured_window["start"]) == dt.timedelta(hours=6)
    assert captured_window["start"].hour == 4
    assert captured_window["start"].minute == 0
    assert captured_window["start"].minute % 5 == 0
    assert captured_window["end"].minute % 5 == 0


@pytest.mark.pipeline
def test_main_exits_on_invalid_market_hours_format(monkeypatch, capsys, mock_error_log_dir):
    # Given: invalid `MARKET_OPEN` format in environment settings.
    # When: running collector `main`.
    # Then: the program exits with code 1 and emits invalid-market-hours error text.

    monkeypatch.setenv("FMP_API_KEY", "test_api_key")
    monkeypatch.setenv("SYMBOLS", "AAPL")
    monkeypatch.setenv("MARKET_OPEN", "bad-time")
    monkeypatch.setenv("MARKET_CLOSE", "21:00")

    with pytest.raises(SystemExit) as exc_info:
        collector.main()

    assert exc_info.value.code == 1
    captured = capsys.readouterr()
    assert "invalid market hours" in captured.err


@pytest.mark.pipeline
def test_main_skips_when_window_outside_market_hours(mock_market_hours_time, monkeypatch, capsys, mock_error_log_dir):
    # Given: market hours configured so current window is outside trading session.
    # When: executing collector `main`.
    # Then: it exits gracefully with code 0 and prints an outside-market-hours notice.

    monkeypatch.setenv("FMP_API_KEY", "test_api_key")
    monkeypatch.setenv("SYMBOLS", "AAPL")
    monkeypatch.setenv("MARKET_OPEN", "11:00")
    monkeypatch.setenv("MARKET_CLOSE", "11:00")

    with pytest.raises(SystemExit) as exc_info:
        collector.main()

    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "outside market hours" in captured.out


@pytest.mark.pipeline
def test_main_exits_when_symbols_missing(mock_market_hours_time, monkeypatch, capsys, mock_error_log_dir):
    # Given: `SYMBOLS` is present but empty.
    # When: running collector `main`.
    # Then: it exits with code 1 and reports that symbols are not set.

    monkeypatch.setenv("FMP_API_KEY", "test_api_key")
    monkeypatch.setenv("SYMBOLS", "")

    with pytest.raises(SystemExit) as exc_info:
        collector.main()

    assert exc_info.value.code == 1
    captured = capsys.readouterr()
    assert "SYMBOLS is not set" in captured.err


@pytest.mark.pipeline
def test_main_warns_when_session_close_raises(mock_market_hours_time, mock_env_complete, monkeypatch, capsys, mock_error_log_dir):
    # Given: a session object whose `close()` method raises an exception.
    # When: collector `main` finishes processing and performs teardown.
    # Then: close failure is surfaced as a warning in stderr without crashing main flow.

    class CloseFailSession(FakeSession):
        def close(self):
            raise RuntimeError("close failed")

    fake_session = CloseFailSession()

    monkeypatch.setattr(collector, "get_engine", lambda *args, **kwargs: object())
    monkeypatch.setattr(collector, "init_db", lambda engine: None)
    monkeypatch.setattr(collector, "get_session_factory", lambda engine: lambda: fake_session)
    monkeypatch.setattr(collector, "_fetch_api_data", lambda *args, **kwargs: [])

    collector.main()

    captured = capsys.readouterr()
    assert "failed to close database session" in captured.err