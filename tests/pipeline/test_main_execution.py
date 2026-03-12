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
    monkeypatch.setenv("WINDOW_MINUTES", "5")
    monkeypatch.setenv("PGHOST", "localhost")
    monkeypatch.setenv("PGPORT", "5432")
    monkeypatch.setenv("PGDATABASE", "test_db")
    monkeypatch.setenv("PGUSER", "test_user")
    monkeypatch.setenv("PGPASSWORD", "test_pass")


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
        return None

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


@pytest.mark.pipeline
def test_main_exits_when_api_key_missing(mock_market_hours_time, monkeypatch, capsys, mock_error_log_dir):
    monkeypatch.setenv("FMP_API_KEY", "")
    monkeypatch.setenv("SYMBOLS", "AAPL")
    monkeypatch.setenv("PGPORT", "5432")

    with pytest.raises(SystemExit) as exc_info:
        collector.main()

    assert exc_info.value.code == 1

    captured = capsys.readouterr()
    assert "API key is missing" in captured.err


@pytest.mark.pipeline
def test_main_exits_when_db_connection_fails(mock_market_hours_time, mock_env_complete, monkeypatch, capsys, mock_error_log_dir):
    def mock_get_engine_fail(*args, **kwargs):
        raise Exception("Connection refused")

    monkeypatch.setattr(collector, "get_engine", mock_get_engine_fail)

    with pytest.raises(SystemExit) as exc_info:
        collector.main()

    assert exc_info.value.code == 2

    captured = capsys.readouterr()
    assert "cannot connect to Postgres" in captured.err


@pytest.mark.pipeline
def test_main_successful_execution(mock_market_hours_time, mock_env_complete, mock_successful_api, monkeypatch, capsys, mock_error_log_dir):
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
    monkeypatch.setenv("FMP_API_KEY", "runtime_key")
    monkeypatch.setenv("SYMBOLS", "GOOGL")
    monkeypatch.setenv("MARKET_TZ", "America/Chicago")
    monkeypatch.setenv("WINDOW_MINUTES", "30")
    monkeypatch.setenv("PGHOST", "testhost")
    monkeypatch.setenv("PGPORT", "5433")
    monkeypatch.setenv("PGDATABASE", "runtime_db")
    monkeypatch.setenv("PGUSER", "runtime_user")
    monkeypatch.setenv("PGPASSWORD", "runtime_pass")

    fake_session = FakeSession()

    def mock_get_engine(host, port, dbname, user, password):
        assert host == "testhost"
        assert port == 5433
        assert dbname == "runtime_db"
        assert user == "runtime_user"
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


@pytest.mark.pipeline
def test_main_computes_time_window(mock_market_hours_time, mock_env_complete, monkeypatch, capsys, mock_error_log_dir):
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
    assert (captured_window["end"] - captured_window["start"]) == dt.timedelta(minutes=5)
    assert captured_window["start"].minute % 5 == 0
    assert captured_window["end"].minute % 5 == 0