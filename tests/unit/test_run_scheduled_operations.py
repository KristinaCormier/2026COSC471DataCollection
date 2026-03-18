"""Unit tests for the Python-based scheduled operations pipeline."""

import datetime as dt
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

import run_scheduled_operations as mod
from src.model.models import MarketData
from utils.scheduled_pipeline import (
    PipelineSummary,
    build_export_payloads,
    classify_quality_issue,
    dedupe_staging_rows,
)


class FakeSession:
    def __init__(self):
        self.added = []
        self.commits = 0
        self.closed = False

    def add(self, obj):
        self.added.append(obj)

    def commit(self):
        self.commits += 1

    def close(self):
        self.closed = True


class FakeSessionFactory:
    def __init__(self):
        self.sessions = []

    def __call__(self):
        session = FakeSession()
        self.sessions.append(session)

        class _ContextManager:
            def __enter__(self_inner):
                return session

            def __exit__(self_inner, exc_type, exc, tb):
                session.close()
                return False

        return _ContextManager()


def make_market_row(**overrides):
    tz = dt.timezone.utc
    base = {
        "ingest_id": 1,
        "symbol": "AAPL",
        "ts": dt.datetime(2026, 3, 10, 14, 5, tzinfo=tz),
        "open": 100,
        "high": 101,
        "low": 99,
        "close": 100.5,
        "volume": 1200,
        "asset_type": "stock",
        "source": "fmp",
        "ingest_time": dt.datetime(2026, 3, 10, 14, 6, tzinfo=tz),
        "raw_payload": {"date": "2026-03-10 14:05:00"},
    }
    base.update(overrides)
    return MarketData(**base)


@pytest.mark.unit
@patch.dict("os.environ", {"DB_NAME": "testdb", "DB_USER": "testuser"}, clear=True)
def test_validate_environment_accepts_db_vars():
    assert mod.validate_environment() is True


@pytest.mark.unit
@patch.dict("os.environ", {"DATABASE_URL": "postgresql+psycopg://user:pass@localhost/db"}, clear=True)
def test_validate_environment_accepts_database_url():
    assert mod.validate_environment() is True


@pytest.mark.unit
@patch.dict("os.environ", {}, clear=True)
def test_validate_environment_fails_without_connection_settings():
    assert mod.validate_environment() is False


@pytest.mark.unit
def test_classify_quality_issue_matches_export_rules():
    assert classify_quality_issue(make_market_row(close=None)) == "invalid_price"
    assert classify_quality_issue(make_market_row(open=None)) == "incomplete_ohlc"
    assert classify_quality_issue(make_market_row(volume=-1)) == "invalid_volume"
    assert classify_quality_issue(make_market_row(ts=None, close=100.5)) == "missing_timestamp"
    assert classify_quality_issue(make_market_row()) is None


@pytest.mark.unit
def test_dedupe_staging_rows_keeps_first_row_and_logs_duplicates():
    winner = make_market_row(ingest_id=10, ingest_time=dt.datetime(2026, 3, 10, 14, 7, tzinfo=dt.timezone.utc))
    duplicate = make_market_row(ingest_id=9, ingest_time=dt.datetime(2026, 3, 10, 14, 6, tzinfo=dt.timezone.utc))

    winners, duplicate_logs = dedupe_staging_rows([winner, duplicate])

    assert winners == [winner]
    assert len(duplicate_logs) == 1
    assert duplicate_logs[0].resolution == "discarded_duplicate_in_staging"
    assert duplicate_logs[0].existing_row["ingest_id"] == 10
    assert duplicate_logs[0].incoming_row["ingest_id"] == 9


@pytest.mark.unit
def test_build_export_payloads_separates_valid_rows_from_quality_errors():
    valid_row = make_market_row(symbol="AAPL")
    invalid_row = make_market_row(symbol="MSFT", close=None)

    export_payloads, quality_errors = build_export_payloads([valid_row, invalid_row])

    assert len(export_payloads) == 1
    assert export_payloads[0]["symbol"] == "AAPL"
    assert export_payloads[0]["volume"] == 1200
    assert len(quality_errors) == 1
    assert quality_errors[0].symbol == "MSFT"
    assert quality_errors[0].error_type == "invalid_price"


@pytest.mark.unit
def test_summarize_step_formats_nonzero_counts_only():
    summary = PipelineSummary(processed_rows=12, duplicate_rows=2, exported_rows=10)
    assert mod.summarize_step(summary) == "processed=12, exported=10, duplicates=2"


@pytest.mark.unit
@patch("run_scheduled_operations.log_execution")
def test_execute_pipeline_step_commits_and_logs_success(mock_log):
    session_factory = FakeSessionFactory()

    def operation(session):
        session.add(object())
        return PipelineSummary(processed_rows=4, exported_rows=4)

    success, error = mod.execute_pipeline_step(session_factory, "export_stg_to_core", operation)

    assert success is True
    assert error is None
    assert session_factory.sessions[0].commits == 1
    assert session_factory.sessions[0].closed is True
    assert mock_log.call_args.args[1] == "export_stg_to_core"
    assert mock_log.call_args.args[2] == "success"
    assert mock_log.call_args.kwargs["detail_message"] == "processed=4, exported=4"


@pytest.mark.unit
@patch("run_scheduled_operations.log_execution")
def test_execute_pipeline_step_logs_failure(mock_log):
    session_factory = FakeSessionFactory()

    def operation(session):
        raise RuntimeError("boom")

    success, error = mod.execute_pipeline_step(session_factory, "export_stg_to_core", operation)

    assert success is False
    assert error == "boom"
    assert session_factory.sessions[0].commits == 0
    assert mock_log.call_args.args[2] == "failed"
    assert mock_log.call_args.kwargs["error_message"] == "boom"


@pytest.mark.unit
def test_log_execution_persists_pipeline_log_record():
    session_factory = FakeSessionFactory()

    mod.log_execution(
        session_factory,
        "truncate_stg_raw",
        "warning",
        0.0,
        detail_message="Skipped because dependency export_stg_to_core did not succeed",
    )

    session = session_factory.sessions[0]
    assert len(session.added) == 1
    assert session.added[0].pipeline_stage == "truncate_stg_raw"
    assert session.added[0].status == "warning"
    assert "Skipped because dependency" in session.added[0].message
    assert session.commits == 1


@pytest.mark.unit
def test_configure_logging_creates_missing_log_directory(tmp_path, monkeypatch):
    missing_dir = tmp_path / "nested" / "logs"
    monkeypatch.setattr(mod, "LOG_DIR", missing_dir)

    logger = mod.configure_logging()

    assert logger is not None
    assert missing_dir.exists()
    assert (missing_dir / "scheduled_operations.log").exists()


@pytest.mark.unit
def test_configure_logging_fails_if_log_dir_not_writable(tmp_path, monkeypatch):
    read_only_dir = tmp_path / "readonly"
    read_only_dir.mkdir()
    read_only_dir.chmod(0o555)

    try:
        monkeypatch.setattr(mod, "LOG_DIR", read_only_dir)
        with pytest.raises(PermissionError):
            mod.configure_logging()
    finally:
        read_only_dir.chmod(0o755)


@pytest.mark.unit
def test_main_returns_failure_when_environment_validation_fails(tmp_path, monkeypatch):
    monkeypatch.setattr(mod, "LOG_DIR", tmp_path / "logs")
    monkeypatch.setattr(mod, "logger", None)

    with patch("run_scheduled_operations.validate_environment", return_value=False):
        assert mod.main() == 1


@pytest.mark.unit
@patch.dict("os.environ", {"DB_NAME": "testdb", "DB_USER": "testuser"}, clear=True)
def test_main_executes_pipeline_in_order_and_skips_failed_dependency(tmp_path, monkeypatch):
    monkeypatch.setattr(mod, "LOG_DIR", tmp_path / "logs")
    monkeypatch.setattr(mod, "logger", None)

    events = []

    def export_step(session):
        events.append("export")
        raise RuntimeError("export failed")

    def truncate_step(session):
        events.append("truncate")
        return PipelineSummary(truncated_rows=2)

    class FakeEngine:
        def connect(self):
            class _Conn:
                def __enter__(self_inner):
                    return self_inner

                def __exit__(self_inner, exc_type, exc, tb):
                    return False

            return _Conn()

        def dispose(self):
            events.append("dispose")

    session_factory = FakeSessionFactory()

    monkeypatch.setattr(mod, "PIPELINE_STEPS", (("export_stg_to_core", None, export_step), ("truncate_stg_raw", "export_stg_to_core", truncate_step)))
    monkeypatch.setattr(mod, "build_runtime_engine", lambda: FakeEngine())
    monkeypatch.setattr(mod, "get_session_factory", lambda engine: session_factory)

    logged = []
    monkeypatch.setattr(mod, "log_execution", lambda *args, **kwargs: logged.append((args, kwargs)))

    result = mod.main()

    assert result == 1
    assert events == ["export", "dispose"]
    assert len(logged) == 2
    assert logged[0][0][1] == "export_stg_to_core"
    assert logged[0][0][2] == "failed"
    assert logged[1][0][1] == "truncate_stg_raw"
    assert logged[1][0][2] == "warning"



