import datetime as dt
import runpy
from unittest.mock import patch

import pytest

import run_scheduled_operations as mod
from utils.scheduled_pipeline import PipelineSummary


pytestmark = pytest.mark.pipeline


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


def test_validate_environment_accepts_database_url(monkeypatch):
    # Given: DATABASE_URL is present in the process environment.
    # When: validate_environment checks required runtime configuration.
    # Then: it returns True to allow pipeline execution.

    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://user:pass@localhost/db")

    assert mod.validate_environment() is True


def test_validate_environment_reports_missing_db_configuration(monkeypatch, tmp_path):
    # Given: DATABASE_URL resolves to an empty value and logging is configured.
    # When: validate_environment verifies database configuration preconditions.
    # Then: it returns False to block the run due to missing DB settings.

    def fake_getenv(key, default=None):
        if key == "DATABASE_URL":
            return ""
        return default

    monkeypatch.setattr(mod.os, "getenv", fake_getenv)
    monkeypatch.setattr(mod, "LOG_DIR", tmp_path / "logs")
    monkeypatch.setattr(mod, "logger", mod.configure_logging())

    assert mod.validate_environment() is False


def test_build_runtime_engine_uses_database_url(monkeypatch):
    # Given: a DATABASE_URL value and a mocked create_engine call.
    # When: build_runtime_engine constructs the SQLAlchemy engine.
    # Then: it passes DATABASE_URL plus expected connection options to create_engine.

    captured = {}

    def fake_create_engine(database_url, future=True, pool_pre_ping=True):
        captured["database_url"] = database_url
        captured["future"] = future
        captured["pool_pre_ping"] = pool_pre_ping
        return "engine-from-url"

    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://u:p@localhost/db")
    monkeypatch.setattr(mod, "create_engine", fake_create_engine)

    engine = mod.build_runtime_engine()

    assert engine == "engine-from-url"
    assert captured["database_url"] == "postgresql+psycopg://u:p@localhost/db"
    assert captured["future"] is True
    assert captured["pool_pre_ping"] is True


@patch("run_scheduled_operations.log_execution")
def test_execute_pipeline_step_commits_and_logs_success(mock_log):
    # Given: a pipeline operation that returns a successful row summary.
    # When: execute_pipeline_step runs the operation in a managed session.
    # Then: it commits once, closes the session, and logs a success detail message.

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


@patch("run_scheduled_operations.log_execution")
def test_execute_pipeline_step_logs_failure(mock_log):
    # Given: a pipeline operation that raises a RuntimeError.
    # When: execute_pipeline_step wraps and executes that failing operation.
    # Then: it returns failure without commit and logs the captured error message.

    session_factory = FakeSessionFactory()

    def operation(session):
        raise RuntimeError("boom")

    success, error = mod.execute_pipeline_step(session_factory, "export_stg_to_core", operation)

    assert success is False
    assert error == "boom"
    assert session_factory.sessions[0].commits == 0
    assert mock_log.call_args.args[2] == "failed"
    assert mock_log.call_args.kwargs["error_message"] == "boom"


@patch.dict("os.environ", {"DATABASE_URL": "postgresql+psycopg://u:p@localhost/db"}, clear=True)
def test_main_executes_pipeline_and_skips_failed_dependency(tmp_path, monkeypatch):
    # Given: a two-step pipeline where step two depends on step one and step one fails.
    # When: main executes scheduled steps with dependency checks enabled.
    # Then: it logs step-one failure, marks dependent step as warning, and disposes engine.

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

    monkeypatch.setattr(
        mod,
        "build_pipeline_steps",
        lambda now_local=None: (
            ("export_stg_to_core", None, export_step),
            ("truncate_stg_raw", "export_stg_to_core", truncate_step),
        ),
    )
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


@patch.dict("os.environ", {"DATABASE_URL": "postgresql+psycopg://u:p@localhost/db"}, clear=True)
def test_main_logs_database_url_connection_mode(tmp_path, monkeypatch):
    # Given: a valid DATABASE_URL and an empty scheduled steps list.
    # When: main initializes runtime and performs the connectivity precheck.
    # Then: it completes successfully and returns exit code 0.

    monkeypatch.setattr(mod, "LOG_DIR", tmp_path / "logs")
    monkeypatch.setattr(mod, "logger", None)

    class FakeEngine:
        def connect(self):
            class _Conn:
                def __enter__(self_inner):
                    return self_inner

                def __exit__(self_inner, exc_type, exc, tb):
                    return False

            return _Conn()

        def dispose(self):
            return None

    monkeypatch.setattr(mod, "build_runtime_engine", lambda: FakeEngine())
    monkeypatch.setattr(mod, "get_session_factory", lambda engine: FakeSessionFactory())
    monkeypatch.setattr(mod, "build_pipeline_steps", lambda now_local=None: ())

    assert mod.main() == 0


def test_summarize_step_includes_duplicate_quality_and_truncated_counts():
    # Given: a PipelineSummary containing processed, duplicate, quality, and truncation counters.
    # When: summarize_step formats that summary for logging.
    # Then: the output string includes all non-zero metric fields.

    summary = PipelineSummary(
        processed_rows=5,
        duplicate_rows=2,
        quality_error_rows=1,
        truncated_rows=4,
    )

    text = mod.summarize_step(summary)

    assert "processed=5" in text
    assert "duplicates=2" in text
    assert "quality_errors=1" in text
    assert "truncated=4" in text


def test_log_execution_joins_detail_and_error_message():
    # Given: log_execution receives both a detail message and an error message.
    # When: it inserts an execution log row through the session factory.
    # Then: the persisted message concatenates detail and error with a separator.

    session_factory = FakeSessionFactory()

    mod.log_execution(
        session_factory=session_factory,
        step_name="export_stg_to_core",
        status="failed",
        duration_seconds=1.0,
        detail_message="processed=3",
        error_message="boom",
    )

    logged_row = session_factory.sessions[0].added[0]
    assert logged_row.message == "processed=3 | boom"


def test_log_execution_logs_error_when_session_write_fails(monkeypatch):
    # Given: the session factory itself fails while writing execution logs.
    # When: log_execution handles that internal logging failure path.
    # Then: it emits a fallback logger.error message about failed log persistence.

    class CollectLogger:
        def __init__(self):
            self.messages = []

        def error(self, message):
            self.messages.append(message)

    collect_logger = CollectLogger()
    monkeypatch.setattr(mod, "logger", collect_logger)

    def failing_factory():
        raise RuntimeError("cannot open session")

    mod.log_execution(
        session_factory=failing_factory,
        step_name="export_stg_to_core",
        status="failed",
        duration_seconds=0.1,
        error_message="boom",
    )

    assert any("Failed to log execution" in message for message in collect_logger.messages)


def test_execute_pipeline_step_logs_success_message_when_logger_available(monkeypatch):
    # Given: a logger is available and the step operation succeeds.
    # When: execute_pipeline_step completes the operation.
    # Then: it returns success and writes a human-readable completion info log.

    class CollectLogger:
        def __init__(self):
            self.info_messages = []

        def info(self, message):
            self.info_messages.append(message)

        def error(self, message):
            return None

    collect_logger = CollectLogger()
    monkeypatch.setattr(mod, "logger", collect_logger)

    session_factory = FakeSessionFactory()

    def operation(session):
        return PipelineSummary(processed_rows=1, exported_rows=1)

    success, _ = mod.execute_pipeline_step(session_factory, "export_stg_to_core", operation)

    assert success is True
    assert any("completed successfully" in message for message in collect_logger.info_messages)


def test_configure_logging_raises_not_a_directory(tmp_path, monkeypatch):
    # Given: LOG_DIR points to a path object that reports it is not a directory.
    # When: configure_logging validates the target log path.
    # Then: it raises NotADirectoryError instead of creating handlers.

    class FakeLogDir:
        def mkdir(self, parents=True, exist_ok=True):
            return None

        def is_dir(self):
            return False

        def __str__(self):
            return "/tmp/not-a-directory"

    monkeypatch.setattr(mod, "LOG_DIR", FakeLogDir())

    with pytest.raises(NotADirectoryError):
        mod.configure_logging()


def test_configure_logging_raises_permission_error(tmp_path, monkeypatch):
    # Given: LOG_DIR exists with simulated read-only permissions.
    # When: configure_logging verifies write access on the log directory.
    # Then: it raises PermissionError to signal insufficient filesystem permissions.

    read_only_dir = tmp_path / "readonly"
    read_only_dir.mkdir()
    read_only_dir.chmod(0o555)
    monkeypatch.setattr(mod, "LOG_DIR", read_only_dir)

    try:
        with pytest.raises(PermissionError):
            mod.configure_logging()
    finally:
        read_only_dir.chmod(0o755)


def test_main_returns_failure_when_configure_logging_fails(monkeypatch, capsys):
    # Given: configure_logging is forced to raise a PermissionError.
    # When: main starts and attempts to initialize logging first.
    # Then: it returns exit code 1 and prints a clear logging setup failure message.

    monkeypatch.setattr(mod, "configure_logging", lambda: (_ for _ in ()).throw(PermissionError("denied")))

    result = mod.main()
    captured = capsys.readouterr()

    assert result == 1
    assert "Failed to configure logging" in captured.err


@patch.dict("os.environ", {"DATABASE_URL": "postgresql+psycopg://u:p@localhost/db"}, clear=True)
def test_main_returns_failure_when_engine_connect_raises(tmp_path, monkeypatch):
    # Given: logging succeeds but build_runtime_engine raises a connection error.
    # When: main tries to establish the database engine for pipeline execution.
    # Then: it returns 1 and records a database connection failure in logger.error.

    class CollectLogger:
        def __init__(self):
            self.info_messages = []
            self.error_messages = []

        def info(self, *args):
            self.info_messages.append(args)

        def warning(self, *args):
            return None

        def error(self, message):
            self.error_messages.append(message)

    collect_logger = CollectLogger()
    monkeypatch.setattr(mod, "configure_logging", lambda: collect_logger)
    monkeypatch.setattr(mod, "build_runtime_engine", lambda: (_ for _ in ()).throw(RuntimeError("connect boom")))

    result = mod.main()

    assert result == 1
    assert any("Failed to connect to database" in message for message in collect_logger.error_messages)


def test_script_entrypoint_calls_main_and_sys_exit(monkeypatch, tmp_path):
    # Given: run_scheduled_operations is launched via its __main__ path with an invalid DB URL.
    # When: runpy executes the module and main returns a failure status.
    # Then: the script raises SystemExit with code 1.

    # Execute the module as a script to cover the __main__ guard and sys.exit path.
    monkeypatch.setenv("LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql+psycopg://invalid:invalid@127.0.0.1:1/invalid?connect_timeout=1",
    )

    with pytest.raises(SystemExit) as exc:
        runpy.run_module("run_scheduled_operations", run_name="__main__")

    assert exc.value.code == 1