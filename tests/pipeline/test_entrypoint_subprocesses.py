import os
import subprocess
import sys

import pytest

import gather_past_data as gather
import historical_csv_data_load as csv_loader
import run_scheduled_operations as scheduled_runner


pytestmark = pytest.mark.pipeline


def test_gather_past_data_subprocess_rejects_invalid_range(project_root):
    # Given: subprocess execution of `gather_past_data.py` with from-date later than to-date.
    # When: invoking the script through `subprocess.run`.
    # Then: it exits with code 1 and emits an invalid-range error on stderr.

    script = project_root / "src" / "gather_past_data.py"
    env = os.environ.copy()
    env.setdefault("PYTHONPATH", "src")

    result = subprocess.run(
        [
            sys.executable,
            str(script),
            "--from-date",
            "2026-03-19",
            "--to-date",
            "2026-03-18",
        ],
        cwd=project_root,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 1
    assert "invalid historical range" in result.stderr


def test_historical_csv_loader_subprocess_rejects_missing_directory(project_root):
    # Given: subprocess execution of `historical_csv_data_load.py` pointing to a missing CSV directory.
    # When: running the loader script in a child process.
    # Then: it exits with code 1 and reports the missing-directory failure.

    script = project_root / "src" / "historical_csv_data_load.py"
    env = os.environ.copy()
    env.setdefault("PYTHONPATH", "src")

    result = subprocess.run(
        [
            sys.executable,
            str(script),
            "--csv-dir",
            "/tmp/does-not-exist-cosc471",
        ],
        cwd=project_root,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 1
    assert "csv directory does not exist" in result.stderr


def test_run_scheduled_operations_subprocess_fails_with_invalid_database_url(project_root, tmp_path):
    # Given: subprocess execution of scheduled operations with an intentionally unreachable DATABASE_URL.
    # When: launching `run_scheduled_operations.py`.
    # Then: startup fails fast with return code 1 and a database-connection error message.

    script = project_root / "src" / "run_scheduled_operations.py"
    env = os.environ.copy()
    env.setdefault("PYTHONPATH", "src")
    env["LOG_DIR"] = str(tmp_path / "logs")

    # Force runtime to use an invalid external database URL.
    env["DATABASE_URL"] = "postgresql+psycopg://invalid:invalid@127.0.0.1:1/invalid"

    result = subprocess.run(
        [sys.executable, str(script)],
        cwd=project_root,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 1
    assert "Failed to connect to database" in result.stderr


def test_gather_past_data_main_rejects_invalid_range_in_process(capsys):
    # Given: in-process call to `gather.main` with reversed date bounds.
    # When: executing the function directly.
    # Then: it raises `SystemExit(1)` and writes an invalid-range error to stderr.

    with pytest.raises(SystemExit) as exc_info:
        gather.main(["--from-date", "2026-03-19", "--to-date", "2026-03-18"])

    captured = capsys.readouterr()
    assert exc_info.value.code == 1
    assert "invalid historical range" in captured.err


def test_historical_csv_loader_main_rejects_missing_directory_in_process(capsys):
    # Given: in-process call to CSV loader with a non-existent directory path.
    # When: executing `historical_csv_data_load.main` directly.
    # Then: it returns status 1 and prints a missing-directory message.

    result = csv_loader.main(["--csv-dir", "/tmp/does-not-exist-cosc471"])

    captured = capsys.readouterr()
    assert result == 1
    assert "csv directory does not exist" in captured.err


def test_run_scheduled_operations_main_fails_with_invalid_database_url_in_process(tmp_path, monkeypatch):
    # Given: in-process scheduled-operations execution configured with an invalid DATABASE_URL.
    # When: calling `run_scheduled_operations.main`.
    # Then: the function reports failure by returning exit code 1.

    monkeypatch.setattr(scheduled_runner, "LOG_DIR", tmp_path / "logs")
    monkeypatch.setattr(scheduled_runner, "logger", None)
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://invalid:invalid@127.0.0.1:1/invalid")

    assert scheduled_runner.main() == 1