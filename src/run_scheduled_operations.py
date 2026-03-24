#!/usr/bin/env python3
# Orchestrates scheduled Python pipeline steps for staging export and cleanup.
# Executes a fixed pipeline order and logs results to operation_logs.pipeline_logs.
# Intended to be called via cron or manually.
#
# Usage: python3 src/run_scheduled_operations.py
#
# Requires DATABASE_URL or DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD env vars.

from __future__ import annotations

import os
import sys
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from model.models import PipelineLog
from model.orm_db import get_engine, get_session_factory
from utils.scheduled_pipeline import (
    PipelineSummary,
    clear_staging_tables,
    export_staging_to_core,
)
from utils.time_utils import parse_hhmm

# Setup logging to both file and stderr
# Log directory must be pre-provisioned by setup script
load_dotenv()  # Load environment variables from .env file
LOG_DIR = Path(os.getenv("LOG_DIR", "./logs")) # Default to ./logs if not set


# Logging configuration is deferred to main() to allow for startup validation
logger = None


def configure_logging() -> logging.Logger:
    """
    Configure logging after creating or validating the log directory.
    Must be called from main() before any logging operations.

    Returns:
        Configured logger instance

    Raises:
        NotADirectoryError: If LOG_DIR points to a non-directory
        PermissionError: If LOG_DIR is not writable
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    if not LOG_DIR.is_dir():
        raise NotADirectoryError(f"Log path exists but is not a directory: {LOG_DIR}")

    # Test write permissions
    if not (LOG_DIR.stat().st_mode & 0o200):
        raise PermissionError(
            f"Log directory is not writable: {LOG_DIR}\n"
            f"Set LOG_DIR to a writable project-local path or adjust permissions."
        )
    scheduled_logger = logging.getLogger("scheduled_operations")
    scheduled_logger.setLevel(logging.INFO)
    scheduled_logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler = logging.FileHandler(LOG_DIR / "scheduled_operations.log")
    stream_handler = logging.StreamHandler(sys.stderr)
    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)
    scheduled_logger.addHandler(file_handler)
    scheduled_logger.addHandler(stream_handler)
    scheduled_logger.propagate = False
    return scheduled_logger


PipelineOperation = Callable[[Session], PipelineSummary]


def should_truncate_staging_after_close(now_local: datetime, close_time_str: str) -> bool:
    """Return True only on weekdays at/after market close in local market time."""
    close_time = parse_hhmm(close_time_str)
    if now_local.weekday() >= 5:
        return False
    return now_local.time() >= close_time


def build_pipeline_steps(now_local: datetime | None = None) -> tuple[tuple[str, Optional[str], PipelineOperation], ...]:
    """
    Build scheduled pipeline steps for the current execution window.

    Export runs on every invocation. Staging truncate only runs after market close
    on trading weekdays, preventing daytime full-day backfill churn.
    """
    market_tz = os.getenv("MARKET_TZ", "America/New_York")
    market_close = os.getenv("MARKET_CLOSE", "21:00")
    if now_local is None:
        now_local = datetime.now(ZoneInfo(market_tz))

    steps: list[tuple[str, Optional[str], PipelineOperation]] = [
        ("export_stg_to_core", None, export_staging_to_core),
    ]

    if should_truncate_staging_after_close(now_local, market_close):
        steps.append(("truncate_stg_raw", "export_stg_to_core", clear_staging_tables))

    return tuple(steps)


def validate_environment() -> bool:
    """Check that DATABASE_URL is set or the required DB* vars are available."""
    if os.getenv("DATABASE_URL"):
        return True

    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")

    if not db_name:
        if logger:
            logger.error("DB_NAME environment variable is not set")
        return False
    if not db_user:
        if logger:
            logger.error("DB_USER environment variable is not set")
        return False
    return True


def build_runtime_engine():
    """Create a SQLAlchemy engine from runtime environment settings."""
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return create_engine(database_url, future=True, pool_pre_ping=True)

    return get_engine(
        os.getenv("DB_HOST", "localhost"),
        int(os.getenv("DB_PORT", "5432")),
        os.getenv("DB_NAME", ""),
        os.getenv("DB_USER", ""),
        os.getenv("DB_PASSWORD", ""),
    )


def summarize_step(summary: PipelineSummary) -> str:
    details: list[str] = []
    if summary.processed_rows:
        details.append(f"processed={summary.processed_rows}")
    if summary.exported_rows:
        details.append(f"exported={summary.exported_rows}")
    if summary.duplicate_rows:
        details.append(f"duplicates={summary.duplicate_rows}")
    if summary.quality_error_rows:
        details.append(f"quality_errors={summary.quality_error_rows}")
    if summary.truncated_rows:
        details.append(f"truncated={summary.truncated_rows}")
    return ", ".join(details) if details else "no changes"


def log_execution(
    session_factory: sessionmaker,
    step_name: str,
    status: str,
    duration_seconds: float,
    error_message: Optional[str] = None,
    detail_message: Optional[str] = None,
) -> None:
    """Log execution metadata to operation_logs.pipeline_logs."""
    message_parts = []
    if detail_message:
        message_parts.append(detail_message)
    if error_message:
        message_parts.append(error_message)
    message = " | ".join(message_parts) if message_parts else None

    try:
        with session_factory() as session:
            session.add(
                PipelineLog(
                    pipeline_stage=step_name,
                    status=status,
                    created_at=datetime.now(timezone.utc),
                    message=message,
                )
            )
            session.commit()
    except Exception as e:
        if logger:
            logger.error(f"Failed to log execution for {step_name}: {e}")


def execute_pipeline_step(
    session_factory: sessionmaker,
    step_name: str,
    operation: PipelineOperation,
) -> tuple[bool, Optional[str]]:
    """Execute a single pipeline step and return (success, error_message)."""
    start_time = datetime.now(timezone.utc)

    try:
        if logger:
            logger.info(f"Executing {step_name}...")

        with session_factory() as session:
            summary = operation(session)
            session.commit()

        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        detail_message = summarize_step(summary)
        if logger:
            logger.info(f"{step_name} completed successfully ({duration:.2f}s) [{detail_message}]")

        log_execution(session_factory, step_name, "success", duration, detail_message=detail_message)

        return True, None

    except Exception as e:
        error_msg = str(e)
        duration = (datetime.now(timezone.utc) - start_time).total_seconds()

        if logger:
            logger.error(f"{step_name} failed: {error_msg}")
            logger.error(f"   Duration: {duration:.2f}s")

        log_execution(session_factory, step_name, "failed", duration, error_message=error_msg)

        return False, error_msg


def main() -> int:
    """Execute scheduled pipeline operations in a fixed, dependency-aware order."""
    global logger

    # Configure logging at startup - must be done before any logger calls
    try:
        logger = configure_logging()
    except (PermissionError, NotADirectoryError) as e:
        print(f"ERROR: Failed to configure logging: {e}", file=sys.stderr)
        return 1

    logger.info("=" * 70)
    logger.info(f"Starting scheduled operations run at {datetime.now(timezone.utc).isoformat()}")
    logger.info("=" * 70)

    # Validate environment
    if not validate_environment():
        logger.error("Environment validation failed. Exiting.")
        return 1

    # Connect to database and prepare session factory.
    try:
        engine = build_runtime_engine()
        with engine.connect():
            pass
        session_factory = get_session_factory(engine)

        if os.getenv("DATABASE_URL"):
            logger.info("Connected to database using DATABASE_URL")
        else:
            logger.info(
                "Connected to %s on %s:%s",
                os.getenv("DB_NAME"),
                os.getenv("DB_HOST", "localhost"),
                os.getenv("DB_PORT", "5432"),
            )
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        return 1

    # Execute steps in fixed order, skipping dependent stages when prerequisites fail.
    results: dict[str, tuple[bool, Optional[str]]] = {}
    skipped_scripts: dict[str, str] = {}
    configured_steps = build_pipeline_steps()

    if not any(step_name == "truncate_stg_raw" for step_name, _, _ in configured_steps):
        skip_reason = "Skipped because market close has not been reached"
        skipped_scripts["truncate_stg_raw"] = skip_reason
        logger.info(f"truncate_stg_raw skipped. {skip_reason}.")
        log_execution(
            session_factory,
            "truncate_stg_raw",
            "warning",
            0.0,
            detail_message=skip_reason,
        )

    try:
        for step_name, dependency_step, operation in configured_steps:
            if dependency_step is not None:
                dependency_result = results.get(dependency_step)
                if not dependency_result or not dependency_result[0]:
                    skip_reason = f"Skipped because dependency {dependency_step} did not succeed"
                    skipped_scripts[step_name] = skip_reason
                    logger.warning(f"{step_name} skipped. {skip_reason}.")
                    log_execution(
                        session_factory,
                        step_name,
                        "warning",
                        0.0,
                        detail_message=skip_reason,
                    )
                    continue

            success, error_msg = execute_pipeline_step(session_factory, step_name, operation)
            results[step_name] = (success, error_msg)
    finally:
        engine.dispose()

    # Summary
    logger.info("=" * 70)
    passed = sum(1 for success, _ in results.values() if success)
    failed = len(results) - passed
    skipped = len(skipped_scripts)
    
    logger.info(f"Execution Summary: {passed} passed, {failed} failed, {skipped} skipped")

    if failed > 0:
        logger.info("Failed steps:")
        for step_name, (success, error_msg) in results.items():
            if not success:
                logger.info(f"  - {step_name}: {error_msg}")

    if skipped_scripts:
        logger.info("Skipped steps:")
        for step_name, reason in skipped_scripts.items():
            logger.info(f"  - {step_name}: {reason}")

    logger.info("=" * 70)

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
