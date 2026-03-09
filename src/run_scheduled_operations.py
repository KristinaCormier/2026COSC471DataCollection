#!/usr/bin/env python3
# Orchestrates scheduled SQL scripts in src/sql_operations/.
# Discovers .sql files, runs them alphabetically, logs results to operation_logs.pipeline_logs.
# Intended to be called via cron or manually.
#
# Usage: python3 src/run_scheduled_operations.py
#
# Requires PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD env vars.

from __future__ import annotations

import os
import sys
import logging
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

import psycopg

# Setup logging to both file and stderr
# Log directory must be pre-provisioned by setup script
load_dotenv()  # Load environment variables from .env file
LOG_DIR = Path(os.getenv("LOG_DIR", "./logs")) # Default to ./logs if not set


# Logging configuration is deferred to main() to allow for startup validation
logger = None


def configure_logging() -> logging.Logger:
    """
    Configure logging after validating that the log directory exists and is writable.
    Must be called from main() before any logging operations.
    
    Returns:
        Configured logger instance
        
    Raises:
        FileNotFoundError: If LOG_DIR does not exist
        PermissionError: If LOG_DIR is not writable
    """
    # Validate log directory exists and is writable
    if not LOG_DIR.exists():
        raise FileNotFoundError(
            f"Log directory does not exist: {LOG_DIR}\n"
            f"Run the setup script: sudo bash setup_scripts/setup_cronjob_scheduled_operations.sh"
        )
    if not LOG_DIR.is_dir():
        raise NotADirectoryError(f"Log path exists but is not a directory: {LOG_DIR}")
    
    # Test write permissions
    if not (LOG_DIR.stat().st_mode & 0o200):
        raise PermissionError(
            f"Log directory is not writable: {LOG_DIR}\n"
            f"Check ownership: ls -ld {LOG_DIR}"
        )
    
    # Configure logging now that directory is validated
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(LOG_DIR / 'scheduled_operations.log'),
            logging.StreamHandler(sys.stderr)
        ]
    )
    return logging.getLogger(__name__)

# Database configuration from environment
PGHOST = os.getenv("PGHOST", "localhost")
PGPORT = int(os.getenv("PGPORT", "5432"))
PGDATABASE = os.getenv("PGDATABASE")
PGUSER = os.getenv("PGUSER")
PGPASSWORD = os.getenv("PGPASSWORD", "")

SRC_DIR = Path(__file__).parent
SQL_SCRIPTS_DIR = SRC_DIR / "sql_operations"


def validate_environment() -> bool:
    """Check that PGDATABASE and PGUSER are set."""
    pgdatabase = os.getenv("PGDATABASE")
    pguser = os.getenv("PGUSER")
    
    if not pgdatabase:
        if logger:
            logger.error("PGDATABASE environment variable is not set")
        return False
    if not pguser:
        if logger:
            logger.error("PGUSER environment variable is not set")
        return False
    return True


def db_connect() -> psycopg.Connection:
    """Create a PostgreSQL database connection."""
    return psycopg.connect(
        host=PGHOST,
        port=PGPORT,
        dbname=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD,
    )


def read_sql_file(path: Path) -> str:
    """Read SQL script from file."""
    return path.read_text(encoding='utf-8')


def log_execution(
    conn: psycopg.Connection,
    script_name: str,
    status: str,
    duration_seconds: float,
    error_message: Optional[str] = None,
    record_count: Optional[int] = None
) -> None:
    """Log execution metadata to operation_logs.pipeline_logs."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO operation_logs.pipeline_logs (
                    pipeline_step,
                    start_time,
                    end_time,
                    status,
                    error_message,
                    record_count
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                )
            """, (
                script_name,
                datetime.now(timezone.utc),
                datetime.now(timezone.utc),
                status,
                error_message,
                record_count
            ))
        conn.commit()
    except Exception as e:
        if logger:
            logger.error(f"Failed to log execution for {script_name}: {e}")


def execute_sql_script(
    conn: psycopg.Connection,
    script_path: Path,
    script_name: str
) -> tuple[bool, Optional[str]]:
    """Execute a single SQL script and return (success, error_message)."""
    start_time = datetime.now(timezone.utc)
    
    try:
        if logger:
            logger.info(f"Executing {script_name}...")
        sql_content = read_sql_file(script_path)
        
        with conn.cursor() as cur:
            cur.execute(sql_content)
        
        conn.commit()
        
        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        if logger:
            logger.info(f"{script_name} completed successfully ({duration:.2f}s)")
        
        # Log success to pipeline_logs
        log_execution(conn, script_name, 'success', duration)
        
        return True, None
        
    except Exception as e:
        conn.rollback()
        error_msg = str(e)
        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        
        if logger:
            logger.error(f"{script_name} failed: {error_msg}")
            logger.error(f"   Duration: {duration:.2f}s")
        
        # Log failure to pipeline_logs
        log_execution(conn, script_name, 'failure', duration, error_msg)
        
        return False, error_msg


def main() -> int:
    """Execute all scheduled SQL operations in alphabetical order."""
    global logger
    
    # Configure logging at startup - must be done before any logger calls
    try:
        logger = configure_logging()
    except (FileNotFoundError, PermissionError, NotADirectoryError) as e:
        print(f"ERROR: Failed to configure logging: {e}", file=sys.stderr)
        return 1
    
    logger.info("=" * 70)
    logger.info(f"Starting scheduled operations run at {datetime.now(timezone.utc).isoformat()}")
    logger.info("=" * 70)
    
    # Validate environment
    if not validate_environment():
        logger.error("Environment validation failed. Exiting.")
        return 1
    
    # Check SQL scripts directory exists
    if not SQL_SCRIPTS_DIR.exists():
        logger.warning(f"SQL scripts directory not found: {SQL_SCRIPTS_DIR}")
        return 1
    
    # Discover SQL files in alphabetical order
    sql_files = sorted(SQL_SCRIPTS_DIR.glob("*.sql"))
    
    if not sql_files:
        logger.warning(f"No .sql files found in {SQL_SCRIPTS_DIR}")
        return 1
    
    # Connect to database
    try:
        conn = db_connect()
        logger.info(f"Connected to {PGDATABASE} on {PGHOST}:{PGPORT}")
    except psycopg.Error as e:
        logger.error(f"Failed to connect to database: {e}")
        return 1
    
    # Execute each script
    results = {}
    try:
        for script in sql_files:
            script_name = script.name
            success, error_msg = execute_sql_script(conn, script, script_name)
            results[script_name] = (success, error_msg)
    finally:
        conn.close()
    
    # Summary
    logger.info("=" * 70)
    passed = sum(1 for success, _ in results.values() if success)
    failed = len(results) - passed
    
    logger.info(f"Execution Summary: {passed} passed, {failed} failed")
    
    if failed > 0:
        logger.info("Failed scripts:")
        for script_name, (success, error_msg) in results.items():
            if not success:
                logger.info(f"  - {script_name}: {error_msg}")
    
    logger.info("=" * 70)
    
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
