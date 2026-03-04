"""
Error logging utilities for stock data collection pipeline.
Handles three types of errors: API request errors, data validation errors, and DB insertion errors.
"""

from __future__ import annotations

import csv
import datetime as dt
import json
from pathlib import Path


ERROR_LOG_DIR = Path("/usr/local/dc_error_logs")


def _ensure_log_file(log_path: Path, headers: list[str]) -> None:
    """
    Ensure the log directory and file exist with proper headers.
    
    Args:
        log_path: Path to the log file
        headers: List of column headers
    """
    log_path.parent.mkdir(parents=True, exist_ok=True)
    if not log_path.exists():
        with log_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(headers)


def log_api_error(
    symbol: str,
    url: str,
    error_type: str,
    error_message: str,
    status_code: int | None = None,
    tz: dt.tzinfo | None = None,
) -> None:
    """
    Log API request errors to /usr/local/dc_error_logs/api_errors.csv
    
    Args:
        symbol: Stock symbol being queried
        url: API endpoint URL
        error_type: Type of error (e.g., 'HTTPError', 'Timeout', 'ConnectionError')
        error_message: Detailed error message
        status_code: HTTP status code if applicable
        tz: Timezone for timestamp
    """
    log_path = ERROR_LOG_DIR / "api_errors.csv"
    headers = ["timestamp", "symbol", "url", "error_type", "error_message", "status_code"]
    _ensure_log_file(log_path, headers)
    
    timestamp = (dt.datetime.now(tz) if tz else dt.datetime.now()).isoformat()
    
    with log_path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([timestamp, symbol, url, error_type, error_message, status_code or ""])
    
    print(f"API error logged: {log_path}")


def log_validation_error(
    symbol: str,
    row_data: dict,
    missing_fields: list[str] | None = None,
    inferred_date: dt.datetime | None = None,
    reason: str | None = None,
    tz: dt.tzinfo | None = None,
) -> None:
    """
    Log data validation errors to /usr/local/dc_error_logs/data_errors.csv
    
    Args:
        symbol: Stock symbol
        row_data: The problematic data row as a dictionary
        missing_fields: List of missing or empty field names
        inferred_date: Inferred timestamp if date was missing/invalid
        reason: Reason for the validation error (e.g., 'all_fields_empty', 'date_missing')
        tz: Timezone for timestamp
    """
    log_path = ERROR_LOG_DIR / "data_errors.csv"
    headers = ["timestamp", "symbol", "missing_fields", "inferred_date", "reason", "row_json"]
    _ensure_log_file(log_path, headers)
    
    timestamp = (dt.datetime.now(tz) if tz else dt.datetime.now()).isoformat()
    inferred_str = inferred_date.isoformat() if inferred_date else ""
    missing_str = "|".join(sorted(set(missing_fields))) if missing_fields else ""
    reason_str = reason or ""
    row_json = json.dumps(row_data, sort_keys=True)
    
    with log_path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([timestamp, symbol, missing_str, inferred_str, reason_str, row_json])
    
    print(f"Validation error logged: {log_path}")


def log_db_error(
    symbol: str,
    operation: str,
    error_type: str,
    error_message: str,
    table_name: str | None = None,
    row_count: int | None = None,
    tz: dt.tzinfo | None = None,
) -> None:
    """
    Log database insertion/operation errors to /usr/local/dc_error_logs/db_insert_errors.csv
    
    Args:
        symbol: Stock symbol being processed
        operation: Database operation (e.g., 'INSERT', 'UPSERT', 'CONNECT', 'COMMIT')
        error_type: Type of error (e.g., 'IntegrityError', 'OperationalError', 'ConnectionError')
        error_message: Detailed error message
        table_name: Target table name if applicable
        row_count: Number of rows being processed when error occurred
        tz: Timezone for timestamp
    """
    log_path = ERROR_LOG_DIR / "db_insert_errors.csv"
    headers = ["timestamp", "symbol", "operation", "error_type", "error_message", "table_name", "row_count"]
    _ensure_log_file(log_path, headers)
    
    timestamp = (dt.datetime.now(tz) if tz else dt.datetime.now()).isoformat()
    
    with log_path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            timestamp,
            symbol,
            operation,
            error_type,
            error_message,
            table_name or "",
            row_count if row_count is not None else ""
        ])
    
    print(f"DB error logged: {log_path}")
