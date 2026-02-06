"""
Data validation and error logging utilities.
Analyzes row completeness, identifies missing fields, and logs data quality issues.
"""

from __future__ import annotations

import csv
import datetime as dt
import json
from pathlib import Path
from typing import Iterable, Mapping, Sequence


def is_empty(value) -> bool:
    """Check if a value is None or an empty string."""
    return value is None or (isinstance(value, str) and value.strip() == "")


def analyze_row(row: Mapping[str, object], fields: Sequence[str]) -> tuple[list[str], bool]:
    """
    Analyze a data row for missing or empty fields.
    
    Args:
        row: Dictionary of field values
        fields: List of expected field names
    
    Returns:
        Tuple of (list of missing field names, whether all fields are empty)
    """
    missing_fields = [field for field in fields if is_empty(row.get(field))]
    all_empty = len(missing_fields) == len(fields)
    return missing_fields, all_empty


def log_row_issues(
    *,
    symbol: str,
    row: Mapping[str, object],
    missing_fields: Iterable[str],
    inferred_date: dt.datetime | None = None,
    reason: str | None = None,
    log_path: str = "logs/data_error_log.csv",
    tz: dt.tzinfo | None = None,
) -> None:
    """
    Log data quality issues to a CSV file.
    
    Args:
        symbol: Stock symbol
        row: Original data row with issues
        missing_fields: List of missing/empty field names
        inferred_date: Inferred timestamp if date was missing
        reason: Reason for the issue (e.g., 'date_missing_prev_row')
        log_path: Path to the log file
        tz: Timezone for timestamp
    """
    if not missing_fields and not reason:
        return

    log_file = Path(log_path)
    log_file.parent.mkdir(parents=True, exist_ok=True)

    run_ts = (dt.datetime.now(tz) if tz else dt.datetime.now()).isoformat()
    inferred_str = inferred_date.isoformat() if inferred_date else ""
    missing_str = "|".join(sorted(set(missing_fields))) if missing_fields else ""
    reason_str = reason or ""
    row_json = json.dumps(row, sort_keys=True)

    header = ["run_ts_market", "symbol", "inferred_date", "missing_fields", "reason", "row_json"]

    is_new = not log_file.exists()
    with log_file.open("a", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        if is_new:
            writer.writerow(header)
        writer.writerow([run_ts, symbol, inferred_str, missing_str, reason_str, row_json])


def validate_row(row: Mapping[str, object], fields: Sequence[str]) -> tuple[list[str], bool]:
    """
    Validate a row and return missing fields.
    
    Alias for analyze_row for backward compatibility.
    
    Args:
        row: Dictionary of field values
        fields: List of expected field names
    
    Returns:
        Tuple of (list of missing field names, whether all fields are empty)
    """
    return analyze_row(row, fields)
