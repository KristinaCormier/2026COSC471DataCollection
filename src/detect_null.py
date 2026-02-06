from __future__ import annotations
import csv
import json
import sys
import datetime as dt
from pathlib import Path
from typing import Mapping, Sequence, Iterable

def validate_row(row:Mapping[str, object], fields:Sequence[str]) -> tuple[list[str], bool]:
    return analyze_row(row, fields)

def analyze_row(row:Mapping[str, object], fields:Sequence[str]) -> tuple[list[str], bool]:
    missing_fields = [field for field in fields if is_empty(row.get(field))]
    all_empty = len(missing_fields) == len(fields)
    return missing_fields, all_empty

def log_row_issues(*, symbol:str, row:Mapping[str, object], missing_fields:Iterable[str], inferred_date:dt.datetime, reason:str, log_path:str = "../../logs/null_data.log", tz:dt.tzinfo) -> None:
    if not missing_fields and not reason:
        return
    log_file = Path(log_path) 
    log_file.parent.mkdir(parents=True, exist_ok=True)
    run_ts = (dt.datetime.now(tz) if tz else dt.datetime.now()).isoformat()
    inferred_string = inferred_date.isoformat() if inferred_date else ""
    missing_string = ", ".join(sorted(set(missing_fields))) if missing_fields else ""
    reason_string = reason if reason else ""
    row_json = json.dumps(row, sort_keys=True)
    header = ["run_timestamp", "symbol", "inferred_date", "missing_fields", "reason", "row_json"]
    is_new_file = not log_file.exists()
    with log_file.open("a", encoding="utf-8", newline = "") as f:
        writer = csv.writer(f)
        if is_new_file:
            writer.writerow(header)
        writer.writerow([run_ts, symbol, inferred_string, missing_string, reason_string, row_json])

def is_empty(value:object) -> bool:
    return value is None or (isinstance(value, str) and value.strip() == "")