

import datetime as dt
import pytest
from zoneinfo import ZoneInfo

# Import the collector module.
# If your repo uses a different path/name, adjust this.
# Option 1 (recommended): make scripts a package by adding scripts/__init__.py the$
#   from scripts import Auto_Data_Collection as collector
#
# Option 2: simple import if running from repo root and python can find it:
import scripts.Auto_Data_Collection as collector

#Test 7 Test for missing required fields. US-10 Sub-Issue: #30
#Detection of missing or null values in required fields 

def test_extract_valid_rows_skips_missing_date():
    tz = ZoneInfo("America/New_York")
    start = dt.datetime(2026, 1, 26, 10, 0, tzinfo=tz)
    end   = dt.datetime(2026, 1, 26, 10, 30, tzinfo=tz)

    api_data = [{"open": 1, "high": 2, "low": 1, "close": 1.5, "volume": 10}]  # no "date"
    rows, stats = collector.extract_valid_rows(api_data, start, end)

    assert rows == []
    assert stats["skipped_missing_date"] == 1

