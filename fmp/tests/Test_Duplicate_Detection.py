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

#Test 8 Test for duplicate records. US-10 Sub-Issue: #31
#Detection and logging of duplicate records.

def test_dedupe_rows_counts_duplicates():
    tz = ZoneInfo("America/New_York")
    ts = dt.datetime(2026, 1, 26, 10, 0, tzinfo=tz)

    rows = [
        (ts, 1, 2, 1, 1.5, 10),
        (ts, 1, 2, 1, 1.6, 11),  # duplicate timestamp, later value
    ]
    deduped, dup_count = collector.dedupe_rows(rows)

    assert dup_count == 1
    assert len(deduped) == 1
    assert deduped[0][4] == 1.6  # keeps last occurrence

