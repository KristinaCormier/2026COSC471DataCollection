
import datetime as dt
import pytest
from zoneinfo import ZoneInfo

# Import the collector module.
# If your repo uses a different path/name, adjust this.
# Option 1 (recommended): make scripts a package by adding scripts/__init__.py then:
#   from scripts import Auto_Data_Collection as collector
#
# Option 2: simple import if running from repo root and python can find it:
import scripts.Auto_Data_Collection as collector

#Test 4. Test for ymd()
# Should return ISO formatted date string

def test_ymd_format():
    d = dt.date(2026, 1, 26)
   
