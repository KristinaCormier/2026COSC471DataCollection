
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

#Test 6 Test for valid timestamps. US-10 Sub-Issue: #29
#Validation rules for expected data ranges and formats (prices > 0, valid timestamps).


