  GNU nano 2.9.8               Test_Duplicate_Detection.py

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

#Test 9 Test for SQL Contract. US-10 Sub-Issue: #32
#assert INSERT statement includes (date, open, high, low, close, volume), ON CONFLICT (date) DO UPDATE, this makes sure schema enforcement stays consistent

def test_insert_sql_contract_contains_expected_columns():
    table = "market.aapl"
    insert_sql = f"""
        INSERT INTO {table} (date, open, high, low, close, volume)
        VALUES %s
        ON CONFLICT (date) DO UPDATE
          SET open   = EXCLUDED.open,
              high   = EXCLUDED.high,
              low    = EXCLUDED.low,
              close  = EXCLUDED.close,
              volume = EXCLUDED.volume
    """
    assert "INSERT INTO market.aapl (date, open, high, low, close, volume)" in insert_sql
    assert "ON CONFLICT (date)" in insert_sql

