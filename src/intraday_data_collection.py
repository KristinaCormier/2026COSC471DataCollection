# On execution, this script should fetch stock data from the current time
# to the bottom of the hour at 5 minute intervals in EST.
# It should insert this data into postgres for each symbol provided
# This script is designed to be executed every hour throughout every extended market day using a job scheduler

from __future__ import annotations

import os
import datetime as dt
import sys
import requests
from zoneinfo import ZoneInfo

# DB client
import psycopg

# Internal utilities
from src import data_validation as dv
from src import time_utils as tu
from src import db_utils as dbu
from src import logging_utils as lu

# load in environment vars 
API_KEY        = os.environ.get("FMP_API_KEY", "")
SYMBOLS        = os.environ.get("SYMBOLS", "AAPL").split(",")
MARKET_TZ      = os.environ.get("MARKET_TZ", "America/New_York")
WINDOW_MIN     = int(os.environ.get("WINDOW_MINUTES", "60")) # prevents grabbing large range of data 
MARKET_OPEN    = os.environ.get("MARKET_OPEN", "04:00")
MARKET_CLOSE   = os.environ.get("MARKET_CLOSE", "21:00")

# DB connection vars
PGHOST = os.environ.get("PGHOST", "")
PGPORT = int(os.environ.get("PGPORT", ""))
PGDATABASE = os.environ.get("PGDATABASE", "")
PGUSER = os.environ.get("PGUSER", "")
PGPASSWORD = os.environ.get("PGPASSWORD", "")

BASE_URL = "https://financialmodelingprep.com/api/v3/historical-chart/5min/{symbol}"

DATA_FIELDS = ("date", "open", "high", "low", "close", "volume")

# cleanup symbols
SYMBOLS = [s.strip() for s in SYMBOLS if s.strip()]

# timezone handling
TZ = ZoneInfo(MARKET_TZ)

#  API Fetch and insert into Postgres
def fetch_and_insert(conn, symbol: str, start: dt.datetime, end: dt.datetime, now_local: dt.datetime | None = None):
    
    day_from = tu.ymd(min(start.date(), end.date())) 
    day_to   = tu.ymd(max(start.date(), end.date())) 
    url = BASE_URL.format(symbol=symbol)
    params = {"from": day_from, "to": day_to, "extended": "true", "apikey": API_KEY}

    print(
        f"\n   Calling API with symbol = {symbol}   Window Used: {start} -> {end}  (from {day_from} to {day_to}) ---"
    )

    try:
        r = requests.get(url, params=params, timeout=25)  # GET API data
        r.raise_for_status()
        data = r.json() if r.content else []
    except requests.exceptions.HTTPError as e:
        lu.log_api_error(
            symbol=symbol,
            url=url,
            error_type="HTTPError",
            error_message=str(e),
            status_code=r.status_code if hasattr(r, 'status_code') else None,
            tz=TZ,
        )
        raise
    except requests.exceptions.Timeout as e:
        lu.log_api_error(
            symbol=symbol,
            url=url,
            error_type="Timeout",
            error_message=str(e),
            tz=TZ,
        )
        raise
    except requests.exceptions.RequestException as e:
        lu.log_api_error(
            symbol=symbol,
            url=url,
            error_type=type(e).__name__,
            error_message=str(e),
            tz=TZ,
        )
        raise
    # Recently added - Liam
    print("API returned rows:", len(data))
    if data:
        print("First:", data[0].get("date"), "Last:", data[-1].get("date"))

    # Filter to the requested window and prepare rows for DB
    rows = []
    last_ts = None
    for row in data: # loop through json response
        missing_fields, all_empty = dv.analyze_row(row, DATA_FIELDS)
        if all_empty:
            lu.log_validation_error(
                symbol=symbol,
                row_data=row,
                missing_fields=missing_fields,
                reason="all_fields_empty",
                tz=TZ,
            )
            continue

        ts_str = row.get("date")
        ts_exch = None
        inferred_reason = None
        inferred_date = None

        # handle missing/invalid timestamp
        if dv.is_empty(ts_str):
            ts_exch, inferred_reason = tu.infer_timestamp(last_ts, now_local, "date_missing", TZ)
            inferred_date = ts_exch
            missing_fields = list(set(missing_fields + ["date"]))
        else:
            try:
                ts_exch = tu.parse_api_time(ts_str, TZ)
            except Exception:
                ts_exch, inferred_reason = tu.infer_timestamp(last_ts, now_local, "date_invalid", TZ)
                inferred_date = ts_exch
                missing_fields = list(set(missing_fields + ["date"]))

        if ts_exch is not None:
            last_ts = ts_exch

        if missing_fields or inferred_reason:
            lu.log_validation_error(
                symbol=symbol,
                row_data=row,
                missing_fields=missing_fields,
                inferred_date=inferred_date,
                reason=inferred_reason,
                tz=TZ,
            )

        # grab all data fields from api 
        open_val   = row.get("open")
        high_val   = row.get("high")
        low_val    = row.get("low")
        close_val  = row.get("close")
        volume_val = row.get("volume")

        # append data to rows list 
        rows.append((ts_exch, open_val, high_val, low_val, close_val, volume_val))

    rows.sort(key=lambda x: x[0])  # sort rows ascending by timestamp

    if not rows:
        print("(no 5 minute bars in this window)")
        return 0

    table = dbu.safe_table_name_for_symbol(symbol)
    dbu.check_table_exists(conn, table)

    # UPSERT with all columns (ts remains the conflict target)*updated from ts to date on two lines below to match db schema*
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
    # Execute sql query on postgres
    try:
        with conn.cursor() as cur:
            cur.executemany(insert_sql.replace("VALUES %s", "VALUES (%s, %s, %s, %s, %s, %s)"), rows)
        conn.commit()
    except psycopg.Error as e:
        lu.log_db_error(
            symbol=symbol,
            operation="UPSERT",
            error_type=type(e).__name__,
            error_message=str(e),
            table_name=table,
            row_count=len(rows),
            tz=TZ,
        )
        raise 

    print(f"inserted/upserted {len(rows)} rows into {table}")
    return len(rows)


def main():
    global API_KEY, SYMBOLS, MARKET_TZ, WINDOW_MIN, MARKET_OPEN, MARKET_CLOSE, PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD, BASE_URL, TZ

    # re-load env vars at runtime (not import time)
    API_KEY = os.environ.get("FMP_API_KEY", "")
    SYMBOLS = [
        s.strip() for s in os.environ.get("SYMBOLS", "AAPL").split(",") if s.strip()
    ]
    MARKET_TZ = os.environ.get("MARKET_TZ", "America/New_York")
    WINDOW_MIN = int(os.environ.get("WINDOW_MINUTES", "60"))
    MARKET_OPEN = os.environ.get("MARKET_OPEN", "04:00")
    MARKET_CLOSE = os.environ.get("MARKET_CLOSE", "21:00")

    PGHOST = os.environ.get("PGHOST", "")
    PGPORT = int(os.environ.get("PGPORT", "5432"))
    PGDATABASE = os.environ.get("PGDATABASE", "")
    PGUSER = os.environ.get("PGUSER", "")
    PGPASSWORD = os.environ.get("PGPASSWORD", "")

    BASE_URL = "https://financialmodelingprep.com/api/v3/historical-chart/5min/{symbol}"

    TZ = ZoneInfo(MARKET_TZ)
    now_local = dt.datetime.now(TZ)

    try:
        market_open_time = tu.parse_hhmm(MARKET_OPEN)
        market_close_time = tu.parse_hhmm(MARKET_CLOSE)
    except ValueError as e:
        print(f"error: invalid market hours: {e}", file=sys.stderr)
        sys.exit(1)

    if not tu.is_market_open(now_local, market_open_time, market_close_time):
        print(
            "[info] market closed; skipping collection "
            f"(hours {MARKET_OPEN}-{MARKET_CLOSE} {MARKET_TZ})"
        )
        sys.exit(0)

    start, end = tu.compute_window(now_local, WINDOW_MIN)

    if not API_KEY:
        print("error: API key is missing; ensure FMP_API_KEY is set", file=sys.stderr)
        sys.exit(1)


    print()
    print(
        f" [Collector Startup] \n Using Symbols:={SYMBOLS} \n Using Timezone: {MARKET_TZ} \n Using Time Window: {start} -> {end}"
    )

    # connect once and reuse connection for all inserts
    try:
        conn = dbu.db_connect(PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD)
    except Exception as e:
        lu.log_db_error(
            symbol="N/A",
            operation="CONNECT",
            error_type=type(e).__name__,
            error_message=str(e),
            tz=TZ,
        )
        print(f"cannot connect to Postgres: {e}", file=sys.stderr)
        sys.exit(2)

    total = 0
    for sym in SYMBOLS:
        try:
            total += fetch_and_insert(conn, sym, start, end)
        except Exception as e:
            print(f"[error] {sym}: {e}", file=sys.stderr)

    try:
        conn.close() # close db connection 
    except Exception as e:
        print(f"[warning] failed to close database connection: {e}", file=sys.stderr)

    print()
    print(f" [done] total rows ingested: {total}")


if __name__ == "__main__":
    main()
