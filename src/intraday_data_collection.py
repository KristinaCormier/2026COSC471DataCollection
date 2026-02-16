# On execution, this script should fetch stock data from the current time
# to the bottom of the hour at 5 minute intervals in EST.
# It should insert this data into postgres for each symbol provided
# This script is designed to be executed every hour throughout every extended market day using a job scheduler

from __future__ import annotations

import os
import datetime as dt
import sys
import requests
from dotenv import load_dotenv
from zoneinfo import ZoneInfo

# DB client
import psycopg

# Internal utilities
import data_validation as dv
import time_utils as tu
import db_utils as dbu
import logging_utils as lu


# load in environment vars 
load_dotenv()
API_KEY        = os.environ.get("FMP_API_KEY", "")
SYMBOLS        = os.environ.get("SYMBOLS", "AAPL,AMD,AMZN,BA,BABA,BAC,C,CSCO,CVX,DIS,F,GE,GOOGL,IBM,INTC,JNJ,JPM,KO,MCD,META,MSFT,NFLX,NVDA,PFE,T,TSLA,VZ,WMT,XOM").split(",")
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
REQUIRED_NUMERIC_FIELDS = ("open", "high", "low", "volume")

# cleanup symbols
SYMBOLS = [s.strip() for s in SYMBOLS if s.strip()]

# timezone handling
TZ = ZoneInfo(MARKET_TZ)


def _coerce_float(value) -> tuple[float | None, bool]:
    """
    Attempt to coerce a value to float.
    
    Returns:
        Tuple of (coerced_value, success)
    """
    if value is None:
        return None, False
    if isinstance(value, bool):
        return None, False
    if isinstance(value, (int, float)):
        return float(value), True
    if isinstance(value, str):
        try:
            return float(value), True
        except ValueError:
            return None, False
    return None, False


def _build_insert_statement(table: str) -> str:
    """
    Build the UPSERT SQL statement for stock data.
    
    Args:
        table: Qualified table name (e.g., 'market.aapl')
    
    Returns:
        SQL string ready for executemany
    """
    return f"""
        INSERT INTO {table} (date, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (date) DO UPDATE
          SET open   = EXCLUDED.open,
              high   = EXCLUDED.high,
              low    = EXCLUDED.low,
              close  = EXCLUDED.close,
              volume = EXCLUDED.volume
    """


def _validate_and_parse_row(
    row: dict,
    symbol: str,
    table: str,
    hard_invalid_found: bool,
    last_ts: dt.datetime | None,
    now_local: dt.datetime | None,
    tz: ZoneInfo,
) -> tuple[dt.datetime | None, dict | None, bool]:
    """
    Validate and parse a single row from API data.
    
    Checks for all-empty fields, timestamp validity, and schema/type compliance.
    Logs errors to db_insert_errors for invalid rows.
    
    Args:
        row: Raw API row dictionary
        symbol: Stock symbol being processed
        table: Target table name
        hard_invalid_found: Whether any hard invalids found so far in batch
        last_ts: Previous row's timestamp (for inference)
        now_local: Current wall-clock time (for inference)
        tz: Timezone for timestamps
    
    Returns:
        Tuple of (timestamp, parsed_values_dict, updated_hard_invalid_found)
        or (None, None, hard_invalid_found) if row rejected
    """
    missing_fields, all_empty = dv.analyze_row(row, DATA_FIELDS)
    if all_empty:
        lu.log_db_error(
            symbol=symbol,
            operation="LOAD",
            error_type="AllFieldsEmpty",
            error_message="all fields empty",
            table_name=table,
            row_count=1,
            tz=tz,
        )
        return None, None, True

    ts_str = row.get("date")
    ts_exch = None

    # handle missing/invalid timestamp
    if dv.is_empty(ts_str):
        if hard_invalid_found:
            lu.log_db_error(
                symbol=symbol,
                operation="LOAD",
                error_type="MissingDate",
                error_message="date field missing",
                table_name=table,
                row_count=1,
                tz=tz,
            )
            return None, None, hard_invalid_found
        ts_exch, _ = tu.infer_timestamp(last_ts, now_local, "date_missing", tz)
    else:
        try:
            ts_exch = tu.parse_api_time(ts_str, tz)
        except Exception:
            lu.log_db_error(
                symbol=symbol,
                operation="LOAD",
                error_type="InvalidTimestamp",
                error_message="invalid timestamp format",
                table_name=table,
                row_count=1,
                tz=tz,
            )
            return None, None, True

    # enforce schema/types before insertion
    invalid_fields = []
    parsed = {}
    for field in REQUIRED_NUMERIC_FIELDS:
        val = row.get(field)
        if dv.is_empty(val):
            invalid_fields.append(field)
            continue
        num, ok = _coerce_float(val)
        if not ok:
            invalid_fields.append(field)
            continue
        parsed[field] = num

    close_val = row.get("close")
    if dv.is_empty(close_val):
        parsed["close"] = None
    else:
        num, ok = _coerce_float(close_val)
        if not ok:
            invalid_fields.append("close")
        else:
            parsed["close"] = num

    if invalid_fields:
        lu.log_db_error(
            symbol=symbol,
            operation="LOAD",
            error_type="SchemaTypeMismatch",
            error_message=f"invalid fields: {','.join(sorted(set(invalid_fields)))}",
            table_name=table,
            row_count=1,
            tz=tz,
        )
        return None, None, True

    return ts_exch, parsed, hard_invalid_found


def _fetch_api_data(
    symbol: str,
    start: dt.datetime,
    end: dt.datetime,
) -> list[dict]:
    """
    Fetch stock data from API.
    
    Single responsibility: API communication layer.
    Raises on network or API errors.
    
    Args:
        symbol: Stock symbol
        start: Start time for data window
        end: End time for data window
    
    Returns:
        List of raw API data rows
    """
    day_from = tu.ymd(min(start.date(), end.date()))
    day_to = tu.ymd(max(start.date(), end.date()))
    url = BASE_URL.format(symbol=symbol)
    params = {"from": day_from, "to": day_to, "extended": "true", "apikey": API_KEY}

    try:
        r = requests.get(url, params=params, timeout=25)
        r.raise_for_status()
        return r.json() if r.content else []
    except requests.exceptions.HTTPError as e:
        lu.log_api_error(
            symbol=symbol,
            url=url,
            error_type="HTTPError",
            error_message=str(e),
            status_code=r.status_code if hasattr(r, "status_code") else None,
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


def _process_data_batch(
    data: list[dict],
    symbol: str,
    table: str,
    now_local: dt.datetime | None,
) -> list[tuple]:
    """
    Process, validate, and deduplicate raw API data.
    
    Single responsibility: data processing and filtering layer.
    Returns validated, de-duplicated data ready for insertion.
    
    Args:
        data: Raw API data rows
        symbol: Stock symbol
        table: Target table name
        now_local: Current wall-clock time for timestamp inference
    
    Returns:
        List of validated tuples ready for database insertion
    """
    hard_invalid_found = False
    rows = []
    last_ts = None
    seen_ts: set[dt.datetime] = set()

    for row in data:
        ts_exch, parsed, hard_invalid_found = _validate_and_parse_row(
            row, symbol, table, hard_invalid_found, last_ts, now_local, TZ
        )
        if ts_exch is None:
            continue

        # Check for duplicate timestamp in batch
        if ts_exch in seen_ts:
            lu.log_db_error(
                symbol=symbol,
                operation="LOAD",
                error_type="DuplicateTimestamp",
                error_message="duplicate timestamp in batch",
                table_name=table,
                row_count=1,
                tz=TZ,
            )
            hard_invalid_found = True
            continue

        seen_ts.add(ts_exch)
        last_ts = ts_exch
        rows.append(
            (
                ts_exch,
                parsed["open"],
                parsed["high"],
                parsed["low"],
                parsed["close"],
                parsed["volume"],
            )
        )

    rows.sort(key=lambda x: x[0])  # sort rows ascending by timestamp
    return rows


def _insert_batch(
    conn,
    table: str,
    rows: list[tuple],
    symbol: str,
) -> int:
    """
    Insert validated rows into database.
    
    Single responsibility: database layer.
    Handles table existence check and UPSERT operation.
    
    Args:
        conn: Database connection
        table: Target table name
        rows: Validated data tuples ready for insertion
        symbol: Stock symbol (for error logging)
    
    Returns:
        Number of rows inserted/upserted
    """
    if not rows:
        return 0

    dbu.check_table_exists(conn, table)

    insert_sql = _build_insert_statement(table)
    try:
        with conn.cursor() as cur:
            cur.executemany(insert_sql, rows)
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
        s.strip() for s in os.environ.get("SYMBOLS", "AAPL,AMD,AMZN,BA,BABA,BAC,C,CSCO,CVX,DIS,F,GE,GOOGL,IBM,INTC,JNJ,JPM,KO,MCD,META,MSFT,NFLX,NVDA,PFE,T,TSLA,VZ,WMT,XOM").split(",") if s.strip()
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
    day_from = tu.ymd(min(start.date(), end.date()))
    day_to = tu.ymd(max(start.date(), end.date()))
    for sym in SYMBOLS:
        try:
            print(
                f"\n   Calling API with symbol = {sym}   Window Used: {start} -> {end}  (from {day_from} to {day_to}) ---"
            )

            # Step 1: Fetch data from API
            data = _fetch_api_data(sym, start, end)

            print("API returned rows:", len(data))
            if data:
                print("First:", data[0].get("date"), "Last:", data[-1].get("date"))

            # Step 2: Get table name for symbol
            table = dbu.safe_table_name_for_symbol(sym)

            # Step 3: Process and validate batch
            rows = _process_data_batch(data, sym, table, now_local)

            if rows:
                # Step 4: Insert into database
                total += _insert_batch(conn, table, rows, sym)
            else:
                print("(no 5 minute bars in this window)")
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
