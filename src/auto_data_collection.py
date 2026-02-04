# On execution, this script should fetch stock data from the current time 
# to the bottom of the hour at 5 minute intervals in EST. 
# It should insert this data into postgres for each symbol provided
# This script is designed to be executed every hour throughout every extended market day using a job scheduler 

import os
import json
import datetime as dt
import sys
import re
import requests
from pathlib import Path
# DB client
import psycopg2
from psycopg2.extras import execute_values

# load in enviornment vars 
API_KEY        = os.environ.get("FMP_API_KEY", "")
SYMBOLS        = os.environ.get("SYMBOLS", "AAPL").split(",")
MARKET_TZ      = os.environ.get("MARKET_TZ", "America/New_York")
WINDOW_MIN     = int(os.environ.get("WINDOW_MINUTES", "60")) # prevents grabbing large range of data 

# DB connection vars 
PGHOST     = os.environ.get("PGHOST", "")
PGPORT     = int(os.environ.get("PGPORT", ""))
PGDATABASE = os.environ.get("PGDATABASE", "")
PGUSER     = os.environ.get("PGUSER", "")
PGPASSWORD = os.environ.get("PGPASSWORD", "")

BASE_URL = "https://financialmodelingprep.com/api/v3/historical-chart/5min/{symbol}"

#moved to main()
#if not API_KEY:
 #   print("error: API key is missing; ensure FMP_API_KEY is set", file=sys.stderr)
  #  sys.exit(1)

# cleanup symbols
SYMBOLS = [s.strip() for s in SYMBOLS if s.strip()]

# timezone handling
from zoneinfo import ZoneInfo
TZ = ZoneInfo(MARKET_TZ)

# helper methods:

def current_hour(now: dt.datetime) -> dt.datetime:
    return now.replace(minute=0, second=0, microsecond=0)

def parse_api_time(s: str) -> dt.datetime:
    # helper to parse timestamp from api
    return dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=TZ)

def ymd(d: dt.date) -> str:
    # years months date format 
    return d.strftime("%Y-%m-%d")

def safe_table_name_for_symbol(sym: str) -> str:
    # get ticker name to match with postgres tables
    base = re.sub(r"[^A-Za-z0-9]", "", sym).lower()
    if not base:
        raise ValueError(f"Invalid symbol for table mapping: {sym!r}")
    return f"market.{base}"

def db_connect():
    return psycopg2.connect(
        host=PGHOST,
        port=PGPORT,
        dbname=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD,
    )

def check_table_exists(conn, table_qualified: str):
    # this method helps to ensure a table exists with the given name before inserting to db
    schema, table = table_qualified.split(".", 1)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema=%s AND table_name=%s
            """, (schema, table))
        if cur.fetchone() is None:
            raise RuntimeError(
                f"Required table {table_qualified} does not exist. "
                f"Create it first with the provided SQL."
            )

#added 2026-01-29 helper for time window for unit testing
def compute_window(now_local: dt.datetime, window_min: int) -> tuple[dt.datetime, dt.datetime]:
    """
    Returns (start, end) where:
      - start is top of the hour
      - end is aligned down to nearest 5-minute boundary
      - end is capped by window_min
      - end is always > start (at least 5 minutes)
    """
    start = current_hour(now_local)
    end = now_local.replace(second=0, microsecond=0)
    end = end - dt.timedelta(minutes=end.minute % 5)
    end = min(end, start + dt.timedelta(minutes=window_min))
    if end <= start:
        end = start + dt.timedelta(minutes=5)
    return start, end

# Added log_fetch_csv logic - (Cade, Kristina - Feb 03, 2026)

def log_fetch_csv(symbol: str, start: dt.datetime, end: dt.datetime, day_from: str, day_to: str,
                  api_rows: int, filtered_rows: int, csv_path: str):
    """
    Appends a single-line fetch log to a CSV file.
    Creates the file + header if it doesn't exist.
    """
    # 1. Use the passed-in csv_path instead of hardcoding "logs"
    log_file = Path(csv_path)
    
    # 2. Ensure the parent directory (e.g., "logs/") exists
    log_file.parent.mkdir(parents=True, exist_ok=True)

    header = "run_ts_market,symbol,window_start,window_end,from_day,to_day,api_rows,filtered_rows\n"
    run_ts = dt.datetime.now(TZ).isoformat()

    line = f"{run_ts},{symbol},{start.isoformat()},{end.isoformat()},{day_from},{day_to},{api_rows},{filtered_rows}\n"

# 3. Write header if new, then append the line
    if not log_file.exists():
        log_file.write_text(header, encoding="utf-8")
    
    with log_file.open("a", encoding="utf-8") as f:
        f.write(line)
        
#  API Fetch and insert into Postgres 

def fetch_and_insert(conn, symbol: str, start: dt.datetime, end: dt.datetime):
    
    day_from = ymd(min(start.date(), end.date())) 
    day_to   = ymd(max(start.date(), end.date())) 
    url = BASE_URL.format(symbol=symbol)
    params = {"from": day_from, "to": day_to, "extended": "true", "apikey": API_KEY}

    print(f"\n   Calling API with symbol = {symbol}   Window Used: {start} -> {end}  (from {day_from} to {day_to}) ---")

    r = requests.get(url, params=params, timeout=25) # GET API data 
    r.raise_for_status()
    data = r.json() if r.content else []
    #Recently added - Liam
    print("API returned rows:", len(data))
    if data:
        print("First:", data[0].get("date"), "Last:", data[-1].get("date"))

    # Filter to the requested window and prepare rows for DB
    rows = []
    for row in data: # loop through json response  
        ts_str = row.get("date")
        if not ts_str:
            continue
        ts_exch = parse_api_time(ts_str)
        if start <= ts_exch < end:
            # grab all data fields from api 
            open_val   = row.get("open")
            high_val   = row.get("high")
            low_val    = row.get("low")
            close_val  = row.get("close")
            volume_val = row.get("volume")
            # require close price to exist 
            if close_val is None:
                continue

            # append data to rows list 
            rows.append((ts_exch, open_val, high_val, low_val, close_val, volume_val))

    rows.sort(key=lambda x: x[0])  # sort rows ascending by timestamp

    # CSV fetch log (per run, per symbol) - (Cade, Kristina - Feb 03, 2026)
    log_fetch_csv(
        symbol=symbol,
        start=start,
        end=end,
        day_from=day_from,
        day_to=day_to,
        api_rows=len(data),
        filtered_rows=len(rows),
        csv_path="logs/fetch_data_log.csv",
    )


    if not rows:
        print("(no 5 minute bars in this window)")
        return 0

    table = safe_table_name_for_symbol(symbol)
    check_table_exists(conn, table)

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
    with conn.cursor() as cur:
        execute_values(cur, insert_sql, rows, template="(%s, %s, %s, %s, %s, %s)") 
    conn.commit() 

    print(f"inserted/upserted {len(rows)} rows into {table}")
    return len(rows)


def main():
    global API_KEY, SYMBOLS, MARKET_TZ, WINDOW_MIN, PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD, BASE_URL, TZ

    # re-load env vars at runtime (not import time)
    API_KEY    = os.environ.get("FMP_API_KEY", "")
    SYMBOLS        = [s.strip() for s in os.environ.get("SYMBOLS", "AAPL").split(",") if s.strip()]
    MARKET_TZ      = os.environ.get("MARKET_TZ", "America/New_York")
    WINDOW_MIN     = int(os.environ.get("WINDOW_MINUTES", "60"))

    PGHOST     = os.environ.get("PGHOST", "")
    PGPORT     = int(os.environ.get("PGPORT", "5432"))
    PGDATABASE = os.environ.get("PGDATABASE", "")
    PGUSER     = os.environ.get("PGUSER", "")
    PGPASSWORD = os.environ.get("PGPASSWORD", "")

    BASE_URL = "https://financialmodelingprep.com/api/v3/historical-chart/5min/{symbol}"

    TZ = ZoneInfo(MARKET_TZ)
    now_local = dt.datetime.now(TZ)
    start, end = compute_window(now_local, WINDOW_MIN)

    if not API_KEY:
        print("error: API key is missing; ensure FMP_API_KEY is set", file=sys.stderr)
        sys.exit(1)

    #start = current_hour(dt.datetime.now(TZ)) # change this to test on off market days

    #now_local = dt.datetime.now(TZ).replace(second=0, microsecond=0) # local time 

    # use the current minute from local time and replace the start time minute with it 
    #end_candidate = start.replace(minute=now_local.minute, second=0, microsecond=0)

    # Align to 5 minute intervals (API updates every 5 mins)
    #end_candidate = end_candidate - dt.timedelta(minutes=end_candidate.minute % 5)

    # cap by WINDOW_MIN so range doesnt get too large
    #end = min(end_candidate, start + dt.timedelta(minutes=WINDOW_MIN))

    #Recently added - Liam
    #start = current_hour(dt.datetime.now(TZ))
    #now_local = dt.datetime.now(TZ).replace(second=0, microsecond=0)

    #end = now_local - dt.timedelta(minutes=now_local.minute % 5)
    #end = min(end, start + dt.timedelta(minutes=WINDOW_MIN))

    if end <= start:
        end = start + dt.timedelta(minutes=5)


    print()
    print(f" [Collector Startup] \n Using Symbols:={SYMBOLS} \n Using Timezone: {MARKET_TZ} \n Using Time Window: {start} -> {end}")

    # connect once and reuse connection for all inserts 
    try:
        conn = db_connect()
    except Exception as e:
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
    except Exception:
        pass

    print()
    print(f" [done] total rows ingested: {total}")

if __name__ == "__main__":
    main()
