# On execution, this script should fetch stock data for the most recent
# completed 5 minute interval in EST.
# It should insert this data into postgres for each symbol provided
# This script is designed to be executed every 5 minutes throughout every extended market day using a job scheduler

from __future__ import annotations

from typing import Final
import datetime as dt
import os
import sys
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from sqlalchemy.orm import Session

from model.models import MarketData
from model.orm_db import get_engine, get_session_factory, init_db

from utils import logging_utils as lu
from utils import time_utils as tu
from utils.collector_shared import (
    STAGING_TABLE_NAME,
    _coerce_float,
    _construct_source_url,
    _fetch_api_data,
    _insert_batch,
    _process_data_batch,
    _validate_and_parse_row,
)

print("Loaded models from:", MarketData.__module__)
print("Table columns:", list(MarketData.__table__.columns.keys()))

DEFAULT_SYMBOLS: Final = (
    "AAPL,AMD,AMZN,BA,BABA,BAC,C,CSCO,CVX,DIS,F,GE,GOOGL,IBM,INTC,"
    "JNJ,JPM,KO,MCD,META,MSFT,NFLX,NVDA,PFE,T,TSLA,VZ,WMT,XOM"
)


def main():
    load_dotenv()

    api_key = os.environ.get("FMP_API_KEY", "")
    symbols = [
        s.strip()
        for s in os.environ.get(
            "SYMBOLS",
            DEFAULT_SYMBOLS,
        ).split(",")
        if s.strip()
    ]
    market_tz = os.environ.get("MARKET_TZ", "America/New_York")
    window_min = int(os.environ.get("WINDOW_MINUTES", "5"))
    market_open = os.environ.get("MARKET_OPEN", "04:00")
    market_close = os.environ.get("MARKET_CLOSE", "21:00")

    pghost = os.environ.get("PGHOST", "")
    pgport = int(os.environ.get("PGPORT", "5432"))
    pgdatabase = os.environ.get("PGDATABASE", "")
    pguser = os.environ.get("PGUSER", "")
    pgpassword = os.environ.get("PGPASSWORD", "")

    tz = ZoneInfo(market_tz)
    now_local = dt.datetime.now(tz)

    try:
        market_open_time = tu.parse_hhmm(market_open)
        market_close_time = tu.parse_hhmm(market_close)
    except ValueError as e:
        print(f"error: invalid market hours: {e}", file=sys.stderr)
        sys.exit(1)

    start, end = tu.compute_window(now_local, window_min)

    # Clamp window to market hours
    market_open_dt = now_local.replace(
        hour=market_open_time.hour,
        minute=market_open_time.minute,
        second=0,
        microsecond=0,
    )
    market_close_dt = now_local.replace(
        hour=market_close_time.hour,
        minute=market_close_time.minute,
        second=0,
        microsecond=0,
    )

    start = max(start, market_open_dt)
    end = min(end, market_close_dt)

    # If window is entirely outside market hours, skip collection
    if start >= end:
        print(
            "[info] computed window falls outside market hours "
            f"({market_open}-{market_close} {market_tz}); skipping collection"
        )
        sys.exit(0)

    if not api_key:
        print("error: API key is missing; ensure FMP_API_KEY is set", file=sys.stderr)
        sys.exit(1)

    print()
    print(
        f" [Collector Startup] \n Using Symbols:={symbols} \n Using Timezone: {market_tz} \n Using Time Window: {start} -> {end}"
    )

    print("Connecting to database with:")
    print("HOST:", pghost)
    print("PORT:", pgport)
    print("DATABASE:", pgdatabase)
    print("USER:", pguser)

    session: Session | None = None

    try:
        engine = get_engine(pghost, pgport, pgdatabase, pguser, pgpassword)
        init_db(engine)
        SessionLocal = get_session_factory(engine)
        session = SessionLocal()
    except Exception as e:
        lu.log_db_error(
            symbol="N/A",
            operation="CONNECT",
            error_type=type(e).__name__,
            error_message=str(e),
            tz=tz,
        )
        print(f"cannot connect to Postgres: {e}", file=sys.stderr)
        sys.exit(2)

    total = 0
    day_from = tu.ymd(min(start.date(), end.date()))
    day_to = tu.ymd(max(start.date(), end.date()))

    try:
        for sym in symbols:
            try:
                print(
                    f"\n   Calling API with symbol = {sym}   Window Used: {start} -> {end}  (from {day_from} to {day_to}) ---"
                )

                # Step 1: Fetch data from API
                data = _fetch_api_data(sym, start, end, api_key, tz)

                print("API returned rows:", len(data))
                if data:
                    print("First:", data[0].get("date"), "Last:", data[-1].get("date"))

                # Step 2: Construct source URL for the source field in the database and for logging purposes
                source_url = _construct_source_url(sym, start, end)

                # Step 3: Process and validate batch
                rows = _process_data_batch(
                    data,
                    sym,
                    STAGING_TABLE_NAME,
                    source_url,
                    start,
                    end,
                    now_local,
                    tz,
                )

                # Step 4: Insert into database
                if rows:
                    total += _insert_batch(session, STAGING_TABLE_NAME, rows, sym, tz)
                else:
                    print("(no 5 minute bars in this window)")

            except Exception as e:
                print(f"[error] {sym}: {e}", file=sys.stderr)

    finally:
        if session is not None:
            try:
                session.close()
            except Exception as e:
                print(f"[warning] failed to close database session: {e}", file=sys.stderr)

    print()
    print(f" [done] total rows ingested: {total}")


if __name__ == "__main__":
    main()
