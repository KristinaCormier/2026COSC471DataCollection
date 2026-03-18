"""
Historical Data Backfill Tool

Purpose:
    Fetch 5-minute OHLCV bars for a user-specified historical date range from the FMP API,
    validate them, and upsert them into the staging layer (stg_raw.market_data).

Intended Use:
    Run on-demand to backfill missing data after outages, recover from gaps, or load historical
    data ranges. Both dates must be in the past relative to MARKET_TZ.

Environment Variables:
    API: FMP_API_KEY, FMP_API_URL, FMP_API_DELAY_SECONDS, SYMBOLS (override with --symbols), MARKET_TZ
    Database: DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
    Runtime: LOG_DIR, MARKET_OPEN, MARKET_CLOSE

Usage:
    python src/gather_past_data.py --from-date 2026-02-01 --to-date 2026-02-07
    python src/gather_past_data.py --from-date 2026-02-01 --to-date 2026-02-07 --symbols AAPL,MSFT

Arguments:
    --from-date YYYY-MM-DD: Inclusive start date (required, must be in past)
    --to-date YYYY-MM-DD: Inclusive end date (required, must be in past and >= from-date)
    --symbols TICKER,TICKER,...: Override SYMBOLS from .env (optional)

Output:
    - Rows inserted/updated in stg_raw.market_data
    - Error logs in ./logs/ (fetch_data_log.csv, data_error_log.csv, db_insert_errors.csv)

Author: Data Collection Team
License: MIT
"""

# On execution, this script should fetch stock data for a user-specified
# historical date range and insert it into postgres for each symbol provided.

from __future__ import annotations

import argparse
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

def _parse_iso_date(value: str) -> dt.date:
    """Parse a YYYY-MM-DD date string for CLI arguments."""
    try:
        return dt.date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"invalid date '{value}'. expected format YYYY-MM-DD"
        ) from exc


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch 5-minute historical bars for a past date range and "
            "insert/upsert them into the database."
        )
    )
    parser.add_argument(
        "--from-date",
        required=True,
        type=_parse_iso_date,
        help="Inclusive start date in YYYY-MM-DD format (must be in the past).",
    )
    parser.add_argument(
        "--to-date",
        required=True,
        type=_parse_iso_date,
        help="Inclusive end date in YYYY-MM-DD format (must be in the past).",
    )
    parser.add_argument(
        "--symbols",
        default=None,
        help="Optional comma-separated symbol list. Overrides SYMBOLS env var.",
    )
    return parser.parse_args(argv)


def _validate_historical_range(
    from_date: dt.date,
    to_date: dt.date,
    today_local: dt.date,
) -> None:
    if from_date > to_date:
        raise ValueError("--from-date must be on or before --to-date")
    if from_date >= today_local or to_date >= today_local:
        raise ValueError(
            "both --from-date and --to-date must be earlier than today "
            f"({today_local.isoformat()})"
        )


def _compute_historical_window(
    from_date: dt.date,
    to_date: dt.date,
    tz: ZoneInfo,
) -> tuple[dt.datetime, dt.datetime]:
    """Return inclusive-by-day datetime bounds for filtering API rows."""
    start = dt.datetime.combine(from_date, dt.time.min, tzinfo=tz)
    end = dt.datetime.combine(to_date, dt.time(23, 59, 59), tzinfo=tz)
    return start, end


def main(argv: list[str] | None = None):
    args = _parse_args(argv)

    # Load environment values at runtime before reading settings.
    load_dotenv()

    api_key = os.environ.get("FMP_API_KEY", "")
    symbols = [
        s.strip()
        for s in os.environ.get(
            "SYMBOLS",
            "",
        ).split(",")
        if s.strip()
    ]
    market_tz = os.environ.get("MARKET_TZ", "America/New_York")

    db_host = os.environ.get("DB_HOST", "")
    db_port = int(os.environ.get("DB_PORT", "5432"))
    db_name = os.environ.get("DB_NAME", "")
    db_user = os.environ.get("DB_USER", "")
    db_password = os.environ.get("DB_PASSWORD", "")

    if args.symbols is not None:
        cli_symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
        if not cli_symbols:
            print("error: --symbols was provided but no valid symbols were found", file=sys.stderr)
            sys.exit(1)
        symbols = cli_symbols

    tz = ZoneInfo(market_tz)
    now_local = dt.datetime.now(tz)

    try:
        _validate_historical_range(args.from_date, args.to_date, now_local.date())
    except ValueError as e:
        print(f"error: invalid historical range: {e}", file=sys.stderr)
        sys.exit(1)

    start, end = _compute_historical_window(args.from_date, args.to_date, tz)

    if not api_key:
        print("error: API key is missing; ensure FMP_API_KEY is set", file=sys.stderr)
        sys.exit(1)

    print()
    print(
        f" [Historical Collector Startup] \n Using Symbols:={symbols} \n Using Timezone: {market_tz} \n Requested Date Range: {args.from_date.isoformat()} -> {args.to_date.isoformat()} \n Using Time Window: {start} -> {end}"
    )

    print("Connecting to database with:")
    print("HOST:", db_host)
    print("PORT:", db_port)
    print("DATABASE:", db_name)
    print("USER:", db_user)

    session: Session | None = None

    try:
        engine = get_engine(db_host, db_port, db_name, db_user, db_password)
        init_db(engine)
        print("Loaded models from:", MarketData.__module__)
        print("Table columns:", list(MarketData.__table__.columns.keys()))
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
        print(f"cannot connect to database: {e}", file=sys.stderr)
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
                rows = _process_data_batch(data, sym, STAGING_TABLE_NAME, source_url, start, end, now_local, tz)

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
