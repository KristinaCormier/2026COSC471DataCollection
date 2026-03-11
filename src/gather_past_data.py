# On execution, this script should fetch stock data for a user-specified
# historical date range and insert it into postgres for each symbol provided.

from __future__ import annotations

import argparse
import os
import datetime as dt
import sys
import requests
from dotenv import load_dotenv
from zoneinfo import ZoneInfo

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from models import MarketData

from orm_db import get_engine, get_session_factory, init_db

# Internal utilities
import data_validation as dv
import time_utils as tu
import logging_utils as lu

print("Loaded models from:", MarketData.__module__)
print("Table columns:", list(MarketData.__table__.columns.keys()))
# load in environment vars
load_dotenv()

API_KEY = os.environ.get("FMP_API_KEY", "")
SYMBOLS = os.environ.get(
    "SYMBOLS",
    "AAPL,AMD,AMZN,BA,BABA,BAC,C,CSCO,CVX,DIS,F,GE,GOOGL,IBM,INTC,JNJ,JPM,KO,MCD,META,MSFT,NFLX,NVDA,PFE,T,TSLA,VZ,WMT,XOM",
).split(",")

MARKET_TZ = os.environ.get("MARKET_TZ", "America/New_York")
WINDOW_MIN = int(os.environ.get("WINDOW_MINUTES", "5"))
MARKET_OPEN = os.environ.get("MARKET_OPEN", "04:00")
MARKET_CLOSE = os.environ.get("MARKET_CLOSE", "21:00")

# DB connection vars
PGHOST = os.environ.get("PGHOST", "")
PGPORT = int(os.environ.get("PGPORT", "5432"))
PGDATABASE = os.environ.get("PGDATABASE", "")
PGUSER = os.environ.get("PGUSER", "")
PGPASSWORD = os.environ.get("PGPASSWORD", "")

BASE_URL = "https://financialmodelingprep.com/api/v3/historical-chart/5min/{symbol}"

ASSET_TYPE = "stock"

DATA_FIELDS = ("date", "open", "high", "low", "close", "volume")
REQUIRED_NUMERIC_FIELDS = ("open", "high", "low", "volume")

# cleanup symbols
SYMBOLS = [s.strip() for s in SYMBOLS if s.strip()]

# timezone handling
TZ = ZoneInfo(MARKET_TZ)

# ORM table name used in logs
STAGING_TABLE_NAME = "stg_raw.market_data"


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


def _validate_and_parse_row(
    row: dict,
    symbol: str,
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
            table_name=STAGING_TABLE_NAME,
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
                table_name=STAGING_TABLE_NAME,
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
                table_name=STAGING_TABLE_NAME,
                row_count=1,
                tz=tz,
            )
            return None, None, True

    # enforce schema/types before insertion
    invalid_fields = []
    parsed: dict[str, float | None] = {}

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
            table_name=STAGING_TABLE_NAME,
            row_count=1,
            tz=tz,
        )
        return None, None, True

    return ts_exch, parsed, hard_invalid_found

def _construct_source_url(
    symbol: str,
    start: dt.datetime,
    end: dt.datetime,
    ) -> str:
    """
    Construct the API URL for a given symbol.
    
    Args:
        symbol: Stock symbol
    Returns:
        Fully formatted API URL for the symbol
    """
    day_from = tu.ymd(min(start.date(), end.date()))
    day_to = tu.ymd(max(start.date(), end.date()))
    url = BASE_URL.format(symbol=symbol)
    params = {"from": day_from, "to": day_to, "extended": "true"}
    return f"{url}?{requests.compat.urlencode(params)}"


def _fetch_api_data(
    symbol: str,
    start: dt.datetime,
    end: dt.datetime
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
    params = {
        "from": day_from,
        "to": day_to,
        "extended": "true",
        "apikey": API_KEY,
    }

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
    start: dt.datetime,
    end: dt.datetime,
    now_local: dt.datetime | None,
) -> list[MarketData]:
    hard_invalid_found = False
    rows: list[MarketData] = []
    last_ts = None
    seen_ts: set[dt.datetime] = set()

    for row in data:
        ts_exch, parsed, hard_invalid_found = _validate_and_parse_row(
            row=row,
            symbol=symbol,
            hard_invalid_found=hard_invalid_found,
            last_ts=last_ts,
            now_local=now_local,
            tz=TZ,
        )

        if ts_exch is None or parsed is None:
            continue

        # keep only rows in the requested collection window
        if ts_exch < start or ts_exch >= end:
            continue

        if ts_exch in seen_ts:
            lu.log_db_error(
                symbol=symbol,
                operation="LOAD",
                error_type="DuplicateTimestamp",
                error_message="duplicate timestamp in batch",
                table_name=STAGING_TABLE_NAME,
                row_count=1,
                tz=TZ,
            )
            hard_invalid_found = True
            continue

        seen_ts.add(ts_exch)
        last_ts = ts_exch

        rows.append(
            MarketData(
                symbol=symbol,
                ts=ts_exch,
                open=parsed["open"],
                high=parsed["high"],
                low=parsed["low"],
                close=parsed["close"],
                volume=parsed["volume"],
                asset_type=ASSET_TYPE,
                source="FMP_intraday",
                raw_payload=row,
            )
        )

    rows.sort(key=lambda x: x.ts)
    return rows


def _insert_batch(
    session: Session,
    rows: list[MarketData],
    symbol: str,
) -> int:
    if not rows:
        return 0

    values = [
        {
            "symbol": row.symbol,
            "ts": row.ts,
            "open": row.open,
            "high": row.high,
            "low": row.low,
            "close": row.close,
            "volume": row.volume,
            "asset_type": row.asset_type,
            "source": row.source,
            "raw_payload": row.raw_payload,
        }
        for row in rows
    ]

    insert_stmt = insert(MarketData).values(values)
    upsert_stmt = insert_stmt.on_conflict_do_update(
        index_elements=[MarketData.symbol, MarketData.ts],
        set_={
            "open": insert_stmt.excluded.open,
            "high": insert_stmt.excluded.high,
            "low": insert_stmt.excluded.low,
            "close": insert_stmt.excluded.close,
            "volume": insert_stmt.excluded.volume,
            "asset_type": insert_stmt.excluded.asset_type,
            "raw_payload": insert_stmt.excluded.raw_payload,
        },
    )

    try:
        session.execute(upsert_stmt)
        session.commit()
    except Exception as e:
        session.rollback()
        err_msg = str(e).lower()

        if (
            "constraint \"unique_symbol_ts_source\"" in err_msg
            or "no unique or exclusion constraint matching the on conflict specification" in err_msg
        ):
            # Backward-compatible fallback for environments that do not have
            # the expected unique key on (symbol, ts).
            try:
                lu.log_db_error(
                    symbol=symbol,
                    operation="UPSERT_FALLBACK",
                    error_type=type(e).__name__,
                    error_message=(
                        "UPSERT key missing; falling back to INSERT-only batch: "
                        f"{e}"
                    ),
                    table_name=STAGING_TABLE_NAME,
                    row_count=len(rows),
                    tz=TZ,
                )
            except Exception as log_err:
                print(
                    "[warning] failed to write UPSERT_FALLBACK log entry: "
                    f"{log_err}",
                    file=sys.stderr,
                )
            try:
                session.execute(insert_stmt)
                session.commit()
                print(
                    "inserted "
                    f"{len(rows)} rows into {STAGING_TABLE_NAME} "
                    "(fallback insert: no upsert key found)"
                )
                return len(rows)
            except Exception as fallback_err:
                session.rollback()
                lu.log_db_error(
                    symbol=symbol,
                    operation="INSERT",
                    error_type=type(fallback_err).__name__,
                    error_message=str(fallback_err),
                    table_name=STAGING_TABLE_NAME,
                    row_count=len(rows),
                    tz=TZ,
                )
                raise

        lu.log_db_error(
            symbol=symbol,
            operation="UPSERT",
            error_type=type(e).__name__,
            error_message=str(e),
            table_name=STAGING_TABLE_NAME,
            row_count=len(rows),
            tz=TZ,
        )
        raise

    print(f"inserted/upserted {len(rows)} rows into {STAGING_TABLE_NAME}")
    return len(rows)


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
    global API_KEY, SYMBOLS, MARKET_TZ
    global PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD, BASE_URL, TZ

    args = _parse_args(argv)

    # re-load env vars at runtime (not import time)
    API_KEY = os.environ.get("FMP_API_KEY", "")
    SYMBOLS = [
        s.strip()
        for s in os.environ.get(
            "SYMBOLS",
            "AAPL,AMD,AMZN,BA,BABA,BAC,C,CSCO,CVX,DIS,F,GE,GOOGL,IBM,INTC,JNJ,JPM,KO,MCD,META,MSFT,NFLX,NVDA,PFE,T,TSLA,VZ,WMT,XOM",
        ).split(",")
        if s.strip()
    ]
    MARKET_TZ = os.environ.get("MARKET_TZ", "America/New_York")

    PGHOST = os.environ.get("PGHOST", "")
    PGPORT = int(os.environ.get("PGPORT", "5432"))
    PGDATABASE = os.environ.get("PGDATABASE", "")
    PGUSER = os.environ.get("PGUSER", "")
    PGPASSWORD = os.environ.get("PGPASSWORD", "")

    BASE_URL = "https://financialmodelingprep.com/api/v3/historical-chart/5min/{symbol}"

    if args.symbols is not None:
        cli_symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
        if not cli_symbols:
            print("error: --symbols was provided but no valid symbols were found", file=sys.stderr)
            sys.exit(1)
        SYMBOLS = cli_symbols

    TZ = ZoneInfo(MARKET_TZ)
    now_local = dt.datetime.now(TZ)

    try:
        _validate_historical_range(args.from_date, args.to_date, now_local.date())
    except ValueError as e:
        print(f"error: invalid historical range: {e}", file=sys.stderr)
        sys.exit(1)

    start, end = _compute_historical_window(args.from_date, args.to_date, TZ)

    if not API_KEY:
        print("error: API key is missing; ensure FMP_API_KEY is set", file=sys.stderr)
        sys.exit(1)

    print()
    print(
        f" [Historical Collector Startup] \n Using Symbols:={SYMBOLS} \n Using Timezone: {MARKET_TZ} \n Requested Date Range: {args.from_date.isoformat()} -> {args.to_date.isoformat()} \n Using Time Window: {start} -> {end}"
    )

    print("Connecting to database with:")
    print("HOST:", PGHOST)
    print("PORT:", PGPORT)
    print("DATABASE:", PGDATABASE)
    print("USER:", PGUSER)

    session: Session | None = None

    try:
        engine = get_engine(PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD)
        init_db(engine)
        SessionLocal = get_session_factory(engine)
        session = SessionLocal()
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

    try:
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

                # Step 2: Process and validate batch
                rows = _process_data_batch(data, sym, start, end, now_local)

                # Step 3: Insert into database
                if rows:
                    total += _insert_batch(session, rows, sym)
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