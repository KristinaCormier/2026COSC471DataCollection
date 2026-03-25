"""
Shared collector helpers for market data ingestion scripts.

This module centralizes the common fetch/validate/process/insert pipeline used by
both intraday and historical data collection scripts.
"""

from __future__ import annotations

import datetime as dt
from typing import Final
from zoneinfo import ZoneInfo

import requests
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from model.models import MarketData
from utils import data_validation as dv
from utils import logging_utils as lu
from utils import time_utils as tu

BASE_URL: Final = "https://financialmodelingprep.com/api/v3/historical-chart/5min/{symbol}"
ASSET_TYPE: Final = "stock"
DATA_FIELDS: Final[tuple[str, ...]] = ("date", "open", "high", "low", "close", "volume")
REQUIRED_NUMERIC_FIELDS: Final[tuple[str, ...]] = ("open", "high", "low", "volume")
STAGING_TABLE_NAME: Final = "stg_raw.market_data"


def _coerce_float(value) -> tuple[float | None, bool]:
    """Attempt to coerce a value to float."""
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

    Returns:
        Tuple of (timestamp, parsed_values_dict, updated_hard_invalid_found)
        or (None, None, hard_invalid_found) if row is rejected.
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


def _construct_source_url(symbol: str, start: dt.datetime, end: dt.datetime) -> str:
    """Construct the source URL for a given symbol and date range."""
    day_from = tu.ymd(min(start.date(), end.date()))
    day_to = tu.ymd(max(start.date(), end.date()))
    url = BASE_URL.format(symbol=symbol)
    params = {"from": day_from, "to": day_to, "extended": "true"}
    return f"{url}?{requests.compat.urlencode(params)}"


def _fetch_api_data(
    symbol: str,
    start: dt.datetime,
    end: dt.datetime,
    api_key: str,
    tz: ZoneInfo,
) -> list[dict]:
    """Fetch stock data from API and return raw rows."""
    day_from = tu.ymd(min(start.date(), end.date()))
    day_to = tu.ymd(max(start.date(), end.date()))
    url = BASE_URL.format(symbol=symbol)
    params = {
        "from": day_from,
        "to": day_to,
        "extended": "true",
        "apikey": api_key,
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
            tz=tz,
        )
        raise
    except requests.exceptions.Timeout as e:
        lu.log_api_error(
            symbol=symbol,
            url=url,
            error_type="Timeout",
            error_message=str(e),
            tz=tz,
        )
        raise
    except requests.exceptions.RequestException as e:
        lu.log_api_error(
            symbol=symbol,
            url=url,
            error_type=type(e).__name__,
            error_message=str(e),
            tz=tz,
        )
        raise


def _process_data_batch(
    data: list[dict],
    symbol: str,
    staging_table_name: str,
    source_url: str,
    start: dt.datetime,
    end: dt.datetime,
    now_local: dt.datetime | None,
    tz: ZoneInfo,
    newest_first: bool = False,
) -> list[MarketData]:
    """Validate rows, filter to window, and return sorted MarketData rows."""
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
            tz=tz,
        )

        if ts_exch is None or parsed is None:
            continue

        if ts_exch < start or ts_exch >= end:
            continue

        if ts_exch in seen_ts:
            lu.log_db_error(
                symbol=symbol,
                operation="LOAD",
                error_type="DuplicateTimestamp",
                error_message="duplicate timestamp in batch",
                table_name=staging_table_name,
                row_count=1,
                tz=tz,
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
                source=source_url,
                raw_payload=row,
            )
        )

    rows.sort(key=lambda x: x.ts, reverse=newest_first)
    return rows


def _insert_batch(
    session: Session,
    staging_table_name: str,
    rows: list[MarketData],
    symbol: str,
    tz: ZoneInfo,
    stop_on_conflict: bool = False,
) -> int:
    """Insert a validated batch and return processed row count."""
    if not rows:
        return 0

    inserted = 0

    for row in rows:
        row_values = {
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

        stmt = insert(MarketData).values(row_values).on_conflict_do_nothing(
            index_elements=[MarketData.symbol, MarketData.ts]
        )

        try:
            result = session.execute(stmt)
            session.commit()
        except Exception as e:
            session.rollback()
            lu.log_db_error(
                symbol=symbol,
                operation="INSERT",
                error_type=type(e).__name__,
                error_message=str(e),
                table_name=staging_table_name,
                row_count=1,
                tz=tz,
            )
            raise

        if result.rowcount == 0:
            if stop_on_conflict:
                lu.log_db_error(
                    symbol=symbol,
                    operation="INSERT_STOP_ON_CONFLICT",
                    error_type="DuplicateTimestamp",
                    error_message=(
                        "existing row encountered; stopping symbol backfill at "
                        f"{row.ts.isoformat()}"
                    ),
                    table_name=staging_table_name,
                    row_count=1,
                    tz=tz,
                )
                print(
                    "stopped symbol "
                    f"{symbol} at existing timestamp {row.ts.isoformat()}"
                )
                break
            continue

        inserted += 1

    if stop_on_conflict:
        print(f"inserted {inserted} rows into {staging_table_name}")
    else:
        print(f"inserted/upserted {inserted} rows into {staging_table_name}")

    return inserted
