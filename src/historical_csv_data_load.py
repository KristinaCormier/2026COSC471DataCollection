#!/usr/bin/env python3
"""Load historical OHLCV CSV files into stg_raw.market_data via SQLAlchemy ORM.

This script replaces the legacy shell + psql loader and keeps behavior explicit,
testable, and portable across environments.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import os
import sys
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from model.models import MarketData
from model.orm_db import get_engine, get_session_factory


REQUIRED_COLUMNS: tuple[str, ...] = ("date", "open", "high", "low", "close", "volume")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load OHLCV CSV files into stg_raw.market_data using SQLAlchemy.",
    )
    parser.add_argument(
        "--csv-dir",
        default=os.getenv("CSV_PATH", ""),
        help="Directory containing CSV files. Can also be set with CSV_PATH.",
    )
    parser.add_argument(
        "--pattern",
        default="*.csv",
        help="Glob pattern used to select files inside --csv-dir (default: *.csv).",
    )
    parser.add_argument(
        "--source",
        default="CSV_bulk_load",
        help="Value written to stg_raw.market_data.source (default: CSV_bulk_load).",
    )
    parser.add_argument(
        "--asset-type",
        default="stock",
        help="Value written to stg_raw.market_data.asset_type (default: stock).",
    )
    parser.add_argument(
        "--market-tz",
        default=os.getenv("MARKET_TZ", "America/New_York"),
        help="Timezone used for naive timestamps (default: MARKET_TZ or America/New_York).",
    )
    parser.add_argument(
        "--skip-invalid-rows",
        action="store_true",
        help="Skip rows that fail parsing instead of aborting the file.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse files and report counts without writing to the database.",
    )

    args = parser.parse_args(argv)
    if not args.csv_dir:
        parser.error("--csv-dir is required (or set CSV_PATH in .env)")
    return args


def _build_runtime_engine():
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return create_engine(database_url, future=True, pool_pre_ping=True)

    return get_engine(
        os.getenv("DB_HOST", "localhost"),
        int(os.getenv("DB_PORT", "5432")),
        os.getenv("DB_NAME", ""),
        os.getenv("DB_USER", ""),
        os.getenv("DB_PASSWORD", ""),
    )


def _normalize_headers(fieldnames: Iterable[str | None]) -> dict[str, str]:
    header_map: dict[str, str] = {}
    for name in fieldnames:
        if name is None:
            continue
        lowered = name.strip().lower()
        if lowered:
            header_map[lowered] = name

    missing = [column for column in REQUIRED_COLUMNS if column not in header_map]
    if missing:
        raise ValueError(f"missing required CSV columns: {', '.join(missing)}")
    return header_map


def _parse_decimal(raw_value: str, field_name: str) -> Decimal:
    value = (raw_value or "").strip()
    if not value:
        raise ValueError(f"field '{field_name}' is empty")
    try:
        return Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(f"field '{field_name}' is not numeric: {raw_value!r}") from exc


def _parse_csv_timestamp(raw_value: str, market_tz: ZoneInfo) -> dt.datetime:
    value = (raw_value or "").strip()
    if not value:
        raise ValueError("field 'date' is empty")

    # Allow ISO timestamps with trailing Z as UTC.
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value

    parsed: dt.datetime | None = None
    try:
        parsed = dt.datetime.fromisoformat(normalized)
    except ValueError:
        pass

    if parsed is None:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                parsed = dt.datetime.strptime(value, fmt)
                break
            except ValueError:
                continue

    if parsed is None:
        raise ValueError(f"field 'date' has invalid format: {raw_value!r}")

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=market_tz)
    return parsed.astimezone(market_tz)


def _build_payload(
    row: dict[str, str],
    header_map: dict[str, str],
    symbol: str,
    source: str,
    asset_type: str,
    market_tz: ZoneInfo,
) -> dict[str, object]:
    ts = _parse_csv_timestamp(row[header_map["date"]], market_tz)
    raw_payload = {str(key): value for key, value in row.items() if key is not None}
    return {
        "symbol": symbol.upper(),
        "ts": ts,
        "open": _parse_decimal(row[header_map["open"]], "open"),
        "high": _parse_decimal(row[header_map["high"]], "high"),
        "low": _parse_decimal(row[header_map["low"]], "low"),
        "close": _parse_decimal(row[header_map["close"]], "close"),
        "volume": _parse_decimal(row[header_map["volume"]], "volume"),
        "asset_type": asset_type,
        "source": source,
        "raw_payload": raw_payload,
    }


def _read_csv_payloads(
    csv_path: Path,
    source: str,
    asset_type: str,
    market_tz: ZoneInfo,
    skip_invalid_rows: bool,
) -> tuple[list[dict[str, object]], list[str]]:
    symbol = csv_path.stem
    payloads: list[dict[str, object]] = []
    warnings: list[str] = []

    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError("CSV has no header row")

        header_map = _normalize_headers(reader.fieldnames)

        for row_number, row in enumerate(reader, start=2):
            try:
                payloads.append(
                    _build_payload(
                        row=row,
                        header_map=header_map,
                        symbol=symbol,
                        source=source,
                        asset_type=asset_type,
                        market_tz=market_tz,
                    )
                )
            except ValueError as exc:
                message = f"{csv_path.name}:{row_number}: {exc}"
                if skip_invalid_rows:
                    warnings.append(message)
                    continue
                raise ValueError(message) from exc

    return payloads, warnings


def _upsert_payloads(session: Session, payloads: list[dict[str, object]]) -> int:
    if not payloads:
        return 0

    insert_stmt = insert(MarketData).values(payloads)
    upsert_stmt = insert_stmt.on_conflict_do_update(
        index_elements=[MarketData.symbol, MarketData.ts],
        set_={
            "open": insert_stmt.excluded.open,
            "high": insert_stmt.excluded.high,
            "low": insert_stmt.excluded.low,
            "close": insert_stmt.excluded.close,
            "volume": insert_stmt.excluded.volume,
            "asset_type": insert_stmt.excluded.asset_type,
            "source": insert_stmt.excluded.source,
            "raw_payload": insert_stmt.excluded.raw_payload,
        },
    )

    try:
        session.execute(upsert_stmt)
        session.commit()
    except Exception as exc:
        session.rollback()
        error_message = str(exc).lower()
        if "no unique or exclusion constraint matching the on conflict specification" in error_message:
            print(
                "[warning] unique(symbol, ts) not found; falling back to INSERT-only batch",
                file=sys.stderr,
            )
            session.execute(insert_stmt)
            session.commit()
        else:
            raise

    return len(payloads)


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    args = _parse_args(argv)

    market_tz = ZoneInfo(args.market_tz)
    csv_dir = Path(args.csv_dir).expanduser()

    if not csv_dir.exists() or not csv_dir.is_dir():
        print(f"error: csv directory does not exist: {csv_dir}", file=sys.stderr)
        return 1

    csv_files = sorted(csv_dir.glob(args.pattern))
    if not csv_files:
        print(f"error: no files matched {args.pattern!r} in {csv_dir}", file=sys.stderr)
        return 1

    engine = _build_runtime_engine()
    session_factory = get_session_factory(engine)

    total_rows = 0
    total_warnings = 0
    failed_files = 0

    try:
        with engine.connect():
            pass
    except Exception as exc:
        print(f"error: database connection failed: {exc}", file=sys.stderr)
        engine.dispose()
        return 2

    print(f"Loading {len(csv_files)} CSV files from {csv_dir}")
    print(f"Source={args.source} AssetType={args.asset_type} DryRun={args.dry_run}")

    try:
        for csv_file in csv_files:
            print(f"Processing {csv_file.name}...")
            try:
                payloads, warnings = _read_csv_payloads(
                    csv_path=csv_file,
                    source=args.source,
                    asset_type=args.asset_type,
                    market_tz=market_tz,
                    skip_invalid_rows=args.skip_invalid_rows,
                )

                for warning in warnings:
                    print(f"[warning] {warning}", file=sys.stderr)
                total_warnings += len(warnings)

                if args.dry_run:
                    inserted = len(payloads)
                else:
                    with session_factory() as session:
                        inserted = _upsert_payloads(session, payloads)

                total_rows += inserted
                print(f"Loaded {inserted} rows from {csv_file.name}")

            except Exception as exc:
                failed_files += 1
                print(f"[error] {csv_file.name}: {exc}", file=sys.stderr)
                if not args.skip_invalid_rows:
                    break
    finally:
        engine.dispose()

    print(
        "Summary: "
        f"rows_loaded={total_rows}, warnings={total_warnings}, failed_files={failed_files}"
    )

    return 0 if failed_files == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
