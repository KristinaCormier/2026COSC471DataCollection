#!/usr/bin/env python3
"""
Script to load historical stock data from CSV into the raw staging area (market schema).

This script:
- Reads a CSV file with historical OHLCV data
- Filters data from a specified start date to the current date
- Loads data into PostgreSQL market tables using UPSERT for idempotency
- Supports the same table structure as auto_data_collection.py

CSV Format Expected:
    date,symbol,open,high,low,close,volume
    2024-01-15 09:30:00,AAPL,185.50,186.00,185.25,185.75,1000000
    ...

Usage:
    python src/load_historical_data.py --csv path/to/historical.csv [--from-date YYYY-MM-DD]
"""

import os
import sys
import csv
import argparse
import datetime as dt
from pathlib import Path
from zoneinfo import ZoneInfo
import re

# DB client
import psycopg

# Database connection vars (same as auto_data_collection.py)
PGHOST = os.environ.get("PGHOST", "")
PGPORT = int(os.environ.get("PGPORT", "5432"))
PGDATABASE = os.environ.get("PGDATABASE", "")
PGUSER = os.environ.get("PGUSER", "")
PGPASSWORD = os.environ.get("PGPASSWORD", "")

# Timezone (same as auto_data_collection.py)
MARKET_TZ = os.environ.get("MARKET_TZ", "America/New_York")
TZ = ZoneInfo(MARKET_TZ)


def safe_table_name_for_symbol(sym: str) -> str:
    """Get ticker name to match with postgres tables (same as auto_data_collection.py)"""
    base = re.sub(r"[^A-Za-z0-9]", "", sym).lower()
    if not base:
        raise ValueError(f"Invalid symbol for table mapping: {sym!r}")
    return f"market.{base}"


def db_connect():
    """Connect to PostgreSQL database"""
    return psycopg.connect(
        host=PGHOST,
        port=PGPORT,
        dbname=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD,
    )


def check_table_exists(conn, table_qualified: str):
    """Ensure a table exists with the given name before inserting to db"""
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


def parse_timestamp(ts_str: str) -> dt.datetime:
    """Parse timestamp from CSV, handling common formats"""
    # Try parsing with microseconds
    for fmt in [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
    ]:
        try:
            parsed = dt.datetime.strptime(ts_str, fmt)
            # Add timezone if not already present
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=TZ)
            return parsed
        except ValueError:
            continue
    
    raise ValueError(f"Could not parse timestamp: {ts_str}")


def load_csv_to_staging(csv_path: str, from_date: dt.date = None):
    """
    Load historical data from CSV into PostgreSQL staging tables.
    
    Args:
        csv_path: Path to CSV file
        from_date: Optional start date to filter data (inclusive)
    
    Returns:
        Total number of rows loaded
    """
    csv_file = Path(csv_path)
    if not csv_file.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    print(f"\nLoading historical data from: {csv_path}")
    if from_date:
        print(f"Filtering data from: {from_date} onwards")
    
    # Connect to database
    try:
        conn = db_connect()
        print("Connected to PostgreSQL")
    except Exception as e:
        print(f"Cannot connect to PostgreSQL: {e}", file=sys.stderr)
        sys.exit(2)
    
    # Read CSV and group by symbol
    symbol_data = {}  # {symbol: [(timestamp, open, high, low, close, volume), ...]}
    row_count = 0
    skipped_count = 0
    
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        # Validate required columns
        required_cols = {'date', 'symbol', 'open', 'high', 'low', 'close', 'volume'}
        if not required_cols.issubset(reader.fieldnames):
            raise ValueError(
                f"CSV missing required columns. Expected: {required_cols}, "
                f"Found: {set(reader.fieldnames)}"
            )
        
        for row in reader:
            try:
                # Parse timestamp
                ts = parse_timestamp(row['date'])
                
                # Apply date filter if specified
                if from_date and ts.date() < from_date:
                    skipped_count += 1
                    continue
                
                # Parse values
                symbol = row['symbol'].strip().upper()
                open_val = float(row['open']) if row['open'] else None
                high_val = float(row['high']) if row['high'] else None
                low_val = float(row['low']) if row['low'] else None
                close_val = float(row['close']) if row['close'] else None
                volume_val = int(float(row['volume'])) if row['volume'] else None
                
                # Require close price to exist
                if close_val is None:
                    skipped_count += 1
                    continue
                
                # Group by symbol
                if symbol not in symbol_data:
                    symbol_data[symbol] = []
                
                symbol_data[symbol].append((ts, open_val, high_val, low_val, close_val, volume_val))
                row_count += 1
                
            except (ValueError, KeyError) as e:
                print(f"Warning: Skipping invalid row: {e}", file=sys.stderr)
                skipped_count += 1
                continue
    
    print(f"Read {row_count} valid rows from CSV (skipped {skipped_count})")
    
    # Insert data per symbol
    total_inserted = 0
    for symbol, rows in symbol_data.items():
        try:
            # Sort by timestamp
            rows.sort(key=lambda x: x[0])
            
            table = safe_table_name_for_symbol(symbol)
            check_table_exists(conn, table)
            
            # UPSERT using ON CONFLICT (same pattern as auto_data_collection.py)
            insert_sql = f"""
                INSERT INTO {table} (date, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (date) DO UPDATE
                  SET open   = EXCLUDED.open,
                      high   = EXCLUDED.high,
                      low    = EXCLUDED.low,
                      close  = EXCLUDED.close,
                      volume = EXCLUDED.volume
            """
            
            with conn.cursor() as cur:
                cur.executemany(insert_sql, rows)
            conn.commit()
            
            print(f"  {symbol}: inserted/upserted {len(rows)} rows into {table}")
            total_inserted += len(rows)
            
        except Exception as e:
            print(f"Error loading {symbol}: {e}", file=sys.stderr)
            conn.rollback()
    
    conn.close()
    print(f"\nTotal rows loaded: {total_inserted}")
    return total_inserted


def main():
    parser = argparse.ArgumentParser(
        description='Load historical stock data from CSV into PostgreSQL staging area',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python src/load_historical_data.py --csv data/historical.csv
  python src/load_historical_data.py --csv data/historical.csv --from-date 2024-01-01
        """
    )
    parser.add_argument(
        '--csv',
        required=True,
        help='Path to CSV file with historical data'
    )
    parser.add_argument(
        '--from-date',
        help='Start date for filtering data (YYYY-MM-DD format). Only data from this date onwards will be loaded.'
    )
    
    args = parser.parse_args()
    
    # Parse from_date if provided
    from_date = None
    if args.from_date:
        try:
            from_date = dt.datetime.strptime(args.from_date, "%Y-%m-%d").date()
        except ValueError:
            print(f"Error: Invalid date format '{args.from_date}'. Use YYYY-MM-DD", file=sys.stderr)
            sys.exit(1)
    
    # Load data
    try:
        load_csv_to_staging(args.csv, from_date)
        print("\n[SUCCESS] Historical data loaded successfully")
    except Exception as e:
        print(f"\n[ERROR] Failed to load historical data: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
