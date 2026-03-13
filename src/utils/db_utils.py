"""
Database utilities for stock data collection.
Handles connection management, table validation, name sanitization, building SQL statements for data insertion and upsert operations.
"""

from __future__ import annotations

import re
import psycopg


def safe_table_name_for_symbol(sym: str) -> str:
    """
    Convert a stock symbol to a safe PostgreSQL table name.
    
    Strips non-alphanumeric characters and returns lowercase name
    with 'market.' schema prefix.
    
    Args:
        sym: Stock symbol (e.g., 'AAPL', '^TNX')
    
    Returns:
        Qualified table name (e.g., 'stg_raw.market_data')
    
    Raises:
        ValueError: If symbol contains no valid characters
    """
    base = re.sub(r"[^A-Za-z0-9]", "", sym).lower()
    if not base:
        raise ValueError(f"Invalid symbol for table mapping: {sym!r}")
    return f"market.{base}"


def db_connect(host: str, port: int, dbname: str, user: str, password: str):
    """
    Create a PostgreSQL database connection.
    
    Args:
        host: Database host
        port: Database port
        dbname: Database name
        user: Database user
        password: Database password
    
    Returns:
        psycopg connection object
    """
    return psycopg.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
    )


def check_table_exists(conn, table_qualified: str):
    """
    Verify that a table exists in the database.
    
    Args:
        conn: Database connection
        table_qualified: Fully qualified table name (e.g., 'market.stg_raw')
    
    Raises:
        RuntimeError: If table does not exist
    """
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

def _build_upsert_statement(table: str) -> str:
    """
    Build the UPSERT SQL statement for stock data.
    
    Args:
        table: raw staging area table name (e.g., 'market.stg_raw')
    
    Returns:
        SQL string ready for executemany
    """
    return f"""
        INSERT INTO {table} (symbol, ts, open, high, low, close, volume, asset_type, source, raw_payload)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, ts) DO UPDATE
          SET symbol = EXCLUDED.symbol,            
              open   = EXCLUDED.open,
              high   = EXCLUDED.high,
              low    = EXCLUDED.low,
              close  = EXCLUDED.close,
              volume = EXCLUDED.volume,
              asset_type = EXCLUDED.asset_type,
              source = EXCLUDED.source,
              raw_payload = EXCLUDED.raw_payload
    """

def _build_insert_statement(table: str) -> str:
    """
    Build the INSERT statement for stock data.
    
    Args:
        table: raw staging area table name (e.g., 'market.stg_raw')
    
    Returns:
        SQL string ready for single-row inserts
    """
    return f"""
        INSERT INTO {table} (symbol, ts, open, high, low, close, volume, asset_type, source, raw_payload)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, ts) DO NOTHING
        RETURNING ts
    """