Market Database Setup Script
Overview

#Table of Contents
1. [setup_error_logs script](setup_error_logs.md)

This SQL script initializes a PostgreSQL market data schema and creates individual tables for a large list of financial instruments, including:

Equities (e.g., AAPL, MSFT, TSLA)

ETFs / Index proxies

Bonds & yields (IRX, FVX, TNX)

Major indices (DJI, IXIC, GSPC)

Commodities (GCF, CLF)

Each ticker gets its own table under the market schema, designed to store OHLCV time-series data.

What the Script Does

Creates the market schema (if it does not already exist).

Defines a list of tickers inside a PostgreSQL DO $$ block.

Iterates over all tickers and:

Converts the symbol to lowercase (Postgres table naming).

Creates a table per ticker (if it doesn’t already exist).

Outputs a success notice once all tables are created.

Table Structure

Each ticker table follows the same schema:

stock_id BIGSERIAL PRIMARY KEY,
date     TIMESTAMP,
open     NUMERIC(18,6),
low      NUMERIC(18,6),
high     NUMERIC(18,6),
close    NUMERIC(18,6),
volume   BIGINT

Notes

Prices use NUMERIC(18,6) for precision (suitable for equities, indices, and commodities).

No unique constraint is enforced on date by default.

Tables are created with IF NOT EXISTS to allow safe re-runs.

Naming Conventions

Schema: market

Table names: lowercase versions of the ticker symbol

Example: AAPL → market.aapl

Example: BRK_B → market.brk_b

How to Run

Ensure you are connected to a PostgreSQL database.

Run the script using one of the following:

psql -d your_database -f market_setup.sql


or directly in a SQL client (psql, DBeaver, DataGrip, etc.).

Optional: Dropping Existing Tables

The script includes commented-out logic to drop all existing tables in the market schema:

-- FOR sym IN SELECT table_name FROM information_schema.tables
-- WHERE table_schema = 'market' LOOP
--     EXECUTE format('DROP TABLE IF EXISTS market.%I CASCADE;', sym);
-- END LOOP;

Warning

Uncommenting this section will delete all existing market tables and their data.
Only enable this if you intentionally want a full reset.