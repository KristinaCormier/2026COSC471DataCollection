FMP 5-Minute Market Data Collector
Overview

This project provides tools for collecting and loading stock market data into PostgreSQL:

1. **Real-time Collection** (`auto_data_collection.py`): Pulls 5-minute OHLCV stock data from the Financial Modeling Prep (FMP) API and loads it into PostgreSQL staging tables. Designed to run hourly via a scheduler (e.g., cron).

2. **Historical Data Loader** (`load_historical_data.py`): Loads historical OHLCV data from CSV files into the same PostgreSQL staging tables. Used for backfilling historical data or importing data from other sources.

On each run of the real-time collector, it:

Calculates a time window from the current hour to the latest 5-minute interval

Retrieves intraday bars for each symbol

Filters results to the window

Inserts/updates rows in PostgreSQL

Requirements

Python 3.9+

Packages:

pip install requests psycopg[binary]

Configuration (Environment Variables)
API
export FMP_API_KEY="YOUR_KEY"
export SYMBOLS="AAPL"
export MARKET_TZ="America/New_York"
export WINDOW_MINUTES="60"

PostgreSQL
export PGHOST="localhost"
export PGPORT="5432"
export PGDATABASE="marketdata"
export PGUSER="etl_user"
export PGPASSWORD="your_password"

Database Table Requirements

Each symbol maps to:

market.<symbol>


Example for AAPL:

CREATE TABLE market.aapl (
  date TIMESTAMPTZ NOT NULL,
  open DOUBLE PRECISION,
  high DOUBLE PRECISION,
  low DOUBLE PRECISION,
  close DOUBLE PRECISION,
  volume DOUBLE PRECISION,
  UNIQUE(date)
);


The UNIQUE(date) constraint is required for UPSERT.

Running the Scripts

Real-time Data Collection:
python src/auto_data_collection.py

Historical Data Loading:
python src/load_historical_data.py --csv path/to/historical.csv

For detailed documentation on the historical data loader, see: docs/load_historical_data.md

Common Issues

401 Unauthorized
→ API key missing or incorrect

permission denied for sequence
→ DB user lacks permission on auto-increment sequence

no unique constraint for ON CONFLICT
→ Add UNIQUE(date) to table

timestamp mismatch (ts vs date)
→ Script column names must match DB schema