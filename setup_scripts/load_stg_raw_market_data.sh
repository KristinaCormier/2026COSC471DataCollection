#!/bin/bash
# ============================================================================
# CSV Bulk Loader for Staging Layer
# ============================================================================
#
# Purpose:
#     Load historical OHLCV data from CSV files into stg_raw.market_data.
#     Handles data type conversion, timestamp parsing, and error logging.
#
# Intended Use:
#     Ad-hoc bulk import for backfill, historical data load, or data migration.
#     NOT used for regular scheduled ingestion (use intraday_data_collection.py instead).
#
# Prerequisites:
#     - PostgreSQL client (psql) installed
#     - Database connection details set in .env (PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD)
#     - stg_raw.market_data table already exists (run schema setup first)
#     - CSV files in a directory with standard naming: {SYMBOL}.csv
#
# CSV Format:
#     - Columns: date, open, high, low, close, volume (any order, with header row)
#     - Dates in ISO format: YYYY-MM-DD HH:MM:SS or YYYY-MM-DD
#     - Numeric values decimal format (e.g., 100.50)
#     - Example: AAPL.csv contains 100+ rows of 5-minute bars
#
# Usage:
#     bash setup_scripts/load_stg_raw_market_data.sh
#
# Configuration:
#     Edit CSV_PATH in this script to point to your CSV directory.
#     Default: /path/to/Your/File/29-stocks-5-min (adjust for your location)
#
# Output:
#     - Rows inserted into stg_raw.market_data with source='CSV_bulk_load'
#     - Temporary SQL files created and cleaned up afterward
#     - Progress messages printed to stdout
#
# Idempotency:
#     Each run will INSERT (with UNIQUE constraint violation) or UPSERT depending on
#     whether the row already exists. To force reload, delete existing rows first:
#     psql -d "$PGDATABASE" -c "DELETE FROM stg_raw.market_data WHERE source = 'CSV_bulk_load';"
#
# Troubleshooting:
#     - "failed with code 1": Check CSV_PATH exists and PGHOST/PGPASSWORD are correct
#     - "invalid input syntax for timestamp": Ensure date column is in YYYY-MM-DD format
#     - "permission denied": Check database user has INSERT privilege on stg_raw.market_data
#
# Author: Data Collection Team
# License: MIT
# ============================================================================

set -e

# load_stg_raw_market_data.sh

ENV_FILE="../.env"
if [ -f "$ENV_FILE" ]; then
    set -a
    . "$ENV_FILE"
    set +a
else
    echo "Error: $ENV_FILE not found. Please create the .env file with the necessary variables."
    exit 1
fi

CSV_PATH="/path/to/Your/File/29-stocks-5-min"   # adjust for *nix path

for f in "$CSV_PATH"/*.csv; do
    # skip if no matches
    [ -e "$f" ] || continue

    filename=$(basename "$f")
    symbol="${filename%.*}"

    printf 'Processing %s…\n' "$filename"

    sqlfile="$(mktemp)"

    cat >"$sqlfile" <<'SQL'
CREATE TEMP TABLE tmp_load (
    date TEXT,
    open TEXT,
    high TEXT,
    low TEXT,
    close TEXT,
    volume TEXT
);

\copy tmp_load FROM '%FILE%' WITH (FORMAT csv, HEADER true);

INSERT INTO stg_raw.market_data
(
    symbol,
    ts,
    open,
    high,
    low,
    close,
    volume,
    asset_type,
    source,
    raw_payload
)
SELECT
    '%SYMBOL%',
    date::timestamptz,
    open::numeric(18,6),
    high::numeric(18,6),
    low::numeric(18,6),
    close::numeric(18,6),
    volume::numeric(20,4),
    'stock',
    'CSV_bulk_load',
    to_jsonb(tmp_load)
FROM tmp_load;

DROP TABLE tmp_load;
SQL

    # substitute the placeholders
    sed -e "s|%FILE%|$f|g" -e "s|%SYMBOL%|$symbol|g" "$sqlfile" >"${sqlfile}.final"
    mv "${sqlfile}.final" "$sqlfile"

    PGPASSWORD="${PGPASSWORD:-}" \
    psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" \
         -v ON_ERROR_STOP=1 -f "$sqlfile"

    printf '%s loaded.\n' "$filename"
    rm -f "$sqlfile"
done

printf 'All files processed.\n'