#!/bin/bash
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