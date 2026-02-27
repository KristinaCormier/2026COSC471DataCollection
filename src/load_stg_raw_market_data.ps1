$PGHOST = "address"
$PGPORT = "5432"
$PGDATABASE = "db_name"
$PGUSER = "db_user"

$CSV_PATH = "C:\Path\to\Your\File\29-stocks-5-min"

Get-ChildItem $CSV_PATH -Filter *.csv | ForEach-Object {

    $file = $_.FullName.Replace("\", "/")
    $filename = $_.Name
    $symbol = [System.IO.Path]::GetFileNameWithoutExtension($_.Name)

    Write-Host "Processing $filename..."

    $sqlFile = "$env:TEMP\load_market_data.sql"

@"
CREATE TEMP TABLE tmp_load (
    date TEXT,
    open TEXT,
    high TEXT,
    low TEXT,
    close TEXT,
    volume TEXT
);

\copy tmp_load FROM '$file' WITH (FORMAT csv, HEADER true);

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
    '$symbol',
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
"@ | Out-File -Encoding ASCII $sqlFile

    psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE -v ON_ERROR_STOP=1 -f $sqlFile

    Write-Host "$filename loaded."
}

Write-Host "All files processed."
