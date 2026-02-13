# Historical Data Loader

## Overview

The `load_historical_data.py` script loads historical stock market data from CSV files into the PostgreSQL raw staging area (market schema). It complements the real-time data collection script (`auto_data_collection.py`) by enabling backfill of historical data.

## Features

- **CSV Loading**: Reads historical OHLCV (Open, High, Low, Close, Volume) data from CSV files
- **Date Filtering**: Optionally filter data from a specific start date to the current date
- **Idempotent**: Uses UPSERT (ON CONFLICT DO UPDATE) to safely reload data without duplicates
- **Multi-Symbol Support**: Automatically handles multiple stock symbols in a single CSV
- **Same Infrastructure**: Uses the same database schema and connection logic as the real-time collector

## CSV Format

The script expects a CSV file with the following columns (with header row):

```csv
date,symbol,open,high,low,close,volume
2024-01-15 09:30:00,AAPL,185.50,186.00,185.25,185.75,1000000
2024-01-15 09:35:00,AAPL,185.75,186.25,185.50,186.00,950000
2024-01-15 09:30:00,MSFT,375.50,376.00,375.25,375.75,500000
...
```

### Column Descriptions

- **date**: Timestamp in format `YYYY-MM-DD HH:MM:SS` (will be converted to market timezone)
- **symbol**: Stock ticker symbol (e.g., AAPL, MSFT, GOOGL)
- **open**: Opening price for the time interval
- **high**: Highest price during the interval
- **low**: Lowest price during the interval
- **close**: Closing price (required - rows without close price are skipped)
- **volume**: Trading volume during the interval

## Prerequisites

1. **Database Setup**: Ensure PostgreSQL tables are created using `setup_scripts/table_creation_script/511TableCreationScript.sql`
2. **Environment Variables**: Same as `auto_data_collection.py`:
   ```bash
   export PGHOST="localhost"
   export PGPORT="5432"
   export PGDATABASE="marketdata"
   export PGUSER="etl_user"
   export PGPASSWORD="your_password"
   export MARKET_TZ="America/New_York"  # Optional, defaults to America/New_York
   ```

## Usage

### Basic Usage

Load all data from a CSV file:

```bash
python src/load_historical_data.py --csv path/to/historical_data.csv
```

### With Date Filter

Load only data from a specific date onwards:

```bash
python src/load_historical_data.py --csv path/to/historical_data.csv --from-date 2024-01-01
```

This will load data from January 1, 2024 to the present (based on timestamps in the CSV).

### Example with Sample Data

```bash
python src/load_historical_data.py --csv tests/data/sample_historical.csv
```

## How It Works

1. **Reads CSV**: Parses the CSV file and validates required columns
2. **Filters Data**: Applies date filter if `--from-date` is specified
3. **Groups by Symbol**: Organizes rows by stock symbol
4. **Validates Tables**: Checks that required PostgreSQL tables exist
5. **UPSERT Data**: Inserts data using `ON CONFLICT (date) DO UPDATE` for idempotency
6. **Reports Results**: Shows number of rows loaded per symbol

## Idempotency

The script is **idempotent** - you can run it multiple times with the same CSV file without creating duplicate records. This is achieved through PostgreSQL's UPSERT mechanism:

```sql
INSERT INTO market.aapl (date, open, high, low, close, volume)
VALUES (...)
ON CONFLICT (date) DO UPDATE
  SET open = EXCLUDED.open,
      high = EXCLUDED.high,
      ...
```

When a row with the same timestamp already exists, it updates the values instead of creating a duplicate.

## Error Handling

- **Missing Tables**: Script validates that all required tables exist before loading
- **Invalid Data**: Rows with missing close prices or invalid formats are skipped
- **Database Errors**: Connection and query errors are caught and reported
- **File Not Found**: Clear error message if CSV file doesn't exist

## Testing

Run the test suite:

```bash
pytest tests/unit/test_load_historical_data.py -v
```

Tests cover:
- Timestamp parsing in various formats
- CSV loading with different data scenarios
- Date filtering
- Idempotency (loading same data multiple times)
- Error conditions (missing columns, invalid data, etc.)

## Integration with Existing Pipeline

This script complements `auto_data_collection.py`:

- **Real-time Collection**: `auto_data_collection.py` runs hourly to collect current data
- **Historical Backfill**: `load_historical_data.py` loads past data to fill gaps
- **Same Schema**: Both scripts use the same `market.*` tables and UPSERT logic
- **Same Timezone**: Both respect the `MARKET_TZ` environment variable

## Common Use Cases

1. **Initial Setup**: Load historical data when setting up the pipeline for the first time
2. **Gap Filling**: Backfill missing data due to outages or maintenance windows
3. **Data Migration**: Import data from other sources into the unified schema
4. **Testing**: Load sample data for development and testing

## Troubleshooting

### "CSV file not found"
- Check that the file path is correct
- Use absolute paths or paths relative to where you run the script

### "CSV missing required columns"
- Verify CSV has header row with: date, symbol, open, high, low, close, volume
- Check for typos in column names

### "Required table does not exist"
- Run the table creation script: `setup_scripts/table_creation_script/511TableCreationScript.sql`
- Ensure the symbol in CSV matches available tables (lowercase, alphanumeric only)

### "Cannot connect to PostgreSQL"
- Verify database environment variables are set correctly
- Check that PostgreSQL is running and accessible
- Test connection with: `psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE`

## Performance Considerations

- The script processes all data in memory, so very large CSV files (>1M rows) may require significant RAM
- Data is grouped by symbol and inserted in bulk using `executemany()` for efficiency
- For very large datasets, consider splitting the CSV by symbol or date ranges
