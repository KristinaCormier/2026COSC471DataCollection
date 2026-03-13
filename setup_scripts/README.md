# Setup & Operations

Scripts for provisioning the COSC471 data collection environment. This guide covers:
1. **One-Time Server Setup** — User creation, database replication, backup configuration
2. **Cron Job Installation** — Scheduling the data collector and transform pipeline
3. **Manual Utilities** — On-demand CSV loading and recovery procedures

All scripts require a `.env` file at the project root with the necessary environment variables. See [.env.template](../.env.template) for a reference.

---

## One-Time Server Setup (`setup_server.sh`)

**Purpose**: Configure the server with privileged users, set up physical streaming replication, and enable automated backups.

**Prerequisites**:
- Root or sudo access
- PostgreSQL 16 installed
- `.env` file with replication and backup configuration
- **Required Environment Variables**
  - `LIST_OF_SUDO_USERS`: Array of usernames (e.g., `("etl" "admin")`)
  - `PRIMARY_IP`: IP address of primary server
  - `PRIMARY_USER`: SSH user on primary (usually `postgres`)
  - `PG_CONF`: Path to primary's `postgresql.conf`
  - `PG_HBA`: Path to primary's `pg_hba.conf`
  - `REPLICATION_USER`: Replication role credentials
  - `REPLICATION_PASSWORD`: Replication role credentials
  - `SLOT_NAME`: Replication slot name (e.g., `replica_slot_1`)
  - `DATA_DIR`: Standby PostgreSQL data directory
  - `BACKUP_DIR`: Root directory for backups (e.g., `/var/backups/postgres`)
  - `DB_USER`
  - `DB_PASSWORD`

**Usage**:
```bash
cd setup_scripts
sudo bash setup_server.sh
```

**What It Does**:
1. Creates sudo-capable users from `LIST_OF_SUDO_USERS` array
2. Runs `database_setup/db_migration.sh` to set up replication (primary → standby)
3. Runs `database_setup/backup_setup.sh` to configure automated backups

**Post-Setup Validation**:
```bash
# Check created users
getent passwd | grep -E "etl|admin"

# Check replication status on primary
psql -c "SELECT slot_name, active FROM pg_replication_slots;"

# Check backup directory
ls -ld "${BACKUP_DIR}"
```

---

## Component Setup Procedures

### Database Replication (`database_setup/db_migration.sh`)

**Purpose**: Configure PostgreSQL physical streaming replication between primary and standby servers.

**Scope**: One-time setup; runs during `setup_server.sh`.

**What It Does**:
1. Configures the primary PostgreSQL server (remote SSH)
   - Enables listening on all interfaces (`listen_addresses = '*'`)
   - Creates replication role with password authentication
   - Creates a physical replication slot for the standby
   - Restarts PostgreSQL to apply changes
2. Configures the local standby server
   - Sets up `.pgpass` for credential-less replication
   - Stops local PostgreSQL and clears the data directory
   - Performs base backup from the primary via streaming
   - Waits for standby to enter recovery mode

**Key Variables**:
- `PRIMARY_IP`: IP address of primary server
- `PRIMARY_USER`: SSH user on primary (usually `postgres`)
- `PG_CONF`: Path to primary's `postgresql.conf`
- `PG_HBA`: Path to primary's `pg_hba.conf`
- `REPLICATION_USER`, `REPLICATION_PASSWORD`: Replication role credentials
- `SLOT_NAME`: Replication slot name (e.g., `replica_slot_1`)
- `DATA_DIR`: Standby PostgreSQL data directory

**Post-Setup Validation**:
```bash
# On the standby, check recovery is active
psql -c "SELECT pg_is_in_recovery();"

# Expected output: t (true, i.e., in recovery mode)
```

### Database Backup (`database_setup/backup_setup.sh`)

**Purpose**: Set up automated base backups and WAL archive recovery for PITR (Point-In-Time Recovery).

**Scope**: One-time setup; runs during `setup_server.sh`.

**What It Does**:
1. Creates the backup directory and WAL archive subdirectory
2. Enables archive mode in PostgreSQL
   - Sets `wal_level = replica`
   - Sets `archive_mode = on`
   - Configures the archive command to copy WAL files
3. Creates a full base backup using `pg_basebackup`
4. Creates an automated backup cron script at `/usr/local/bin/backup_database.sh`
5. Schedules it in `/etc/cron.d/postgres_backup`

**Key Variables** (from `.env`):
- `BACKUP_DIR`: Root directory for backups (e.g., `/var/backups/postgres`)
- `DB_USER`, `DB_PASSWORD`: Database user for backup execution
- `BACKUP_CRON_SCHEDULE`: Cron schedule (e.g., `0 2 * * *` = 2 AM daily)
- `PG_CONF`: Path to `postgresql.conf` (requires write access)

**Backup Retention**: The script creates timestamped directories; you must manage retention separately (cleanup old backups older than your desired retention window).

**Post-Setup Validation**:
```bash
# Check backup directory exists and has correct ownership
ls -ld "${BACKUP_DIR}"
ls -ld "${BACKUP_DIR}/base_backup"
ls -ld "${BACKUP_DIR}/wal_archives"

# Check archive mode is on
sudo -u postgres psql -c "SHOW archive_mode;"

# Check the cron job was registered
sudo crontab -l | grep backup_database
```

---

## Cron Job Installation

Two separate cron scripts install the data collection and transformation pipelines.

### Daily Intraday Collector (`setup_cronjob_daily_collector.sh`)

**Purpose**: Schedule `src/intraday_data_collection.py` to run regularly during market hours.

**Prerequisites**:
- Python virtual environment installed at `$PROJECT_DIR/.venv`
- `.env` file with FMP_API_KEY and database credentials
- Sudo access

**Usage**:
```bash
sudo bash setup_scripts/setup_cronjob_daily_collector.sh
```

**What It Does**:
1. Creates `logs/` directory (if missing) with proper permissions
2. Creates wrapper script at `/usr/local/bin/run_stock_collector.sh`
3. Registers cron job in `/etc/cron.d/stock_collector_daily`

**Cron Schedule** (from `.env`):
- `COLLECTION_SCHEDULE`: Default `0 * * * *` (every hour)

**Post-Setup Validation**:
```bash
# Check the wrapper script was created
cat /usr/local/bin/run_stock_collector.sh

# Check the cron job was registered
sudo crontab -l | grep run_stock_collector

# Check logs directory permissions
ls -ld ./logs
```

### Scheduled Operations (`setup_cronjob_scheduled_operations.sh`)

**Purpose**: Schedule `src/run_scheduled_operations.py` to run SQL transformation scripts after collection.

**Prerequisites**:
- Python virtual environment installed at `$PROJECT_DIR/.venv`
- `.env` file with database credentials
- `operation_logs.pipeline_logs` table created in the database
- Sudo access

**Usage**:
```bash
sudo bash setup_scripts/setup_cronjob_scheduled_operations.sh
```

**What It Does**:
1. Creates `logs/` directory (if missing) with proper permissions
2. Creates wrapper script at `/usr/local/bin/run_scheduled_operations.sh`
3. Registers cron job in `/etc/cron.d/scheduled_operations`

**Cron Schedule** (from `.env`):
- `STG_TO_CORE_SCHEDULE`: Default `0 2 * * *` (2 AM UTC daily)

**Post-Setup Validation**:
```bash
# Check the wrapper script was created
cat /usr/local/bin/run_scheduled_operations.sh

# Check the cron job was registered
sudo crontab -l | grep run_scheduled_operations

# Verify operation_logs.pipeline_logs is accessible
psql -d "$PGDATABASE" -c "SELECT COUNT(*) FROM operation_logs.pipeline_logs;"
```

---

## Manual Utilities

### CSV Bulk Load (`load_stg_raw_market_data.sh`)

**Purpose**: Load historical OHLCV data from CSV files into `stg_raw.market_data`.

**Use Case**: Backfill from external data sources; one-time data imports.

**Prerequisites**:
- CSV files in a directory with standard naming: `{SYMBOL}.csv`
- CSV columns: `date, open, high, low, close, volume` (in any order, with headers)
- `.env` file with PostgreSQL credentials
- `stg_raw.market_data` table already exists

**Usage**:
```bash
bash setup_scripts/load_stg_raw_market_data.sh
```

**What It Does**:
1. Iterates over all `.csv` files in the configured directory
2. For each file, creates a temporary SQL script that:
   - Creates a temp table with text columns
   - Copies CSV data into temp table
   - Parses and casts columns to the correct types
   - Inserts into `stg_raw.market_data` with source = `'CSV_bulk_load'`
3. Deletes the temp table and cleans up

**Configuration** (edit in script):
- `CSV_PATH`: Directory containing CSV files (default: `/path/to/Your/File/29-stocks-5-min`)

**Post-Upload Validation**:
```bash
# Count rows loaded per symbol
psql -d "$PGDATABASE" -c "SELECT symbol, COUNT(*) FROM stg_raw.market_data WHERE source = 'CSV_bulk_load' GROUP BY symbol ORDER BY symbol;"
```

---

## Environment Variables Reference

### Server Setup
```bash
# Users to create with passwordless sudo
LIST_OF_SUDO_USERS=("etl" "admin")
```

### Replication (Primary → Standby)
```bash
PRIMARY_USER="postgres"
PRIMARY_IP="10.0.0.10"
PG_CONF="/var/lib/pgsql/16/data/postgresql.conf"
PG_HBA="/var/lib/pgsql/16/data/pg_hba.conf"
REPLICATION_USER="replicator"
REPLICATION_PASSWORD="change_me"
SLOT_NAME="replica_slot_1"
DATA_DIR="/var/lib/pgsql/16/data"
```

### Backup Configuration
```bash
BACKUP_DIR="/var/backups/postgres"
DB_USER="postgres"
DB_PASSWORD="change_me"
BACKUP_CRON_SCHEDULE="0 2 * * *"  # 2 AM UTC daily
```

### Cron Schedules
```bash
COLLECTION_SCHEDULE="0 * * * *"      # Every hour
STG_TO_CORE_SCHEDULE="0 2 * * *"     # 2 AM UTC daily
```

### PostgreSQL Connection (inherited from `.env`)
```bash
PGHOST="localhost"
PGPORT="5432"
PGDATABASE="market_data"
PGUSER="etl_user"
PGPASSWORD="your_password"
```

---

## Typical Setup Flow

For a complete production deployment:

```bash
# 1. Prepare the environment
cp .env.template .env
# Edit .env with your credentials and settings

# 2. Run one-time server setup (includes replication + backup)
cd setup_scripts
sudo bash setup_server.sh

# 3. Wait for replication to establish
sleep 60
psql -c "SELECT pg_is_in_recovery();"  # Standby should return 't'

# 4. Install cron jobs
sudo bash setup_cronjob_daily_collector.sh
sudo bash setup_cronjob_scheduled_operations.sh

# 5. Verify cron jobs are registered
sudo crontab -l

# 6. Optionally load historical data
bash load_stg_raw_market_data.sh
```

---

## Troubleshooting

| Issue | Check | Fix |
|-------|-------|-----|
| Replication fails to start | `pg_is_in_recovery()` returns false | Check PRIMARY_IP, REPLICATION_USER credentials, and pg_hba.conf |
| Backup script cannot write | `ls -ld "$BACKUP_DIR"` | Ensure postgres user owns the backup directory: `sudo chown postgres:postgres $BACKUP_DIR` |
| Cron job doesn't run | `sudo crontab -l` | Ensure sudo bash was used; check wrapper script at `/usr/local/bin/` exists and is executable |
| CSV import fails | Check CSV column names and types | Ensure columns are: `date, open, high, low, close, volume` and all numeric values parse as valid decimals |
| Permission denied on user creation | Check `/etc/sudoers` | Only run `setup_server.sh` as root or with sudo; don't use it in a restricted shell |

---

## See Also

- [README.md](../README.md) — Project overview and quick start
- [.env.template](../.env.template) — Full environment variable reference
- [setup_scripts/table_creation_script/](table_creation_script/) — Schema definitions