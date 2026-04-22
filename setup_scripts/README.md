# Setup & Operations

Scripts for provisioning the COSC471 data collection environment. This guide covers:
1. **One-Time Server Setup** — User creation, database replication, backup configuration
2. **Cron Job Installation** — Scheduling the data collector and transform pipeline
3. **Manual Utilities** — On-demand CSV loading and recovery procedures

All scripts require a `.env` file at the project root with the necessary environment variables. See [.env.template](../.env.template) for a reference.

For local developer onboarding, prefer the root [README quick start](../README.md). This directory mainly documents operational/server workflows.

---

<!-- --8<-- [start:one-time-server-setup] -->
## One-Time Server Setup (`setup_server.sh`)

**Purpose**: Configure production servers with privileged users, set up physical streaming replication, and enable automated backups.

**Prerequisites**:
- Root or sudo access
- PostgreSQL 16 installed
- `.env` file with replication and backup configuration
- **Required Environment Variables**
  - `LIST_OF_SUDO_USERS`: Array of usernames (e.g., `("etl" "admin")`)
  - `PRIMARY_IP`: IP address of primary server
  - `PRIMARY_USER`: SSH user on primary (usually `cosc-admin`)
  - `PG_CONF`: Path to primary's `postgresql.conf`
  - `PG_HBA`: Path to primary's `pg_hba.conf`
  - `REPLICATION_USER`: Replication role credentials (for db migrations)
  - `REPLICATION_PASSWORD`: Replication role credentials
  - `SLOT_NAME`: Replication slot name (e.g., `replica_slot_1`)
  - `DATA_DIR`: Standby PostgreSQL data directory
  - `BACKUP_DIR`: Root directory for backups (e.g., `/var/lib/pgsql/16/backups`)
   - `DB_NAME`
   - `USER_FOR_DB_BACKUPS`
   - `PASSWORD_FOR_DB_BACKUPS`

**Usage**:
```bash
cd setup_scripts/server_setup
sudo bash setup_server.sh
```

Do not use this path for local-first development bootstrap.

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
<!-- --8<-- [end:one-time-server-setup] -->

---

<!-- --8<-- [start:component-setup-procedures] -->
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
- `USER_FOR_DB_BACKUPS`, `PASSWORD_FOR_DB_BACKUPS`: Database user credentials for backup execution
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
<!-- --8<-- [end:component-setup-procedures] -->

---

<!-- --8<-- [start:cron-job-installation] -->
## Cron Job Installation

Two separate cron scripts install the data collection and transformation pipelines.

Both scripts support two install modes via `CRON_INSTALL_MODE` in `.env`:
- `user` (default): installs wrappers in `CRON_WRAPPER_DIR` (default: `.ops/bin`) and writes to the target user's crontab.
- `system`: installs wrappers in `/usr/local/bin` and writes files into `/etc/cron.d` (requires sudo).

### Daily Intraday Collector (`setup_cronjob_daily_collector.sh`)

**Purpose**: Schedule `src/intraday_data_collection.py` to run regularly during market hours.

**Prerequisites**:
- Python virtual environment installed at `$PROJECT_DIR/.venv`
- `.env` file with FMP_API_KEY and database credentials
- `crontab` command available for the target user
- Sudo access only when `CRON_INSTALL_MODE=system`

**Usage**:
```bash
# Default local-first install (user mode)
bash setup_scripts/setup_cronjob_daily_collector.sh

# Optional system install for shared servers
sudo CRON_INSTALL_MODE=system bash setup_scripts/setup_cronjob_daily_collector.sh
```

**What It Does**:
1. Ensures Python dependencies exist in `.venv`
2. Ensures `LOG_DIR` exists and is writable by the runtime user
3. Installs wrapper and cron entry based on `CRON_INSTALL_MODE`
4. Replaces any prior collector entry tagged `COSC471_STOCK_COLLECTOR`

**Cron Schedule** (from `.env`):
- `COLLECTION_SCHEDULE`: Default `0 * * * *` (every hour)

**Post-Setup Validation**:
```bash
# Check user-mode cron entry
crontab -l | grep COSC471_STOCK_COLLECTOR

# Check user-mode wrapper
cat .ops/bin/run_stock_collector.sh

# Check system-mode cron entry (if used)
sudo cat /etc/cron.d/stock_collector_daily

# Check logs directory permissions
ls -ld ./logs
```

### Scheduled Operations (`setup_cronjob_scheduled_operations.sh`)

**Purpose**: Schedule `src/run_scheduled_operations.py` to run the Python transform/load pipeline after collection.

**Prerequisites**:
- Python virtual environment installed at `$PROJECT_DIR/.venv`
- `.env` file with database credentials
- Database schema initialized (recommended: `python -m alembic upgrade head`)
- `crontab` command available for the target user
- Sudo access only when `CRON_INSTALL_MODE=system`

**Usage**:
```bash
# Default local-first install (user mode)
bash setup_scripts/setup_cronjob_scheduled_operations.sh

# Optional system install for shared servers
sudo CRON_INSTALL_MODE=system bash setup_scripts/setup_cronjob_scheduled_operations.sh
```

**What It Does**:
1. Ensures Python dependencies exist in `.venv`
2. Ensures `LOG_DIR` exists and is writable by the runtime user
3. Installs wrapper and cron entry based on `CRON_INSTALL_MODE`
4. Replaces any prior scheduled-operations entry tagged `COSC471_SCHEDULED_OPERATIONS`

**Cron Schedule** (from `.env`):
- `STG_TO_CORE_SCHEDULE`: Default `0 2 * * *` (2 AM UTC daily)

**Post-Setup Validation**:
```bash
# Check user-mode cron entry
crontab -l | grep COSC471_SCHEDULED_OPERATIONS

# Check user-mode wrapper
cat .ops/bin/run_scheduled_operations.sh

# Check system-mode cron entry (if used)
sudo cat /etc/cron.d/scheduled_operations

# Check logs directory permissions
ls -ld ./logs
```
<!-- --8<-- [end:cron-job-installation] -->

---

<!-- --8<-- [start:manual-utilities] -->
## Manual Utilities

### CSV Bulk Load (`src/historical_csv_data_load.py`)

**Purpose**: Load historical OHLCV data from CSV files into `stg_raw.market_data`.

**Use Case**: Backfill from external data sources; one-time data imports.

**Prerequisites**:
- CSV files in a directory with standard naming: `{SYMBOL}.csv`
- CSV columns: `date, open, high, low, close, volume` (in any order, with headers)
- `.env` file with PostgreSQL credentials
- `stg_raw.market_data` table already exists

**Usage**:
```bash
# Python ORM loader (canonical entrypoint)
python src/historical_csv_data_load.py --csv-dir /path/to/csv/files
```

**What It Does**:
1. Iterates over all `.csv` files in the configured directory
2. Validates required columns (`date, open, high, low, close, volume`)
3. Parses/casts rows in Python and upserts to `stg_raw.market_data`
4. Falls back to INSERT-only batches if a legacy database is missing the upsert key

**Configuration**:
- `--csv-dir`: Directory containing CSV files (required unless `CSV_PATH` is set in `.env`)
- `--pattern`: File glob pattern (default: `*.csv`)
- `--skip-invalid-rows`: Continue loading valid rows when malformed rows are present
- `--dry-run`: Parse/validate only, no database writes

**Post-Upload Validation**:
```bash
# Count rows loaded per symbol
psql -d "$DB_NAME" -c "SELECT symbol, COUNT(*) FROM stg_raw.market_data WHERE source = 'CSV_bulk_load' GROUP BY symbol ORDER BY symbol;"
```
<!-- --8<-- [end:manual-utilities] -->

---

<!-- --8<-- [start:environment-variables-reference] -->
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
USER_FOR_DB_BACKUPS="postgres"
PASSWORD_FOR_DB_BACKUPS="change_me"
BACKUP_CRON_SCHEDULE="0 2 * * *"  # 2 AM UTC daily
```

### Cron Schedules
```bash
COLLECTION_SCHEDULE="0 * * * *"      # Every hour
STG_TO_CORE_SCHEDULE="0 2 * * *"     # 2 AM UTC daily
```

### Cron Installer Mode
```bash
CRON_INSTALL_MODE="user"    # user or system
CRON_WRAPPER_DIR=".ops/bin" # used in user mode
```

### Database Connection (inherited from `.env`)
```bash
DB_HOST="localhost"
DB_PORT="5432"
DB_NAME="market_data"
DB_USER="etl_user"
DB_PASSWORD="your_password"
```
<!-- --8<-- [end:environment-variables-reference] -->

---

<!-- --8<-- [start:typical-setup-flow] -->
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
bash setup_cronjob_daily_collector.sh
bash setup_cronjob_scheduled_operations.sh

# 5. Verify cron jobs are registered
crontab -l

# 6. Optionally load historical data
python ../src/historical_csv_data_load.py --csv-dir /path/to/csv/files
```
<!-- --8<-- [end:typical-setup-flow] -->

---

<!-- --8<-- [start:setup-troubleshooting] -->
## Troubleshooting

| Issue | Check | Fix |
|-------|-------|-----|
| Replication fails to start | `pg_is_in_recovery()` returns false | Check PRIMARY_IP, REPLICATION_USER credentials, and pg_hba.conf |
| Backup script cannot write | `ls -ld "$BACKUP_DIR"` | Ensure postgres user owns the backup directory: `sudo chown postgres:postgres $BACKUP_DIR` |
| Cron job doesn't run | `crontab -l` or `sudo cat /etc/cron.d/...` | Verify `CRON_INSTALL_MODE`, wrapper path (`.ops/bin` for user mode), and executable permissions |
| CSV import fails | Check CSV column names and types | Ensure columns are: `date, open, high, low, close, volume` and all numeric values parse as valid decimals |
| Timestamps appear shifted to server local time | `psql -c "SHOW TIMEZONE;"` returns an unexpected value | Set DB/session timezone explicitly (commonly UTC): `ALTER DATABASE <db_name> SET TIMEZONE TO 'UTC';`. For display in market time, use `AT TIME ZONE 'America/New_York'`. |
| Permission denied on user creation | Check `/etc/sudoers` | Only run `setup_server.sh` as root or with sudo; don't use it in a restricted shell |
<!-- --8<-- [end:setup-troubleshooting] -->

---

## See Also

- [README.md](../README.md) — Project overview and quick start
- [.env.template](../.env.template) — Full environment variable reference
- [alembic/versions/](../alembic/versions/) — Canonical migration history
