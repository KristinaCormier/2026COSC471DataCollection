# Setup Scripts

This folder contains the server setup, database replication, backup, and cron job setup scripts for the COSC471 data collection environment. The scripts are written for PostgreSQL 16 and assume sudo access.

## Script Locations

- Server setup entrypoint: `setup_scripts/server_setup/setup_server.sh`
- Replication/migration: `setup_scripts/database_setup/db_migration.sh`
- Backups: `setup_scripts/database_setup/backup_setup.sh`
- Daily stock collector cron: `setup_scripts/setup_cronjob_daily_collector.sh`
- Scheduled operations cron: `setup_scripts/setup_cronjob_scheduled_operations.sh`

## .env Location (Required)

All setup scripts load environment variables from a single .env file located at:

- `../.env` # the project root directory

The scripts reference it using `../.env`, so run them from the `setup_scripts/` directory (or update `ENV_FILE` to an absolute path).

## .env Variables

### Server setup (`setup_server.sh`)
- `LIST_OF_SUDO_USERS`  
  Bash array of usernames to create and grant passwordless sudo.

Example:
```LIST_OF_SUDO_USERS=("etl" "admin")```

### Replication / migration (`db_migration.sh`)
- `PRIMARY_USER`  
  SSH user on the primary PostgreSQL server.
- `PRIMARY_IP`  
  IP address (or hostname) of the primary server.
- `PG_CONF`  
  Full path to the primary `postgresql.conf`.
- `PG_HBA`  
  Full path to the primary `pg_hba.conf`.
- `REPLICATION_USER`  
  Replication role name.
- `REPLICATION_PASSWORD`  
  Password for the replication role.
- `SLOT_NAME`  
  Physical replication slot name.
- `DATA_DIR`  
  Local PostgreSQL data directory on the replica.

Example:
```
PRIMARY_USER="postgres"
PRIMARY_IP="10.0.0.10"
PG_CONF="/var/lib/pgsql/16/data/postgresql.conf"
PG_HBA="/var/lib/pgsql/16/data/pg_hba.conf"
REPLICATION_USER="replicator"
REPLICATION_PASSWORD="change_me"
SLOT_NAME="replica_slot_1"
DATA_DIR="/var/lib/pgsql/16/data"
```


### Backups (`backup_setup.sh`)
- `BACKUP_DIR`  
  Destination directory for base backups and WAL archives.
- `DB_USER`  
  Database user used by `pg_basebackup`.
- `DB_PASSWORD`  
  Password for `DB_USER`.
- `BACKUP_CRON_SCHEDULE`  
  Cron schedule string for automated backups.

Example:
```
BACKUP_DIR="/var/backups/postgres"
DB_USER="postgres"
DB_PASSWORD="change_me"
BACKUP_CRON_SCHEDULE="0 2 * * *"
```

## Cron Job Setup

### Daily Stock Collector (`setup_cronjob_daily_collector.sh`)

Installs a cron job that runs `src/intraday_data_collection.py` on a scheduled interval.

**Requirements:**
- Must be run via `sudo` to determine the invoking user for cron execution and log directory ownership
- Virtual environment must be installed at `$PROJECT_DIR/.venv`

**What it does:**
1. Creates a log directory (if missing). Default is `./logs` but can be changed in .env
2. Sets ownership to the user running `sudo` (who launched the script)
3. Sets permissions to `750` for owner/group read+execute, others none
4. Creates wrapper script at `/usr/local/bin/run_stock_collector.sh`
5. Registers cron job in `/etc/cron.d/stock_collector_daily`

**Example usage:**
```bash
sudo bash setup_scripts/setup_cronjob_daily_collector.sh
```

**Environment variables (from `.env`):**
- `COLLECTION_SCHEDULE` (optional): Cron schedule string (default: `0 * * * *` = hourly)

### Scheduled Operations (`setup_cronjob_scheduled_operations.sh`)

Installs a cron job that runs `src/run_scheduled_operations.py` on a scheduled interval.

**Requirements:**
- Must be run via `sudo` to determine the invoking user for cron execution and log directory ownership
- Virtual environment must be installed at `$PROJECT_DIR/.venv`
- PostgreSQL pipeline_logs table must exist in `operation_logs` schema

**What it does:**
1. Creates a log directory (if missing). Default is `./logs` but can be changed in .env
2. Sets ownership to the user running `sudo` (who launched the script)
3. Sets permissions to `750` for owner/group read+execute, others none
4. Creates wrapper script at `/usr/local/bin/run_scheduled_operations.sh`
5. Registers cron job in `/etc/cron.d/scheduled_operations`

**Example usage:**
```bash
sudo bash setup_scripts/setup_cronjob_scheduled_operations.sh
```

**Environment variables (from `.env`):**
- `STG_TO_CORE_SCHEDULE` (optional): Cron schedule string (default: `0 2 * * *` = daily at 2 AM)

### Log Directory Ownership

Both cron setup scripts provision `./logs` and assign it to the user who invoked `sudo`. This ensures:

- **Single owner**: Log files are consistently owned by the sudo user, preventing permission issues
- **Consistent validation**: Python startup code validates the directory exists and is writable before proceeding
- **Fail-fast errors**: Missing or unpermitted log directories cause immediate startup failure with clear guidance

To verify ownership after setup:
```bash
ls -ld ./logs
```

## Running the Scripts

From the repository root:
```
cd setup_scripts
bash setup_server.sh
```

This entrypoint runs the replication setup first, then installs the automated backup job.

### Setting up Cron Jobs

After running `setup_server.sh` (or after creating required database tables), install cron jobs:

```bash
sudo bash setup_scripts/setup_cronjob_daily_collector.sh
sudo bash setup_scripts/setup_cronjob_scheduled_operations.sh
```

# Add Other Script Instructions Here: