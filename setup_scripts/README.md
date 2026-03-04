# Setup Scripts

This folder contains the server setup, database replication, and backup scripts used for the COSC471 data collection environment. The scripts are written for PostgreSQL 16 and assume sudo access.

## Script Locations

- Server setup entrypoint: `setup_scripts/server_setup/setup_server.sh`
- Replication/migration: `setup_scripts/database_setup/db_migration.sh`
- Backups: `setup_scripts/database_setup/backup_setup.sh`

## .env Location (Required)

All setup scripts load environment variables from a single .env file located at:

- `./setup_scripts/.env`

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

## Running the Scripts

From the repository root:
```
cd setup_scripts
bash setup_server.sh
```

This entrypoint runs the replication setup first, then installs the automated backup job.

# Add Other Script Instructions Here: