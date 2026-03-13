#!/bin/bash
# ============================================================================
# PostgreSQL Backup Configuration
# ============================================================================
#
# Purpose:
#     Configure automated PostgreSQL base backups with WAL archiving for
#     Point-In-Time Recovery (PITR). Enables disaster recovery workflows.
#
# Intended Use:
#     One-time setup during initial server provisioning. Called by setup_server.sh.
#     Schedules recurring backups via cron and enables WAL archiving.
#
# Prerequisites:
#     - PostgreSQL 16 installed and running
#     - Backup directory must be writable by postgres user
#     - Sufficient disk space for multiple base backups
#     - WAL archiving enabled (wal_level = replica)
#     - .env file with BACKUP_DIR, DB_USER, DB_PASSWORD, BACKUP_CRON_SCHEDULE
#
# What It Does:
#     1. Creates BACKUP_DIR and WAL archive subdirectory
#     2. Enables archive mode in postgresql.conf:
#        - Sets wal_level = replica
#        - Sets archive_mode = on
#        - Configures archive_command to copy WAL files
#     3. Creates initial base backup using pg_basebackup
#     4. Generates automated backup script at /usr/local/bin/backup_database.sh
#     5. Schedules backup cron job in /etc/cron.d/postgres_backup
#
# Environment Variables (from .env):
#     BACKUP_DIR: Root directory for backups (e.g., '/var/backups/postgres')
#     DB_USER: Database user for backup execution (usually 'postgres')
#     DB_PASSWORD: Password for DB_USER
#     BACKUP_CRON_SCHEDULE: Cron schedule (e.g., '0 2 * * *' = 2 AM daily)
#     PG_CONF: Full path to postgresql.conf
#
# Output:
#     - Base backup in BACKUP_DIR/base_backup/ (timestamped subdirectory)
#     - WAL archives in BACKUP_DIR/wal_archives/ (auto-maintained)
#     - Backup script at /usr/local/bin/backup_database.sh
#     - Cron job in /etc/cron.d/postgres_backup
#
# Side Effects:
#     - Modifies postgresql.conf (appends wal_level, archive_mode, archive_command)
#     - Restarts PostgreSQL to apply archive_mode changes
#     - Creates large files in BACKUP_DIR (size = database size)
#     - Runs pg_basebackup which can impact database performance
#
# Retention Policy:
#     The script does NOT manage retention. Old backups must be manually cleaned up
#     or deleted based on your recovery window requirements. Example cleanup:
#       find "$BACKUP_DIR/base_backup" -type d -mtime +30 -exec rm -rf {} \;
#
# Recovery:
#     To restore from backup:
#     1. Stop PostgreSQL
#     2. Clear data directory
#     3. Extract base backup: tar xzf base_backup/...tar.gz -C /var/lib/pgsql/16/data
#     4. Copy recovery config from backup to data directory
#     5. Start PostgreSQL (will replay WAL archives to point-in-time)
#
# Post-Setup Validation:
#     # Check archive mode is on:
#     sudo -u postgres psql -c "SHOW archive_mode;"
#     > on
#
#     # Check base backup was created:
#     ls -lh "$BACKUP_DIR/base_backup/"
#     > (should see timestamped tar.gz file)
#
#     # Check cron job is registered:
#     sudo crontab -l | grep backup_database
#     > (should see one line)
#
# Author: Data Collection Team
# License: MIT
# ============================================================================

set -e

# This script sets up a backup system for the postgres16 database.
# It creates a backup directory, sets permissions, and schedules a cron job for regular backups.

# Define variables from .env file: source the .env file to get the necessary variables
ENV_FILE="../.env"
if [ -f "$ENV_FILE" ]; then
    set -a
    . "$ENV_FILE"
    set +a
else
    echo "Error: $ENV_FILE not found. Please create the .env file with the necessary variables."
    exit 1
fi

# Create backup directory if it doesn't exist
if [ ! -d "$BACKUP_DIR" ]; then
    mkdir -p "$BACKUP_DIR"
    echo "Created backup directory at $BACKUP_DIR"
fi

#=============================================================================
# 1. Create backup scripts
#=============================================================================

# - Enable Archive Mode in PostgreSQL
#           This step is optional and depends on whether you want to enable Point-In-Time Recovery (PITR).
#           Uncomment the following lines if you want to enable archive mode.


PG_CONF="/var/lib/pgsql/16/data/postgresql.conf" # DONE: adjust as needed
if ! grep -q "archive_mode = on" "$PG_CONF"; then
    echo "Enabling archive mode in $PG_CONF"
    echo "wal_level = replica" >> "$PG_CONF"
    echo "archive_mode = on" >> "$PG_CONF"
    echo "archive_command = 'cp %p $BACKUP_DIR/wal_archives/%f'" >> "$PG_CONF"
    mkdir -p "$BACKUP_DIR/wal_archives"
    chown -R postgres:postgres "$BACKUP_DIR/wal_archives"
    sudo systemctl restart postgresql-16  # Restart PostgreSQL to apply changes
    echo "Archive mode enabled and PostgreSQL restarted."
else
    echo "Archive mode is already enabled in $PG_CONF"
fi

#=============================================================================
# 2. Create a full base backup
#=============================================================================

echo "Creating base backup..."
# if the base_backup directory does not exist, create it
if [ ! -d "$BACKUP_DIR/base_backup" ]; then
    mkdir -p "$BACKUP_DIR/base_backup"
    chown -R postgres:postgres "$BACKUP_DIR/base_backup"
    # Perform base backup using pg_basebackup as postgres user
    export PGPASSWORD="$DB_PASSWORD"
    sudo -u postgres pg_basebackup -h localhost -D "$BACKUP_DIR/base_backup" -U "$DB_USER" -P -v -Ft -z
    echo "Created base backup at $BACKUP_DIR/base_backup"
else
    echo "Base backup directory already exists at $BACKUP_DIR/base_backup"
fi

#=============================================================================
# 3. Create automated backup process and schedule cron jobs
#=============================================================================

# Setup backup script
DB_BACKUP_SCRIPT="/usr/local/bin/backup_database.sh" # Path to the backup script (DONE: adjust as needed)
cat <<EOL > "$DB_BACKUP_SCRIPT" # DONE: adjust as needed
#!/bin/bash
# Set explicit path so cron finds the binaries
export PATH=$PATH:/usr/pgsql-16/bin

TIMESTAMP=\$(date +"%F_%H-%M-%S")
BACKUP_DIR="$BACKUP_DIR"
DB_USER="$DB_USER"
export PGPASSWORD="$DB_PASSWORD"

# Perform base backup using pg_basebackup as postgres user
pg_basebackup -h localhost -U "$DB_USER" -D "$BACKUP_DIR/base_backup_\$TIMESTAMP" -Ft -z

# Remove backups older than 7 days
find "$BACKUP_DIR" -maxdepth 1 -type d -name "base_backup_*" -daystart -mtime +7 -exec rm -rf {} +
EOL

chmod +x "$DB_BACKUP_SCRIPT"
# change ownership to postgres
chown postgres:postgres "$DB_BACKUP_SCRIPT"
echo "Created backup script at $DB_BACKUP_SCRIPT"

# Schedule cron job if not already scheduled
echo "Scheduling cron job for database backups..."

# Check if there is an existing job in /etc/cron.d/postgres_backup
if sudo test -f /etc/cron.d/postgres_backup; then
    sudo rm /etc/cron.d/postgres_backup
fi

if sudo -u postgres crontab -l 2>/dev/null | grep -q "$DB_BACKUP_SCRIPT"; then
    # Delete existing cron job
    sudo -u postgres crontab -l 2>/dev/null | grep -v "$DB_BACKUP_SCRIPT" | sudo -u postgres crontab -
fi
# add cron job in /etc/cron.d/postgres_backup
echo "$BACKUP_CRON_SCHEDULE postgres $DB_BACKUP_SCRIPT" | sudo tee /etc/cron.d/postgres_backup

echo "Scheduled cron job for database backups"

echo "Database backup setup complete."
# Note: Ensure that the PostgreSQL server has the necessary permissions to write to the backup directory.