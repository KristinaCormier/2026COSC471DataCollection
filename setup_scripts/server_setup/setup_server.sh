#!/bin/bash
# ============================================================================
# Complete Server Setup Entrypoint
# ============================================================================
#
# Purpose:
#     One-time server provisioning script that orchestrates user creation,
#     database replication setup, and automated backup configuration.
#
# Intended Use:
#     Run once during initial server deployment to establish HA, replication,
#     and disaster recovery capabilities for the PostgreSQL cluster.
#
# Prerequisites:
#     - Root or sudo access
#     - PostgreSQL 16 installed on both primary and local server
#     - Network connectivity from local to primary server
#     - .env file configured with all required variables (see below)
#     - Sufficient disk space for backups and replication data
#
# What It Does:
#     1. Loads configuration from ../.env
#     2. Creates sudo-capable system users from LIST_OF_SUDO_USERS array
#     3. Calls database_setup/db_migration.sh to set up replication
#     4. Calls database_setup/backup_setup.sh to configure backups
#
# Environment Variables (from .env):
#     LIST_OF_SUDO_USERS: Bash array of usernames to create and grant sudo
#                         (e.g., LIST_OF_SUDO_USERS=("etl" "admin"))
#
#     Plus all variables required by db_migration.sh and backup_setup.sh
#     (PRIMARY_IP, REPLICATION_USER, BACKUP_DIR, etc. - see those scripts)
#
# Output:
#     - System users created with passwordless sudo
#     - Replication configured and validated
#     - Backup job scheduled in cron
#     - Wrapper scripts created at /usr/local/bin/
#     - Log messages printed to stdout
#
# Side Effects:
#     ⚠️  ELEVATED PRIVILEGES - Modifies /etc/sudoers, pg_hba.conf, postgresql.conf
#     - Creates system users and adds to sudoers
#     - Wipes local PostgreSQL data directory (part of replication setup)
#     - Restarts PostgreSQL services on primary and local
#     - Modifies /etc/cron.d/
#
# Post-Setup Validation:
#     # Check users were created:
#     getent passwd | grep -E "etl|admin"
#
#     # Check replication is active:
#     sudo -u postgres psql -c "SELECT pg_is_in_recovery();"  # Should return 't'
#
#     # Check backup cron is scheduled:
#     sudo crontab -l
#
# Typical Usage:
#     cd setup_scripts
#     sudo bash setup_server.sh
#
# Recovery:
#     If the script fails partway:
#     1. Fix the cause (e.g., network issue, PRIMARY_IP typo)
#     2. Manually undo partial changes if needed
#     3. Re-run the script to complete the setup
#
# Author: Data Collection Team
# License: MIT
# ============================================================================

set -e

# This script sets up a backup system
ENV_FILE="../../.env"
if [ -f "$ENV_FILE" ]; then
    set -a
    . "$ENV_FILE"
    set +a
else
    echo "Error: $ENV_FILE not found. Please create the .env file with the necessary variables. server_setup.sh cannot continue without it."
    exit 1
fi

DEFAULT_SUDO_PASSWORD="COSC2024"

#=============================================================================
# 1. Create Sudo Users
#=============================================================================
echo "Creating sudo users..."
for USER in "${LIST_OF_SUDO_USERS[@]}"; do
    if id "$USER" &>/dev/null; then
        echo "User $USER already exists. Skipping creation."
    else
        echo "Creating user $USER..."
        # Create user with home directory and bash shell with default password 'COSC2024'
        sudo useradd -m -s /bin/bash "$USER"
        echo "$USER:$DEFAULT_SUDO_PASSWORD" | sudo chpasswd
        echo "User $USER created."
    fi

    # Add user to sudoers if not already present
    if sudo grep -q "^$USER ALL=(ALL) NOPASSWD: ALL" /etc/sudoers; then
        echo "User $USER already has sudo privileges. Skipping sudoers modification."
    else
        echo "Adding $USER to sudoers..."
        echo "$USER ALL=(ALL) NOPASSWD: ALL" | sudo tee -a /etc/sudoers
        echo "User $USER added to sudoers."
    fi
done

#=============================================================================
# 2. DB Initialization Setup
#=============================================================================
# Run DB initialization script
echo "Starting database initialization setup..."

sudo -u postgres createdb $DB_NAME
echo "Database $DB_NAME created."
#=============================================================================
# 3. Database Backup Setup
#=============================================================================
# Setup automated backups for the database
echo "Starting backup setup..."
bash ../database_setup/backup_setup.sh
echo "Backup setup complete."
#=============================================================================
echo "Server setup complete."
# The server is now configured with necessary users, database replication, and backup systems.