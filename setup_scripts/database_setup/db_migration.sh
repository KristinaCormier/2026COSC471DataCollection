#!/bin/bash
# ============================================================================
# Physical Streaming Replication Setup
# ============================================================================
#
# Purpose:
#     Configure PostgreSQL physical streaming replication from a primary server
#     to a local standby/replica server. Enables HA, disaster recovery, and failover.
#
# Intended Use:
#     One-time setup during initial server provisioning. Called by setup_server.sh.
#     Sets up the local instance as a read-only replica following the primary.
#
# Prerequisites:
#     - SSH access to primary server (REPLICATION_USER with password)
#     - PostgreSQL 16 installed on both primary and local server
#     - Local PostgreSQL 16 must be stopped before running (data directory will be wiped)
#     - .env file with PRIMARY_IP, PRIMARY_USER, REPLICATION_USER, REPLICATION_PASSWORD, etc.
#     - Sufficient disk space for base backup on local system
#
# What It Does:
#     Phase 1 (Remote Primary):
#         1. Enable listening on all interfaces (listen_addresses = '*')
#         2. Add replication permission to pg_hba.conf
#         3. Create replication role with password
#         4. Create physical replication slot (for WAL retention on primary)
#         5. Restart PostgreSQL to apply changes
#
#     Phase 2 (Local Standby):
#         1. Set up .pgpass for credential-less replication
#         2. Stop local PostgreSQL 16 service
#         3. Clear local data directory (/var/lib/pgsql/16/data)
#         4. Run pg_basebackup from primary with streaming (-R flag includes recovery config)
#         5. Verify filesystem is ready and standby is in recovery mode
#
# Environment Variables (from .env):
#     PRIMARY_USER: SSH user on primary (usually 'postgres')
#     PRIMARY_IP: IP hostname of primary server
#     PG_CONF: Full path to primary's postgresql.conf
#     PG_HBA: Full path to primary's pg_hba.conf
#     REPLICATION_USER: Role name for replication
#     REPLICATION_PASSWORD: Password for replication role
#     SLOT_NAME: Physical replication slot name (e.g., 'replica_slot_1')
#     DATA_DIR: Local PostgreSQL data directory (e.g., '/var/lib/pgsql/16/data')
#
# Side Effects:
#     ⚠️  DESTRUCTIVE - Wipes local PostgreSQL data directory
#     - Restarts PostgreSQL on both primary and local
#     - Modifies postgresql.conf and pg_hba.conf on primary (appends lines)
#     - Creates .pgpass file in postgres home directory
#
# Post-Setup Validation:
#     # On standby, verify it's in recovery:
#     sudo -u postgres psql -c "SELECT pg_is_in_recovery();"
#     > t (true = standby is ready)
#
#     # On primary, verify slot exists:
#     psql -c "SELECT slot_name, active FROM pg_replication_slots WHERE slot_name = 'SLOT_NAME';"
#     > slot_name | active
#     > SLOT_NAME | t
#
# Rollback/Recovery:
#     If setup fails partway:
#     1. Restore local data directory from backup
#     2. Fix the error on primary (check pg_hba.conf, REPLICATION_USER permissions)
#     3. Re-run this script to retry the full setup
#
# Author: Data Collection Team
# License: MIT
# ============================================================================

set -e

# This script sets up a Physical Streaming Replication (PSR) system for the postgres16 database.
# It configures the primary and standby servers for replication.

ENV_FILE="../.env"
if [ -f "$ENV_FILE" ]; then
    set -a
    . "$ENV_FILE"
    set +a
else
    echo "Error: $ENV_FILE not found. Please create the .env file with the necessary variables."
    exit 1
fi

SSH_CONTROL_DIR="/tmp"
SSH_CONTROL_SOCKET="$SSH_CONTROL_DIR/ssh-${PRIMARY_USER}@${PRIMARY_IP}.sock"
SSH_OPTS="-o ControlMaster=auto -o ControlPersist=10m -o ControlPath=$SSH_CONTROL_SOCKET"
SSH="ssh $SSH_OPTS"

open_control_master() {
    $SSH -MNf "$PRIMARY_USER@$PRIMARY_IP"
}

close_control_master() {
    ssh -S $SSH_CONTROL_SOCKET -O exit "$PRIMARY_USER@$PRIMARY_IP" 2>/dev/null || true
}
trap close_control_master EXIT

#=============================================================================
# 1. Configure Primary Server
#=============================================================================
echo "PHASE 1: Configuring Primary Server..."
echo "Update listen_addresses"
# Enable listening on all addresses
#ssh $PRIMARY_USER@$PRIMARY_IP "sudo sed -i \"s/^#listen_addresses = 'localhost'/listen_addresses = '*'/\" $PG_CONF"
open_control_master
$SSH $PRIMARY_USER@$PRIMARY_IP "sudo sed -i \"s/^#listen_addresses = 'localhost'/listen_addresses = '*'/\" $PG_CONF"

# Append replication permission to pg_hba.conf if not already there
echo "Configuring pg_hba.conf for replication..."
REPL_LINE="host replication $REPLICATION_USER 0.0.0.0/0 scram-sha-256"
$SSH $PRIMARY_USER@$PRIMARY_IP "sudo grep -qxF '$REPL_LINE' $PG_HBA || echo '$REPL_LINE' | sudo tee -a $PG_HBA"

# Create replication role and Physical Slot
#echo "Step 2: Creating Replication Role and Restarting Primary..."
echo "Creating Replication Role and Physical Slot..."
$SSH $PRIMARY_USER@$PRIMARY_IP "sudo -u postgres psql -c \"CREATE ROLE $REPLICATION_USER WITH REPLICATION PASSWORD '$REPLICATION_PASSWORD' LOGIN;\" 2>/dev/null || echo 'Role already exists, moving on...'"

echo "Creating Physical Replication Slot..."
$SSH $PRIMARY_USER@$PRIMARY_IP "sudo -u postgres psql -c \"SELECT pg_create_physical_replication_slot('$SLOT_NAME') WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '$SLOT_NAME');\""

# Restart PostgreSQL to apply changes
echo "Restarting Primary PostgreSQL Service..."
$SSH $PRIMARY_USER@$PRIMARY_IP "sudo systemctl restart postgresql-16"

#=============================================================================
# Local Replica Server Configuration
#=============================================================================
echo "PHASE 2: Primary is ready. Starting local Replica setup..."
# 1. Prepare local credentials (.pgpass)
echo "Setting up credentials..."
sudo -u postgres bash -c "echo '${PRIMARY_IP}:5432:replication:${REPLICATION_USER}:${REPLICATION_PASSWORD}' > /var/lib/pgsql/.pgpass"
sudo chmod 0600 /var/lib/pgsql/.pgpass
sudo chown postgres:postgres /var/lib/pgsql/.pgpass

# 2. Stop local PostgreSQL and wipe existing data
echo "Stopping local PostgreSQL service and clearing $DATA_DIR..."
sudo systemctl stop postgresql-16
sudo rm -rf $DATA_DIR/*

# 3. Perform base backup
# -R automatically creates standby.signal and postgresql.auto.conf
echo "Streaming data from Primary..."
sudo -u postgres pg_basebackup \
    -h $PRIMARY_IP \
    -D $DATA_DIR \
    -U $REPLICATION_USER \
    -v -P -R \
    --slot=$SLOT_NAME \
    --wal-method=stream

# 4. Final Permissions and Start
echo "Starting Replica..."
sudo chown -R postgres:postgres $DATA_DIR
sudo chmod 700 $DATA_DIR
sudo systemctl start postgresql-16

echo "--- SUCCESS: Verification ---"

#=============================================================================
# 3. Verify Replication Status
#=============================================================================
echo "Verifying Replication Status on Primary Server..."
sudo -u postgres psql -c "SELECT pg_is_in_recovery() AS is_replica, now();"
sudo -u postgres psql -c "SELECT client_addr, state, sync_state FROM pg_stat_replication;"
echo "Verification complete."

# Promote replication standby to primary (if needed)
sudo -u postgres psql -c "SELECT pg_promote();"


#=============================================================================
# End of Script
#=============================================================================