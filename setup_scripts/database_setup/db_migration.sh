#!/bin/bash
set -e

# This script sets up a Physical Streaming Replication (PSR) system for the postgres16 database.
# It configures the primary and standby servers for replication.
# Define variables
#PRIMARY_IP="10.12.43.225"
#PRIMARY_USER="cosc-admin" #The SSH user for the primary server
#REPLICATION_USER="rep_user"
#REPLICATION_PASSWORD="replica_pass"
#PG_CONF="/var/lib/pgsql/16/data/postgresql.conf" # DONE: adjust as needed
#PG_HBA="/var/lib/pgsql/16/data/pg_hba.conf" # DONE: adjust as needed
#SLOT_NAME="replica_slot_1"
#DATA_DIR="/var/lib/pgsql/16/data"

SSH_CONTROL_DIR="/tmp"
SSH_CONTROL_SOCKET="$SSH_CONTROL_DIR/ssh-${PRIMARY_USER}@${PRIMARY_IP}.sock"
SSH_OPTS="-o ControlMaster=auto -o ControlPersist=10m -o ControlPath=$SSH_CONTROL_SOCKET"
SSH="ssh $SSH_OPTS"

open_control_master() {
    $SSH -MNf "PRIMARY_USER@$PRIMARY_IP"
}

close_control_master() {
    ssh -S $SSH_CONTROL_SOCKET -O exit "PRIMARY_USER@$PRIMARY_IP" 2>/dev/null || true
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
