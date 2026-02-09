#!/bin/bash
set -e

# This script sets up a backup system

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
        echo "$USER:COSC2024" | sudo chpasswd
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
# 2. DB Migration Setup
#=============================================================================
# Setup SSH keys for passwordless SSH between primary and standby servers
if [ ! -f /home/cosc-admin/.ssh/id_rsa ]; then
    sudo -u cosc-admin ssh-keygen -t rsa -b 4096 -N "" -f /home/cosc-admin/.ssh/id_rsa
    echo "SSH keys generated for cosc-admin."
else
    echo "SSH keys for cosc-admin already exist. Skipping key generation."
fi

# Copy key to primary server
sudo -u cosc-admin ssh-copy-id $PRIMARY_USER@$PRIMARY_IP

# Sudo passwordless SSH setup on primary server if not already set
ssh $PRIMARY_USER@$PRIMARY_IP "sudo grep -q '^$PRIMARY_USER ALL=(ALL) NOPASSWD:ALL' /etc/sudoers || " \
"echo '$PRIMARY_USER ALL=(ALL) NOPASSWD:ALL' | sudo tee -a /etc/sudoers"

# Run DB migration script
echo "Starting database migration setup..."

bash ../database_setup/db_migration.sh
#=============================================================================
# 3. Database Backup Setup
#=============================================================================
# Setup automated backups for the database
bash ../database_setup/backup_setup.sh
#=============================================================================
echo "Server setup complete."
# The server is now configured with necessary users, database replication, and backup systems.