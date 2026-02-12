#!/usr/bin/env bash
set -e

# Setup the python virtual environment and install dependencies
# This script should be run as root (e.g., via sudo) to ensure it can write to /etc/cron.d and set permissions correctly.
if [[ $EUID -ne 0 ]]; then
   echo "This script must be run as root (e.g., via sudo)" 
   exit 1
fi

# Determine the project directory (assume script is in setup_scripts/)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
ENVFILE="$PROJECT_DIR/.env"

# Load configuration from .env file
if [[ -f "$ENVFILE" ]]; then
    set -a
    . "$ENVFILE"
    set +a
else
    echo "Error: Missing $ENVFILE"
    echo "Please create a .env file in the project root directory"
    echo "You can copy variables from .env.template."
    echo "Most importantly, set RUN_AS_USER to the system user that should run the cron job."
    exit 1
fi

# Installs a daily cron.d job for stock data collection
# Runs as configured system user
# Similar pattern to postgres backup setup

# Load deployment configuration with defaults
RUN_AS_USER="${RUN_AS_USER:-cosc-admin}"
CRON_TIME="${CRON_TIME:-0 * * * *}"

USER_HOME="/home/$RUN_AS_USER"
PYTHON="$PROJECT_DIR/.venv/bin/python"
SCRIPT="$PROJECT_DIR/src/intraday_data_collection.py"
WRAPPER="/usr/local/bin/run_stock_collector.sh"
CRON_FILE="/etc/cron.d/stock_collector_daily"

echo "Installing stock collector cron job"
echo "User:        $RUN_AS_USER"
echo "Project dir: $PROJECT_DIR"
echo "Schedule:    $CRON_TIME"

# ------------------------------------------------------------
# Basic prereq. checks
# ------------------------------------------------------------
id "$RUN_AS_USER" &>/dev/null || { echo "Error: User $RUN_AS_USER does not exist"; exit 1; }
[[ -d "$PROJECT_DIR/.venv" ]] || { echo "Missing virtual environment at $PROJECT_DIR/.venv"; exit 1; }
[[ -x "$PYTHON"  ]] || { echo "Missing $PYTHON"; exit 1; }
[[ -f "$SCRIPT"  ]] || { echo "Missing $SCRIPT"; exit 1; }
[[ -f "$ENVFILE" ]] || { echo "Missing $ENVFILE"; exit 1; }

# Check if requirements are installed
echo "Checking if python virtual environment requirements are installed..."
if ! sudo -u "$RUN_AS_USER" "$PYTHON" -c "import requests, psycopg, dotenv" 2>/dev/null; then
    echo "Installing requirements..."
    sudo -u "$RUN_AS_USER" "$PYTHON" -m pip install -r "$PROJECT_DIR/requirements.txt"
    echo "Requirements installed successfully"
else
    echo "All requirements are already installed"
fi

# ------------------------------------------------------------
# Create wrapper script
# ------------------------------------------------------------
sudo tee "$WRAPPER" > /dev/null <<EOF
#!/usr/bin/env bash
set -e

exec "$PYTHON" "$SCRIPT"
EOF

sudo chmod +x "$WRAPPER"
sudo chown root:root "$WRAPPER"

# ------------------------------------------------------------
# Create cron.d entry
# ------------------------------------------------------------
sudo tee "$CRON_FILE" > /dev/null <<EOF
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

$CRON_TIME $RUN_AS_USER $WRAPPER
EOF

sudo chmod 644 "$CRON_FILE"

echo "Cron job installed successfully"
echo "Cron file:  $CRON_FILE"
echo "Cron will run this script:    $WRAPPER"
