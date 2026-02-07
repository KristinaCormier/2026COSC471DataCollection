#!/usr/bin/env bash
set -e

# Installs a daily cron.d job for stock data collection
# Runs as system user: cosc-admin
# Similar pattern to postgres backup setup

RUN_AS_USER="cosc-admin"
USER_HOME="/home/$RUN_AS_USER"

PROJECT_DIR="$USER_HOME/2026COSC471DataCollection"
PYTHON="$PROJECT_DIR/.venv/bin/python"
SCRIPT="$PROJECT_DIR/fmp/src/Auto_Data_Collection.py"
ENVFILE="$USER_HOME/.collector_env"
LOGFILE="$USER_HOME/collector.log"

# 6:05 PM PST (server local time)
CRON_TIME="5 18 * * *"

WRAPPER="/usr/local/bin/run_stock_collector.sh"
CRON_FILE="/etc/cron.d/stock_collector_daily"

echo "Installing daily stock collector cron job"
echo "User:        $RUN_AS_USER"
echo "Project dir: $PROJECT_DIR"

# ------------------------------------------------------------
# Basic prereq. checks
# ------------------------------------------------------------
[[ -f "$ENVFILE" ]] || { echo "Missing $ENVFILE"; exit 1; }
[[ -x "$PYTHON"  ]] || { echo "Missing $PYTHON"; exit 1; }
[[ -f "$SCRIPT"  ]] || { echo "Missing $SCRIPT"; exit 1; }

# ------------------------------------------------------------
# Create wrapper script
# ------------------------------------------------------------
sudo tee "$WRAPPER" > /dev/null <<EOF
#!/usr/bin/env bash
set -e

# Load collector environment
if [[ -f "$ENVFILE" ]]; then
  set -a
  . "$ENVFILE"
  set +a
fi

exec "$PYTHON" "$SCRIPT" >> "$LOGFILE" 2>&1
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

echo "Cron job installed"
echo "Cron file:  $CRON_FILE"
echo "Wrapper:    $WRAPPER"
echo "Env file:   $ENVFILE"
echo "Log file:   $LOGFILE"
