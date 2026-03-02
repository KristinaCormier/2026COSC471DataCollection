#!/usr/bin/env bash
set -e

# Setup cron job for scheduled SQL operations orchestrator
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

# Load deployment configuration with defaults
RUN_AS_USER="${RUN_AS_USER:-cosc-admin}"
CRON_TIME="${CRON_TIME_SCHEDULED_OPS:-0 * * * *}"  # Default: hourly (top of every hour)

USER_HOME="/home/$RUN_AS_USER"
PYTHON="$PROJECT_DIR/.venv/bin/python"
SCRIPT="$PROJECT_DIR/src/run_scheduled_operations.py"
WRAPPER="/usr/local/bin/run_scheduled_operations.sh"
CRON_FILE="/etc/cron.d/scheduled_operations"

echo "Installing scheduled operations cron job"
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
if ! sudo -u "$RUN_AS_USER" "$PYTHON" -c "import psycopg, dotenv" 2>/dev/null; then
    echo "Installing requirements..."
    sudo -u "$RUN_AS_USER" "$PYTHON" -m pip install -r "$PROJECT_DIR/requirements.txt"
    echo "Requirements installed successfully"
else
    echo "All requirements are already installed"
fi

# Check if pipeline_logs table exists (prerequisite)
echo "Checking if operation_logs schema exists..."
TEMP_CHECK=$(mktemp)
if ! sudo -u "$RUN_AS_USER" "$PYTHON" -c "
import psycopg
import os
try:
    conn = psycopg.connect(
        host=os.getenv('PGHOST', 'localhost'),
        port=int(os.getenv('PGPORT', 5432)),
        dbname=os.getenv('PGDATABASE'),
        user=os.getenv('PGUSER'),
        password=os.getenv('PGPASSWORD', '')
    )
    with conn.cursor() as cur:
        cur.execute(
            '''SELECT 1 FROM information_schema.tables 
               WHERE table_schema='operation_logs' AND table_name='pipeline_logs' '''
        )
        if cur.fetchone() is None:
            print('ERROR: operation_logs.pipeline_logs table does not exist', file=__import__('sys').stderr)
            exit(1)
    conn.close()
except Exception as e:
    print(f'ERROR: {e}', file=__import__('sys').stderr)
    exit(1)
" 2>/dev/null; then
    echo "Warning: operation_logs.pipeline_logs table not found"
    echo "Make sure to run: bash setup_scripts/table_creation_script/operation_logs/*.sql"
fi
rm -f "$TEMP_CHECK"

# ------------------------------------------------------------
# Create wrapper script
# ------------------------------------------------------------
sudo tee "$WRAPPER" > /dev/null <<'EOF'
#!/usr/bin/env bash
set -e

# Load environment variables for database connection
if [[ -f /root/.env.scheduled_ops ]]; then
    set -a
    . /root/.env.scheduled_ops
    set +a
fi

exec "$PYTHON" "$SCRIPT"
EOF

# Replace placeholders in wrapper
sed -i "s|\$PYTHON|$PYTHON|g" "$WRAPPER"
sed -i "s|\$SCRIPT|$SCRIPT|g" "$WRAPPER"

sudo chmod +x "$WRAPPER"
sudo chown root:root "$WRAPPER"

# ------------------------------------------------------------
# Create cron.d entry with environment variables
# The cron job inherits from .env, but we also source them explicitly
# ------------------------------------------------------------
sudo tee "$CRON_FILE" > /dev/null <<EOF
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# Load database environment variables
PGHOST=${PGHOST:-localhost}
PGPORT=${PGPORT:-5432}
PGDATABASE=$PGDATABASE
PGUSER=$PGUSER
PGPASSWORD=$PGPASSWORD

# Run scheduled operations
$CRON_TIME $RUN_AS_USER $WRAPPER
EOF

sudo chmod 644 "$CRON_FILE"

echo "Cron job installed successfully"
echo "Cron file:  $CRON_FILE"
echo "Cron will run this script:    $WRAPPER"
echo ""
echo "Next step:"
echo "1. Verify .env has correct database credentials (PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD)"
echo "2. Test manually: python3 $SCRIPT"
echo "3. Check logs: tail -f /usr/local/dc_error_logs/scheduled_operations.log"
