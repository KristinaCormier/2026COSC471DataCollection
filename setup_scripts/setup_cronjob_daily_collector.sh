#!/usr/bin/env bash
set -euo pipefail

# Install cron job for intraday collection.
# Default mode uses user crontab + project-local wrapper to avoid system path dependencies.
# Set CRON_INSTALL_MODE=system to install into /etc/cron.d with /usr/local/bin wrapper.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
ENVFILE="$PROJECT_DIR/.env"

if [[ ! -f "$ENVFILE" ]]; then
    echo "Error: Missing $ENVFILE"
    echo "Please create a .env file in the project root directory"
    echo "You can copy variables from .env.template"
    exit 1
fi

set -a
. "$ENVFILE"
set +a

CURRENT_USER="$(id -un)"
RUN_AS_USER="${RUN_AS_USER:-${SUDO_USER:-$CURRENT_USER}}"

CRON_INSTALL_MODE="${CRON_INSTALL_MODE:-user}"
COLLECTION_SCHEDULE="${COLLECTION_SCHEDULE:-0 * * * *}"

PYTHON="$PROJECT_DIR/.venv/bin/python"
SCRIPT="$PROJECT_DIR/src/intraday_data_collection.py"
CRON_MARKER="COSC471_STOCK_COLLECTOR"

LOG_DIR="${LOG_DIR:-./logs}"
if [[ "$LOG_DIR" = /* ]]; then
    LOG_DIR_PATH="$LOG_DIR"
else
    LOG_DIR_PATH="$PROJECT_DIR/$LOG_DIR"
fi

USER_WRAPPER_DIR="${CRON_WRAPPER_DIR:-$PROJECT_DIR/.ops/bin}"
USER_WRAPPER="$USER_WRAPPER_DIR/run_stock_collector.sh"

SYSTEM_WRAPPER="${SYSTEM_WRAPPER:-/usr/local/bin/run_stock_collector.sh}"
SYSTEM_CRON_FILE="${SYSTEM_CRON_FILE:-/etc/cron.d/stock_collector_daily}"

run_as_user() {
    if [[ $EUID -eq 0 && "$RUN_AS_USER" != "$CURRENT_USER" ]]; then
        sudo -u "$RUN_AS_USER" "$@"
    else
        "$@"
    fi
}

get_user_crontab() {
    if [[ $EUID -eq 0 && "$RUN_AS_USER" != "$CURRENT_USER" ]]; then
        sudo -u "$RUN_AS_USER" crontab -l 2>/dev/null || true
    else
        crontab -l 2>/dev/null || true
    fi
}

set_user_crontab() {
    if [[ $EUID -eq 0 && "$RUN_AS_USER" != "$CURRENT_USER" ]]; then
        sudo -u "$RUN_AS_USER" crontab -
    else
        crontab -
    fi
}

if [[ "$CRON_INSTALL_MODE" != "user" && "$CRON_INSTALL_MODE" != "system" ]]; then
    echo "Error: CRON_INSTALL_MODE must be 'user' or 'system'"
    exit 1
fi

if [[ $EUID -ne 0 && "$RUN_AS_USER" != "$CURRENT_USER" ]]; then
    echo "Error: RUN_AS_USER can only differ from current user when running as root"
    exit 1
fi

if [[ "$CRON_INSTALL_MODE" == "system" && $EUID -ne 0 ]]; then
    echo "Error: system mode requires root privileges (sudo)"
    exit 1
fi

id "$RUN_AS_USER" &>/dev/null || { echo "Error: user '$RUN_AS_USER' does not exist"; exit 1; }
[[ -x "$PYTHON" ]] || { echo "Error: missing python executable at $PYTHON"; exit 1; }
[[ -f "$SCRIPT" ]] || { echo "Error: missing script at $SCRIPT"; exit 1; }

echo "Installing daily collector cron job"
echo "Mode:        $CRON_INSTALL_MODE"
echo "User:        $RUN_AS_USER"
echo "Project dir: $PROJECT_DIR"
echo "Schedule:    $COLLECTION_SCHEDULE"
echo "Log dir:     $LOG_DIR_PATH"

echo "Checking python dependencies..."
if ! run_as_user "$PYTHON" -c "import requests, sqlalchemy, psycopg, dotenv" >/dev/null 2>&1; then
    echo "Installing requirements into $PROJECT_DIR/.venv"
    run_as_user "$PYTHON" -m pip install -r "$PROJECT_DIR/requirements.txt"
fi

if [[ "$CRON_INSTALL_MODE" == "system" ]]; then
    mkdir -p "$LOG_DIR_PATH"
    chown -R "$RUN_AS_USER:$RUN_AS_USER" "$LOG_DIR_PATH"
    chmod 750 "$LOG_DIR_PATH"
else
    run_as_user mkdir -p "$LOG_DIR_PATH"
    run_as_user chmod 750 "$LOG_DIR_PATH" || true
fi

tmp_wrapper="$(mktemp)"
cat > "$tmp_wrapper" <<EOF
#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$PROJECT_DIR"
ENVFILE="\$PROJECT_DIR/.env"

if [[ -f "\$ENVFILE" ]]; then
    set -a
    . "\$ENVFILE"
    set +a
fi

cd "\$PROJECT_DIR"
exec "$PYTHON" "$SCRIPT"
EOF

if [[ "$CRON_INSTALL_MODE" == "system" ]]; then
    install -m 0755 "$tmp_wrapper" "$SYSTEM_WRAPPER"
    chown root:root "$SYSTEM_WRAPPER"

    cat > "$SYSTEM_CRON_FILE" <<EOF
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

$COLLECTION_SCHEDULE $RUN_AS_USER $SYSTEM_WRAPPER # $CRON_MARKER
EOF
    chmod 644 "$SYSTEM_CRON_FILE"

    echo "Installed system cron entry at $SYSTEM_CRON_FILE"
    echo "Wrapper script: $SYSTEM_WRAPPER"
else
    run_as_user mkdir -p "$USER_WRAPPER_DIR"
    if [[ $EUID -eq 0 && "$RUN_AS_USER" != "$CURRENT_USER" ]]; then
        install -m 0755 -o "$RUN_AS_USER" -g "$RUN_AS_USER" "$tmp_wrapper" "$USER_WRAPPER"
    else
        install -m 0755 "$tmp_wrapper" "$USER_WRAPPER"
    fi

    existing_cron="$(get_user_crontab)"
    filtered_cron="$(printf '%s\n' "$existing_cron" | grep -v "$CRON_MARKER" || true)"
    new_entry="$COLLECTION_SCHEDULE $USER_WRAPPER # $CRON_MARKER"

    if [[ -n "$filtered_cron" ]]; then
        printf '%s\n%s\n' "$filtered_cron" "$new_entry" | set_user_crontab
    else
        printf '%s\n' "$new_entry" | set_user_crontab
    fi

    echo "Installed user cron entry for $RUN_AS_USER"
    echo "Wrapper script: $USER_WRAPPER"
fi

rm -f "$tmp_wrapper"

echo "Daily collector cron setup complete"
