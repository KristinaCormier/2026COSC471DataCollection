#!/usr/bin/env bash
set -euo pipefail

# Compatibility wrapper for the Python CSV loader.
# Preferred direct command:
#   python src/load_stg_raw_market_data.py --csv-dir /path/to/csv

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
ENV_FILE="$PROJECT_DIR/.env"

if [[ -f "$ENV_FILE" ]]; then
    set -a
    . "$ENV_FILE"
    set +a
fi

PYTHON_BIN="${PYTHON_BIN:-$PROJECT_DIR/.venv/bin/python}"
PYTHON_SCRIPT="$PROJECT_DIR/src/load_stg_raw_market_data.py"

if [[ ! -x "$PYTHON_BIN" ]]; then
    echo "Error: python executable not found at $PYTHON_BIN"
    echo "Create/activate the project virtual environment first."
    exit 1
fi

if [[ ! -f "$PYTHON_SCRIPT" ]]; then
    echo "Error: loader script not found at $PYTHON_SCRIPT"
    exit 1
fi

if [[ $# -eq 0 ]]; then
    echo "No CLI arguments provided; delegating to python loader with env defaults."
    echo "Tip: pass --csv-dir /path/to/csv, or set CSV_PATH in .env"
fi

exec "$PYTHON_BIN" "$PYTHON_SCRIPT" "$@"