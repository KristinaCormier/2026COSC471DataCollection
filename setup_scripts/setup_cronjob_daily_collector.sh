#!/usr/bin/env bash

# Adds cron job to crontab -e for daily end-of-day data collection.
# Assumes required environment variables are set in ~/.collector_env

#Usage:
    # Ensure permissions are set:
      # $ chmod +x ./setup_cron_daily_run.sh
    # Run this script once:
      # $ ./setup_cron_daily_run.sh
    # Confirm crontab edit:
      # $ crontab -e

PYTHON="$HOME/2026COSC471DataCollection/.venv/bin/python"
SCRIPT="$HOME/2026COSC471DataCollection/fmp/src/Auto_Data_Collection.py"
LOGFILE="$HOME/collector.log"
ENVFILE="$HOME/.collector_env"

# Run every day at 6:05 PM PST (9:05 PM EST)
CRON_TIME="5 18 * * *"

# Basic prereq. checks
[[ -f "$ENVFILE" ]] || { echo "Missing $ENVFILE"; exit 1; }
[[ -x "$PYTHON"  ]] || { echo "Missing $PYTHON"; exit 1; }
[[ -f "$SCRIPT"  ]] || { echo "Missing $SCRIPT"; exit 1; }

# Adds cron job without removing existing ones
(
  crontab -l 2>/dev/null #prints existing cron jobs & handles error msgs
  echo "$CRON_TIME . \"$ENVFILE\" && \"$PYTHON\" \"$SCRIPT\" >> \"$LOGFILE\" 2>&1"
) | crontab - #Adds existing jobs & new job to crontab

echo "Cron job added"
echo "Schedule: $CRON_TIME"
echo "Env file: $ENVFILE"
echo "Logs: $LOGFILE"