import os
import datetime as dt
from auto_data_collection import log_fetch_csv

def run_setup():
    print("Initializing log files...")
    # Matches the 8 arguments: symbol, start, end, day_from, day_to, api_rows, filtered_rows, csv_path
    log_fetch_csv(
        "SETUP", 
        dt.datetime.now(), 
        dt.datetime.now(), 
        "N/A", 
        "N/A", 
        0, 
        0, 
        "logs/fetch_data_log.csv"
    )
    print("Success: logs/fetch_data_log.csv has been initialized.")

if __name__ == "__main__":
    run_setup()
