Setup_error_logs.py 

[Back to Readme](readme.md)

1. Overview of the Problem

- This documentation summarizes the structural and logic fixes made to the Automated Data Collection script. It serves as a guide for why these changes were necessary and how to maintain the environment moving forward.
The project initially faced several "Pathing" and "Naming" errors. 

- Python was unable to find the source code (src/) when scripts were run from subdirectories, and several files had inconsistent naming conventions (missing .py extensions and mismatched capitalization).



2. Key Accomplishments

- Module Resolution: Implemented the use of PYTHONPATH=src to allow scripts in setup_scripts/ to communicate with the core engine in src/.

- Standardization: Renamed core files to lowercase (auto_data_collection.py) and added proper extensions to executable scripts to comply with PEP 8 standards.

- Function Synchronization: Aligned the logging logic. The setup script now correctly calls the 8-argument signature of log_fetch_csv.

- Environment Validation: Verified the fix by successfully recreating logs/fetch_data_log.csv and achieving a 100% pass rate (27/27) on the pytest suite.


The "Insurance Policy": Logging

- Instead of the script just crashing and leaving you wondering what happened, you are implementing Pre-condition Logging.

- The Setup: Before the "real work" starts, you check if a log file exists. If not, you create it with headers.

- The Audit Trail: Every time the script runs or fails, it writes a line to that CSV. This allows you to look back in a week and say, "Oh, it failed at 3:00 AM because of a ConnectionError."



3. The Safety Net: Automated Testing

- You are using pytest to prove that your code works before you actually trust it with real data.

- Isolation: You are using tmp_path to make sure tests don't mess up your real log files.
Verification: When testing the "edge cases", like what happens if the logs/ folder is deleted. So code is smart enough to recreate it on the fly.



How the Pieces Fit Together:

.venv - The Bubble
Keeps your project dependencies (like psycopg2 or pytest) isolated from your main computer.

src/ - The Engine
This is where the actual logic lives.

setup_scripts/ - The Mechanic
Scripts that prepare the environment (like creating database tables or initializing logs).

tests/ - The Inspector
Ensures that when you change one thing, you don't accidentally break three other things.

PYTHONPATH - The Map
Tells Python: "When I say import, look in the src folder!"



The New Setup Script:

Path: setup_scripts/table_creation_script/setup_error_logs.py


Why it is necessary:

In a data collection pipeline, if the logger tries to write to a file or directory that doesn't exist, the entire program crashes. This script acts as a Pre-Flight Check. It ensures:

- The logs/ directory is present.
- The fetch_data_log.csv file is initialized with the correct headers.
- The file structure matches the requirements of the database ingestion engine.


How it works:

The script utilizes an if __name__ == "__main__": block to ensure it only runs when explicitly called. It imports the production logging function and "primes" it with a SETUP entry:

Python
# Core logic snippet
log_fetch_csv("SETUP", now, now, "N/A", "N/A", 0, 0, "path/to/log.csv")


Usage Guide:

Running the Setup

To reset or initialize your logging environment, always run this from the project root:

Bash: PYTHONPATH=src python3 setup_scripts/table_creation_script/setup_error_logs.py

Running Tests:

To ensure no regressions have occurred in the logic or paths:

Bash - PYTHONPATH=src pytest


File Structure Summary:

src/auto_data_collection.py - The main engine containing the log_fetch_csv logic.

setup_scripts/.../setup_error_logs.py - Initializes local environment/files.

logs/fetch_data_log.csv - The generated output tracking API performance and errors.


How to test:

1. Remove the log file that currently exists so there is nothing for the script to find.

rm logs/fetch_data_log.csv

2. Run the script again. If it's working, it should realize the file is gone and recreate it from scratch.

cd /home/lhec/2026COSC471DataCollection
PYTHONPATH=src python3 setup_scripts/setup_error_logs.py

3. Check that the file was created and look at the first few lines to see the headers and your "SETUP" entry.

ls -l logs/fetch_data_log.csv
head -n 2 logs/fetch_data_log.csv


If the script is working correctly, the head command will output exactly two lines:

The Header: run_ts_market,symbol,window_start,window_end,from_day,to_day,api_rows,filtered_rows

The Entry: A row starting with 2026-02-04... and the word SETUP.



4. The script we just ran is a "helper." To test if your Actual Data Collection is working, you would run a "Dry Run" of the main engine:

PYTHONPATH=src python3 src/auto_data_collection.py --help