# Database Table Creation Scripts

## Overview

This directory contains SQL scripts for creating the necessary database tables and schemas for the 2026COSC471DataCollection project.

## Project Structure

```
2026COSC471DataCollection/
├── src/                              # Application source code
│   ├── intraday_data_collection.py   # Main data collection logic
│   ├── data_validation.py            # Data validation rules
│   ├── db_utils.py                   # Database operations
│   ├── logging_utils.py              # Logging configuration
│   └── time_utils.py                 # Time utilities
├── tests/                            # Test suite
│   ├── unit/                         # Unit tests
│   ├── integration/                  # Integration tests
│   ├── pipeline/                     # End-to-end pipeline tests
│   └── conftest.py                   # pytest configuration
├── setup_scripts/                    # Deployment & setup
│   ├── setup_cronjob_daily_collector.sh
│   └── table_creation_script/
│       └── 511TableCreationScript.sql
├── requirements.txt                  # Production dependencies
├── requirements-dev.txt              # Development dependencies
├── pytest.ini                        # pytest configuration
├── README.md                         # Project overview
└── LICENSE
```

## Files in This Directory

- **511TableCreationScript.sql** - Master SQL script for initializing all required database tables and schemas

## Usage

Run the table creation script to initialize your database:

```bash
psql -U <username> -d <database> -f 511TableCreationScript.sql
```

Ensure your database connection is properly configured before execution.

## Database Initialization

The scripts in this directory set up:
- Schema definitions
- Core tables for market data
- Indexes and constraints
- Sequence generators

## Related Documentation

- See [../setup_cronjob_daily_collector.sh](../setup_cronjob_daily_collector.sh) for scheduling automated data collection
- See [../../README.md](../../README.md) for full project documentation