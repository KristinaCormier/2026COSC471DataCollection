## Description

Brief summary of this PR. This will appear in release notes.

## Changes

- Change 1
- Change 2
- Change 3

## Type of Change

- [ ] **feat**: New data source, collection script, or pipeline feature
- [ ] **fix**: Bug fix (API parsing, data validation, database operation)
- [ ] **test**: Test additions or improvements (unit, integration, pipeline)
- [ ] **docs**: Documentation updates (README, docstrings, setup guides)
- [ ] **schema**: Database schema changes (new tables, columns, constraints)
- [ ] **ops**: Operational changes (cron schedule, backup configuration, setup scripts)
- [ ] **refactor**: Code restructuring (no behavior change)
- [ ] **chore**: Dependencies, build, or CI/CD configuration

## Why

Motivation and context. Why is this change needed?

- Example: "Addresses collection failures for symbols with missing data points"
- Example: "Improves deduplication accuracy by adding source tracking"
- Example: "Enables backfill from new data provider"

## Testing

How was this tested? What scenarios were verified?

- [ ] Unit tests added/updated for data validation or time calculations
- [ ] Integration tests added/updated for database operations
- [ ] Pipeline tests added/updated for end-to-end collection/transform flow
- [ ] Manual testing of collection (e.g., ran `python src/intraday_data_collection.py`)
- [ ] SQL changes tested in test database (if schema/operations modified)

Describe test scenarios:
- Example: "Tested intraday collection with 5 symbols, 100+ rows; verified upsert behavior"
- Example: "Tested gather_past_data.py with historical date range; verified deduplication"
- Example: "Ran pytest on unit/ and integration/ directories; all pass with no warnings"

## Breaking Changes

Do any changes affect the pipeline behavior, schema, environment variables, or cron setup?

- [ ] No breaking changes
- [ ] Yes, breaking changes (describe below)

If yes, describe:
- Example: "Renamed COLLECTION_INTERVAL_MINUTES to WINDOW_MINUTES (update .env)"
- Example: "Added NOT NULL constraint to stg_raw.market_data.source (backfill required)"
- Example: "Cron schedule for scheduled_operations changed from hourly to daily"

## Database/Schema Changes

If this PR modifies the database:

- [ ] No database changes
- [ ] Added new table
- [ ] Added new column to existing table
- [ ] Modified column type or constraint
- [ ] Dropped table or column

Describe the migration:
- Example: "Run `python -m alembic upgrade head`"
- Example: "Add and document a new Alembic revision under `alembic/versions/`"
- Example: "No migration needed (docs/tests/runtime-only change)"

## Environment Variables

Does this PR add, remove, or change environment variables?

- [ ] No environment changes
- [ ] New `.env` variables required (list below)
- [ ] Removed/renamed variables (list below)
- [ ] Changed default values (list below)

If yes, describe:
- Example: "Added `DATA_SOURCE_API_KEY` for new FMP endpoint"
- Example: "Removed legacy `COLLECTION_INTERVAL_MINUTES` (replaced by `WINDOW_MINUTES`)"

## Documentation Updated

- [ ] README.md (project overview, quick start)
- [ ] src/README.md (script descriptions)
- [ ] setup_scripts/README.md (setup procedures)
- [ ] tests/README.md (test guidelines)
- [ ] .env.template (new variables documented)
- [ ] Module docstrings (updated purpose/usage)
- [ ] SQL script headers (if `.sql` files modified)
- [ ] Not applicable (typo fix, internal refactor only)

## Related Issues

Closes #<issue_number>

## Checklist

- [ ] Code follows project style guidelines (PEP 8 for Python, consistent SQL formatting)
- [ ] Self-review completed
- [ ] Comments added for complex logic (especially data validation, SQL transforms)
- [ ] Documentation updated if behavior changed (README, docstrings, headers)
- [ ] Module/script has a purpose header or docstring explaining intent
- [ ] No new warnings generated (linter, imports, logger calls)
- [ ] Tests pass locally (`pytest` on unit/ + integration/ + pipeline/)
- [ ] Environment variables are set correctly in `.env` for testing
