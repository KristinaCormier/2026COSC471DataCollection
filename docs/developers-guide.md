# Developers Guide

## Overview

<div style="width: 640px; height: 480px; margin: 10px; position: relative;"><iframe allowfullscreen frameborder="0" style="width:640px; height:480px" src="https://lucid.app/documents/embedded/f2376022-5220-42eb-8ae5-f99807d9338d" id="pIjIkG0I1LRy"></iframe></div>

--8<-- "README.md:architecture-notes"

--8<-- "src/README.md:operational-entry-points"

--8<-- "src/README.md:supporting-modules"

# Contributing
--8<-- "tests/README.md:execution-tracks"

--8<-- "README.md:contributing-checklist"

Use the repository pull request checklist when preparing changes for review.

--8<-- "tests/README.md:naming-conventions"

--8<-- "tests/README.md:test-common-issues"

--8<-- "src/README.md:design-patterns"

--8<-- "src/README.md:running-from-root"

--8<-- "tests/README.md:test-organization"

# Test Suite
--8<-- "tests/README.md:test-setup"

--8<-- "tests/README.md:running-tests"
### Unit Tests

--8<-- "tests/unit/README.md:summary"

### Integration Tests

--8<-- "tests/integration/README.md:summary"

### Pipeline Tests

--8<-- "tests/pipeline/README.md:summary"

### Test Data

--8<-- "tests/data/README.md:summary"

## Shared Fixtures

All fixtures are defined in `tests/conftest.py` and available globally:

--8<-- "tests/README.md:shared-fixtures-body"

--8<-- "tests/README.md:adding-tests"

## Continuous Integration

Tests are automatically run on pull requests, pushes to `main` and `dev`, and merge queue runs via the `pytest.yml` GitHub Actions workflow.

--8<-- "tests/README.md:continuous-integration-body"



