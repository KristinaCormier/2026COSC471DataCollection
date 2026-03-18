"""
Data validation utilities.
Analyzes row completeness and identifies missing fields.
"""

from __future__ import annotations

from typing import Mapping, Sequence


def is_empty(value) -> bool:
    """Check if a value is None or an empty string."""
    return value is None or (isinstance(value, str) and value.strip() == "")


def analyze_row(row: Mapping[str, object], fields: Sequence[str]) -> tuple[list[str], bool]:
    """
    Analyze a data row for missing or empty fields.
    
    Args:
        row: Dictionary of field values
        fields: List of expected field names
    
    Returns:
        Tuple of (list of missing field names, whether all fields are empty)
    """
    missing_fields = [field for field in fields if is_empty(row.get(field))]
    all_empty = len(missing_fields) == len(fields)
    return missing_fields, all_empty
