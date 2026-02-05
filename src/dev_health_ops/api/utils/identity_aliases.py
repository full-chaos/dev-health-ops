"""Shared utilities for identity alias normalization and reverse mapping.

This module provides centralized functions for normalizing identity aliases
and constructing reverse alias maps, used across people, heatmap, and quadrant services.
"""

from typing import Dict, List


def normalize_alias(value: str) -> str:
    """Normalize an alias string for consistent comparison.

    Args:
        value: The alias string to normalize

    Returns:
        Normalized string (stripped and lowercased)

    Examples:
        >>> normalize_alias("  John Doe  ")
        'john doe'
        >>> normalize_alias("JANE@EXAMPLE.COM")
        'jane@example.com'
    """
    return (value or "").strip().lower()


def build_reverse_alias_map(aliases: Dict[str, List[str]]) -> Dict[str, str]:
    """Build a reverse mapping from normalized aliases to canonical identities.

    Args:
        aliases: Dictionary mapping canonical identities to lists of aliases

    Returns:
        Dictionary mapping normalized aliases to their canonical identity

    Examples:
        >>> aliases = {"john.doe@example.com": ["jdoe", "John Doe"]}
        >>> reverse = build_reverse_alias_map(aliases)
        >>> reverse["jdoe"]
        'john.doe@example.com'
        >>> reverse["john doe"]
        'john.doe@example.com'
    """
    reverse: Dict[str, str] = {}
    for canonical, alias_list in aliases.items():
        for alias in alias_list:
            key = normalize_alias(alias)
            if key:
                reverse[key] = canonical
    return reverse
