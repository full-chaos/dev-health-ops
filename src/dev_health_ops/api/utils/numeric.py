"""Safe numeric transform utilities for API services.

This module provides common numeric transformation helpers used across
API service modules (home, people, explain, etc.) to ensure consistent
handling of potentially invalid or non-finite values.
"""

from __future__ import annotations

import math
from typing import Any, Callable, Optional

__all__ = [
    "safe_float",
    "safe_optional_float",
    "safe_transform",
    "delta_pct",
]


def safe_float(value: Any, default: float = 0.0) -> float:
    """Convert value to float, returning default for invalid/non-finite values.

    Args:
        value: Any value to convert to float.
        default: Value to return if conversion fails or result is non-finite.

    Returns:
        The float value, or default if conversion fails or result is inf/nan.
    """
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def safe_optional_float(value: Any) -> Optional[float]:
    """Convert value to float, returning None for invalid/non-finite values.

    Args:
        value: Any value to convert to float.

    Returns:
        The float value, or None if conversion fails or result is inf/nan.
    """
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def safe_transform(transform: Callable[[float], float], value: float) -> float:
    """Apply a transform function and ensure result is a valid float.

    Args:
        transform: A callable that takes a float and returns a float.
        value: The input value to transform.

    Returns:
        The transformed value, or 0.0 if the result is invalid/non-finite.
    """
    return safe_float(transform(value))


def delta_pct(current: float, previous: float) -> float:
    """Calculate percentage change from previous to current value.

    Args:
        current: The current (newer) value.
        previous: The previous (older) value.

    Returns:
        The percentage change, or 0.0 if previous is zero.
    """
    if previous == 0:
        return 0.0
    return (current - previous) / previous * 100.0
