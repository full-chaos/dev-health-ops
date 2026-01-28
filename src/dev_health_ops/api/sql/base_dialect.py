from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Optional


class SqlDialect(ABC):
    """Abstract base class for SQL dialect abstractions."""

    @abstractmethod
    def date_trunc(self, unit: str, column: str) -> str:
        """Truncate a date to a specific unit (day, week, month)."""
        pass

    @abstractmethod
    def to_string(self, column: str) -> str:
        """Convert a column to a string."""
        pass

    @abstractmethod
    def if_null(self, column: str, default: Any) -> str:
        """Return a default value if the column is NULL."""
        pass

    @abstractmethod
    def null_if(self, column: str, value: Any) -> str:
        """Return NULL if the column equals the value."""
        pass

    @abstractmethod
    def count_if(self, condition: str) -> str:
        """Count rows matching a condition."""
        pass

    @abstractmethod
    def sum_if(self, column: str, condition: str) -> str:
        """Sum column values matching a condition."""
        pass

    @abstractmethod
    def var_pop_if(self, column: str, condition: str) -> str:
        """Calculate population variance matching a condition."""
        pass

    @abstractmethod
    def count_distinct(self, column: str) -> str:
        """Count distinct values in a column."""
        pass

    @abstractmethod
    def arg_max(self, column: str, version_column: str) -> str:
        """Return the value of a column for the row with the maximum version."""
        pass

    @abstractmethod
    def quantile(self, probability: float, column: str) -> str:
        """Return the quantile of a column."""
        pass

    @abstractmethod
    def array_join(
        self, column: str, alias: str, type_str: Optional[str] = None
    ) -> str:
        """Unnest an array column into rows."""
        pass

    @abstractmethod
    def json_extract(self, column: str, path: str, type_str: str) -> str:
        """Extract a value from a JSON column."""
        pass

    @abstractmethod
    def split_by_char(self, char: str, column: str, index: int) -> str:
        """Split a string by a character and return the element at an index (1-based)."""
        pass

    @abstractmethod
    def map_key_access(self, column: str, key: str) -> str:
        """Access a value in a map/JSON by key."""
        pass

    @abstractmethod
    def tuple_element(self, column: str, index: int) -> str:
        """Access an element in a tuple (1-based)."""
        pass

    @abstractmethod
    def array_element(self, column: str, index: int) -> str:
        """Access an element in an array (1-based)."""
        pass

    @abstractmethod
    def json_has_any_key(self, column: str, keys_param: str) -> str:
        """Check if a JSON/Map column has any of the keys in a parameter."""
        pass

    @abstractmethod
    def json_has_any_theme(self, column: str, themes_param: str) -> str:
        """Check if a JSON/Map column has any subcategory keys belonging to themes."""
        pass

    @abstractmethod
    def day_of_week(self, column: str) -> str:
        """Return the day of the week (1-7) for a date."""
        pass

    @abstractmethod
    def hour(self, column: str) -> str:
        """Return the hour (0-23) for a date/time."""
        pass

    @abstractmethod
    def date_diff(self, unit: str, start_column: str, end_column: str) -> str:
        """Return the difference between two dates in a specific unit."""
        pass

    @abstractmethod
    def to_date(self, column: str) -> str:
        """Convert a date/time to a date (midnight)."""
        pass

    @abstractmethod
    def concat(self, *parts: str) -> str:
        """Concatenate strings."""
        pass

    @abstractmethod
    def to_datetime(self, value: str) -> str:
        """Convert a value to a date/time (timestamp)."""
        pass

    @abstractmethod
    def query_settings(self, timeout_seconds: int) -> str:
        """Return query settings (e.g. timeout)."""
        pass

    @abstractmethod
    def to_timestamp_tz(self, column: str) -> str:
        """Cast a column (possibly TEXT) to a timestamp with timezone for comparison."""
        pass
