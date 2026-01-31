"""Runtime validation for TypedDict loader outputs.

This module provides opt-in validation at the loader→compute boundary.
Enable by setting the VALIDATE_LOADER_OUTPUT=1 environment variable or
by passing validate=True to the validation functions.

Usage:
    from dev_health_ops.metrics.loaders.validation import validate_rows

    # Validate a list of TypedDicts
    commits = loader.load_git_rows(...)
    errors = validate_rows(commits, CommitStatRow)
    if errors:
        raise ValueError(f"Validation failed: {errors}")

Performance note:
    Validation adds ~5% overhead for typical payloads. Disable in production
    by leaving VALIDATE_LOADER_OUTPUT unset (default).
"""

from __future__ import annotations

import os
import uuid
from datetime import date, datetime
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Sequence,
    Type,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)

try:
    from typing_extensions import NotRequired, is_typeddict
except ImportError:
    from typing import _TypedDictMeta  # type: ignore[attr-defined]

    def is_typeddict(tp: Any) -> bool:
        return isinstance(tp, _TypedDictMeta)

    NotRequired = None  # type: ignore[misc,assignment]


_VALIDATE_ENABLED = os.environ.get("VALIDATE_LOADER_OUTPUT", "").lower() in (
    "1",
    "true",
    "yes",
)


class ValidationError:
    """Represents a single validation error with context."""

    __slots__ = ("field", "message", "row_index")

    def __init__(self, field: str, message: str, row_index: Optional[int] = None):
        self.field = field
        self.message = message
        self.row_index = row_index

    def __str__(self) -> str:
        if self.row_index is not None:
            return f"Row {self.row_index}, field '{self.field}': {self.message}"
        return f"Field '{self.field}': {self.message}"

    def __repr__(self) -> str:
        return f"ValidationError({self.field!r}, {self.message!r}, row_index={self.row_index})"


def _is_optional(type_hint: Any) -> bool:
    """Check if a type hint is Optional[T] (i.e., Union[T, None])."""
    origin = get_origin(type_hint)
    if origin is Union:
        args = get_args(type_hint)
        return type(None) in args
    return False


def _is_not_required(type_hint: Any) -> bool:
    """Check if a type hint is NotRequired[T]."""
    if NotRequired is None:
        return False
    origin = get_origin(type_hint)
    return origin is NotRequired


def _unwrap_optional(type_hint: Any) -> Any:
    """Unwrap Optional[T] to get T."""
    origin = get_origin(type_hint)
    if origin is Union:
        args = get_args(type_hint)
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            return non_none[0]
    return type_hint


def _unwrap_not_required(type_hint: Any) -> Any:
    """Unwrap NotRequired[T] to get T."""
    if _is_not_required(type_hint):
        args = get_args(type_hint)
        if args:
            return args[0]
    return type_hint


def _check_type(value: Any, expected: Any) -> bool:
    """Check if a value matches the expected type.

    Supports: primitives, Optional, List, Dict, UUID, datetime, date.
    """
    if value is None:
        return True

    if _is_optional(expected):
        expected = _unwrap_optional(expected)
        if value is None:
            return True

    origin = get_origin(expected)

    if origin is list:
        if not isinstance(value, list):
            return False
        args = get_args(expected)
        if args:
            elem_type = args[0]
            return all(_check_type(elem, elem_type) for elem in value)
        return True

    if origin is dict:
        if not isinstance(value, dict):
            return False
        args = get_args(expected)
        if len(args) == 2:
            key_type, val_type = args
            return all(
                _check_type(k, key_type) and _check_type(v, val_type)
                for k, v in value.items()
            )
        return True

    if expected is uuid.UUID:
        return isinstance(value, uuid.UUID)
    if expected is datetime:
        return isinstance(value, datetime)
    if expected is date and not isinstance(value, datetime):
        # date check excludes datetime (datetime is subclass of date)
        return isinstance(value, date)
    if expected is int:
        # bool is subclass of int - reject bools for int fields
        return isinstance(value, int) and not isinstance(value, bool)
    if expected is float:
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if expected is str:
        return isinstance(value, str)
    if expected is bool:
        return isinstance(value, bool)

    try:
        return isinstance(value, expected)
    except TypeError:
        return True


def validate_typed_dict(
    data: Dict[str, Any],
    td_class: Type,
    row_index: Optional[int] = None,
) -> List[ValidationError]:
    """Validate a dict against a TypedDict schema.

    Args:
        data: The dictionary to validate.
        td_class: The TypedDict class to validate against.
        row_index: Optional index for error context when validating lists.

    Returns:
        List of ValidationError objects (empty if valid).
    """
    if not is_typeddict(td_class):
        return [
            ValidationError(
                "__class__",
                f"{td_class} is not a TypedDict",
                row_index,
            )
        ]

    if not isinstance(data, dict):
        return [
            ValidationError(
                "__type__",
                f"Expected dict, got {type(data).__name__}",
                row_index,
            )
        ]

    errors: List[ValidationError] = []

    try:
        hints = get_type_hints(td_class, include_extras=True)
    except Exception as e:
        return [
            ValidationError("__hints__", f"Failed to get type hints: {e}", row_index)
        ]

    optional_keys = getattr(td_class, "__optional_keys__", set())

    for key, type_hint in hints.items():
        if key in data:
            continue
        if key in optional_keys:
            continue
        if _is_not_required(type_hint):
            continue
        errors.append(ValidationError(key, "Missing required field", row_index))

    for key, value in data.items():
        if key not in hints:
            continue

        type_hint = hints[key]
        inner_hint = _unwrap_not_required(type_hint)
        is_optional = _is_optional(inner_hint) or key in optional_keys

        if value is None:
            if not is_optional:
                errors.append(
                    ValidationError(
                        key, "Got None but field is not Optional", row_index
                    )
                )
            continue

        if not _check_type(value, inner_hint):
            expected_name = getattr(inner_hint, "__name__", str(inner_hint))
            actual_name = type(value).__name__
            errors.append(
                ValidationError(
                    key,
                    f"Expected {expected_name}, got {actual_name}",
                    row_index,
                )
            )

    return errors


def validate_rows(
    rows: Sequence[Dict[str, Any]],
    td_class: Type,
    *,
    max_errors: int = 10,
    validate: Optional[bool] = None,
) -> List[ValidationError]:
    """Validate a sequence of dicts against a TypedDict schema.

    Args:
        rows: Sequence of dictionaries to validate.
        td_class: The TypedDict class to validate against.
        max_errors: Maximum number of errors to collect before stopping.
        validate: Override environment-based validation. If None, uses
            VALIDATE_LOADER_OUTPUT environment variable.

    Returns:
        List of ValidationError objects (empty if all valid or validation disabled).
    """
    should_validate = validate if validate is not None else _VALIDATE_ENABLED
    if not should_validate:
        return []

    errors: List[ValidationError] = []
    for idx, row in enumerate(rows):
        row_errors = validate_typed_dict(row, td_class, row_index=idx)
        errors.extend(row_errors)
        if len(errors) >= max_errors:
            break

    return errors


def validate_or_raise(
    rows: Sequence[Dict[str, Any]],
    td_class: Type,
    context: str = "",
    *,
    validate: Optional[bool] = None,
) -> None:
    """Validate rows and raise ValueError if validation fails.

    Args:
        rows: Sequence of dictionaries to validate.
        td_class: The TypedDict class to validate against.
        context: Context string for error message (e.g., "load_git_rows").
        validate: Override environment-based validation.

    Raises:
        ValueError: If validation fails.
    """
    errors = validate_rows(rows, td_class, validate=validate)
    if errors:
        error_strs = [str(e) for e in errors[:5]]
        remaining = len(errors) - 5
        msg = f"Schema validation failed for {td_class.__name__}"
        if context:
            msg = f"{msg} in {context}"
        msg = f"{msg}:\n  " + "\n  ".join(error_strs)
        if remaining > 0:
            msg += f"\n  ... and {remaining} more errors"
        raise ValueError(msg)
