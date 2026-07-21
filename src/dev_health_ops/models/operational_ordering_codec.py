from __future__ import annotations

import struct
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta, timezone
from typing import TypeAlias
from uuid import UUID

from dev_health_ops.models.operational_ordering_types import (
    CLICKHOUSE_DATETIME64_MAX,
    CONFLICT_DOMAIN,
    UINT64_MAX,
    UNIX_EPOCH,
    OperationalOrderingEncodingError,
    OperationalOrderingOverflowError,
    SourceConflictKey,
)

ConflictValue: TypeAlias = (
    str
    | int
    | float
    | bool
    | None
    | datetime
    | UUID
    | Mapping[str, "ConflictValue"]
    | list["ConflictValue"]
    | tuple["ConflictValue", ...]
)


def _length_prefix(value: bytes, width: int) -> bytes:
    return len(value).to_bytes(width, byteorder="big", signed=False) + value


def canonical_datetime(value: datetime, field_name: str) -> bytes:
    if value.tzinfo is None or value.utcoffset() != timedelta(0) or value.fold:
        raise OperationalOrderingEncodingError(
            field_name, "UTC datetime with fold=0 required"
        )
    utc = value.astimezone(timezone.utc)
    if utc < UNIX_EPOCH or utc > CLICKHOUSE_DATETIME64_MAX:
        raise OperationalOrderingEncodingError(
            field_name, "ClickHouse DateTime64(6) range required"
        )
    encoded = (
        f"{utc.year:04d}-{utc.month:02d}-{utc.day:02d}T"
        f"{utc.hour:02d}:{utc.minute:02d}:{utc.second:02d}.{utc.microsecond:06d}Z"
    )
    return encoded.encode("ascii")


def _encode_value(value: ConflictValue, field_name: str) -> tuple[bytes, bytes, bytes]:
    match value:
        case None:
            return b"null", b"\x00", b""
        case bool() as boolean:
            return b"bool", b"\x01", b"\x01" if boolean else b"\x00"
        case str() as text:
            try:
                return b"string", b"\x01", text.encode("utf-8", errors="strict")
            except UnicodeEncodeError as error:
                raise OperationalOrderingEncodingError(
                    field_name, "invalid UTF-8 text"
                ) from error
        case datetime() as timestamp:
            return b"datetime", b"\x01", canonical_datetime(timestamp, field_name)
        case UUID() as identifier:
            return b"uuid", b"\x01", str(identifier).lower().encode("ascii")
        case int() as integer:
            return b"integer", b"\x01", str(integer).encode("ascii")
        case float() as number:
            return b"float64", b"\x01", struct.pack(">d", number)
        case Mapping() as mapping:
            encoded: list[bytes] = []
            for key in sorted(mapping):
                if not isinstance(key, str):
                    raise OperationalOrderingEncodingError(
                        field_name, "map keys must be strings"
                    )
                encoded.append(_encode_field(key, mapping[key]))
            return b"map", b"\x01", len(encoded).to_bytes(8, "big") + b"".join(encoded)
        case list() as sequence:
            encoded = [
                _encode_field(str(index), item) for index, item in enumerate(sequence)
            ]
            return (
                b"list",
                b"\x01",
                len(encoded).to_bytes(8, "big") + b"".join(encoded),
            )
        case tuple() as sequence:
            encoded = [
                _encode_field(str(index), item) for index, item in enumerate(sequence)
            ]
            return (
                b"tuple",
                b"\x01",
                len(encoded).to_bytes(8, "big") + b"".join(encoded),
            )
    raise OperationalOrderingEncodingError(
        field_name, f"unsupported value type {type(value).__name__}"
    )


def _encode_field(name: str, value: ConflictValue) -> bytes:
    if not name:
        raise OperationalOrderingEncodingError("field_name", "non-empty name required")
    try:
        name_bytes = name.encode("utf-8", errors="strict")
    except UnicodeEncodeError as error:
        raise OperationalOrderingEncodingError(
            "field_name", "invalid UTF-8 text"
        ) from error
    value_type, null_marker, encoded = _encode_value(value, name)
    return (
        _length_prefix(name_bytes, 4)
        + _length_prefix(value_type, 2)
        + null_marker
        + _length_prefix(encoded, 8)
    )


def encode_conflict_fields(
    entity_family: str, values: Sequence[tuple[str, ConflictValue]]
) -> SourceConflictKey:
    names = [name for name, _value in values]
    if len(names) != len(set(names)):
        raise OperationalOrderingEncodingError(
            "fields", "duplicate names are not allowed"
        )
    encoded = [_encode_field("entity_family", entity_family)]
    encoded.extend(_encode_field(name, value) for name, value in values)
    return SourceConflictKey((CONFLICT_DOMAIN + b"".join(encoded)).hex())


def conflict_key_bytes(value: SourceConflictKey | str) -> bytes:
    if value != value.lower():
        raise OperationalOrderingEncodingError(
            "source_conflict_key", "lowercase hex required"
        )
    try:
        decoded = bytes.fromhex(value)
    except ValueError as error:
        raise OperationalOrderingEncodingError(
            "source_conflict_key", "valid hex required"
        ) from error
    if not decoded.startswith(CONFLICT_DOMAIN):
        raise OperationalOrderingEncodingError(
            "source_conflict_key", "operational-conflict-v1 TLV required"
        )
    return decoded


def utc_microseconds(value: datetime, field_name: str) -> int:
    canonical_datetime(value, field_name)
    delta = value.astimezone(timezone.utc) - UNIX_EPOCH
    microseconds = (
        delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds
    )
    if microseconds < 0:
        raise OperationalOrderingEncodingError(
            field_name, "non-negative Unix time required"
        )
    if microseconds > UINT64_MAX:
        raise OperationalOrderingOverflowError(field_name, microseconds)
    return microseconds
