from __future__ import annotations

import hashlib
from dataclasses import Field, fields
from datetime import datetime
from typing import ClassVar, Protocol

from dev_health_ops.models.operational_ordering_codec import (
    ConflictValue,
    conflict_key_bytes,
    encode_conflict_fields,
    utc_microseconds,
)
from dev_health_ops.models.operational_ordering_types import (
    CONFLICT_EXCLUDED_FIELDS,
    OPERATIONAL_ORDERING_CONTRACT,
    REVISION_DOMAIN,
    UINT64_MAX,
    UINT128_MAX,
    IngestRevision,
    OperationalOrderingEncodingError,
    OperationalOrderingOverflowError,
    OperationalOrderingValues,
    OperationRank,
    SourceConflictKey,
    SourceRevision,
)


class OperationalOrderingEntity(Protocol):
    __dataclass_fields__: ClassVar[dict[str, Field[ConflictValue]]]

    @property
    def entity_family(self) -> str: ...

    @property
    def source_version_at(self) -> datetime: ...

    @property
    def observed_at(self) -> datetime: ...

    @property
    def last_synced(self) -> datetime: ...

    @property
    def source_revision(self) -> SourceRevision: ...

    @property
    def source_conflict_key(self) -> SourceConflictKey: ...

    @property
    def ingest_revision(self) -> IngestRevision: ...

    @property
    def ordering_contract(self) -> int: ...


__all__ = [
    "OPERATIONAL_ORDERING_CONTRACT",
    "IngestRevision",
    "OperationRank",
    "OperationalOrderingEncodingError",
    "OperationalOrderingOverflowError",
    "OperationalOrderingValues",
    "SourceConflictKey",
    "SourceRevision",
    "build_entity_ordering",
    "build_ingest_revision",
    "build_ingest_revision_from_microseconds",
    "build_source_revision",
    "build_source_revision_from_microseconds",
    "encode_conflict_fields",
    "entity_conflict_key",
    "operation_rank_from_revision",
    "validate_operational_entity_ordering",
]


def _operation_rank(value: OperationRank | int) -> OperationRank:
    if isinstance(value, bool):
        raise OperationalOrderingEncodingError("operation_rank", "0, 1, or 2 required")
    try:
        return OperationRank(value)
    except ValueError as error:
        raise OperationalOrderingEncodingError(
            "operation_rank", "0, 1, or 2 required"
        ) from error


def build_source_revision_from_microseconds(
    timestamp_us: int,
    operation_rank: OperationRank | int,
    source_conflict_key: SourceConflictKey | str,
) -> SourceRevision:
    if timestamp_us < 0:
        raise OperationalOrderingEncodingError(
            "timestamp_us", "non-negative value required"
        )
    if timestamp_us > UINT64_MAX:
        raise OperationalOrderingOverflowError("timestamp_us", timestamp_us)
    rank = _operation_rank(operation_rank)
    tie56 = int.from_bytes(
        hashlib.sha256(
            REVISION_DOMAIN + conflict_key_bytes(source_conflict_key)
        ).digest()[:7],
        byteorder="big",
    )
    revision = (timestamp_us << 64) | (int(rank) << 56) | tie56
    if revision > UINT128_MAX:
        raise OperationalOrderingOverflowError("source_revision", revision)
    return SourceRevision(revision)


def build_source_revision(
    timestamp: datetime,
    operation_rank: OperationRank | int,
    source_conflict_key: SourceConflictKey | str,
) -> SourceRevision:
    return build_source_revision_from_microseconds(
        utc_microseconds(timestamp, "source_version_at"),
        operation_rank,
        source_conflict_key,
    )


def build_ingest_revision_from_microseconds(
    last_synced_us: int, observed_at_us: int
) -> IngestRevision:
    for field_name, value in (
        ("last_synced_us", last_synced_us),
        ("observed_at_us", observed_at_us),
    ):
        if value < 0:
            raise OperationalOrderingEncodingError(
                field_name, "non-negative value required"
            )
        if value > UINT64_MAX:
            raise OperationalOrderingOverflowError(field_name, value)
    revision = (last_synced_us << 64) | observed_at_us
    if revision > UINT128_MAX:
        raise OperationalOrderingOverflowError("ingest_revision", revision)
    return IngestRevision(revision)


def build_ingest_revision(
    last_synced: datetime, observed_at: datetime
) -> IngestRevision:
    return build_ingest_revision_from_microseconds(
        utc_microseconds(last_synced, "last_synced"),
        utc_microseconds(observed_at, "observed_at"),
    )


def _entity_is_tombstone(entity: OperationalOrderingEntity) -> bool:
    return (
        bool(getattr(entity, "is_deleted", False))
        or getattr(entity, "is_active", True) is False
    )


def entity_conflict_key(entity: OperationalOrderingEntity) -> SourceConflictKey:
    values = tuple(
        (item.name, getattr(entity, item.name))
        for item in fields(entity)
        if item.name not in CONFLICT_EXCLUDED_FIELDS
    )
    return encode_conflict_fields(entity.entity_family, values)


def build_entity_ordering(
    entity: OperationalOrderingEntity, operation_rank: OperationRank | None = None
) -> OperationalOrderingValues:
    tombstone = _entity_is_tombstone(entity)
    rank = (
        OperationRank.TOMBSTONE
        if tombstone
        else operation_rank
        if operation_rank is not None
        else OperationRank.ACTIVE_UPDATE
    )
    if tombstone != (rank is OperationRank.TOMBSTONE):
        raise OperationalOrderingEncodingError(
            "operation_rank", "tombstone state and rank must agree"
        )
    conflict_key = entity_conflict_key(entity)
    return OperationalOrderingValues(
        source_revision=build_source_revision(
            entity.source_version_at, rank, conflict_key
        ),
        source_conflict_key=conflict_key,
        ingest_revision=build_ingest_revision(entity.last_synced, entity.observed_at),
    )


def operation_rank_from_revision(
    source_revision: SourceRevision | int,
) -> OperationRank:
    return _operation_rank((int(source_revision) >> 56) & 0xFF)


def validate_operational_entity_ordering(entity: OperationalOrderingEntity) -> None:
    if entity.ordering_contract != OPERATIONAL_ORDERING_CONTRACT:
        raise OperationalOrderingEncodingError(
            "ordering_contract", "contract 2 required"
        )
    rank = operation_rank_from_revision(entity.source_revision)
    expected = build_entity_ordering(entity, rank)
    actual = (
        entity.source_revision,
        entity.source_conflict_key,
        entity.ingest_revision,
        entity.ordering_contract,
    )
    if actual != (
        expected.source_revision,
        expected.source_conflict_key,
        expected.ingest_revision,
        expected.ordering_contract,
    ):
        raise OperationalOrderingEncodingError(
            "ordering_fields", "derived values do not match row"
        )
