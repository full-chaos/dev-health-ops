from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import IntEnum
from typing import Final, NewType

SourceRevision = NewType("SourceRevision", int)
SourceConflictKey = NewType("SourceConflictKey", str)
IngestRevision = NewType("IngestRevision", int)

OPERATIONAL_ORDERING_CONTRACT: Final = 2
ORDERING_FIELD_NAMES: Final[frozenset[str]] = frozenset(
    {
        "source_revision",
        "source_conflict_key",
        "ingest_revision",
        "ordering_contract",
    }
)
CONFLICT_EXCLUDED_FIELDS: Final[frozenset[str]] = frozenset(
    {"id", *ORDERING_FIELD_NAMES, "observed_at", "last_synced"}
)
CONFLICT_DOMAIN: Final = b"operational-conflict-v1"
REVISION_DOMAIN: Final = b"operational-source-revision-v1"
UINT64_MAX: Final = (1 << 64) - 1
UINT128_MAX: Final = (1 << 128) - 1
UNIX_EPOCH: Final = datetime(1970, 1, 1, tzinfo=timezone.utc)
CLICKHOUSE_DATETIME64_MAX: Final = datetime(
    2299, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc
)


class OperationRank(IntEnum):
    CREATE = 0
    ACTIVE_UPDATE = 1
    TOMBSTONE = 2


@dataclass(slots=True)
class OperationalOrderingEncodingError(ValueError):
    field_name: str
    reason: str

    def __str__(self) -> str:
        return f"invalid operational ordering field {self.field_name}: {self.reason}"


@dataclass(slots=True)
class OperationalOrderingOverflowError(OverflowError):
    field_name: str
    value: int

    def __str__(self) -> str:
        return f"operational ordering field {self.field_name} exceeds UInt128 input bounds: {self.value}"


@dataclass(frozen=True, slots=True)
class OperationalOrderingValues:
    source_revision: SourceRevision
    source_conflict_key: SourceConflictKey
    ingest_revision: IngestRevision
    ordering_contract: int = OPERATIONAL_ORDERING_CONTRACT
