from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from enum import IntEnum
from typing import Final

from dev_health_ops.models.operational import OPERATIONAL_ENTITY_TABLES
from dev_health_ops.models.operational_ordering_types import ORDERING_FIELD_NAMES

logger = logging.getLogger(__name__)

OPERATIONAL_ORDERING_CONTRACT_ENV: Final = "OPERATIONAL_ORDERING_CONTRACT"


class OperationalOrderingContract(IntEnum):
    LEGACY = 1
    CURRENT = 2


@dataclass(slots=True)
class OperationalOrderingConfigurationError(ValueError):
    value: str | None

    def __str__(self) -> str:
        return f"{OPERATIONAL_ORDERING_CONTRACT_ENV} must be exactly '1' or '2', got {self.value!r}"


@dataclass(frozen=True, slots=True)
class OperationalWriterIdentity:
    table: str
    service: str
    version: str


@dataclass(slots=True)
class OperationalOldWriterRejectedError(RuntimeError):
    identity: OperationalWriterIdentity

    def __str__(self) -> str:
        return (
            f"legacy operational writer rejected for {self.identity.table} "
            f"service={self.identity.service} version={self.identity.version}"
        )


@dataclass(slots=True)
class OperationalOrderingStaleStateError(RuntimeError):
    table: str
    configured_contract: int
    table_contract: int | None

    def __str__(self) -> str:
        return (
            f"operational ordering stale_state table={self.table} "
            f"configured={self.configured_contract} stored={self.table_contract}"
        )


def parse_operational_ordering_contract(
    raw: str | None,
) -> OperationalOrderingContract:
    if raw is None:
        return OperationalOrderingContract.LEGACY
    if raw not in {"1", "2"}:
        raise OperationalOrderingConfigurationError(raw)
    return OperationalOrderingContract(int(raw))


def configured_operational_ordering_contract() -> OperationalOrderingContract:
    return parse_operational_ordering_contract(
        os.environ.get(OPERATIONAL_ORDERING_CONTRACT_ENV)
    )


def operational_ordering_contract_is_explicit() -> bool:
    return OPERATIONAL_ORDERING_CONTRACT_ENV in os.environ


def operational_table_contract(ddl: str, table: str) -> OperationalOrderingContract:
    normalized = re.sub(r"\s+", " ", ddl.replace("`", " ")).strip()
    normalized = re.sub(r"\s*,\s*", ", ", normalized)
    present = {name for name in ORDERING_FIELD_NAMES if name in normalized}
    legacy = (
        not present
        and "ReplacingMergeTree(source_version_at)" in normalized
        and "ORDER BY (org_id, id)" in normalized
    )
    current_markers = (
        "source_revision UInt128",
        "source_conflict_key String",
        "ingest_revision UInt128",
        "ordering_contract UInt8",
        "CONSTRAINT ordering_contract_v2 CHECK ordering_contract = 2",
        "ReplacingMergeTree(ingest_revision)",
        "PRIMARY KEY (org_id, id)",
        "ORDER BY (org_id, id, source_revision, source_conflict_key)",
    )
    current = present == ORDERING_FIELD_NAMES and all(
        marker in normalized for marker in current_markers
    )
    if legacy:
        return OperationalOrderingContract.LEGACY
    if current:
        return OperationalOrderingContract.CURRENT
    raise OperationalOrderingStaleStateError(table, 0, None)


def ensure_operational_writer_admission(
    identity: OperationalWriterIdentity,
    configured_contract: OperationalOrderingContract,
    table_contract: OperationalOrderingContract,
) -> None:
    if configured_contract is table_contract:
        return
    if (
        configured_contract is OperationalOrderingContract.LEGACY
        and table_contract is OperationalOrderingContract.CURRENT
    ):
        logger.error(
            "operational_old_writer_rejected",
            extra={
                "table": identity.table,
                "service": identity.service,
                "version": identity.version,
            },
        )
        raise OperationalOldWriterRejectedError(identity)
    raise OperationalOrderingStaleStateError(
        identity.table, int(configured_contract), int(table_contract)
    )


def guard_operational_writer_tables(
    client,
    configured_contract: OperationalOrderingContract,
    service: str,
    version: str,
) -> None:
    for table in OPERATIONAL_ENTITY_TABLES.values():
        result = client.query(f"SHOW CREATE TABLE `{table}`")
        if not result.result_rows:
            raise OperationalOrderingStaleStateError(
                table, int(configured_contract), None
            )
        table_contract = operational_table_contract(
            str(result.result_rows[0][0]), table
        )
        ensure_operational_writer_admission(
            OperationalWriterIdentity(table, service, version),
            configured_contract,
            table_contract,
        )
