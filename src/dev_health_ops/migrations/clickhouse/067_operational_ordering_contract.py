from __future__ import annotations

import logging
import re
from dataclasses import asdict, fields
from datetime import datetime, timezone
from typing import NamedTuple

from dev_health_ops.migrations.clickhouse import MigrationDeferred
from dev_health_ops.models.operational import (
    OPERATIONAL_ENTITY_TABLES,
    CanonicalOperationalEntity,
    operational_columns,
)
from dev_health_ops.models.operational_ordering_types import ORDERING_FIELD_NAMES
from dev_health_ops.storage.operational_ordering_guard import (
    OperationalOrderingContract,
    configured_operational_ordering_contract,
)

log = logging.getLogger(__name__)

TABLES = tuple(OPERATIONAL_ENTITY_TABLES.values())
_ENTITY_BY_TABLE = {
    table: entity for entity, table in OPERATIONAL_ENTITY_TABLES.items()
}
_TABLE_NAME = re.compile(
    r"(CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:`?[\w]+`?\.)?`?)"
    r"(?P<table>[\w]+)(`?\s*\()",
    re.IGNORECASE,
)
_SOURCE_VERSION = re.compile(
    r"(`?source_version_at`?\s+DateTime64\(6,\s*'UTC'\)\s*,)", re.IGNORECASE
)
_ENGINE = re.compile(
    r"ENGINE\s*=\s*ReplacingMergeTree\s*\(\s*source_version_at\s*\)",
    re.IGNORECASE,
)
_ORDER_BY = re.compile(r"ORDER\s+BY\s*\(\s*org_id\s*,\s*id\s*\)", re.IGNORECASE)


class OperationalOrderingMigrationStateError(RuntimeError):
    def __init__(self, table: str, state: str) -> None:
        self.table = table
        self.state = state
        super().__init__(table, state)

    def __str__(self) -> str:
        return f"{self.table}: operational ordering migration stale_state={self.state}"


class _CopyStats(NamedTuple):
    raw_rows: int
    candidate_count: int
    logical_count: int
    maximum_tuple: tuple[int, str, int] | None


def _normalize_ddl(ddl: str) -> str:
    normalized = re.sub(r"\s+", " ", ddl.replace("`", " ")).strip()
    return re.sub(r"\s*,\s*", ", ", normalized)


def _is_v2_ddl(ddl: str) -> bool:
    normalized = _normalize_ddl(ddl)
    required = (
        "source_revision UInt128",
        "source_conflict_key String",
        "ingest_revision UInt128",
        "ordering_contract UInt8",
        "CONSTRAINT ordering_contract_v2 CHECK ordering_contract = 2",
        "ENGINE = ReplacingMergeTree(ingest_revision)",
        "PRIMARY KEY (org_id, id)",
        "ORDER BY (org_id, id, source_revision, source_conflict_key)",
    )
    return all(marker in normalized for marker in required)


def _schema_contract(ddl: str, table: str) -> int:
    present = {name for name in ORDERING_FIELD_NAMES if name in ddl}
    if not present and _ENGINE.search(ddl) and _ORDER_BY.search(ddl):
        return 1
    if present == ORDERING_FIELD_NAMES and _is_v2_ddl(ddl):
        return 2
    raise OperationalOrderingMigrationStateError(table, "mixed_or_malformed_schema")


def _rewrite_ddl(ddl: str, table: str, shadow: str) -> str:
    if _schema_contract(ddl, table) != 1:
        raise OperationalOrderingMigrationStateError(table, "expected_legacy_schema")
    rewritten, table_count = _TABLE_NAME.subn(
        lambda match: f"{match.group(1)}{shadow}{match.group(3)}",
        ddl,
        count=1,
    )
    ordering_columns = """
    source_revision UInt128,
    source_conflict_key String,
    ingest_revision UInt128,
    ordering_contract UInt8,
    CONSTRAINT ordering_contract_v2 CHECK ordering_contract = 2,"""
    rewritten, column_count = _SOURCE_VERSION.subn(
        lambda match: f"{match.group(1)}{ordering_columns}", rewritten, count=1
    )
    rewritten, engine_count = _ENGINE.subn(
        "ENGINE = ReplacingMergeTree(ingest_revision)", rewritten, count=1
    )
    rewritten, order_count = _ORDER_BY.subn(
        "PRIMARY KEY (org_id, id)\n"
        "ORDER BY (org_id, id, source_revision, source_conflict_key)",
        rewritten,
        count=1,
    )
    if (table_count, column_count, engine_count, order_count) != (1, 1, 1, 1):
        raise OperationalOrderingMigrationStateError(table, "ddl_rewrite_miss")
    if not _is_v2_ddl(rewritten):
        raise OperationalOrderingMigrationStateError(table, "rewritten_schema_invalid")
    return rewritten


def _table_exists(client, table: str) -> bool:
    result = client.query(
        "SELECT count() FROM system.tables "
        "WHERE database = currentDatabase() AND name = {name:String}",
        parameters={"name": table},
    )
    rows = getattr(result, "result_rows", None)
    if not isinstance(rows, (list, tuple)) or not rows:
        return False
    value = rows[0][0]
    return isinstance(value, (bool, int)) and bool(value)


def _show_create(client, table: str) -> str:
    result = client.query(f"SHOW CREATE TABLE `{table}`")
    if not result.result_rows:
        raise OperationalOrderingMigrationStateError(table, "missing_ddl")
    return str(result.result_rows[0][0])


def _count(client, expression: str, table: str) -> int:
    result = client.query(f"SELECT {expression} FROM `{table}`")
    return int(result.result_rows[0][0])


def _candidate_count(client, table: str) -> int:
    return _count(
        client,
        "uniqExact((org_id, id, source_revision, source_conflict_key))",
        table,
    )


def _logical_count(client, table: str) -> int:
    return _count(client, "uniqExact((org_id, id))", table)


def _maximum_tuple(client, table: str) -> tuple[int, str, int] | None:
    if _candidate_count(client, table) == 0:
        return None
    result = client.query(
        f"SELECT max((source_revision, source_conflict_key, ingest_revision)) FROM `{table}`"
    )
    value = result.result_rows[0][0]
    return int(value[0]), str(value[1]), int(value[2])


def _legacy_columns(entity_type) -> tuple[str, ...]:
    return tuple(
        item.name
        for item in fields(entity_type)
        if item.name not in ORDERING_FIELD_NAMES
    )


def _canonical_value(name: str, value):
    if isinstance(value, datetime):
        return (
            value.replace(tzinfo=timezone.utc)
            if value.tzinfo is None
            else value.astimezone(timezone.utc)
        )
    if name in {"is_deleted", "is_active"}:
        return bool(value)
    return value


def _entity_from_row(
    entity_type, columns: tuple[str, ...], row
) -> CanonicalOperationalEntity:
    values = {
        name: _canonical_value(name, value)
        for name, value in zip(columns, row, strict=True)
        if name != "id"
    }
    return entity_type(**values)


def _copy_raw_rows(
    client,
    source: str,
    target: str,
    entity_type,
) -> _CopyStats:
    source_columns = _legacy_columns(entity_type)
    target_columns = operational_columns(entity_type)
    candidate_count = 0
    logical_count = 0
    previous_candidate: tuple[str, str, int, str] | None = None
    previous_logical: tuple[str, str] | None = None
    maximum: tuple[int, str, int] | None = None
    raw_rows = 0
    candidate_columns = tuple(
        name
        for name in source_columns
        if name not in {"id", "observed_at", "last_synced"}
    )
    sort_columns = tuple(
        dict.fromkeys(
            ("org_id", "id", *candidate_columns, "observed_at", "last_synced")
        )
    )
    query = (
        f"SELECT {', '.join(source_columns)} FROM `{source}` "
        f"ORDER BY {', '.join(sort_columns)}"
    )
    with client.query_row_block_stream(
        query, settings={"max_block_size": 1_000}
    ) as blocks:
        for block in blocks:
            matrix = []
            for row in block:
                entity = _entity_from_row(entity_type, source_columns, row)
                values = asdict(entity)
                matrix.append([values[column] for column in target_columns])
                candidate = (
                    entity.org_id,
                    entity.id,
                    int(entity.source_revision),
                    str(entity.source_conflict_key),
                )
                total = (
                    int(entity.source_revision),
                    str(entity.source_conflict_key),
                    int(entity.ingest_revision),
                )
                logical = (entity.org_id, entity.id)
                if candidate != previous_candidate:
                    candidate_count += 1
                    previous_candidate = candidate
                if logical != previous_logical:
                    logical_count += 1
                    previous_logical = logical
                maximum = total if maximum is None or total > maximum else maximum
                raw_rows += 1
            if matrix:
                client.insert(target, matrix, column_names=target_columns)
    return _CopyStats(raw_rows, candidate_count, logical_count, maximum)


def _verify_copy(
    client, source: str, target: str, stats: _CopyStats, exact: bool
) -> None:
    if stats.raw_rows != _count(client, "count()", source):
        raise OperationalOrderingMigrationStateError(source, "raw_row_count_changed")
    candidate_count = _candidate_count(client, target)
    logical_count = _logical_count(client, target)
    maximum = _maximum_tuple(client, target)
    if exact:
        matches = (
            candidate_count == stats.candidate_count
            and logical_count == stats.logical_count
            and maximum == stats.maximum_tuple
        )
    else:
        matches = (
            candidate_count >= stats.candidate_count
            and logical_count >= stats.logical_count
            and (
                stats.maximum_tuple is None
                or maximum is not None
                and maximum >= stats.maximum_tuple
            )
        )
    if not matches:
        raise OperationalOrderingMigrationStateError(target, "copy_verification_failed")


def _resume_exchanged_shadow(client, table: str, shadow: str) -> None:
    if _schema_contract(_show_create(client, shadow), shadow) != 1:
        raise OperationalOrderingMigrationStateError(
            shadow, "expected_legacy_resume_shadow"
        )
    entity_type = _ENTITY_BY_TABLE[table]
    stats = _copy_raw_rows(client, shadow, table, entity_type)
    _verify_copy(client, shadow, table, stats, exact=False)
    client.command(f"DROP TABLE `{shadow}`")


def _rebuild_table(client, table: str) -> None:
    shadow = f"{table}__ordering_v2"
    if not _table_exists(client, table):
        raise OperationalOrderingMigrationStateError(table, "missing_table")
    main_ddl = _show_create(client, table)
    main_contract = _schema_contract(main_ddl, table)
    if main_contract == 2:
        if _table_exists(client, shadow):
            _resume_exchanged_shadow(client, table, shadow)
        return
    if _table_exists(client, shadow):
        client.command(f"DROP TABLE `{shadow}`")
    client.command(_rewrite_ddl(main_ddl, table, shadow))
    if _schema_contract(_show_create(client, shadow), shadow) != 2:
        raise OperationalOrderingMigrationStateError(shadow, "created_schema_invalid")
    entity_type = _ENTITY_BY_TABLE[table]
    stats = _copy_raw_rows(client, table, shadow, entity_type)
    _verify_copy(client, table, shadow, stats, exact=True)
    client.command(f"EXCHANGE TABLES `{table}` AND `{shadow}`")
    _resume_exchanged_shadow(client, table, shadow)


def upgrade(client) -> None:
    if configured_operational_ordering_contract() is OperationalOrderingContract.LEGACY:
        raise MigrationDeferred(
            "067_operational_ordering_contract.py",
            "bridge is explicitly configured for contract 1",
        )
    existing_tables = tuple(table for table in TABLES if _table_exists(client, table))
    if not existing_tables:
        log.info("operational ordering migration skipped: no canonical tables")
        return
    if len(existing_tables) != len(TABLES):
        missing = next(table for table in TABLES if table not in existing_tables)
        raise OperationalOrderingMigrationStateError(missing, "missing_table")
    for table in existing_tables:
        log.info("operational ordering migration table=%s", table)
        _rebuild_table(client, table)
