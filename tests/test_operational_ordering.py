from __future__ import annotations

import asyncio
import importlib
from dataclasses import fields, replace
from datetime import datetime, timedelta, timezone
from types import ModuleType
from typing import Any, cast
from uuid import UUID

import pytest

from dev_health_ops.models.operational import (
    OPERATIONAL_ENTITY_TABLES,
    OperationalContractError,
    OperationalService,
)
from dev_health_ops.storage.clickhouse import ClickHouseStore
from tests.fixtures.operational_entities import all_operational_entities

_AT = datetime(2026, 7, 20, 12, 0, 0, 123456, tzinfo=timezone.utc)
_ORDERING_FIELDS = {
    "source_revision",
    "source_conflict_key",
    "ingest_revision",
    "ordering_contract",
}
_CONFLICT_EXCLUSIONS = {
    "id",
    *_ORDERING_FIELDS,
    "observed_at",
    "last_synced",
}
_DATETIME_FIELDS = {
    "source_event_at",
    "source_version_at",
    "deleted_at",
    "started_at",
    "resolved_at",
    "triggered_at",
    "acknowledged_at",
    "occurred_at",
    "created_at",
    "requested_at",
    "assigned_at",
    "completed_at",
    "starts_at",
    "ends_at",
    "valid_from",
    "valid_to",
}


def _ordering() -> ModuleType:
    return importlib.import_module("dev_health_ops.models.operational_ordering")


def _replacement(field_name: str, value: object) -> object:
    match field_name:
        case "normalized_status":
            return "active" if value != "active" else "resolved"
        case "normalized_severity":
            return "high" if value != "high" else "low"
        case "normalized_priority":
            return "high" if value != "high" else "low"
        case "relationship_confidence":
            return 0.5 if value != 0.5 else 0.75
        case "source_id" | "repo_id":
            return UUID("00000000-0000-0000-0000-000000000099")
        case name if name in _DATETIME_FIELDS:
            return _AT + timedelta(seconds=1)
        case _:
            if isinstance(value, bool):
                return not value
            if isinstance(value, str):
                return f"{value}-changed"
            if isinstance(value, int | float):
                return value + 1
            if isinstance(value, UUID):
                return UUID("00000000-0000-0000-0000-000000000099")
            if value is None:
                return "changed"
            raise AssertionError(f"No replacement for {field_name}={value!r}")


def test_all_actual_families_expose_the_persisted_ordering_contract() -> None:
    # Given: one canonical instance from every actual entity family.
    entities_by_type = {type(entity): entity for entity in all_operational_entities()}

    # When: the persisted dataclass fields and derived values are inspected.
    contracts = {
        entity_type: (
            _ORDERING_FIELDS <= {item.name for item in fields(entity_type)},
            entities_by_type[entity_type].ordering_contract,
        )
        for entity_type in OPERATIONAL_ENTITY_TABLES
    }

    # Then: all twelve families carry contract 2 and UInt128-compatible revisions.
    assert len(contracts) == 12
    assert all(
        has_fields and contract == 2 for has_fields, contract in contracts.values()
    )


def test_conflict_tlv_is_injective_for_boundaries_nulls_types_and_unicode() -> None:
    # Given: inputs that collide under concatenation or lossy JSON normalization.
    encode = _ordering().encode_conflict_fields
    candidates = (
        encode("family", (("left", "ab"), ("right", "c"))),
        encode("family", (("left", "a"), ("right", "bc"))),
        encode("family", (("value", None),)),
        encode("family", (("value", ""),)),
        encode("family", (("value", 1),)),
        encode("family", (("value", "1"),)),
        encode("family", (("value", True),)),
        encode("family", (("value", "é"),)),
        encode("family", (("value", "e\u0301"),)),
        encode("family", (("value", [1, 2]),)),
        encode("family", (("value", (1, 2)),)),
    )

    # When: the complete TLV byte strings are compared.
    unique = set(candidates)

    # Then: field boundaries, nulls, runtime types, and exact UTF-8 remain distinct.
    assert len(unique) == len(candidates)
    assert all(
        bytes.fromhex(value).startswith(b"operational-conflict-v1")
        for value in candidates
    )


def test_conflict_tlv_sorts_maps_recursively_and_preserves_sequence_order() -> None:
    # Given: equivalent maps with different insertion order and reversed sequences.
    encode = _ordering().encode_conflict_fields
    first = encode("family", (("value", {"b": [1, 2], "a": {"z": False}}),))
    reordered = encode("family", (("value", {"a": {"z": False}, "b": [1, 2]}),))
    reversed_sequence = encode("family", (("value", {"a": {"z": False}, "b": [2, 1]}),))

    # When: their canonical encodings are compared.
    same_map = first == reordered

    # Then: map insertion order is irrelevant while sequence order is retained.
    assert same_map
    assert first != reversed_sequence


def test_every_persisted_source_or_business_field_affects_every_family_key() -> None:
    # Given: every populated canonical family and its fixed dataclass field order.
    entities = {type(entity): entity for entity in all_operational_entities()}

    # When: each included persisted field is changed independently.
    unchanged: list[tuple[str, str]] = []
    for entity_type in OPERATIONAL_ENTITY_TABLES:
        entity = entities[entity_type]
        for item in fields(entity):
            if item.name in _CONFLICT_EXCLUSIONS:
                continue
            mutated = replace(
                cast(Any, entity),
                **{item.name: _replacement(item.name, getattr(entity, item.name))},
            )
            if mutated.source_conflict_key == entity.source_conflict_key:
                unchanged.append((entity.entity_family, item.name))

    # Then: no source or business field is omitted from the injective key.
    assert unchanged == []


def test_receive_time_changes_only_ingest_revision_for_an_identical_replay() -> None:
    # Given: one source-identical row replayed at a later receive time.
    entity = all_operational_entities()[0]
    replay = replace(
        entity,
        observed_at=entity.observed_at + timedelta(seconds=1),
        last_synced=entity.last_synced + timedelta(seconds=2),
    )

    # When: source truth and replay compaction fields are compared.
    source_identity = (entity.source_revision, entity.source_conflict_key)
    replay_source_identity = (replay.source_revision, replay.source_conflict_key)

    # Then: source ordering is stable and only the ingestion version advances.
    assert replay_source_identity == source_identity
    assert replay.ingest_revision > entity.ingest_revision


def test_source_revision_orders_time_then_operation_rank_then_key_hash() -> None:
    # Given: one timestamp and conflict key across every operation rank.
    ordering = _ordering()
    key = ordering.encode_conflict_fields("family", (("value", "same"),))

    # When: source revisions are built for create, update, tombstone, and later create.
    create = ordering.build_source_revision(_AT, ordering.OperationRank.CREATE, key)
    update = ordering.build_source_revision(
        _AT, ordering.OperationRank.ACTIVE_UPDATE, key
    )
    tombstone = ordering.build_source_revision(
        _AT, ordering.OperationRank.TOMBSTONE, key
    )
    later_create = ordering.build_source_revision(
        _AT + timedelta(microseconds=1), ordering.OperationRank.CREATE, key
    )

    # Then: timestamp dominates rank, and equal-time tombstones dominate updates and creates.
    assert create < update < tombstone < later_create


def test_entity_construction_preserves_explicit_create_rank_zero() -> None:
    # Given: a non-tombstone entity explicitly representing a provider create event.
    ordering = _ordering()

    # When: the entity derives its persisted ordering tuple.
    entity = OperationalService(
        org_id="org-a",
        provider="pagerduty",
        provider_instance_id="pd-a",
        source_entity_type="service",
        external_id="created-service",
        source_version_at=_AT,
        observed_at=_AT,
        last_synced=_AT,
        name="Created service",
        operation_rank=ordering.OperationRank.CREATE,
    )

    # Then: integer rank zero survives instead of falling back to ACTIVE_UPDATE.
    assert (
        ordering.operation_rank_from_revision(entity.source_revision)
        is ordering.OperationRank.CREATE
    )


def test_revision_builders_reject_rank_time_and_uint128_overflow() -> None:
    # Given: malformed temporal and numeric revision inputs.
    ordering = _ordering()
    key = ordering.encode_conflict_fields("family", (("value", "same"),))

    # When: each invalid boundary is encoded.
    # Then: invalid encodings and overflow are terminal typed failures.
    with pytest.raises(ordering.OperationalOrderingEncodingError):
        ordering.build_source_revision(_AT.replace(tzinfo=None), 1, key)
    with pytest.raises(ordering.OperationalOrderingEncodingError):
        ordering.build_source_revision(
            _AT.astimezone(timezone(timedelta(hours=1))), 1, key
        )
    with pytest.raises(ordering.OperationalOrderingEncodingError):
        ordering.build_source_revision(
            datetime(1969, 1, 1, tzinfo=timezone.utc), 1, key
        )
    with pytest.raises(ordering.OperationalOrderingEncodingError):
        ordering.build_source_revision(_AT, 3, key)
    with pytest.raises(ordering.OperationalOrderingOverflowError):
        ordering.build_source_revision_from_microseconds(1 << 64, 1, key)
    with pytest.raises(ordering.OperationalOrderingOverflowError):
        ordering.build_ingest_revision_from_microseconds(1 << 64, 0)


def test_revision_builders_enforce_clickhouse_25_1_datetime64_bounds() -> None:
    # Given: the final representable microsecond and first unsupported instant.
    ordering = _ordering()
    key = ordering.encode_conflict_fields("family", (("value", "same"),))
    maximum = datetime(2299, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc)
    beyond = datetime(2300, 1, 1, tzinfo=timezone.utc)

    # When: source and ingestion revisions are built at the supported boundary.
    source_revision = ordering.build_source_revision(
        maximum, ordering.OperationRank.ACTIVE_UPDATE, key
    )
    ingest_revision = ordering.build_ingest_revision(maximum, maximum)

    # Then: the boundary is accepted and later values fail before ClickHouse insertion.
    assert source_revision > 0
    assert ingest_revision > 0
    with pytest.raises(ordering.OperationalOrderingEncodingError):
        ordering.build_source_revision(
            beyond, ordering.OperationRank.ACTIVE_UPDATE, key
        )
    with pytest.raises(ordering.OperationalOrderingEncodingError):
        ordering.build_ingest_revision(beyond, maximum)


def test_typed_insert_rejects_tampered_or_wrong_family_ordering() -> None:
    # Given: a valid service whose derived conflict key is tampered after construction.
    ordering = _ordering()
    service = all_operational_entities()[0]
    assert isinstance(service, OperationalService)
    object.__setattr__(service, "source_conflict_key", "00")
    store = ClickHouseStore("clickhouse://unused")

    # When: the generic writer validates the row and table family.
    # Then: invalid ordering fails before any ClickHouse insert is attempted.
    with pytest.raises(ordering.OperationalOrderingEncodingError):
        asyncio.run(store.insert_operational_services([service]))
    with pytest.raises(OperationalContractError, match="table"):
        asyncio.run(store._insert_operational_rows("operational_incidents", [service]))


def test_current_row_sql_selects_total_tuple_before_domain_filters() -> None:
    # Given: a tombstone-sensitive current-row query for canonical incidents.
    current = importlib.import_module("dev_health_ops.storage.operational_current")
    guard = importlib.import_module("dev_health_ops.storage.operational_ordering_guard")

    # When: the centralized table expression is rendered.
    query = current.current_operational_rows_sql(
        "operational_incidents",
        ("is_deleted = 0", "started_at < {end:DateTime}"),
        ordering_contract=guard.OperationalOrderingContract.CURRENT,
    )

    # Then: the total winner is selected before active and domain filters are applied.
    assert "FINAL" not in query
    assert (
        "ORDER BY org_id, id, source_revision DESC, source_conflict_key DESC, ingest_revision DESC"
        in query
    )
    assert "LIMIT 1 BY org_id, id" in query
    assert query.index("LIMIT 1 BY org_id, id") < query.index("is_deleted = 0")
    assert "WHERE org_id = {org_id:String}" in query
