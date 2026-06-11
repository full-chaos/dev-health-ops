"""Live-ClickHouse regression tests for CHAOS-2290.

ReplacingMergeTree deduplicates rows sharing the same sorting-key tuple.
Before migrations 027/042, several tables keyed only on natural keys like
``(repo_id, work_item_id)`` — so two tenants whose rows collided on the
natural key collapsed into ONE row on a background merge.

Each test inserts two rows that are identical except for org_id, forces a
merge with ``OPTIMIZE TABLE ... FINAL``, and asserts BOTH rows survive.

Opt-in (filtered from unit/CI runs): ``pytest -m clickhouse``.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone

import pytest

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(
        not CLICKHOUSE_URI,
        reason="Requires CLICKHOUSE_URI (e.g. clickhouse://ch:ch@localhost:8123/default)",
    ),
]


@pytest.fixture(scope="module")
def sink():
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    assert CLICKHOUSE_URI is not None  # skipif guard guarantees it
    s = ClickHouseMetricsSink(CLICKHOUSE_URI)
    # Apply pending migrations (including 042) regardless of
    # AUTO_RUN_MIGRATIONS — the fix under test is a migration.
    s.ensure_schema(force=True)
    yield s
    s.close()


def _two_orgs() -> tuple[str, str]:
    return (
        f"test-chaos-2290-{uuid.uuid4()}",
        f"test-chaos-2290-{uuid.uuid4()}",
    )


def _surviving_orgs(sink, table: str, where: str, params: dict) -> set[str]:
    """Force a merge, then return the org_ids still present for the key."""
    sink.client.command(f"OPTIMIZE TABLE {table} FINAL")
    result = sink.client.query(
        f"SELECT DISTINCT org_id FROM {table} WHERE {where}",
        parameters=params,
    )
    return {row[0] for row in (result.result_rows or [])}


def _cleanup(sink, table: str, org_a: str, org_b: str) -> None:
    sink.client.command(
        f"ALTER TABLE {table} DELETE WHERE org_id IN ({{a:String}}, {{b:String}}) "
        "SETTINGS mutations_sync=2",
        parameters={"a": org_a, "b": org_b},
    )


def test_work_items_dedup_does_not_cross_tenants(sink) -> None:
    """Same (repo_id, work_item_id) under two orgs must survive a merge."""
    org_a, org_b = _two_orgs()
    repo_id = uuid.uuid4()
    work_item_id = f"WI-{uuid.uuid4()}"
    now = datetime.now(timezone.utc)

    try:
        sink.client.insert(
            "work_items",
            [
                [repo_id, work_item_id, "linear", f"item of {org}", now, now, now, org]
                for org in (org_a, org_b)
            ],
            column_names=[
                "repo_id",
                "work_item_id",
                "provider",
                "title",
                "created_at",
                "updated_at",
                "last_synced",
                "org_id",
            ],
        )

        survivors = _surviving_orgs(
            sink,
            "work_items",
            "repo_id = {repo_id:UUID} AND work_item_id = {wid:String}",
            {"repo_id": str(repo_id), "wid": work_item_id},
        )
        assert survivors == {org_a, org_b}, (
            f"cross-tenant dedup in work_items: expected both orgs to survive "
            f"OPTIMIZE FINAL, got {survivors}"
        )
    finally:
        _cleanup(sink, "work_items", org_a, org_b)


def test_work_item_transitions_dedup_does_not_cross_tenants(sink) -> None:
    org_a, org_b = _two_orgs()
    repo_id = uuid.uuid4()
    work_item_id = f"WI-{uuid.uuid4()}"
    occurred_at = datetime.now(timezone.utc)

    try:
        sink.client.insert(
            "work_item_transitions",
            [
                [repo_id, work_item_id, occurred_at, "linear", occurred_at, org]
                for org in (org_a, org_b)
            ],
            column_names=[
                "repo_id",
                "work_item_id",
                "occurred_at",
                "provider",
                "last_synced",
                "org_id",
            ],
        )

        survivors = _surviving_orgs(
            sink,
            "work_item_transitions",
            "repo_id = {repo_id:UUID} AND work_item_id = {wid:String}",
            {"repo_id": str(repo_id), "wid": work_item_id},
        )
        assert survivors == {org_a, org_b}
    finally:
        _cleanup(sink, "work_item_transitions", org_a, org_b)


def test_security_alerts_dedup_does_not_cross_tenants(sink) -> None:
    """Same (repo_id, alert_id) under two orgs must survive a merge."""
    org_a, org_b = _two_orgs()
    repo_id = uuid.uuid4()
    alert_id = f"GHSA-{uuid.uuid4()}"
    now = datetime.now(timezone.utc)

    try:
        sink.client.insert(
            "security_alerts",
            [[repo_id, alert_id, "github", now, now, org] for org in (org_a, org_b)],
            column_names=[
                "repo_id",
                "alert_id",
                "source",
                "created_at",
                "last_synced",
                "org_id",
            ],
        )

        survivors = _surviving_orgs(
            sink,
            "security_alerts",
            "repo_id = {repo_id:UUID} AND alert_id = {aid:String}",
            {"repo_id": str(repo_id), "aid": alert_id},
        )
        assert survivors == {org_a, org_b}, (
            f"cross-tenant dedup in security_alerts: got {survivors}"
        )
    finally:
        _cleanup(sink, "security_alerts", org_a, org_b)


def test_test_suite_results_dedup_does_not_cross_tenants(sink) -> None:
    """Same (repo_id, run_id, suite_id) under two orgs must survive a merge."""
    org_a, org_b = _two_orgs()
    repo_id = uuid.uuid4()
    run_id = f"run-{uuid.uuid4()}"
    suite_id = "suite-1"
    now = datetime.now(timezone.utc)

    try:
        sink.client.insert(
            "test_suite_results",
            [
                [repo_id, run_id, suite_id, "unit", 10, 10, 0, 0, now, org]
                for org in (org_a, org_b)
            ],
            column_names=[
                "repo_id",
                "run_id",
                "suite_id",
                "suite_name",
                "total_count",
                "passed_count",
                "failed_count",
                "skipped_count",
                "last_synced",
                "org_id",
            ],
        )

        survivors = _surviving_orgs(
            sink,
            "test_suite_results",
            "repo_id = {repo_id:UUID} AND run_id = {rid:String} "
            "AND suite_id = {sid:String}",
            {"repo_id": str(repo_id), "rid": run_id, "sid": suite_id},
        )
        assert survivors == {org_a, org_b}
    finally:
        _cleanup(sink, "test_suite_results", org_a, org_b)
