"""Live-ClickHouse proof that CHAOS-3563 actually recovers a project's
declared state as of a past instant, after it has since changed.

The unit suite (test_native_status_change.py,
test_project_scope_status_snapshot_repositories.py) replaces the ClickHouse
round trip with a fake that returns a canned row -- honest about what it
covers (Python-side row consumption, params threading) and explicit about
what it cannot: whether `argMax(...)` over real ReplacingMergeTree parts
actually returns the OLDER version for an `as_of` strictly between two real
writes. Only a live engine, over rows written through the real
`ClickHouseCore.write_projects` sink path (not hand-built fixture rows), can
prove that.

**Differential proof, not just a green assertion**: this seeds TWO real
syncs of the SAME project through the real sink (an initial state, then a
later state change), then shows both halves of the fix:

1. `projects FINAL` (the pre-CHAOS-3563 read source) answers with the
   CURRENT state regardless of `as_of` -- reading it for the earlier instant
   would silently mislabel the newer state as of the past. This is the bug
   CHAOS-3563 closes, reproduced against the real engine.
2. `_PROJECT_DECLARED_FACTS_SQL` (the fixed read, over
   `project_declared_state_history`) answers with the state that was
   actually true at that earlier instant.

Opt-in (filtered from unit/CI by ``ci/run_tests.sh``'s
``-m "not benchmark and not clickhouse"``): ``pytest -m clickhouse`` with
``CLICKHOUSE_URI`` pointing at a SCRATCH database -- never the dev
``default``.
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from dev_health_ops.api.dev.native_status_change import _PROJECT_DECLARED_FACTS_SQL
from dev_health_ops.api.queries.client import query_dicts
from dev_health_ops.metrics.schemas import ProjectRecord

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")
NOW = datetime.now(UTC).replace(microsecond=0)

#: Database names this file refuses to write real rows into.
_PROTECTED_DATABASES = frozenset({"", "default"})


def _database_of(dsn: str | None) -> str:
    from urllib.parse import urlparse

    return urlparse(dsn or "").path.lstrip("/").strip().lower()


_SKIP_REASON = (
    "Requires a migrated SCRATCH CLICKHOUSE_URI "
    "(e.g. clickhouse://ch:ch@localhost:8123/ci_local_validate); "
    f"got database {_database_of(CLICKHOUSE_URI) or '<unset>'!r}, which this "
    "suite refuses to seed"
)

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(
        not CLICKHOUSE_URI or _database_of(CLICKHOUSE_URI) in _PROTECTED_DATABASES,
        reason=_SKIP_REASON,
    ),
]


@pytest.fixture
def sink() -> Any:
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    assert CLICKHOUSE_URI is not None
    if _database_of(CLICKHOUSE_URI) in _PROTECTED_DATABASES:
        raise RuntimeError("refusing to seed a protected ClickHouse database")
    metrics_sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    try:
        yield metrics_sink
    finally:
        metrics_sink.close()


@pytest.fixture
def raw_client() -> Any:
    import clickhouse_connect

    assert CLICKHOUSE_URI is not None
    client = clickhouse_connect.get_client(dsn=CLICKHOUSE_URI)
    try:
        yield client
    finally:
        client.close()


async def _declared_facts(
    client: Any, *, org_id: str, project_id: str, as_of: datetime
) -> dict[str, Any]:
    """The real query always returns exactly one row (a cross join of two
    single-row, no-``GROUP BY`` aggregates -- see
    ``_PROJECT_DECLARED_FACTS_SQL``'s own docstring), never an empty result
    set. Callers distinguish outcomes via the row's own
    ``bounded_count``/``total_count`` fields, not via row presence.
    """
    rows = await query_dicts(
        client,
        _PROJECT_DECLARED_FACTS_SQL,
        {"org_id": org_id, "entity_id": project_id, "as_of": as_of},
    )
    assert len(rows) == 1, (
        "the bounded/unbounded cross join must always yield exactly one row"
    )
    return rows[0]


@pytest.mark.asyncio
async def test_recovers_the_declared_state_as_of_an_instant_before_a_later_change(
    sink: Any, raw_client: Any
) -> None:
    org_id = f"proj-history-{uuid.uuid4().hex[:16]}"
    project_id = str(uuid.uuid4())

    first_updated_at = NOW - timedelta(days=10)
    second_updated_at = NOW - timedelta(days=1)
    between = NOW - timedelta(days=5)

    # Two REAL syncs through the production write path, each producing the
    # row shape a real provider sync would: the project started, then later
    # completed.
    sink.write_projects(
        [
            ProjectRecord(
                id=project_id,
                org_id=org_id,
                provider="linear",
                project_key="PLAT",
                name="Platform",
                is_active=1,
                state="started",
                updated_at=first_updated_at,
                last_synced=first_updated_at,
            )
        ]
    )
    sink.write_projects(
        [
            ProjectRecord(
                id=project_id,
                org_id=org_id,
                provider="linear",
                project_key="PLAT",
                name="Platform",
                is_active=1,
                state="completed",
                updated_at=second_updated_at,
                last_synced=second_updated_at,
            )
        ]
    )

    # 1. The bug this migration closes, reproduced live: `projects FINAL`
    #    always answers with the CURRENT state, even for an `as_of` strictly
    #    before the change that produced it.
    current_rows = await query_dicts(
        raw_client,
        "SELECT state FROM projects FINAL "
        "WHERE org_id = {org_id:String} AND id = {entity_id:String}",
        {"org_id": org_id, "entity_id": project_id},
    )
    assert current_rows and current_rows[0]["state"] == "completed", (
        "projects FINAL only ever holds the current row -- sanity-checking "
        "the fixture, not the fix"
    )

    # 2. The fix: the history-backed read recovers what was actually true at
    #    `between`, strictly before the second sync.
    at_between = await _declared_facts(
        raw_client, org_id=org_id, project_id=project_id, as_of=between
    )
    assert at_between["bounded_count"] > 0, (
        "expected the FIRST declared state to be recoverable as of an "
        "instant before the second sync -- this is the whole point of "
        "CHAOS-3563"
    )
    assert at_between["state"] == "started"

    # 3. As of (at or after) the second sync, the NEW state is returned.
    at_second = await _declared_facts(
        raw_client, org_id=org_id, project_id=project_id, as_of=second_updated_at
    )
    assert at_second["bounded_count"] > 0
    assert at_second["state"] == "completed"

    # 4. CHAOS-3563 review condition: strictly before the FIRST sync, every
    #    retained row postdates `as_of` -- this is the explicit
    #    floor-breach signal (bounded_count == 0, total_count > 0), NOT
    #    plain absence, and the state must never be fabricated from the
    #    earliest retained row.
    before_first = await _declared_facts(
        raw_client,
        org_id=org_id,
        project_id=project_id,
        as_of=first_updated_at - timedelta(days=1),
    )
    assert before_first["bounded_count"] == 0
    assert before_first["total_count"] > 0, (
        "history DOES exist for this project -- it just postdates as_of, "
        "which must read as an explicit floor breach, not as this project "
        "having no history at all"
    )
    assert not before_first["state"]

    # 5. A project this org has NEVER synced has genuinely NO history --
    #    the other half of the same distinction: total_count == 0, not a
    #    floor breach.
    never_synced = await _declared_facts(
        raw_client,
        org_id=org_id,
        project_id=str(uuid.uuid4()),
        as_of=NOW,
    )
    assert never_synced["total_count"] == 0
    assert never_synced["bounded_count"] == 0
    assert not never_synced["state"]


@pytest.mark.asyncio
async def test_unchanged_resync_does_not_duplicate_history_rows(
    sink: Any, raw_client: Any
) -> None:
    """A re-sync that observes the SAME declared state (same provider mtime)
    must collapse via ReplacingMergeTree, not accumulate a row per sync --
    the history table is keyed on `updated_at`, so an unchanged resync
    shares the exact same key as the row it "replaces" (itself).
    """
    org_id = f"proj-history-nochange-{uuid.uuid4().hex[:16]}"
    project_id = str(uuid.uuid4())
    updated_at = NOW - timedelta(days=3)

    record = ProjectRecord(
        id=project_id,
        org_id=org_id,
        provider="linear",
        project_key="PLAT",
        name="Platform",
        is_active=1,
        state="started",
        updated_at=updated_at,
        last_synced=updated_at,
    )
    for _ in range(3):
        sink.write_projects([record])

    raw_client.command("OPTIMIZE TABLE project_declared_state_history FINAL")
    rows = await query_dicts(
        raw_client,
        "SELECT count() AS n FROM project_declared_state_history FINAL "
        "WHERE org_id = {org_id:String} AND id = {entity_id:String}",
        {"org_id": org_id, "entity_id": project_id},
    )
    assert rows[0]["n"] == 1, (
        "three identical re-syncs (same updated_at) must collapse to one "
        "history row after a merge, not three"
    )
