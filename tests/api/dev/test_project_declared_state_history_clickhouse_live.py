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


@pytest.mark.asyncio
async def test_f1_cleared_target_date_does_not_resurrect_stale_value(
    sink: Any, raw_client: Any
) -> None:
    """PR #1602 review F1: `argMax` skips NULL args per-column. Before the
    tuple-wrap fix, each of state/target_date/last_synced/is_active was its
    own independent `argMax(col, (updated_at, ...))` -- a winning row whose
    `target_date` is NULL (a legitimate clear) was skipped by THAT column's
    argMax, which fell through to the next-highest-`updated_at` row that
    still had a non-NULL `target_date`. The columns then came from two
    DIFFERENT rows: `state` from the newest sync, `target_date` resurrected
    from a stale, superseded one.
    """
    org_id = f"proj-history-f1-{uuid.uuid4().hex[:16]}"
    project_id = str(uuid.uuid4())
    first_updated_at = NOW - timedelta(days=10)
    second_updated_at = NOW - timedelta(days=1)

    sink.write_projects(
        [
            ProjectRecord(
                id=project_id,
                org_id=org_id,
                provider="linear",
                project_key="PLAT",
                name="Platform",
                is_active=1,
                state="planned",
                target_date=(NOW - timedelta(days=100)).date(),
                updated_at=first_updated_at,
                last_synced=first_updated_at,
            )
        ]
    )
    # Second sync genuinely CLEARS the target date -- a real provider event,
    # not a data-entry omission.
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
                target_date=None,
                updated_at=second_updated_at,
                last_synced=second_updated_at,
            )
        ]
    )

    row = await _declared_facts(
        raw_client, org_id=org_id, project_id=project_id, as_of=NOW
    )
    assert row["bounded_count"] > 0
    assert row["state"] == "started", "state must come from the newest (second) sync"
    assert row["target_date"] is None, (
        "target_date must come from the SAME winning row as state -- the "
        "clear must not resurrect the FIRST sync's stale target_date"
    )


@pytest.mark.asyncio
async def test_f3_ambiguous_provider_suppresses_watermark(
    sink: Any, raw_client: Any
) -> None:
    """PR #1602 review F3: the pre-CHAOS-3563 `projects FINAL` read used
    `HAVING count() = 1` to fail closed on provider ambiguity -- an
    ambiguous project returned ZERO rows, so `_read`'s generic
    `last_synced`-based watermark collection never saw it. The history-based
    read always returns exactly one (bounded, unbounded) row now, and the
    Python-side caller correctly refuses to surface `state`/`target_date`
    when `provider_count != 1` -- but `last_synced` must ALSO come back
    NULL in that case, or `_read` still computes a "confirmed fresh"
    watermark from a row nothing downstream actually trusts.
    """
    org_id = f"proj-history-f3-ambiguous-{uuid.uuid4().hex[:16]}"
    project_id = str(uuid.uuid4())
    updated_at = NOW - timedelta(days=1)

    for provider in ("linear", "jira"):
        sink.write_projects(
            [
                ProjectRecord(
                    id=project_id,
                    org_id=org_id,
                    provider=provider,
                    project_key="PLAT",
                    name="Platform",
                    is_active=1,
                    state="started",
                    updated_at=updated_at,
                    last_synced=updated_at,
                )
            ]
        )

    row = await _declared_facts(
        raw_client, org_id=org_id, project_id=project_id, as_of=NOW
    )
    assert row["bounded_count"] > 0
    assert row["provider_count"] == 2
    assert row["last_synced"] is None, (
        "an ambiguous project must not leak a watermark -- nothing "
        "downstream trusts this row's declared state"
    )


@pytest.mark.asyncio
async def test_f3_as_of_inactive_winner_suppresses_watermark(
    sink: Any, raw_client: Any
) -> None:
    """PR #1602 review F3, the other fail-closed case: the pre-CHAOS-3563
    read filtered `is_active = 1` in its WHERE clause -- a retired project
    returned zero rows. The history-based read now evaluates `is_active` AS
    OF the winning version and still returns the row (so the caller can
    read the retirement fact), but `last_synced` must stay NULL when the
    winning version is retired, matching the old fail-closed contract.
    """
    org_id = f"proj-history-f3-inactive-{uuid.uuid4().hex[:16]}"
    project_id = str(uuid.uuid4())
    updated_at = NOW - timedelta(days=1)

    sink.write_projects(
        [
            ProjectRecord(
                id=project_id,
                org_id=org_id,
                provider="linear",
                project_key="PLAT",
                name="Platform",
                is_active=0,
                state="completed",
                updated_at=updated_at,
                last_synced=updated_at,
            )
        ]
    )

    row = await _declared_facts(
        raw_client, org_id=org_id, project_id=project_id, as_of=NOW
    )
    assert row["bounded_count"] > 0
    assert row["is_active"] in (0, False)
    assert row["last_synced"] is None, (
        "a retired-as-of-the-winning-version project must not leak a watermark either"
    )


@pytest.mark.asyncio
async def test_f6_argmax_tie_break_agrees_with_rmt_keep_rule(
    sink: Any, raw_client: Any
) -> None:
    """PR #1602 review F6: `project_declared_state_history` is
    `ReplacingMergeTree(last_synced)` -- a background merge keeps the row
    with the HIGHEST `last_synced` among rows sharing the full ORDER BY key
    `(org_id, provider, id, updated_at)`. If the reader's `argMax` tie-break
    disagrees with that keep-rule, the SAME query can answer differently
    before and after a merge happens -- a query result that silently flips
    with no write, no `as_of` change, nothing but background compaction
    timing.

    Constructed so the two tie-break candidates disagree with each other:
    row A is inserted FIRST but carries the LARGER `last_synced` (the value
    RMT's merge will keep); row B is inserted SECOND (so it would win an
    ingestion-order tie-break) but carries the SMALLER `last_synced`. The
    fix's tie-break is `(updated_at, last_synced)`, so it must agree with
    RMT's own keep-rule regardless of insertion order.
    """
    org_id = f"proj-history-f6-{uuid.uuid4().hex[:16]}"
    project_id = str(uuid.uuid4())
    shared_updated_at = NOW - timedelta(days=1)

    row_a_last_synced = NOW
    row_b_last_synced = NOW - timedelta(hours=2)
    assert row_b_last_synced < row_a_last_synced

    # Row A first (earlier ingestion), but the HIGHER last_synced -- the one
    # RMT will keep after a merge.
    sink.write_projects(
        [
            ProjectRecord(
                id=project_id,
                org_id=org_id,
                provider="linear",
                project_key="PLAT",
                name="Platform",
                is_active=1,
                state="a_should_survive",
                updated_at=shared_updated_at,
                last_synced=row_a_last_synced,
            )
        ]
    )
    # Row B second (later ingestion), but the LOWER last_synced.
    sink.write_projects(
        [
            ProjectRecord(
                id=project_id,
                org_id=org_id,
                provider="linear",
                project_key="PLAT",
                name="Platform",
                is_active=1,
                state="b_should_not_survive",
                updated_at=shared_updated_at,
                last_synced=row_b_last_synced,
            )
        ]
    )

    # Pre-merge: the argMax-based read must already agree with what a merge
    # will keep.
    row = await _declared_facts(
        raw_client, org_id=org_id, project_id=project_id, as_of=NOW
    )
    assert row["state"] == "a_should_survive", (
        "argMax's tie-break must pick the row RMT's own version column "
        "(last_synced) would keep, not insertion order"
    )

    # Post-merge: confirm that is genuinely what RMT keeps, so the
    # assertion above is proven against the real engine, not assumed.
    raw_client.command("OPTIMIZE TABLE project_declared_state_history FINAL")
    final_rows = await query_dicts(
        raw_client,
        "SELECT state FROM project_declared_state_history FINAL "
        "WHERE org_id = {org_id:String} AND id = {entity_id:String}",
        {"org_id": org_id, "entity_id": project_id},
    )
    assert len(final_rows) == 1
    assert final_rows[0]["state"] == "a_should_survive"


@pytest.mark.asyncio
async def test_f4_genuine_floor_breach_warns(raw_client: Any) -> None:
    """PR #1602 review F4 (CONFIRMED), the genuine-floor-breach half: the
    earliest retained row for this project IS a migration-074 backfill seed
    (``is_backfill_floor = 1``) -- real state may have existed even earlier
    that the backfill floor could not recover. `as_of` strictly before that
    seed's `updated_at` must surface `earliest_is_backfill_floor = 1` so the
    caller renders the explicit "unknown, not absent" warning.

    Seeded directly via `raw_client`, mirroring exactly what migration 074's
    own backfill INSERT does (``_BACKFILL_SQL`` in
    ``074_project_declared_state_history.py``) -- no production write path
    ever sets `is_backfill_floor = 1` itself.
    """
    org_id = f"proj-history-f4-floor-{uuid.uuid4().hex[:16]}"
    project_id = str(uuid.uuid4())
    seeded_at = NOW - timedelta(days=30)

    raw_client.insert(
        "project_declared_state_history",
        [
            [
                org_id,
                "linear",
                project_id,
                None,
                "Platform",
                1,
                "in_progress",
                None,
                "",
                seeded_at,
                seeded_at,
                1,  # is_backfill_floor
            ]
        ],
        column_names=[
            "org_id",
            "provider",
            "id",
            "project_key",
            "name",
            "is_active",
            "state",
            "target_date",
            "url",
            "updated_at",
            "last_synced",
            "is_backfill_floor",
        ],
    )

    row = await _declared_facts(
        raw_client,
        org_id=org_id,
        project_id=project_id,
        as_of=seeded_at - timedelta(days=1),
    )
    assert row["bounded_count"] == 0
    assert row["total_count"] > 0
    assert row["earliest_is_backfill_floor"] in (1, True), (
        "the earliest retained row IS a backfill seed -- a genuine floor "
        "breach, must be distinguishable from plain absence"
    )


@pytest.mark.asyncio
async def test_f4_created_after_as_of_is_not_a_floor_breach(
    sink: Any, raw_client: Any
) -> None:
    """PR #1602 review F4, the other half: this project has NO backfill
    seed at all -- every retained row is an ordinary sync, so its history
    already IS the complete history back to true creation. `as_of` before
    the project's first-ever sync means the project simply did not exist
    yet -- plain absence, never a floor-breach warning, even though
    `bounded_count == 0 and total_count > 0` (the same raw shape the
    genuine floor-breach case has).
    """
    org_id = f"proj-history-f4-created-{uuid.uuid4().hex[:16]}"
    project_id = str(uuid.uuid4())
    created_at = NOW - timedelta(days=5)

    sink.write_projects(
        [
            ProjectRecord(
                id=project_id,
                org_id=org_id,
                provider="linear",
                project_key="PLAT",
                name="Platform",
                is_active=1,
                state="planned",
                updated_at=created_at,
                last_synced=created_at,
            )
        ]
    )

    row = await _declared_facts(
        raw_client,
        org_id=org_id,
        project_id=project_id,
        as_of=created_at - timedelta(days=1),
    )
    assert row["bounded_count"] == 0
    assert row["total_count"] > 0
    assert row["earliest_is_backfill_floor"] in (0, False, None), (
        "the earliest (and only) retained row is an ORDINARY sync, not a "
        "backfill seed -- this project's full history is known back to "
        "its true creation, so `as_of` before it is plain absence, not an "
        "unknown/unrecoverable past"
    )
