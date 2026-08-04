"""Live-ClickHouse proof for CHAOS-3377's residual blocker-duplication defect.

The live acceptance probe found the SAME blocker rendered 5 times in one
§10 answer, each backed by the SAME evidence -- root-caused to
``_BLOCKERS_SQL`` (``native_status_change.py``): under PROJECT scope it
joins ``work_graph_edges`` with NO ``GROUP BY``, so a single blocker with
``blocks`` edges to N different blocked issues in scope returns N identical-
entity rows. ``ORDER BY``/``LIMIT`` then apply to those MULTIPLIED rows, not
to distinct blockers.

Codex adversarial review on the first fix (a renderer-side dedup in
``status_answer_render.py``) correctly flagged that seam as too late: SQL
``LIMIT`` and evidence minting both happen BEFORE the renderer ever runs, so
a blocker with more edges than the page size can fill the entire result
page and push OTHER, genuinely distinct blockers out of the answer
entirely -- unrecoverable by any dedup downstream. The real fix collapses
duplicate rows to one per blocker INSIDE the SQL, before ``ORDER BY``/
``LIMIT``.

This test proves that collapse against a REAL engine with REAL rows (a unit
fake is a predicate evaluator, not a SQL engine, and cannot see a query
plan's actual row multiplication): one blocker wired to more blocked issues
than the page ``limit``, plus a second, genuinely distinct blocker, and
asserts the second blocker's row -- and therefore its evidence -- survives.

Opt-in (filtered from unit/CI by ``ci/run_tests.sh``'s
``-m "not benchmark and not clickhouse"``): ``pytest -m clickhouse`` with
``CLICKHOUSE_URI`` pointing at a SCRATCH database -- never the dev ``default``.
"""

from __future__ import annotations

import os
import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock

import pytest

from dev_health_ops.api.dev.native_status_change import _BLOCKERS_SQL
from dev_health_ops.api.queries.client import query_dicts
from dev_health_ops.metrics.schemas import ProjectRecord, WorkGraphEdgeRecord
from dev_health_ops.providers.identity import IdentityResolver
from dev_health_ops.providers.linear.normalize import linear_issue_to_work_item
from dev_health_ops.providers.status_mapping import StatusMapping

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")
NOW = datetime.now(UTC).replace(microsecond=0)

#: Mirrors test_project_scope_clickhouse_live.py's own guard verbatim -- this
#: file writes rows too, and must never touch the dev ``default`` database.
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
    metrics_sink = ClickHouseMetricsSink(CLICKHOUSE_URI)
    try:
        yield metrics_sink
    finally:
        metrics_sink.close()


def _work_item(
    identifier: str,
    *,
    org_id: str,
    project_id: str | None,
    repo_id: uuid.UUID,
    updated_at: datetime,
) -> Any:
    """One work item through the real Linear normalizer (never hand-built)."""

    identity = MagicMock(spec=IdentityResolver)
    identity.resolve.side_effect = lambda **kwargs: "user:dev@example.com"
    status_mapping = MagicMock(spec=StatusMapping)
    status_mapping.normalize_status.return_value = "in_progress"
    status_mapping.normalize_type.return_value = "task"

    issue = {
        "id": f"issue-{identifier}",
        "identifier": identifier,
        "title": f"Work item {identifier}",
        "createdAt": (updated_at - timedelta(days=1)).isoformat(),
        "updatedAt": updated_at.isoformat(),
        "state": {"id": "s1", "name": "In Progress", "type": "started"},
        "team": {"id": "t1", "key": "CHAOS", "name": "Fullchaos"},
        **(
            {"project": {"id": project_id, "name": "Blockers Live Project"}}
            if project_id
            else {}
        ),
    }
    item, _ = linear_issue_to_work_item(
        issue=issue, status_mapping=status_mapping, identity=identity
    )
    return replace(item, org_id=org_id, repo_id=repo_id, updated_at=updated_at)


def _edge(
    *,
    source_id: str,
    target_id: str,
    repo_id: uuid.UUID,
    org_id: str,
    event_ts: datetime,
) -> WorkGraphEdgeRecord:
    return WorkGraphEdgeRecord(
        edge_id=f"blocks:{source_id}:{target_id}",
        source_type="issue",
        source_id=source_id,
        target_type="issue",
        target_id=target_id,
        edge_type="blocks",
        repo_id=repo_id,
        provider="linear",
        provenance="native",
        confidence=1.0,
        evidence="{}",
        discovered_at=event_ts,
        last_synced=event_ts,
        event_ts=event_ts,
        day=event_ts.date(),
        org_id=org_id,
    )


@pytest.mark.asyncio
async def test_blocker_with_more_edges_than_limit_never_hides_a_distinct_blocker(
    sink: Any,
) -> None:
    """Fail->pass repro for the Codex HIGH finding.

    One blocker (``over-limit-blocker``) has ``blocks`` edges to 4 different
    blocked issues in the SAME project scope -- more than the page
    ``limit`` (3) below. A second, genuinely distinct blocker
    (``second-blocker``) blocks just one of the same issues.

    Pre-fix: ``_BLOCKERS_SQL`` returns 4 identical ``over-limit-blocker``
    rows (one per edge) ordered ahead of the 1 ``second-blocker`` row (its
    edges/updates are deliberately older); ``LIMIT 3`` keeps 3 of those 4
    duplicate rows and ``second-blocker`` never appears at all -- an
    actionable, distinct blocker silently dropped by a duplicate-inflated
    page, unrecoverable by any dedup downstream of the SQL.

    Post-fix: the SQL collapses to one row per blocker BEFORE ``LIMIT``, so
    both distinct blockers (2 rows total) fit under ``limit=3`` and
    ``second-blocker`` survives.
    """

    org_id = f"chaos-3377-blockers-{uuid.uuid4().hex[:16]}"
    project_id = str(uuid.uuid4())
    repo_id = uuid.uuid4()

    older = NOW - timedelta(days=5)
    newer = NOW - timedelta(hours=1)
    # The query-side ``as_of`` bound is intentionally NOT "now": a known,
    # pre-existing ClickHouse/driver quirk on this stack (see the 3380
    # lane's flaky-test finding) can land sink-written timestamps hours
    # ahead of the wall clock a query parameter is bound against, which is
    # exactly the kind of accidental exclusion this test must not be
    # sensitive to -- it is testing row COLLAPSE, not freshness bounding.
    # Fixed far in the future so every seeded row is unambiguously "in the
    # past" relative to it, regardless of that skew's direction or size.
    query_as_of = datetime(2030, 1, 1, tzinfo=UTC)

    blocked_identifiers = [f"BLOCKED-{index}" for index in range(4)]
    over_limit_blocker_identifier = "OVER-LIMIT-BLOCKER"
    second_blocker_identifier = "SECOND-BLOCKER"

    sink.write_projects(
        [
            ProjectRecord(
                id=project_id,
                org_id=org_id,
                provider="linear",
                name="Blockers Live Project",
                is_active=1,
                updated_at=NOW,
                last_synced=NOW,
            )
        ]
    )

    sink.write_work_items(
        [
            *(
                _work_item(
                    identifier,
                    org_id=org_id,
                    project_id=project_id,
                    repo_id=repo_id,
                    updated_at=newer,
                )
                for identifier in blocked_identifiers
            ),
            _work_item(
                over_limit_blocker_identifier,
                org_id=org_id,
                project_id=None,
                repo_id=repo_id,
                updated_at=newer,
            ),
            _work_item(
                second_blocker_identifier,
                org_id=org_id,
                project_id=None,
                repo_id=repo_id,
                updated_at=older,
            ),
        ]
    )

    # The real producer (linear_issue_to_work_item) mints
    # ``work_item_id = f"linear:{identifier}"`` -- derive every id from that
    # SAME rule rather than hand-guessing the format.
    over_limit_blocker_id = f"linear:{over_limit_blocker_identifier}"
    second_blocker_id = f"linear:{second_blocker_identifier}"
    blocked_work_item_ids = [
        f"linear:{identifier}" for identifier in blocked_identifiers
    ]

    sink.write_work_graph_edges(
        [
            *(
                _edge(
                    source_id=over_limit_blocker_id,
                    target_id=blocked_id,
                    repo_id=repo_id,
                    org_id=org_id,
                    event_ts=newer,
                )
                for blocked_id in blocked_work_item_ids
            ),
            _edge(
                source_id=second_blocker_id,
                target_id=blocked_work_item_ids[0],
                repo_id=repo_id,
                org_id=org_id,
                event_ts=older,
            ),
        ]
    )

    assert CLICKHOUSE_URI is not None
    rows = await query_dicts(
        sink.client,
        _BLOCKERS_SQL,
        {
            "org_id": org_id,
            "entity_id": project_id,
            "scope_type": "project",
            "repository_ids": [str(repo_id)],
            "member_issue_ids": [],
            "as_of": query_as_of,
            "limit": 3,
        },
    )

    entity_ids = [row["entity_id"] for row in rows]
    assert len(entity_ids) == len(set(entity_ids)), (
        f"_BLOCKERS_SQL returned duplicate entity_id rows: {entity_ids}"
    )
    assert second_blocker_id in entity_ids, (
        "the second, genuinely distinct blocker was crowded out of the "
        f"page by the over-limit blocker's duplicate rows: {entity_ids}"
    )
    assert over_limit_blocker_id in entity_ids
    assert set(entity_ids) == {over_limit_blocker_id, second_blocker_id}
