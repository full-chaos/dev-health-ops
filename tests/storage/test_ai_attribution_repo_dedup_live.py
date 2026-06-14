"""Live-ClickHouse regression test for the ai_attribution base-table dedup key.

CHAOS-2379. ``ai_attribution`` is a ReplacingMergeTree(computed_at). Migration
035 keyed it on ``(org_id, provider, subject_type, subject_id, source)`` —
``repo_id`` was NOT in the ORDER BY. ``subject_id`` is the bare, repo-local
provider PR/MR number, so two repos in ONE org that each have MR/PR #1 labeled
ai-assisted from the SAME source produced IDENTICAL dedup keys and one row was
permanently collapsed on a background merge (the resolved view reads
``FROM ai_attribution FINAL``). Migration 043's repo-scoped view cannot recover
a base row the engine already merged away — only the base-table ORDER BY can.

Migration 044 adds ``repo_id`` to the ORDER BY (before ``subject_id``). This
test exercises the BASE TABLE (not the view's window function) for the exact
lossy case: same org + same subject_id + SAME source + DIFFERENT repo_id →
BOTH rows survive ``OPTIMIZE TABLE ... FINAL``.

Opt-in (filtered from unit/CI runs): ``pytest -m clickhouse``.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone

import pytest

# Import connectors FIRST (before any other dev_health_ops import) to break the
# providers._base <-> connectors circular import that otherwise ERRORs at
# collection in isolated runs (see CHAOS-2370).
import dev_health_ops.connectors  # noqa: F401,E402,I001
from dev_health_ops.models.ai_attribution import (  # noqa: E402
    AIAttributionKind,
    AIAttributionRecord,
    AIAttributionSource,
)

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
    # Apply pending migrations (including 044) regardless of AUTO_RUN_MIGRATIONS
    # — the fix under test is a migration.
    s.ensure_schema(force=True)
    yield s
    s.close()


def _record(
    *, org_id: uuid.UUID, repo_id: uuid.UUID, subject_id: str
) -> AIAttributionRecord:
    """An AI-attribution record fixed except for repo_id (the discriminator)."""
    return AIAttributionRecord(
        org_id=org_id,
        provider="gitlab",
        subject_type="pull_request",
        subject_id=subject_id,
        repo_id=repo_id,
        kind=AIAttributionKind.AI_ASSISTED,
        source=AIAttributionSource.PR_LABEL,  # SAME source for both rows
        confidence=0.9,
        actor=None,
        evidence={"label": "ai-assisted"},
        observed_at=datetime.now(timezone.utc),
    )


def test_base_table_dedup_keeps_both_repos(sink) -> None:
    """Same org + subject_id + source, different repo_id → BOTH survive FINAL.

    This is the precise silent-data-loss case. The two records collide on every
    pre-044 ORDER BY column; only repo_id differs. After OPTIMIZE FINAL both
    repo_ids must still be present — proving repo_id is part of the dedup key.
    """
    org_id = uuid.uuid4()
    repo_a = uuid.uuid4()
    repo_b = uuid.uuid4()
    subject_id = "1"  # bare repo-local MR iid — collides across repos

    try:
        sink.write_ai_attribution(
            [
                _record(org_id=org_id, repo_id=repo_a, subject_id=subject_id),
                _record(org_id=org_id, repo_id=repo_b, subject_id=subject_id),
            ]
        )

        sink.client.command("OPTIMIZE TABLE ai_attribution FINAL")
        result = sink.client.query(
            "SELECT DISTINCT repo_id FROM ai_attribution FINAL "
            "WHERE org_id = {org:UUID} AND subject_id = {sid:String} "
            "AND source = 'pr_label'",
            parameters={"org": str(org_id), "sid": subject_id},
        )
        survivors = {str(row[0]) for row in (result.result_rows or [])}

        assert survivors == {str(repo_a), str(repo_b)}, (
            "base-table dedup collapsed a cross-repo ai_attribution row: "
            f"expected both repos to survive OPTIMIZE FINAL, got {survivors}. "
            "repo_id must be in the ReplacingMergeTree ORDER BY (migration 044)."
        )
    finally:
        sink.client.command(
            "ALTER TABLE ai_attribution DELETE WHERE org_id = {org:UUID} "
            "SETTINGS mutations_sync=2",
            parameters={"org": str(org_id)},
        )


def test_base_table_dedup_collapses_true_duplicate(sink) -> None:
    """Same org + repo + subject_id + source → ONE row survives FINAL.

    Confirms migration 044 did not weaken intra-repo dedup: a genuine duplicate
    (every ORDER BY column equal) still collapses to a single row.
    """
    org_id = uuid.uuid4()
    repo_id = uuid.uuid4()
    subject_id = "1"

    try:
        rec1 = _record(org_id=org_id, repo_id=repo_id, subject_id=subject_id)
        rec2 = _record(org_id=org_id, repo_id=repo_id, subject_id=subject_id)
        rec2.confidence = 0.5  # differs only on a non-key column
        sink.write_ai_attribution([rec1, rec2])

        sink.client.command("OPTIMIZE TABLE ai_attribution FINAL")
        result = sink.client.query(
            "SELECT count() FROM ai_attribution FINAL "
            "WHERE org_id = {org:UUID} AND repo_id = {repo:UUID} "
            "AND subject_id = {sid:String} AND source = 'pr_label'",
            parameters={
                "org": str(org_id),
                "repo": str(repo_id),
                "sid": subject_id,
            },
        )
        count = (result.result_rows or [[0]])[0][0]
        assert count == 1, (
            f"true duplicate must collapse to one row under FINAL, got {count}"
        )
    finally:
        sink.client.command(
            "ALTER TABLE ai_attribution DELETE WHERE org_id = {org:UUID} "
            "SETTINGS mutations_sync=2",
            parameters={"org": str(org_id)},
        )
