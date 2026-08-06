"""CHAOS-3219 Codex adversarial review (HIGH-3, 2026-08-05): live proof that
``compute_world_digest`` -- the actual function ``fixtures world
--verify-digest`` calls, not a stand-in -- is sensitive to the three
mutation shapes the finding names verbatim: "a stale watermark flip, a
metric value change, and a work-graph row change".

``tests/test_fixtures_world_digest.py`` proves ``_clickhouse_table_digest``'s
per-table exclusion-set logic in isolation with a stub client. This file
proves the SAME property survives through the full ``compute_world_digest``
orchestration path (per-org iteration over the real, expanded
``_CLICKHOUSE_DIGEST_TABLES``/``_POSTGRES_DIGEST_TABLES``, real ClickHouse
AND Postgres engines, canonical JSON, top-level sha256) against a real
migrated scratch database -- an orchestration-level regression (e.g. a table
silently dropped back out of ``_CLICKHOUSE_DIGEST_TABLES``, or the wrong org
column used) would not be caught by the stub tests alone.

Each mutation is driven through the REAL production write path, not a
hand-rolled row:
  * stale watermark flip -- ``source_health.age_source_rows`` (the exact
    function ``fixtures world`` itself calls to realize every "stale"
    sources.json state) against ``git_commits``.
  * metric value change -- ``ClickHouseMetricsSink.write_dora_metrics`` (the
    real DORA sink) against ``dora_metrics_daily``, which HIGH-3 also newly
    added to ``_CLICKHOUSE_DIGEST_TABLES`` -- before this fix, no digest
    mutation test could even have been written for this table because it
    was not covered at all.
  * work-graph row change -- ``ClickHouseMetricsSink.write_work_graph_issue_pr``
    against ``work_graph_issue_pr``, likewise newly covered by HIGH-3.

Opt-in, mirrors ``tests/test_ask_dev_linear_project_subject_live.py``'s
convention: skipped unless BOTH ``CLICKHOUSE_URI`` and ``POSTGRES_URI`` point
at migrated SCRATCH databases. Never the shared dev ``default``/``devhealth``
-- enforced here via the production ``_require_scratch_database`` guard this
file dogfoods directly (the same CRITICAL-finding fix ``fixtures world``
itself is gated on), not a separate ad hoc check that could drift from it.
"""

from __future__ import annotations

import argparse
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")
POSTGRES_URI = os.environ.get("POSTGRES_URI")

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.skipif(
        not CLICKHOUSE_URI or not POSTGRES_URI,
        reason=(
            "Requires migrated SCRATCH CLICKHOUSE_URI and POSTGRES_URI "
            "(e.g. .../chaos3219_digest_mut_scratch) -- see module docstring."
        ),
    ),
]

#: A dedicated synthetic org for this file only -- never collides with
#: ask-dev-world.v1's own "primary"/"sibling" orgs, and every row this file
#: writes is scoped to it, so nothing here interferes with any other live
#: suite that might share the same scratch database.
_ORG_ALIAS = "chaos3219-high3-mutation-live"


def _require_env_scratch() -> None:
    from dev_health_ops.fixtures.world import _require_scratch_database

    _require_scratch_database(CLICKHOUSE_URI, kind="clickhouse")
    _require_scratch_database(POSTGRES_URI, kind="postgres")


@pytest.fixture(scope="module")
def manifest() -> Any:
    from dev_health_ops.fixtures.world import WorldManifest

    _require_env_scratch()
    return WorldManifest(
        manifest_path=Path("/dev/null"),
        world={
            "master_seed": 20260805,
            "orgs": [{"alias": _ORG_ALIAS, "id_seed": f"org:{_ORG_ALIAS}"}],
            "users": [],
        },
        subjects={},
        sources={},
    )


@pytest.fixture(scope="module")
def org_id(manifest: Any) -> str:
    return str(manifest.org_id(_ORG_ALIAS))


@pytest.fixture(scope="module", autouse=True)
def _migrated_scratch_schemas(manifest: Any) -> None:
    """Apply real migrations to both scratch databases once per module --
    ``compute_world_digest`` unconditionally opens a Postgres engine and
    queries ``users`` even when only ClickHouse tables are being asserted
    on, so both schemas must exist."""

    from dev_health_ops import migrate as migrate_mod
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    assert CLICKHOUSE_URI is not None
    assert POSTGRES_URI is not None
    _require_env_scratch()

    sink = ClickHouseMetricsSink(dsn=CLICKHOUSE_URI)
    try:
        sink.ensure_schema(force=True)
    finally:
        sink.close()

    prior = os.environ.get("POSTGRES_URI")
    os.environ["POSTGRES_URI"] = POSTGRES_URI
    try:
        migrate_mod._run_upgrade(argparse.Namespace(revision="head", db=None))
    finally:
        if prior is None:
            os.environ.pop("POSTGRES_URI", None)
        else:
            os.environ["POSTGRES_URI"] = prior


@pytest.fixture
def client() -> Any:
    import clickhouse_connect

    c = clickhouse_connect.get_client(dsn=CLICKHOUSE_URI)
    try:
        yield c
    finally:
        c.close()


async def _digest(manifest: Any) -> dict[str, Any]:
    from dev_health_ops.fixtures.world import compute_world_digest

    assert CLICKHOUSE_URI is not None
    assert POSTGRES_URI is not None
    return await compute_world_digest(
        manifest, sink=CLICKHOUSE_URI, postgres_uri=POSTGRES_URI
    )


def _ch_hash(doc: dict[str, Any], table: str) -> str:
    hash_value = doc["components"]["clickhouse"][table][_ORG_ALIAS]["content_hash"]
    assert isinstance(hash_value, str)
    return hash_value


@pytest.mark.asyncio
class TestStaleWatermarkFlipLive:
    """``git_commits.last_synced`` -- HIGH-3's own named example. Watermark
    aging must be visible to the digest that exists to catch a regression in
    it, driven through the real ``age_source_rows`` production path."""

    async def test_flip_changes_digest(
        self, manifest: Any, org_id: str, client: Any
    ) -> None:
        from dev_health_ops.fixtures.generators.source_health import (
            age_source_rows,
        )

        repo_id = str(uuid.uuid4())
        where = (
            "org_id = {org_id:String} AND repo_id = {repo_id:String} "
            "SETTINGS mutations_sync = 1"
        )
        client.command(
            f"ALTER TABLE git_commits DELETE WHERE {where}",  # noqa: S608
            parameters={"org_id": org_id, "repo_id": repo_id},
        )
        client.insert(
            "git_commits",
            [
                [
                    repo_id,
                    "deadbeefcafe",
                    "initial commit",
                    "author-a",
                    "author-a@example.com",
                    datetime(2026, 1, 1, tzinfo=timezone.utc),
                    "author-a",
                    "author-a@example.com",
                    datetime(2026, 1, 1, tzinfo=timezone.utc),
                    0,
                    datetime(2026, 1, 1, tzinfo=timezone.utc),
                    org_id,
                ]
            ],
            column_names=[
                "repo_id",
                "hash",
                "message",
                "author_name",
                "author_email",
                "author_when",
                "committer_name",
                "committer_email",
                "committer_when",
                "parents",
                "last_synced",
                "org_id",
            ],
        )

        before = await _digest(manifest)
        await age_source_rows(
            client,
            org_id=org_id,
            repo_id=repo_id,
            source="commits",
            stale_watermark=datetime(2020, 6, 1, tzinfo=timezone.utc),
        )
        after = await _digest(manifest)

        assert _ch_hash(before, "git_commits") != _ch_hash(after, "git_commits"), (
            "a stale-watermark flip via the real age_source_rows path must "
            "change compute_world_digest's git_commits hash -- an unchanged "
            "hash here means the digest can no longer catch a regression in "
            "watermark aging, which is exactly what HIGH-3 found"
        )


@pytest.mark.asyncio
class TestMetricValueChangeLive:
    """``dora_metrics_daily.value`` -- newly covered by HIGH-3's expanded
    ``_CLICKHOUSE_DIGEST_TABLES`` (this table was not digested at all
    before). Seeded and mutated via the real DORA sink write path."""

    async def test_value_change_changes_digest(
        self, manifest: Any, org_id: str, client: Any
    ) -> None:
        from dev_health_ops.metrics.schemas import DORAMetricsRecord
        from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

        repo_id = uuid.uuid4()
        day = datetime(2026, 6, 1, tzinfo=timezone.utc).date()
        where = (
            "org_id = {org_id:String} AND repo_id = {repo_id:String} "
            "AND day = {day:Date} AND metric_name = {metric_name:String} "
            "SETTINGS mutations_sync = 1"
        )
        params = {
            "org_id": org_id,
            "repo_id": str(repo_id),
            "day": day,
            "metric_name": "lead_time_for_changes_days",
        }
        # Org-wide (not key-scoped) cleanup: this file's synthetic org_id is
        # stable across every run against the same persistent scratch
        # database, but `repo_id` above is a fresh uuid4 each run -- a
        # key-scoped delete would leave every PRIOR run's row behind
        # (dora_metrics_daily has no ReplacingMergeTree dedup to paper over
        # that), which would make the row-count assertion below flaky
        # across reruns rather than a property of this test's own mutation.
        client.command(
            "ALTER TABLE dora_metrics_daily DELETE WHERE org_id = {org_id:String} "
            "SETTINGS mutations_sync = 1",
            parameters={"org_id": org_id},
        )

        assert CLICKHOUSE_URI is not None
        sink = ClickHouseMetricsSink(dsn=CLICKHOUSE_URI, client=client)
        sink.write_dora_metrics(
            [
                DORAMetricsRecord(
                    repo_id=repo_id,
                    day=day,
                    metric_name="lead_time_for_changes_days",
                    value=2.5,
                    computed_at=datetime(2026, 6, 1, tzinfo=timezone.utc),
                    org_id=org_id,
                )
            ]
        )

        before = await _digest(manifest)

        # dora_metrics_daily is a plain MergeTree -- no ReplacingMergeTree
        # dedup, so the same delete-then-reinsert discipline age_source_rows
        # uses is required here too: an in-place ALTER ... UPDATE would work
        # (value is not part of ORDER BY), but delete+reinsert keeps this
        # test's mutation shape uniform with the production aging path and
        # sidesteps ever needing to special-case which columns are safe to
        # UPDATE in place per engine.
        client.command(
            f"ALTER TABLE dora_metrics_daily DELETE WHERE {where}",  # noqa: S608
            parameters=params,
        )
        sink.write_dora_metrics(
            [
                DORAMetricsRecord(
                    repo_id=repo_id,
                    day=day,
                    metric_name="lead_time_for_changes_days",
                    value=9.75,
                    computed_at=datetime(2026, 6, 1, tzinfo=timezone.utc),
                    org_id=org_id,
                )
            ]
        )

        after = await _digest(manifest)

        assert _ch_hash(before, "dora_metrics_daily") != _ch_hash(
            after, "dora_metrics_daily"
        ), (
            "a DORA metric value change must move compute_world_digest's "
            "dora_metrics_daily hash -- this table had NO digest coverage "
            "at all before HIGH-3, so an unchanged hash here means the gap "
            "is still open"
        )
        assert (
            before["components"]["clickhouse"]["dora_metrics_daily"][_ORG_ALIAS][
                "row_count"
            ]
            == after["components"]["clickhouse"]["dora_metrics_daily"][_ORG_ALIAS][
                "row_count"
            ]
            == 1
        ), "row count must stay 1 -- this proves a VALUE change, not a row-count change"


@pytest.mark.asyncio
class TestWorkGraphRowChangeLive:
    """``work_graph_issue_pr`` -- newly covered by HIGH-3's expanded
    ``_CLICKHOUSE_DIGEST_TABLES``. Mutated the way production sync actually
    would: a second, later-``last_synced`` version of the same
    (repo_id, work_item_id, pr_number) key, relying on the table's real
    ``ReplacingMergeTree(last_synced)`` + the digest query's own ``FINAL``
    to resolve to the latest version -- not a raw DELETE/UPDATE.
    """

    async def test_row_change_changes_digest(
        self, manifest: Any, org_id: str, client: Any
    ) -> None:
        from dev_health_ops.metrics.schemas import WorkGraphIssuePRRecord
        from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

        repo_id = uuid.uuid4()
        work_item_id = "gh:owner/repo#42"
        pr_number = 7
        where = (
            "org_id = {org_id:String} AND repo_id = {repo_id:String} "
            "AND work_item_id = {work_item_id:String} "
            "AND pr_number = {pr_number:UInt32} SETTINGS mutations_sync = 1"
        )
        params = {
            "org_id": org_id,
            "repo_id": str(repo_id),
            "work_item_id": work_item_id,
            "pr_number": pr_number,
        }
        client.command(
            f"ALTER TABLE work_graph_issue_pr DELETE WHERE {where}",  # noqa: S608
            parameters=params,
        )

        assert CLICKHOUSE_URI is not None
        sink = ClickHouseMetricsSink(dsn=CLICKHOUSE_URI, client=client)
        sink.write_work_graph_issue_pr(
            [
                WorkGraphIssuePRRecord(
                    repo_id=repo_id,
                    work_item_id=work_item_id,
                    pr_number=pr_number,
                    confidence=0.4,
                    provenance="heuristic",
                    evidence="branch-name-match",
                    last_synced=datetime(2026, 6, 1, tzinfo=timezone.utc),
                    org_id=org_id,
                )
            ]
        )

        before = await _digest(manifest)

        sink.write_work_graph_issue_pr(
            [
                WorkGraphIssuePRRecord(
                    repo_id=repo_id,
                    work_item_id=work_item_id,
                    pr_number=pr_number,
                    confidence=0.95,
                    provenance="native",
                    evidence="api_pr_commits",
                    last_synced=datetime(2026, 6, 2, tzinfo=timezone.utc),
                    org_id=org_id,
                )
            ]
        )

        after = await _digest(manifest)

        assert _ch_hash(before, "work_graph_issue_pr") != _ch_hash(
            after, "work_graph_issue_pr"
        ), (
            "a work-graph row's content (confidence/provenance/evidence) "
            "changing via a later ReplacingMergeTree version must move "
            "compute_world_digest's work_graph_issue_pr hash -- this table "
            "had NO digest coverage at all before HIGH-3"
        )
