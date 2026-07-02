"""Live-ClickHouse regression tests for CHAOS-2787.

``discover_repos`` (``dev_health_ops.metrics.job_daily``) SELECTs from the
ClickHouse ``repos`` table, a ``ReplacingMergeTree(last_synced)`` ordered by
``(org_id, id)`` (migration 027). ``ClickHouseStorage.insert_repo`` always
writes a fresh row per sync rather than short-circuiting on an existing row
(CHAOS-1775: needed so a re-run under a different ``--org`` refreshes
``org_id`` rather than stranding it), so multiple logical versions of the same
``(org_id, id)`` routinely coexist as separate physical rows until a
background merge collapses them. A plain ``SELECT * FROM repos`` therefore
returned those pre-merge duplicates as separate ``DiscoveredRepo`` entries,
causing duplicate per-project fetches downstream in every provider's
work-item sync.

These tests insert raw, un-merged duplicate rows directly (no ``OPTIMIZE
TABLE ... FINAL``) and assert ``discover_repos`` already collapses them
server-side via ``argMax(..., last_synced)`` -- proving the fix does not
depend on background merges ever running.

One test (``..._tied_last_synced_returns_internally_consistent_row``) inserts
two versions sharing the exact SAME ``last_synced`` -- realistic since
``last_synced`` is only ``DateTime64(3)`` and ``insert_repo`` stamps it from
``datetime.now()``, so rapid re-syncs can land in the same millisecond. It
guards against a Frankenstein result: three independent
``argMax(col, last_synced)`` aggregates could each resolve the tie to a
*different* physical row (e.g. a new repo name paired with a stale
provider). The fix aggregates all three columns as a single
``argMax(tuple(repo, settings, provider), last_synced)`` so exactly one row
wins and all three values come from it together. Either version winning is
acceptable -- ties resolve arbitrarily, matching ReplacingMergeTree's own
tie semantics -- but the result must never mix values across versions.

Opt-in (filtered from unit/CI runs): ``pytest -m clickhouse``.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone

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
    s.ensure_schema(force=True)
    yield s
    s.close()


def _insert_repo_version(
    sink,
    *,
    repo_id: uuid.UUID,
    org_id: str,
    repo: str,
    provider: str,
    settings: str | None,
    last_synced: datetime,
) -> None:
    """Insert one raw physical version of a ``repos`` row.

    Bypasses ``insert_repo`` (which always stamps ``last_synced=now()``) so
    tests can control ordering explicitly and simulate two pre-merge versions
    of the same ``(org_id, id)`` coexisting as separate parts.
    """
    sink.client.insert(
        "repos",
        [
            [
                repo_id,
                repo,
                None,  # ref
                last_synced,  # created_at (reused; irrelevant to this test)
                settings,
                None,  # tags
                last_synced,
                provider,
                org_id,
            ]
        ],
        column_names=[
            "id",
            "repo",
            "ref",
            "created_at",
            "settings",
            "tags",
            "last_synced",
            "provider",
            "org_id",
        ],
    )


def _cleanup(sink, org_id: str) -> None:
    sink.client.command(
        "ALTER TABLE repos DELETE WHERE org_id = {org_id:String} "
        "SETTINGS mutations_sync=2",
        parameters={"org_id": org_id},
    )


def test_discover_repos_returns_latest_version_when_duplicates_exist(sink) -> None:
    """Two un-merged versions of the same (org_id, id) must collapse to ONE
    DiscoveredRepo carrying the LATEST version's repo name/provider/settings."""
    from dev_health_ops.metrics.job_daily import discover_repos

    org_id = f"test-chaos-2787-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    t1 = t0 + timedelta(days=1)

    try:
        # Older version. Physical insert order is irrelevant -- only
        # last_synced determines "latest" -- so this is written first but
        # carries the EARLIER timestamp.
        _insert_repo_version(
            sink,
            repo_id=repo_id,
            org_id=org_id,
            repo="org/old-name",
            provider="unknown",
            settings='{"legacy": true}',
            last_synced=t0,
        )
        # Newer version. No merge/OPTIMIZE performed -- both rows remain
        # physically present as separate parts, exactly as they would
        # in production between merges.
        _insert_repo_version(
            sink,
            repo_id=repo_id,
            org_id=org_id,
            repo="org/new-name",
            provider="github",
            settings='{"legacy": false, "new": 1}',
            last_synced=t1,
        )

        results = discover_repos(backend="clickhouse", primary_sink=sink, org_id=org_id)

        assert len(results) == 1, (
            f"expected exactly one deduped DiscoveredRepo for repo_id={repo_id}, "
            f"got {len(results)}: {results}"
        )
        (repo,) = results
        assert repo.repo_id == repo_id
        assert repo.full_name == "org/new-name"
        assert repo.source == "github"
        assert repo.settings == '{"legacy": false, "new": 1}'
    finally:
        _cleanup(sink, org_id)


def test_discover_repos_latest_null_settings_not_masked_by_older_non_null(
    sink,
) -> None:
    """Latest version has NULL settings; older version has non-NULL settings.

    A bare ``argMax(settings, last_synced)`` SKIPS NULL values entirely, so it
    would incorrectly resurrect the OLDER non-NULL settings blob instead of
    the genuinely-NULL latest value. This pins the outer
    ``tuple(repo, settings, provider)`` wrap (never NULL itself, even when
    its ``settings`` element is) that fixes that (CHAOS-2787)."""
    from dev_health_ops.metrics.job_daily import discover_repos

    org_id = f"test-chaos-2787-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    t1 = t0 + timedelta(days=1)

    try:
        _insert_repo_version(
            sink,
            repo_id=repo_id,
            org_id=org_id,
            repo="org/repo",
            provider="github",
            settings='{"had": "settings"}',
            last_synced=t0,
        )
        _insert_repo_version(
            sink,
            repo_id=repo_id,
            org_id=org_id,
            repo="org/repo",
            provider="github",
            settings=None,
            last_synced=t1,
        )

        results = discover_repos(backend="clickhouse", primary_sink=sink, org_id=org_id)

        assert len(results) == 1
        (repo,) = results
        assert repo.settings == {}, (
            f"expected the latest (NULL) settings to survive as {{}}, got "
            f"{repo.settings!r} -- a bare argMax(settings, last_synced) would "
            "incorrectly resurrect the older non-NULL value here"
        )
    finally:
        _cleanup(sink, org_id)


def test_discover_repos_distinct_repo_ids_each_returned_once(sink) -> None:
    """Two distinct repo ids under the same org must each surface exactly
    once, with no cross-id contamination from the GROUP BY (org_id, id)."""
    from dev_health_ops.metrics.job_daily import discover_repos

    org_id = f"test-chaos-2787-{uuid.uuid4()}"
    repo_id_a = uuid.uuid4()
    repo_id_b = uuid.uuid4()
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)

    try:
        _insert_repo_version(
            sink,
            repo_id=repo_id_a,
            org_id=org_id,
            repo="org/repo-a",
            provider="github",
            settings=None,
            last_synced=now,
        )
        _insert_repo_version(
            sink,
            repo_id=repo_id_b,
            org_id=org_id,
            repo="org/repo-b",
            provider="gitlab",
            settings=None,
            last_synced=now,
        )

        results = discover_repos(backend="clickhouse", primary_sink=sink, org_id=org_id)

        by_id = {r.repo_id: r for r in results}
        assert set(by_id) == {repo_id_a, repo_id_b}, (
            f"expected both distinct repo ids to survive independently, got {by_id}"
        )
        assert by_id[repo_id_a].full_name == "org/repo-a"
        assert by_id[repo_id_a].source == "github"
        assert by_id[repo_id_b].full_name == "org/repo-b"
        assert by_id[repo_id_b].source == "gitlab"
    finally:
        _cleanup(sink, org_id)


def test_discover_repos_tied_last_synced_returns_internally_consistent_row(
    sink,
) -> None:
    """Two versions sharing the exact SAME last_synced (a realistic tie --
    DateTime64(3) resolution, insert_repo stamps from datetime.now()) must
    not be split-brained across independent argMax aggregates.

    Before the fix, three independent argMax(col, last_synced) calls could
    each resolve the tie to a different physical row, e.g. returning
    version A's repo name with version B's provider/settings -- a
    Frankenstein row that never existed. The single
    argMax(tuple(repo, settings, provider), last_synced) aggregate
    guarantees whichever version wins, ALL three projected values come from
    that one row together. Either version winning is acceptable; only
    cross-version mixing is a bug."""
    from dev_health_ops.metrics.job_daily import discover_repos

    org_id = f"test-chaos-2787-{uuid.uuid4()}"
    repo_id = uuid.uuid4()
    tie = datetime(2026, 1, 1, tzinfo=timezone.utc)

    version_a = ("org/tie-a", "github", '{"which": "a"}')
    version_b = ("org/tie-b", "gitlab", '{"which": "b"}')

    try:
        for repo, provider, settings in (version_a, version_b):
            _insert_repo_version(
                sink,
                repo_id=repo_id,
                org_id=org_id,
                repo=repo,
                provider=provider,
                settings=settings,
                last_synced=tie,
            )

        results = discover_repos(backend="clickhouse", primary_sink=sink, org_id=org_id)

        assert len(results) == 1, (
            f"expected exactly one deduped DiscoveredRepo under a last_synced "
            f"tie, got {len(results)}: {results}"
        )
        (repo,) = results
        got = (repo.full_name, repo.source, repo.settings)
        assert got in (version_a, version_b), (
            f"result {got!r} does not match EITHER tied version whole -- "
            f"expected {version_a!r} or {version_b!r}, got a mix of both "
            "(three independent argMax aggregates resolved the tie "
            "differently per column)"
        )
    finally:
        _cleanup(sink, org_id)
