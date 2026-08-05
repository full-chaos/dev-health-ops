"""CHAOS-3219: WORLD_DIGEST content-hash guards.

Stub-based (mirrors ``tests/test_fixtures_mixed_org_guard.py``'s
``_StubClient`` pattern) so these run in the standard unit tier with no live
ClickHouse/Postgres connection -- the live reproducibility proof (two full
`fixtures world` generations -> identical WORLD_DIGEST) is a separate,
manually-run verification against a scratch database (see the Lane 1a
report), not something the checked-in unit suite can require infra for.

Covers the four guard properties the task calls for:
  1. seed-idempotency is a property of _row_content_key/_clickhouse_table_
     digest being pure functions of (column_names, rows) -- same input,
     same digest, proven directly here.
  2. digest-drift RED: mutate one stubbed row's non-volatile field, observe
     the digest change.
  3. volatile columns (last_synced etc.) are excluded -- mutating ONLY a
     volatile column must NOT change the digest (the two-real-generations-
     apart-in-wall-clock-time reproducibility property).
  4. missing-table tolerance mirrors runner.py's existing
     _MISSING_TABLE_MARKERS convention.
"""

from __future__ import annotations

from typing import Any

import pytest

from dev_health_ops.fixtures.world import (
    _VOLATILE_COLUMNS,
    _clickhouse_table_digest,
    _diff_components,
    _row_content_key,
    _volatile_columns_for_table,
)


class _StubResult:
    def __init__(self, column_names: list[str], rows: list[tuple[Any, ...]]) -> None:
        self.column_names = column_names
        self.result_rows = rows


class _StubClient:
    def __init__(self, column_names: list[str], rows: list[tuple[Any, ...]]) -> None:
        self.column_names = column_names
        self.rows = rows
        self.queries: list[tuple[str, dict[str, Any]]] = []

    def query(self, query: str, parameters: dict[str, Any]) -> _StubResult:
        self.queries.append((query, parameters))
        return _StubResult(self.column_names, self.rows)


class _ExplodingMissingTableClient:
    def query(self, query: str, parameters: dict[str, Any]) -> _StubResult:
        raise RuntimeError("Table default.repos does not exist")


class _ExplodingConnectivityClient:
    def query(self, query: str, parameters: dict[str, Any]) -> _StubResult:
        raise RuntimeError("Connection refused: clickhouse:8123")


def test_row_content_key_excludes_volatile_columns() -> None:
    columns = ["id", "org_id", "name", "last_synced", "updated_at"]
    row = ("repo-1", "org-1", "web-app", "2026-08-01T00:00:00Z", "2026-08-01T00:00:00Z")
    key = _row_content_key(columns, row, volatile=_VOLATILE_COLUMNS)
    assert "last_synced" not in key
    assert "updated_at" not in key
    assert "id='repo-1'" in key
    assert "name='web-app'" in key


def test_row_content_key_stable_column_order_independent() -> None:
    """Digest content must not depend on which order the DB happens to
    return columns in -- the key sorts its parts."""

    columns_a = ["id", "name"]
    columns_b = ["name", "id"]
    row_a = ("repo-1", "web-app")
    row_b = ("web-app", "repo-1")
    assert _row_content_key(
        columns_a, row_a, volatile=_VOLATILE_COLUMNS
    ) == _row_content_key(columns_b, row_b, volatile=_VOLATILE_COLUMNS)


def test_row_content_key_keeps_table_specific_watermark_column() -> None:
    """Codex HIGH-3 (2026-08-05): for tables in
    ``_WATERMARK_COLUMNS_TO_KEEP_BY_TABLE``, the listed watermark column is
    NOT excluded -- it is exactly what a claimed source state (stale/
    current/measured-zero) is about, so blanket-hashing it out would let a
    regression in the aging/zeroing step go undetected forever."""

    columns = ["id", "org_id", "last_synced"]
    row = ("repo-1", "org-1", "2026-08-01T00:00:00Z")
    key = _row_content_key(
        columns, row, volatile=_volatile_columns_for_table("git_commits")
    )
    assert "last_synced='2026-08-01T00:00:00Z'" in key


def test_volatile_columns_for_table_default_excludes_watermark() -> None:
    """``_volatile_columns_for_table`` returns the EXCLUSION set. A table
    with NO entry in the keep-list (e.g. plain ``repos``) still gets the
    full blanket exclusion -- ``last_synced`` stays volatile (excluded) --
    while a table WITH an entry (``git_commits``) has it removed from the
    exclusion set, i.e. hashed. The per-table override is additive, not a
    replacement of the default behavior."""

    assert "last_synced" in _volatile_columns_for_table("repos")
    assert "last_synced" not in _volatile_columns_for_table("git_commits")


@pytest.mark.asyncio
async def test_seed_idempotency_same_rows_same_digest() -> None:
    """Two 'generations' that produce identical content-bearing rows (even
    with DIFFERENT wall-clock last_synced values, simulating two runs
    executed minutes apart) yield the SAME digest."""

    columns = ["id", "org_id", "name", "last_synced"]
    rows_run1 = [("repo-1", "org-1", "web-app", "2026-08-01T00:00:00Z")]
    rows_run2 = [("repo-1", "org-1", "web-app", "2026-08-01T00:09:12Z")]

    client1 = _StubClient(columns, rows_run1)
    client2 = _StubClient(columns, rows_run2)

    digest1 = await _clickhouse_table_digest(client1, "repos", "org-1")
    digest2 = await _clickhouse_table_digest(client2, "repos", "org-1")

    assert digest1["content_hash"] == digest2["content_hash"]
    assert digest1["row_count"] == digest2["row_count"] == 1


@pytest.mark.asyncio
async def test_digest_drift_red_on_content_mutation() -> None:
    """Mutating a NON-volatile field (status) must change the digest --
    the RED test: plant the defect, observe the digest verify fail."""

    columns = ["id", "org_id", "status"]
    original = [("wi-1", "org-1", "done")]
    mutated = [("wi-1", "org-1", "in_progress")]

    original_digest = await _clickhouse_table_digest(
        _StubClient(columns, original), "work_items", "org-1"
    )
    mutated_digest = await _clickhouse_table_digest(
        _StubClient(columns, mutated), "work_items", "org-1"
    )

    assert original_digest["content_hash"] != mutated_digest["content_hash"], (
        "digest must change when a real content field is mutated -- a "
        "digest verify that stayed GREEN here would be a guard that cannot "
        "catch drift"
    )


@pytest.mark.asyncio
async def test_digest_unaffected_by_volatile_only_mutation() -> None:
    """The inverse of the drift test: mutating ONLY a volatile column must
    NOT trip the digest (this is what makes two real generations, run at
    different real times, reproducible). Uses ``teams`` -- NOT one of the
    HIGH-3 watermark-keep tables -- deliberately: ``work_items`` (used here
    before HIGH-3) now keeps ``last_synced`` in its digest on purpose, so it
    would no longer demonstrate this property (see the paired test below,
    which asserts exactly that difference for ``work_items``)."""

    columns = ["id", "org_id", "status", "last_synced"]
    row_a = [("t-1", "org-1", "done", "2026-08-01T00:00:00Z")]
    row_b = [("t-1", "org-1", "done", "2026-08-02T12:34:56Z")]

    digest_a = await _clickhouse_table_digest(
        _StubClient(columns, row_a), "teams", "org-1"
    )
    digest_b = await _clickhouse_table_digest(
        _StubClient(columns, row_b), "teams", "org-1"
    )
    assert digest_a["content_hash"] == digest_b["content_hash"]


@pytest.mark.asyncio
async def test_digest_affected_by_watermark_mutation_on_kept_table() -> None:
    """Codex HIGH-3 (2026-08-05): the exact counterpart to the test above.
    ``work_items`` IS one of the tables whose watermark column
    (``last_synced``) is deliberately kept in the digest -- mutating ONLY
    that column now DOES trip the digest, because a "stale" claim in
    sources.json is a claim ABOUT that column."""

    columns = ["id", "org_id", "status", "last_synced"]
    row_a = [("wi-1", "org-1", "done", "2026-08-01T00:00:00Z")]
    row_b = [("wi-1", "org-1", "done", "2026-08-02T12:34:56Z")]

    digest_a = await _clickhouse_table_digest(
        _StubClient(columns, row_a), "work_items", "org-1"
    )
    digest_b = await _clickhouse_table_digest(
        _StubClient(columns, row_b), "work_items", "org-1"
    )
    assert digest_a["content_hash"] != digest_b["content_hash"], (
        "work_items.last_synced must be part of the digest -- if this "
        "passes with an unchanged hash, a regression in watermark aging "
        "for work_items would go undetected"
    )


@pytest.mark.asyncio
async def test_digest_drift_on_row_count_change() -> None:
    columns = ["id", "org_id"]
    before = [("wi-1", "org-1")]
    after = [("wi-1", "org-1"), ("wi-2", "org-1")]

    before_digest = await _clickhouse_table_digest(
        _StubClient(columns, before), "work_items", "org-1"
    )
    after_digest = await _clickhouse_table_digest(
        _StubClient(columns, after), "work_items", "org-1"
    )
    assert before_digest["row_count"] == 1
    assert after_digest["row_count"] == 2
    assert before_digest["content_hash"] != after_digest["content_hash"]


@pytest.mark.asyncio
async def test_missing_table_treated_as_empty_not_fatal() -> None:
    digest = await _clickhouse_table_digest(
        _ExplodingMissingTableClient(), "repos", "org-1"
    )
    assert digest["row_count"] == 0


@pytest.mark.asyncio
async def test_connectivity_error_fails_closed() -> None:
    """A transient outage must not be silently treated as 'zero rows' --
    mirrors runner.py's _detect_live_providers fail-closed contract."""

    with pytest.raises(RuntimeError, match="Connection refused"):
        await _clickhouse_table_digest(_ExplodingConnectivityClient(), "repos", "org-1")


def test_diff_components_reports_only_drifted_tables() -> None:
    pinned = {
        "clickhouse": {
            "repos": {"primary": {"row_count": 3, "content_hash": "aaa"}},
            "teams": {"primary": {"row_count": 1, "content_hash": "bbb"}},
        }
    }
    live = {
        "clickhouse": {
            "repos": {"primary": {"row_count": 3, "content_hash": "aaa"}},
            "teams": {"primary": {"row_count": 1, "content_hash": "ccc"}},
        }
    }
    drifted = _diff_components(pinned, live)
    assert drifted == ["clickhouse.teams"]


def test_diff_components_empty_when_identical() -> None:
    doc = {"clickhouse": {"repos": {"primary": {"row_count": 1, "content_hash": "x"}}}}
    assert _diff_components(doc, doc) == []


def test_volatile_columns_cover_every_timestamp_this_world_writes() -> None:
    # Documents the exact exclusion set relied on above -- a change here is
    # a deliberate contract change, not an incidental typo.
    assert _VOLATILE_COLUMNS == frozenset(
        {
            "last_synced",
            "updated_at",
            "created_at",
            "computed_at",
            "synced_at",
            "started_at",
            "ended_at",
            "queued_at",
            "finished_at",
            "last_sync_at",
            "feature_id",
            "categorization_run_id",
        }
    )
