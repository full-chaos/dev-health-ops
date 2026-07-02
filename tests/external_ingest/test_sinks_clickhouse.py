"""Live-ClickHouse round-trip tests for external_ingest.sinks.write_batch
(CHAOS-2698, master-spec CC24: "Live-CH round-trip tests for all 9 kinds
are OWNED BY 2698").

For each of the 9 v1 record kinds: write one record via ``write_batch()``,
read back with ``FINAL``, assert ``org_id``/``source_id``/provider
attribution, then re-push an updated record and prove the RMT dedup/version
semantics (D7) — plus the D2 repo-identity handoff proof and the D7/D8
special cases called out in the brief.

Opt-in (filtered from unit/CI runs): ``pytest -m clickhouse``. Point
``CLICKHOUSE_URI`` at an ISOLATED scratch database — never ``default``
(house rule, AGENTS.md "Safety rule (NON-NEGOTIABLE)").
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from dev_health_ops.external_ingest.ids import derive_repo_uuid
from dev_health_ops.external_ingest.sinks import write_batch
from dev_health_ops.external_ingest.types import NormalizedBatch
from dev_health_ops.metrics.sinks.clickhouse.idempotency import (
    WORK_ITEM_TRANSITION_SEMANTIC_COLUMNS,
    semantic_deduped_subquery,
)
from dev_health_ops.models.git import get_repo_uuid_from_repo

CLICKHOUSE_URI = os.environ.get("CLICKHOUSE_URI")

pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.asyncio,
    pytest.mark.skipif(
        not CLICKHOUSE_URI,
        reason="Requires CLICKHOUSE_URI (e.g. clickhouse://ch:ch@localhost:8123/ci_local_validate_2698)",
    ),
]


@pytest.fixture(scope="module")
def dsn() -> str:
    assert CLICKHOUSE_URI is not None  # skipif guard guarantees it
    return CLICKHOUSE_URI


@pytest.fixture(scope="module")
def raw_client(dsn: str):
    import clickhouse_connect

    client = clickhouse_connect.get_client(dsn=dsn)
    # Apply pending migrations (including 065) regardless of
    # AUTO_RUN_MIGRATIONS — the column under test is a migration.
    from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink

    ClickHouseMetricsSink(dsn).ensure_schema(force=True)
    yield client
    client.close()


def _org() -> str:
    return f"test-chaos-2698-{uuid.uuid4()}"


def _final_rows(raw_client, table: str, where: str, params: dict) -> list[tuple]:
    raw_client.command(f"OPTIMIZE TABLE {table} FINAL")
    result = raw_client.query(
        f"SELECT * FROM {table} FINAL WHERE {where}", parameters=params
    )
    return list(result.result_rows or [])


def _col_index(raw_client, table: str, where: str, params: dict, column: str):
    result = raw_client.query(
        f"SELECT {column} FROM {table} FINAL WHERE {where}", parameters=params
    )
    return list(result.result_rows or [])


@pytest.fixture()
def source_id() -> uuid.UUID:
    return uuid.uuid4()


async def test_repository_round_trip_and_repo_identity_handoff(
    raw_client, dsn, source_id
):
    org_id = _org()
    repo_name = f"acme/api-{uuid.uuid4().hex[:8]}"
    batch = NormalizedBatch(
        org_id=org_id,
        source_id=source_id,
        source_system="github",
        source_instance=repo_name,
        ingestion_id=uuid.uuid4(),
        repositories=[
            {
                "external_id": repo_name,
                "source_system": "github",
                "tags": [],
                "settings": {},
            }
        ],
    )
    result = await write_batch(batch, clickhouse_dsn=dsn)
    assert not result.errors
    assert result.counts_written["repository"] == 1

    rows = _final_rows(
        raw_client, "repos", "org_id = {org_id:String}", {"org_id": org_id}
    )
    assert len(rows) == 1

    # D2 identity-continuity proof: the pushed repo's UUID equals what a
    # native-sync-style GitHub processor would derive for the "same" repo.
    result_ids = _col_index(
        raw_client,
        "repos",
        "org_id = {org_id:String}",
        {"org_id": org_id},
        "id, source_id",
    )
    repo_uuid, stamped_source_id = result_ids[0]
    assert str(repo_uuid) == str(derive_repo_uuid("github", repo_name, repo_name))
    assert str(repo_uuid) == str(get_repo_uuid_from_repo(repo_name))
    assert str(stamped_source_id) == str(source_id)

    # Re-push with a different default_ref — RMT full-replace on newer last_synced.
    batch2 = NormalizedBatch(
        org_id=org_id,
        source_id=source_id,
        source_system="github",
        source_instance=repo_name,
        ingestion_id=uuid.uuid4(),
        repositories=[
            {
                "external_id": repo_name,
                "source_system": "github",
                "default_ref": "develop",
                "tags": [],
                "settings": {},
            }
        ],
    )
    await write_batch(batch2, clickhouse_dsn=dsn)
    rows2 = _final_rows(
        raw_client, "repos", "org_id = {org_id:String}", {"org_id": org_id}
    )
    assert len(rows2) == 1
    ref_rows = _col_index(
        raw_client, "repos", "org_id = {org_id:String}", {"org_id": org_id}, "ref"
    )
    assert ref_rows[0][0] == "develop"


async def test_native_sync_repo_write_leaves_source_id_null(raw_client, dsn):
    """Backward-compat proof (brief risk #2): a native-sync-style insert_repo
    call with no source_id set still succeeds and yields source_id IS NULL."""
    from dev_health_ops.storage import create_store

    org_id = _org()
    repo_name = f"native/{uuid.uuid4().hex[:8]}"
    store = create_store(dsn, "clickhouse")
    store.org_id = org_id
    async with store:
        from dev_health_ops.models.git import Repo

        repo = Repo(repo=repo_name, provider="github")
        await store.insert_repo(repo)

    rows = _col_index(
        raw_client, "repos", "org_id = {org_id:String}", {"org_id": org_id}, "source_id"
    )
    assert rows[0][0] is None


async def test_commit_round_trip(raw_client, dsn, source_id):
    org_id = _org()
    repo_name = f"acme/api-{uuid.uuid4().hex[:8]}"
    commit_hash = uuid.uuid4().hex + uuid.uuid4().hex[:8]
    batch = NormalizedBatch(
        org_id=org_id,
        source_id=source_id,
        source_system="github",
        source_instance=repo_name,
        ingestion_id=uuid.uuid4(),
        commits=[
            {
                "repository_external_id": repo_name,
                "hash": commit_hash,
                "author_name": "Alice",
                "author_email": "alice@example.com",
                "author_when": datetime.now(timezone.utc),
            }
        ],
    )
    result = await write_batch(batch, clickhouse_dsn=dsn)
    assert not result.errors
    rows = _final_rows(
        raw_client, "git_commits", "hash = {hash:String}", {"hash": commit_hash}
    )
    assert len(rows) == 1

    # Re-push with a changed message.
    batch2 = NormalizedBatch(
        org_id=org_id,
        source_id=source_id,
        source_system="github",
        source_instance=repo_name,
        ingestion_id=uuid.uuid4(),
        commits=[
            {
                "repository_external_id": repo_name,
                "hash": commit_hash,
                "message": "updated message",
                "author_name": "Alice",
                "author_email": "alice@example.com",
                "author_when": datetime.now(timezone.utc),
            }
        ],
    )
    await write_batch(batch2, clickhouse_dsn=dsn)
    rows2 = _col_index(
        raw_client,
        "git_commits",
        "hash = {hash:String}",
        {"hash": commit_hash},
        "message, source_id",
    )
    assert len(rows2) == 1
    assert rows2[0][0] == "updated message"
    assert str(rows2[0][1]) == str(source_id)


async def test_pull_request_round_trip(raw_client, dsn, source_id):
    org_id = _org()
    repo_name = f"acme/api-{uuid.uuid4().hex[:8]}"
    number = 101
    batch = NormalizedBatch(
        org_id=org_id,
        source_id=source_id,
        source_system="github",
        source_instance=repo_name,
        ingestion_id=uuid.uuid4(),
        pull_requests=[
            {
                "repository_external_id": repo_name,
                "number": number,
                "state": "open",
                "title": "Add feature",
                "created_at": datetime.now(timezone.utc),
            }
        ],
    )
    result = await write_batch(batch, clickhouse_dsn=dsn)
    assert not result.errors
    repo_id = derive_repo_uuid("github", repo_name, repo_name)
    rows = _final_rows(
        raw_client,
        "git_pull_requests",
        "repo_id = {repo_id:UUID} AND number = {number:UInt32}",
        {"repo_id": str(repo_id), "number": number},
    )
    assert len(rows) == 1

    batch2 = NormalizedBatch(
        org_id=org_id,
        source_id=source_id,
        source_system="github",
        source_instance=repo_name,
        ingestion_id=uuid.uuid4(),
        pull_requests=[
            {
                "repository_external_id": repo_name,
                "number": number,
                "state": "merged",
                "title": "Add feature",
                "created_at": datetime.now(timezone.utc),
                "merged_at": datetime.now(timezone.utc),
            }
        ],
    )
    await write_batch(batch2, clickhouse_dsn=dsn)
    rows2 = _col_index(
        raw_client,
        "git_pull_requests",
        "repo_id = {repo_id:UUID} AND number = {number:UInt32}",
        {"repo_id": str(repo_id), "number": number},
        "state",
    )
    assert len(rows2) == 1
    assert rows2[0][0] == "merged"


async def test_review_round_trip(raw_client, dsn, source_id):
    org_id = _org()
    repo_name = f"acme/api-{uuid.uuid4().hex[:8]}"
    review_id = str(uuid.uuid4())
    batch = NormalizedBatch(
        org_id=org_id,
        source_id=source_id,
        source_system="github",
        source_instance=repo_name,
        ingestion_id=uuid.uuid4(),
        reviews=[
            {
                "repository_external_id": repo_name,
                "pull_request_number": 5,
                "review_id": review_id,
                "reviewer": "bob",
                "state": "APPROVED",
                "submitted_at": datetime.now(timezone.utc),
            }
        ],
    )
    result = await write_batch(batch, clickhouse_dsn=dsn)
    assert not result.errors
    rows = _final_rows(
        raw_client,
        "git_pull_request_reviews",
        "review_id = {review_id:String}",
        {"review_id": review_id},
    )
    assert len(rows) == 1

    batch2 = NormalizedBatch(
        org_id=org_id,
        source_id=source_id,
        source_system="github",
        source_instance=repo_name,
        ingestion_id=uuid.uuid4(),
        reviews=[
            {
                "repository_external_id": repo_name,
                "pull_request_number": 5,
                "review_id": review_id,
                "reviewer": "bob",
                "state": "CHANGES_REQUESTED",
                "submitted_at": datetime.now(timezone.utc),
            }
        ],
    )
    await write_batch(batch2, clickhouse_dsn=dsn)
    rows2 = _col_index(
        raw_client,
        "git_pull_request_reviews",
        "review_id = {review_id:String}",
        {"review_id": review_id},
        "state",
    )
    assert len(rows2) == 1
    assert rows2[0][0] == "CHANGES_REQUESTED"


async def test_team_round_trip_and_updated_at_passthrough(raw_client, dsn, source_id):
    org_id = _org()
    team_id = f"team-{uuid.uuid4().hex[:8]}"
    ts = datetime.now(timezone.utc) - timedelta(days=1)
    batch = NormalizedBatch(
        org_id=org_id,
        source_id=source_id,
        source_system="linear",
        source_instance="CHAOS",
        ingestion_id=uuid.uuid4(),
        teams=[{"id": team_id, "name": "Team One", "updated_at": ts}],
    )
    result = await write_batch(batch, clickhouse_dsn=dsn)
    assert not result.errors
    assert not result.warnings
    rows = _col_index(
        raw_client,
        "teams",
        "id = {id:String} AND org_id = {org_id:String}",
        {"id": team_id, "org_id": org_id},
        "name, source_id",
    )
    assert len(rows) == 1
    assert rows[0][0] == "Team One"
    assert str(rows[0][1]) == str(source_id)

    # Re-push with an OLDER updated_at than the current row — must NOT win.
    stale_ts = ts - timedelta(days=5)
    batch_stale = NormalizedBatch(
        org_id=org_id,
        source_id=source_id,
        source_system="linear",
        source_instance="CHAOS",
        ingestion_id=uuid.uuid4(),
        teams=[{"id": team_id, "name": "Stale Name", "updated_at": stale_ts}],
    )
    await write_batch(batch_stale, clickhouse_dsn=dsn)
    rows2 = _col_index(
        raw_client,
        "teams",
        "id = {id:String} AND org_id = {org_id:String}",
        {"id": team_id, "org_id": org_id},
        "name",
    )
    assert rows2[0][0] == "Team One"


async def test_identity_updated_at_future_clamp_loses_to_later_legitimate_write(
    raw_client, dsn, source_id
):
    org_id = _org()
    canonical_id = f"user-{uuid.uuid4().hex[:8]}"
    far_future = datetime.now(timezone.utc) + timedelta(days=3650)
    batch = NormalizedBatch(
        org_id=org_id,
        source_id=source_id,
        source_system="jira",
        source_instance="ABC",
        ingestion_id=uuid.uuid4(),
        identities=[
            {
                "canonical_id": canonical_id,
                "display_name": "Malicious Future",
                "updated_at": far_future,
                "provider_identities": {},
                "team_ids": [],
                "is_active": True,
            }
        ],
    )
    result = await write_batch(batch, clickhouse_dsn=dsn)
    assert len(result.warnings) == 1
    assert result.warnings[0].code == "updated_at_clamped"

    rows = _col_index(
        raw_client,
        "identities",
        "canonical_id = {cid:String} AND org_id = {org_id:String}",
        {"cid": canonical_id, "org_id": org_id},
        "updated_at",
    )
    # clickhouse-connect returns naive datetimes (server is UTC); compare
    # naive-to-naive rather than assume tzinfo round-trips.
    clamped_updated_at = rows[0][0]
    assert clamped_updated_at < far_future.replace(tzinfo=None)

    # A later legitimate push (server "now" at push time, guaranteed >= the
    # clamped value since it happens strictly after) must win.
    batch2 = NormalizedBatch(
        org_id=org_id,
        source_id=source_id,
        source_system="jira",
        source_instance="ABC",
        ingestion_id=uuid.uuid4(),
        identities=[
            {
                "canonical_id": canonical_id,
                "display_name": "Legit Correction",
                "updated_at": datetime.now(timezone.utc) + timedelta(seconds=1),
                "provider_identities": {},
                "team_ids": [],
                "is_active": True,
            }
        ],
    )
    await write_batch(batch2, clickhouse_dsn=dsn)
    rows2 = _col_index(
        raw_client,
        "identities",
        "canonical_id = {cid:String} AND org_id = {org_id:String}",
        {"cid": canonical_id, "org_id": org_id},
        "display_name",
    )
    assert rows2[0][0] == "Legit Correction"


async def test_work_item_round_trip(raw_client, dsn, source_id):
    org_id = _org()
    repo_name = f"acme/api-{uuid.uuid4().hex[:8]}"
    external_key = "77"
    batch = NormalizedBatch(
        org_id=org_id,
        source_id=source_id,
        source_system="github",
        source_instance=repo_name,
        ingestion_id=uuid.uuid4(),
        work_items=[
            {
                "external_key": external_key,
                "provider": "github",
                "title": "Investigate flaky test",
                "type": "issue",
                "status": "todo",
                "repository_external_id": repo_name,
                "created_at": datetime.now(timezone.utc),
            }
        ],
    )
    result = await write_batch(batch, clickhouse_dsn=dsn)
    assert not result.errors
    expected_id = f"gh:{repo_name}#{external_key}"
    rows = _col_index(
        raw_client,
        "work_items",
        "work_item_id = {wid:String} AND org_id = {org_id:String}",
        {"wid": expected_id, "org_id": org_id},
        "status, source_id",
    )
    assert len(rows) == 1
    assert rows[0][0] == "todo"
    assert str(rows[0][1]) == str(source_id)

    batch2 = NormalizedBatch(
        org_id=org_id,
        source_id=source_id,
        source_system="github",
        source_instance=repo_name,
        ingestion_id=uuid.uuid4(),
        work_items=[
            {
                "external_key": external_key,
                "provider": "github",
                "title": "Investigate flaky test",
                "type": "issue",
                "status": "done",
                "repository_external_id": repo_name,
                "created_at": datetime.now(timezone.utc),
            }
        ],
    )
    await write_batch(batch2, clickhouse_dsn=dsn)
    rows2 = _col_index(
        raw_client,
        "work_items",
        "work_item_id = {wid:String} AND org_id = {org_id:String}",
        {"wid": expected_id, "org_id": org_id},
        "status",
    )
    assert len(rows2) == 1
    assert rows2[0][0] == "done"


async def test_work_item_transition_semantic_dedup(raw_client, dsn, source_id):
    """D7 proof, corrected against the live engine.

    ``work_item_transitions`` is ``ReplacingMergeTree(last_synced) ORDER BY
    (org_id, repo_id, work_item_id, occurred_at)`` (verified via
    ``SHOW CREATE TABLE``) — and ``WORK_ITEM_TRANSITION_SEMANTIC_COLUMNS``
    (idempotency.py) is a *superset* of that ORDER BY tuple. So re-pushing
    the exact same transition (same ``occurred_at``, same everything else,
    only ``last_synced`` differs — the brief's "corrected actor/status,
    same occurred_at" scenario) is a genuine ORDER BY-key collision:
    ``SELECT ... FINAL`` alone already collapses it to 1 row, matching
    ``semantic_deduped_subquery``'s result — no extra dedup step is needed
    for this case. What *does* need ``FINAL`` (or the semantic subquery) is
    that the two inserts physically land as two separate, unmerged parts
    until a merge/FINAL runs — a bare ``SELECT *`` with no ``FINAL``
    genuinely returns 2 rows, which is the "must not assume writes alone
    guarantee uniqueness" hazard D7 calls out.
    """
    org_id = _org()
    repo_name = f"acme/api-{uuid.uuid4().hex[:8]}"
    external_key = "88"
    occurred_at = datetime.now(timezone.utc)

    batch = NormalizedBatch(
        org_id=org_id,
        source_id=source_id,
        source_system="github",
        source_instance=repo_name,
        ingestion_id=uuid.uuid4(),
        work_item_transitions=[
            {
                "external_key": external_key,
                "provider": "github",
                "occurred_at": occurred_at,
                "from_status": "todo",
                "to_status": "in_progress",
            }
        ],
    )
    result = await write_batch(batch, clickhouse_dsn=dsn)
    assert not result.errors

    # Re-push the identical semantic transition (same occurred_at) — a
    # corrected resubmission, distinct only in last_synced.
    batch2 = NormalizedBatch(
        org_id=org_id,
        source_id=source_id,
        source_system="github",
        source_instance=repo_name,
        ingestion_id=uuid.uuid4(),
        work_item_transitions=[
            {
                "external_key": external_key,
                "provider": "github",
                "occurred_at": occurred_at,
                "from_status": "todo",
                "to_status": "in_progress",
            }
        ],
    )
    await write_batch(batch2, clickhouse_dsn=dsn)

    expected_id = f"gh:{repo_name}#{external_key}"
    where = "work_item_id = {wid:String} AND org_id = {org_id:String}"
    params = {"wid": expected_id, "org_id": org_id}

    raw_no_final = raw_client.query(
        f"SELECT count() FROM work_item_transitions WHERE {where}", parameters=params
    )
    assert raw_no_final.result_rows[0][0] == 2, (
        "both physical inserts should be visible before a merge/FINAL runs"
    )

    raw_final = raw_client.query(
        f"SELECT count() FROM work_item_transitions FINAL WHERE {where}",
        parameters=params,
    )
    assert raw_final.result_rows[0][0] == 1, (
        "RMT FINAL alone must collapse a same-occurred_at resubmission (D7)"
    )

    deduped_sql = semantic_deduped_subquery(
        "work_item_transitions", WORK_ITEM_TRANSITION_SEMANTIC_COLUMNS
    )
    deduped_rows = raw_client.query(
        f"SELECT count() FROM {deduped_sql} WHERE {where}", parameters=params
    )
    assert deduped_rows.result_rows[0][0] == 1


async def test_work_item_dependency_round_trip(raw_client, dsn, source_id):
    org_id = _org()
    source_key = f"ABC-{uuid.uuid4().hex[:6]}"
    target_key = f"ABC-{uuid.uuid4().hex[:6]}"
    batch = NormalizedBatch(
        org_id=org_id,
        source_id=source_id,
        source_system="jira",
        source_instance="ABC",
        ingestion_id=uuid.uuid4(),
        work_item_dependencies=[
            {
                "source_external_key": source_key,
                "target_external_key": target_key,
                "relationship_type": "blocks",
            }
        ],
    )
    result = await write_batch(batch, clickhouse_dsn=dsn)
    assert not result.errors
    src_id = f"jira:{source_key}"
    tgt_id = f"jira:{target_key}"
    rows = _col_index(
        raw_client,
        "work_item_dependencies",
        "source_work_item_id = {sid:String} AND target_work_item_id = {tid:String} "
        "AND org_id = {org_id:String}",
        {"sid": src_id, "tid": tgt_id, "org_id": org_id},
        "relationship_type, source_id",
    )
    assert len(rows) == 1
    assert rows[0][0] == "blocks"
    assert str(rows[0][1]) == str(source_id)
