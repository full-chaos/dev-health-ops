"""Unit tests for external_ingest.sinks.write_batch (CHAOS-2698).

Pure Python + mocked clients — no live ClickHouse. Live round-trip proofs
(RMT dedup, FINAL reads, the D2 repo-identity handoff) live in
``test_sinks_clickhouse.py`` (``@pytest.mark.clickhouse``).
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dev_health_ops.external_ingest import sinks as sinks_mod
from dev_health_ops.external_ingest.feature_gate import (
    CanonicalIncidentIngestionDisabledError,
)
from dev_health_ops.external_ingest.ids import derive_repo_uuid, derive_work_item_id
from dev_health_ops.external_ingest.sinks import write_batch
from dev_health_ops.external_ingest.types import NormalizedBatch
from dev_health_ops.models.git import get_repo_uuid_from_repo


class FakeStore:
    """Fakes the async ClickHouseStore surface write_batch() uses."""

    def __init__(self) -> None:
        self.org_id: str | None = None
        self.insert_repo = AsyncMock()
        self.insert_git_commit_data = AsyncMock()
        self.insert_git_pull_requests = AsyncMock()
        self.insert_git_pull_request_reviews = AsyncMock()
        self.insert_teams = AsyncMock()
        self.insert_identities = AsyncMock()

    async def __aenter__(self) -> FakeStore:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        return None


class FakeSink:
    """Fakes the sync ClickHouseMetricsSink surface write_batch() uses."""

    def __init__(self) -> None:
        self.org_id: str | None = None
        self.write_work_items = MagicMock()
        self.write_work_item_transitions = MagicMock()
        self.write_work_item_dependencies = MagicMock()
        self.close = MagicMock()


def _awaited_args(mock: AsyncMock) -> tuple:
    """``AsyncMock.await_args`` is typed ``Optional`` — narrow it for the
    unpacking call sites below (a mock that was never awaited is a real
    test bug, not a case to silently tolerate)."""
    assert mock.await_args is not None
    return mock.await_args


def _base_batch(**overrides: object) -> NormalizedBatch:
    defaults: dict[str, object] = dict(
        org_id="org-1",
        source_id=uuid.uuid4(),
        source_system="github",
        source_instance="acme/api",
        ingestion_id=uuid.uuid4(),
    )
    defaults.update(overrides)
    return NormalizedBatch(**defaults)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_operational_write_fails_closed_before_opening_clickhouse() -> None:
    batch = _base_batch(operational_incidents=[object()])
    canonical_allowed = AsyncMock(return_value=False)
    with (
        patch.object(
            sinks_mod,
            "_operational_ingestion_allowed",
            canonical_allowed,
            create=True,
        ),
        patch.object(sinks_mod, "create_store") as create_store,
        pytest.raises(
            CanonicalIncidentIngestionDisabledError, match="feature_disabled"
        ),
    ):
        await write_batch(batch, clickhouse_dsn="clickhouse://x/y")

    canonical_allowed.assert_awaited_once_with(batch.org_id)
    create_store.assert_not_called()


@pytest.mark.asyncio
async def test_write_batch_repo_uuid_matches_native_sync_case_insensitively() -> None:
    batch = _base_batch(
        repositories=[
            {
                "external_id": "Owner/Repo",
                "source_system": "github",
                "default_ref": "main",
                "tags": [],
                "settings": {},
            }
        ],
    )
    fake_store = FakeStore()
    with patch(
        "dev_health_ops.external_ingest.sinks.create_store", return_value=fake_store
    ):
        result = await write_batch(batch, clickhouse_dsn="clickhouse://x/y")

    fake_store.insert_repo.assert_awaited_once()
    (repo_obj,), _kwargs = _awaited_args(fake_store.insert_repo)
    assert repo_obj.id == get_repo_uuid_from_repo("owner/repo")
    assert result.counts_written["repository"] == 1
    assert repo_obj.id in result.affected_scope.repo_ids


@pytest.mark.asyncio
async def test_write_batch_stamps_org_id_on_work_item_family_rows() -> None:
    batch = _base_batch(
        work_items=[
            {
                "external_key": "42",
                "provider": "github",
                "title": "Fix bug",
                "type": "issue",
                "status": "todo",
                "repository_external_id": "acme/api",
                "created_at": datetime.now(timezone.utc),
            }
        ],
        work_item_transitions=[
            {
                "external_key": "42",
                "provider": "github",
                "occurred_at": datetime.now(timezone.utc),
                "from_status": "todo",
                "to_status": "in_progress",
            }
        ],
    )
    fake_sink = FakeSink()
    with patch(
        "dev_health_ops.external_ingest.sinks.create_sink", return_value=fake_sink
    ):
        result = await write_batch(batch, clickhouse_dsn="clickhouse://x/y")

    fake_sink.write_work_items.assert_called_once()
    (work_item_rows,), _ = fake_sink.write_work_items.call_args
    assert work_item_rows[0]["org_id"] == "org-1"
    assert work_item_rows[0]["work_item_id"] == "gh:acme/api#42"

    fake_sink.write_work_item_transitions.assert_called_once()
    (transition_rows,), _ = fake_sink.write_work_item_transitions.call_args
    assert transition_rows[0]["org_id"] == "org-1"
    assert result.counts_written["work_item"] == 1
    assert result.counts_written["work_item_transition"] == 1


@pytest.mark.asyncio
async def test_write_batch_dependency_uses_dataclass_row_with_org_and_source_id() -> (
    None
):
    batch = _base_batch(
        source_system="jira",
        source_instance="ABC",
        work_item_dependencies=[
            {
                "source_external_key": "ABC-1",
                "target_external_key": "ABC-2",
                "relationship_type": "blocks",
            }
        ],
    )
    fake_sink = FakeSink()
    with patch(
        "dev_health_ops.external_ingest.sinks.create_sink", return_value=fake_sink
    ):
        result = await write_batch(batch, clickhouse_dsn="clickhouse://x/y")

    fake_sink.write_work_item_dependencies.assert_called_once()
    (deps,), _ = fake_sink.write_work_item_dependencies.call_args
    assert deps[0].source_work_item_id == "jira:ABC-1"
    assert deps[0].target_work_item_id == "jira:ABC-2"
    assert deps[0].org_id == "org-1"
    assert deps[0].source_id == batch.source_id
    assert result.counts_written["work_item_dependency"] == 1


@pytest.mark.asyncio
async def test_identity_and_team_updated_at_pass_through_verbatim() -> None:
    ts = datetime.now(timezone.utc) - timedelta(days=3)
    batch = _base_batch(
        identities=[
            {
                "canonical_id": "u1",
                "updated_at": ts,
                "provider_identities": {},
                "team_ids": [],
                "is_active": True,
            }
        ],
        teams=[
            {
                "id": "t1",
                "name": "Team One",
                "updated_at": ts,
            }
        ],
    )
    fake_store = FakeStore()
    with patch(
        "dev_health_ops.external_ingest.sinks.create_store", return_value=fake_store
    ):
        result = await write_batch(batch, clickhouse_dsn="clickhouse://x/y")

    (identity_rows,), _ = _awaited_args(fake_store.insert_identities)
    assert identity_rows[0]["updated_at"] == ts
    (team_rows,), _ = _awaited_args(fake_store.insert_teams)
    assert team_rows[0]["updated_at"] == ts
    assert not result.warnings


@pytest.mark.asyncio
async def test_identity_updated_at_future_is_clamped_with_warning() -> None:
    future = datetime.now(timezone.utc) + timedelta(hours=1)
    batch = _base_batch(
        identities=[
            {
                "canonical_id": "u1",
                "updated_at": future,
                "provider_identities": {},
                "team_ids": [],
                "is_active": True,
            }
        ],
    )
    fake_store = FakeStore()
    with patch(
        "dev_health_ops.external_ingest.sinks.create_store", return_value=fake_store
    ):
        result = await write_batch(batch, clickhouse_dsn="clickhouse://x/y")

    (identity_rows,), _ = _awaited_args(fake_store.insert_identities)
    assert identity_rows[0]["updated_at"] < future
    assert len(result.warnings) == 1
    assert result.warnings[0].code == "updated_at_clamped"
    assert result.warnings[0].kind == "identity"


@pytest.mark.asyncio
async def test_identity_updated_at_within_skew_is_not_clamped() -> None:
    near_future = datetime.now(timezone.utc) + timedelta(minutes=2)
    batch = _base_batch(
        identities=[
            {
                "canonical_id": "u1",
                "updated_at": near_future,
                "provider_identities": {},
                "team_ids": [],
                "is_active": True,
            }
        ],
    )
    fake_store = FakeStore()
    with patch(
        "dev_health_ops.external_ingest.sinks.create_store", return_value=fake_store
    ):
        result = await write_batch(batch, clickhouse_dsn="clickhouse://x/y")

    (identity_rows,), _ = _awaited_args(fake_store.insert_identities)
    assert identity_rows[0]["updated_at"] == near_future
    assert not result.warnings


@pytest.mark.asyncio
async def test_one_failed_kind_does_not_block_other_kinds() -> None:
    batch = _base_batch(
        pull_requests=[
            {
                "repository_external_id": "acme/api",
                "number": 1,
                "state": "open",
                "created_at": datetime.now(timezone.utc),
            }
        ],
        commits=[
            {
                "repository_external_id": "acme/api",
                "hash": "a" * 40,
                "author_when": datetime.now(timezone.utc),
            }
        ],
    )
    fake_store = FakeStore()
    fake_store.insert_git_pull_requests.side_effect = RuntimeError("boom")
    with patch(
        "dev_health_ops.external_ingest.sinks.create_store", return_value=fake_store
    ):
        result = await write_batch(batch, clickhouse_dsn="clickhouse://x/y")

    assert result.counts_written.get("pull_request") is None
    assert any(
        e.kind == "pull_request" and e.code == "clickhouse_insert_failed"
        for e in result.errors
    )
    fake_store.insert_git_commit_data.assert_awaited_once()
    assert result.counts_written["commit"] == 1


@pytest.mark.asyncio
async def test_affected_scope_aggregates_across_mixed_batch() -> None:
    batch = _base_batch(
        repositories=[{"external_id": "acme/api", "source_system": "github"}],
        pull_requests=[
            {
                "repository_external_id": "acme/api",
                "number": 7,
                "state": "open",
                "created_at": datetime.now(timezone.utc),
            }
        ],
        work_items=[
            {
                "external_key": "42",
                "provider": "github",
                "title": "Fix bug",
                "type": "issue",
                "status": "todo",
                "repository_external_id": "acme/api",
                "created_at": datetime.now(timezone.utc),
            }
        ],
    )
    fake_store = FakeStore()
    fake_sink = FakeSink()
    with (
        patch(
            "dev_health_ops.external_ingest.sinks.create_store", return_value=fake_store
        ),
        patch(
            "dev_health_ops.external_ingest.sinks.create_sink", return_value=fake_sink
        ),
    ):
        result = await write_batch(batch, clickhouse_dsn="clickhouse://x/y")

    scope = result.affected_scope
    assert scope.record_kinds == {"repository", "pull_request", "work_item"}
    expected_repo_id = derive_repo_uuid("github", "acme/api", "acme/api")
    assert scope.repo_ids == {expected_repo_id}
    assert scope.work_item_ids == {"gh:acme/api#42"}
    assert scope.org_id == "org-1"
    assert scope.source_systems == {"github"}
    assert scope.source_instances == {"acme/api"}


@pytest.mark.parametrize(
    "system,instance,external_key,work_item_type,expected",
    [
        ("github", "owner/repo", "42", None, "gh:owner/repo#42"),
        ("github", "owner/repo", "42", "pr", "ghpr:owner/repo#42"),
        ("gitlab", "group/project", "7", None, "gitlab:group/project#7"),
        ("gitlab", "group/project", "7", "merge_request", "gitlab:group/project!7"),
        ("jira", None, "ABC-123", None, "jira:ABC-123"),
        ("linear", None, "CHAOS-1", None, "linear:CHAOS-1"),
        ("custom", "my-instance", "ext-1", None, "custom:my-instance:ext-1"),
    ],
)
def test_derive_work_item_id_matches_native_sync_formats(
    system: str,
    instance: str | None,
    external_key: str,
    work_item_type: str | None,
    expected: str,
) -> None:
    assert (
        derive_work_item_id(system, instance, external_key, work_item_type) == expected
    )


def test_derive_repo_uuid_custom_system_uses_distinct_namespace() -> None:
    real = derive_repo_uuid("github", "acme/api", "acme/api")
    custom = derive_repo_uuid("custom", "acme/api", "acme/api")
    assert real != custom
    assert custom == get_repo_uuid_from_repo("custom:acme/api:acme/api")


@pytest.mark.asyncio
@pytest.mark.parametrize("system", ["github", "gitlab", "jira"])
async def test_native_team_key_dropped_for_non_linear_work_items(system: str) -> None:
    """Codex adversarial review (CHAOS-2698): a non-Linear work_item.v1
    payload must not be able to forge a top-precedence native_team_key —
    native sync only ever populates it for Linear (WorkItem.native_team_key
    docstring: "None for GitHub/GitLab ... and Jira"). ``work_item.v1``'s
    ``provider`` field only allows jira/github/gitlab/linear (CC6 excludes
    a "custom" work-item provider), so those are the three non-Linear
    cases to prove."""
    batch = _base_batch(
        source_system=system,
        source_instance="ABC" if system == "jira" else "acme/api",
        work_items=[
            {
                "external_key": "42",
                "provider": system,
                "title": "Attribution forgery attempt",
                "type": "issue",
                "status": "todo",
                "native_team_key": "FORGED-TEAM",
                "repository_external_id": "acme/api"
                if system in ("github", "gitlab")
                else None,
            }
        ],
    )
    fake_sink = FakeSink()
    with patch(
        "dev_health_ops.external_ingest.sinks.create_sink", return_value=fake_sink
    ):
        await write_batch(batch, clickhouse_dsn="clickhouse://x/y")

    (rows,), _ = fake_sink.write_work_items.call_args
    assert rows[0]["native_team_key"] is None


@pytest.mark.asyncio
async def test_native_team_key_preserved_for_linear_work_items() -> None:
    batch = _base_batch(
        source_system="linear",
        source_instance="CHAOS",
        work_items=[
            {
                "external_key": "42",
                "provider": "linear",
                "title": "Legit Linear item",
                "type": "issue",
                "status": "todo",
                "native_team_key": "CHAOS",
            }
        ],
    )
    fake_sink = FakeSink()
    with patch(
        "dev_health_ops.external_ingest.sinks.create_sink", return_value=fake_sink
    ):
        await write_batch(batch, clickhouse_dsn="clickhouse://x/y")

    (rows,), _ = fake_sink.write_work_items.call_args
    assert rows[0]["native_team_key"] == "CHAOS"


@pytest.mark.asyncio
async def test_work_item_spoofed_provider_cannot_escape_batch_namespace() -> None:
    """Codex adversarial review, rounds 2-3 (CHAOS-2698): a github-scoped
    batch smuggling a row claiming ``provider: "linear"`` must not just lose
    ``native_team_key`` (round 2's fix) — the record must land under the
    batch's OWN provider/work_item_id namespace, not "linear:...", since
    trusting the spoofed value for derivation would let a github source
    pollute/collide with real Linear data (round 3's finding). A
    ``record_provider_mismatch`` warning surfaces the discrepancy instead of
    silently swallowing it."""
    batch = _base_batch(
        source_system="github",
        source_instance="acme/api",
        work_items=[
            {
                "external_key": "42",
                "provider": "linear",
                "title": "Spoofed provider forgery attempt",
                "type": "issue",
                "status": "todo",
                "native_team_key": "FORGED-TEAM",
                "repository_external_id": "acme/api",
            }
        ],
    )
    fake_sink = FakeSink()
    with patch(
        "dev_health_ops.external_ingest.sinks.create_sink", return_value=fake_sink
    ):
        result = await write_batch(batch, clickhouse_dsn="clickhouse://x/y")

    (rows,), _ = fake_sink.write_work_items.call_args
    assert rows[0]["native_team_key"] is None
    assert rows[0]["provider"] == "github"
    assert rows[0]["work_item_id"] == "gh:acme/api#42"
    assert any(
        w.code == "record_provider_mismatch" and w.kind == "work_item"
        for w in result.warnings
    )


@pytest.mark.asyncio
async def test_repository_external_id_mismatch_flagged_as_warning_not_rejected() -> (
    None
):
    """Codex adversarial review (CHAOS-2698): CC6 says instance-match
    rejection is CHAOS-2697's job and this layer "may assert-but-not-
    reject" — proves the assertion surfaces in warnings while the record
    still gets written (the layer trusts its documented input contract but
    makes drift from it visible)."""
    batch = _base_batch(
        source_system="github",
        source_instance="acme/real-repo",
        repositories=[
            {
                "external_id": "acme/different-repo",
                "source_system": "github",
            }
        ],
    )
    fake_store = FakeStore()
    with patch(
        "dev_health_ops.external_ingest.sinks.create_store", return_value=fake_store
    ):
        result = await write_batch(batch, clickhouse_dsn="clickhouse://x/y")

    fake_store.insert_repo.assert_awaited_once()
    assert result.counts_written["repository"] == 1
    assert any(
        w.code == "record_outside_source_instance" and w.kind == "repository"
        for w in result.warnings
    )


@pytest.mark.asyncio
async def test_pull_request_matching_source_instance_has_no_warning() -> None:
    batch = _base_batch(
        source_system="github",
        source_instance="acme/api",
        pull_requests=[
            {
                "repository_external_id": "acme/api",
                "number": 1,
                "state": "open",
                "created_at": datetime.now(timezone.utc),
            }
        ],
    )
    fake_store = FakeStore()
    with patch(
        "dev_health_ops.external_ingest.sinks.create_store", return_value=fake_store
    ):
        result = await write_batch(batch, clickhouse_dsn="clickhouse://x/y")

    assert not result.warnings
