"""Unit tests for ``schedule_or_coalesce()`` (CHAOS-2699, brief D3/D10).

Uses ``fakeredis.FakeValkey`` (same fixture family as
``tests/test_ingest_streams.py``) instead of a live Valkey instance.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

fakeredis = pytest.importorskip("fakeredis")
from fakeredis import FakeServer, FakeValkey  # noqa: E402

import dev_health_ops.external_ingest.recompute as recompute_mod  # noqa: E402
from dev_health_ops.external_ingest.recompute import schedule_or_coalesce  # noqa: E402

ORG = "org-1"
SYSTEM = "github"
INSTANCE = "acme/api"
PENDING_KEY = f"external-ingest:recompute:pending:{ORG}:{SYSTEM}:{INSTANCE}"
GUARD_KEY = f"external-ingest:recompute:scheduled:{ORG}:{SYSTEM}:{INSTANCE}"


def _fake_client() -> FakeValkey:
    return FakeValkey(decode_responses=True)


def _ttl(client: FakeValkey, key: str) -> int:
    # FakeValkey's stubs type sync-mode calls as `Awaitable[Any] | Any`
    # (the same client class backs fakeredis's async mode); narrow to the
    # actual sync return type instead of asserting past it.
    value = client.ttl(key)
    assert isinstance(value, int)
    return value


def _get_str(client: FakeValkey, key: str) -> str | None:
    value = client.get(key)
    assert value is None or isinstance(value, str)
    return value


def _call(
    client,
    *,
    ingestion_id="ing-1",
    repo_ids=None,
    team_ids=None,
    record_kinds=None,
    window_start=None,
    window_end=None,
):
    with patch(
        "dev_health_ops.external_ingest.recompute._get_redis_client",
        return_value=client,
    ):
        mock_task = MagicMock()
        with patch(
            "dev_health_ops.workers.external_ingest_recompute.flush_external_ingest_recompute",
            mock_task,
        ):
            schedule_or_coalesce(
                org_id=ORG,
                source_system=SYSTEM,
                source_instance=INSTANCE,
                ingestion_id=ingestion_id,
                repo_ids=repo_ids or set(),
                team_ids=team_ids or set(),
                window_start=window_start,
                window_end=window_end,
                record_kinds=record_kinds or set(),
            )
        return mock_task


def test_first_call_acquires_guard_and_schedules_flush() -> None:
    client = _fake_client()
    mock_task = _call(client, repo_ids={"repo-a"}, record_kinds={"pull_request.v1"})

    mock_task.apply_async.assert_called_once()
    kwargs = mock_task.apply_async.call_args.kwargs
    assert kwargs["kwargs"] == {
        "org_id": ORG,
        "source_system": SYSTEM,
        "source_instance": INSTANCE,
    }
    assert kwargs["countdown"] == 45

    assert _ttl(client, GUARD_KEY) > 0
    raw = _get_str(client, PENDING_KEY)
    assert raw is not None
    blob = json.loads(raw)
    assert blob["repo_ids"] == ["repo-a"]
    assert blob["ingestion_ids"] == ["ing-1"]


def test_second_call_within_window_widens_blob_without_rescheduling() -> None:
    client = _fake_client()
    mock_task_1 = _call(
        client,
        ingestion_id="ing-1",
        repo_ids={"repo-a"},
        record_kinds={"pull_request.v1"},
        window_start=datetime(2026, 6, 25, tzinfo=timezone.utc),
        window_end=datetime(2026, 6, 25, 12, tzinfo=timezone.utc),
    )
    mock_task_2 = _call(
        client,
        ingestion_id="ing-2",
        repo_ids={"repo-b"},
        record_kinds={"work_item.v1"},
        window_start=datetime(2026, 6, 25, 13, tzinfo=timezone.utc),
        window_end=datetime(2026, 6, 26, tzinfo=timezone.utc),
    )

    mock_task_1.apply_async.assert_called_once()
    mock_task_2.apply_async.assert_not_called()

    raw = _get_str(client, PENDING_KEY)
    assert raw is not None
    blob = json.loads(raw)
    assert sorted(blob["repo_ids"]) == ["repo-a", "repo-b"]
    assert sorted(blob["ingestion_ids"]) == ["ing-1", "ing-2"]
    assert sorted(blob["record_kinds"]) == ["pull_request.v1", "work_item.v1"]
    assert (
        blob["window_start"] == datetime(2026, 6, 25, tzinfo=timezone.utc).isoformat()
    )
    assert blob["window_end"] == datetime(2026, 6, 26, tzinfo=timezone.utc).isoformat()


def test_guard_expiry_allows_rescheduling() -> None:
    client = _fake_client()
    mock_task_1 = _call(client, ingestion_id="ing-1")
    assert mock_task_1.apply_async.call_count == 1

    # Simulate the debounce window elapsing (Valkey TTL eviction).
    client.delete(GUARD_KEY)

    mock_task_2 = _call(client, ingestion_id="ing-2")
    assert mock_task_2.apply_async.call_count == 1


def test_different_source_instances_debounce_independently() -> None:
    client = _fake_client()
    with patch(
        "dev_health_ops.external_ingest.recompute._get_redis_client",
        return_value=client,
    ):
        mock_task = MagicMock()
        with patch(
            "dev_health_ops.workers.external_ingest_recompute.flush_external_ingest_recompute",
            mock_task,
        ):
            schedule_or_coalesce(
                org_id=ORG,
                source_system="github",
                source_instance=INSTANCE,
                ingestion_id="ing-1",
                repo_ids=set(),
                team_ids=set(),
                window_start=None,
                window_end=None,
                record_kinds=set(),
            )
            schedule_or_coalesce(
                org_id=ORG,
                source_system="gitlab",
                source_instance=INSTANCE,
                ingestion_id="ing-2",
                repo_ids=set(),
                team_ids=set(),
                window_start=None,
                window_end=None,
                record_kinds=set(),
            )
    assert mock_task.apply_async.call_count == 2


def test_no_redis_url_falls_back_to_synchronous_dispatch(monkeypatch) -> None:
    monkeypatch.delenv("REDIS_URL", raising=False)
    with patch(
        "dev_health_ops.external_ingest.recompute.dispatch_and_persist_scope"
    ) as mock_dispatch:
        schedule_or_coalesce(
            org_id=ORG,
            source_system=SYSTEM,
            source_instance=INSTANCE,
            ingestion_id="ing-1",
            repo_ids={"repo-a"},
            team_ids=set(),
            window_start=None,
            window_end=None,
            record_kinds={"pull_request.v1"},
        )

    mock_dispatch.assert_called_once()
    call_kwargs = mock_dispatch.call_args.kwargs
    assert call_kwargs["org_id"] == ORG
    assert call_kwargs["ingestion_ids"] == ["ing-1"]
    assert call_kwargs["repo_ids"] == ["repo-a"]


def test_valkey_connection_error_falls_back_to_synchronous_dispatch(
    monkeypatch,
) -> None:
    class _BoomClient:
        def get(self, *args, **kwargs):
            raise ConnectionError("boom")

    monkeypatch.setattr(
        "dev_health_ops.external_ingest.recompute._get_redis_client",
        lambda: _BoomClient(),
    )
    with patch(
        "dev_health_ops.external_ingest.recompute.dispatch_and_persist_scope"
    ) as mock_dispatch:
        schedule_or_coalesce(
            org_id=ORG,
            source_system=SYSTEM,
            source_instance=INSTANCE,
            ingestion_id="ing-1",
            repo_ids=set(),
            team_ids=set(),
            window_start=None,
            window_end=None,
            record_kinds=set(),
        )

    mock_dispatch.assert_called_once()


def test_debounce_seconds_override_used_for_countdown_and_guard_ttl() -> None:
    client = _fake_client()
    with patch(
        "dev_health_ops.external_ingest.recompute._get_redis_client",
        return_value=client,
    ):
        mock_task = MagicMock()
        with patch(
            "dev_health_ops.workers.external_ingest_recompute.flush_external_ingest_recompute",
            mock_task,
        ):
            schedule_or_coalesce(
                org_id=ORG,
                source_system=SYSTEM,
                source_instance=INSTANCE,
                ingestion_id="ing-1",
                repo_ids=set(),
                team_ids=set(),
                window_start=None,
                window_end=None,
                record_kinds=set(),
                debounce_seconds=10,
            )
    assert mock_task.apply_async.call_args.kwargs["countdown"] == 10
    assert _ttl(client, GUARD_KEY) <= 10


def test_concurrent_callers_do_not_lose_either_ingestion_id() -> None:
    """Adversarial-review finding: a plain GET -> merge -> SET is not
    atomic, so two truly concurrent callers for the same debounce key can
    race and one's widened blob is silently overwritten. Simulates a
    second caller finishing its own write (against the same backing
    Valkey server) while the first caller is still inside its WATCH/MULTI
    window, proving the retry-on-WatchError path recovers both ingestion
    ids instead of dropping one."""
    server = FakeServer()
    client_a = FakeValkey(server=server, decode_responses=True)
    client_b = FakeValkey(server=server, decode_responses=True)

    real_merge = recompute_mod._merge_pending_blob
    call_count = {"n": 0}

    def _racy_merge(existing, new_call):
        call_count["n"] += 1
        if call_count["n"] == 1:
            # A second caller's schedule_or_coalesce() completes its own
            # write to the SAME pending key while the first caller's
            # WATCH/MULTI transaction is still open on client_a.
            client_b.set(
                PENDING_KEY,
                json.dumps(
                    {
                        "org_id": ORG,
                        "source_system": SYSTEM,
                        "source_instance": INSTANCE,
                        "repo_ids": ["repo-b"],
                        "team_ids": [],
                        "record_kinds": ["work_item.v1"],
                        "ingestion_ids": ["ing-2"],
                        "window_start": None,
                        "window_end": None,
                    }
                ),
                ex=300,
            )
        return real_merge(existing, new_call)

    with patch(
        "dev_health_ops.external_ingest.recompute._merge_pending_blob",
        side_effect=_racy_merge,
    ):
        _call(
            client_a,
            ingestion_id="ing-1",
            repo_ids={"repo-a"},
            record_kinds={"pull_request.v1"},
        )

    # WatchError forced a retry: the merge function ran twice (once
    # aborted by the concurrent write, once successful against the
    # now-current value).
    assert call_count["n"] == 2

    raw = _get_str(client_a, PENDING_KEY)
    assert raw is not None
    blob = json.loads(raw)
    assert sorted(blob["ingestion_ids"]) == ["ing-1", "ing-2"]
    assert sorted(blob["repo_ids"]) == ["repo-a", "repo-b"]
    assert sorted(blob["record_kinds"]) == ["pull_request.v1", "work_item.v1"]
