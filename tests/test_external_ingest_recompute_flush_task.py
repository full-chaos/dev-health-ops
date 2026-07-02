"""Unit tests for the ``flush_external_ingest_recompute`` Celery task itself
(CHAOS-2699).

The debounce tests (``test_external_ingest_recompute_debounce.py``) mock
this task's ``.apply_async`` entirely and never exercise its body -- these
tests call ``.run()`` directly (bypassing the Celery machinery, matching
``tests/test_post_sync_investment_dispatch.py``'s
``run_investment_materialize.run(...)`` convention) with the real Valkey
GETDEL + ``dispatch_and_persist_scope`` wiring, so the two are patched
independently of the debounce layer.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest

fakeredis = pytest.importorskip("fakeredis")
from fakeredis import FakeValkey  # noqa: E402

from dev_health_ops.workers.external_ingest_recompute import (  # noqa: E402
    flush_external_ingest_recompute,
)

ORG = "org-1"
SYSTEM = "github"
INSTANCE = "acme/api"
PENDING_KEY = f"external-ingest:recompute:pending:{ORG}:{SYSTEM}:{INSTANCE}"


def _blob(**overrides: object) -> dict:
    base: dict = {
        "org_id": ORG,
        "source_system": SYSTEM,
        "source_instance": INSTANCE,
        "repo_ids": ["repo-a"],
        "team_ids": [],
        "record_kinds": ["pull_request.v1"],
        "ingestion_ids": ["ing-1"],
        "window_start": datetime(2026, 6, 25, tzinfo=timezone.utc).isoformat(),
        "window_end": datetime(2026, 6, 26, tzinfo=timezone.utc).isoformat(),
    }
    base.update(overrides)
    return base


def _run_flush() -> dict:
    task = cast(Any, flush_external_ingest_recompute)
    result: dict = task.run(org_id=ORG, source_system=SYSTEM, source_instance=INSTANCE)
    return result


def test_flush_reads_and_atomically_clears_pending_blob_via_getdel() -> None:
    client = FakeValkey(decode_responses=True)
    client.set(PENDING_KEY, json.dumps(_blob()), ex=300)

    with (
        patch(
            "dev_health_ops.external_ingest.recompute._get_redis_client",
            return_value=client,
        ),
        patch(
            "dev_health_ops.external_ingest.recompute.dispatch_and_persist_scope"
        ) as mock_dispatch,
    ):
        mock_dispatch.return_value = MagicMock(
            status="dispatched", jobs=(1, 2), capped_days=False, capped_repos=False
        )
        result = _run_flush()

    assert result["status"] == "dispatched"
    assert result["jobs"] == 2
    assert result["ingestion_ids"] == ["ing-1"]

    # GETDEL means the key is gone after exactly one read -- no separate
    # DELETE call, no window for a third party to slip a fresher blob in
    # between (adversarial-review finding).
    assert client.get(PENDING_KEY) is None

    mock_dispatch.assert_called_once()
    call_kwargs = mock_dispatch.call_args.kwargs
    assert call_kwargs["org_id"] == ORG
    assert call_kwargs["ingestion_ids"] == ["ing-1"]
    assert call_kwargs["repo_ids"] == ["repo-a"]
    assert call_kwargs["window_start"] == datetime(2026, 6, 25, tzinfo=timezone.utc)


def test_flush_leaves_guard_key_untouched() -> None:
    """The flush task must not explicitly delete the guard key -- its own
    TTL governs its lifecycle (adversarial-review finding: an explicit
    DELETE here could erase a guard a newer schedule_or_coalesce() call
    just acquired)."""
    client = FakeValkey(decode_responses=True)
    client.set(PENDING_KEY, json.dumps(_blob()), ex=300)
    guard_key = f"external-ingest:recompute:scheduled:{ORG}:{SYSTEM}:{INSTANCE}"
    client.set(guard_key, "1", ex=30)

    with (
        patch(
            "dev_health_ops.external_ingest.recompute._get_redis_client",
            return_value=client,
        ),
        patch(
            "dev_health_ops.external_ingest.recompute.dispatch_and_persist_scope"
        ) as mock_dispatch,
    ):
        mock_dispatch.return_value = MagicMock(
            status="dispatched", jobs=(), capped_days=False, capped_repos=False
        )
        _run_flush()

    assert client.get(guard_key) == "1"


def test_flush_empty_blob_no_op_returns_no_pending_scope() -> None:
    client = FakeValkey(decode_responses=True)
    # No SET -- pending_key was never written or was already consumed.

    with (
        patch(
            "dev_health_ops.external_ingest.recompute._get_redis_client",
            return_value=client,
        ),
        patch(
            "dev_health_ops.external_ingest.recompute.dispatch_and_persist_scope"
        ) as mock_dispatch,
    ):
        result = _run_flush()

    assert result == {"status": "no_pending_scope"}
    mock_dispatch.assert_not_called()


def test_flush_valkey_read_error_goes_through_retry_path_not_dispatch() -> None:
    class _BoomClient:
        def getdel(self, *args, **kwargs):
            raise ConnectionError("boom")

    with (
        patch(
            "dev_health_ops.external_ingest.recompute._get_redis_client",
            return_value=_BoomClient(),
        ),
        patch(
            "dev_health_ops.external_ingest.recompute.dispatch_and_persist_scope"
        ) as mock_dispatch,
        patch.object(
            flush_external_ingest_recompute,
            "retry",
            side_effect=RuntimeError("retried"),
        ) as mock_retry,
        pytest.raises(RuntimeError, match="retried"),
    ):
        _run_flush()

    # The Valkey read failure goes through self.retry(), never straight to
    # dispatch_and_persist_scope -- a transient Valkey hiccup gets a Celery
    # redelivery instead of silently skipping this flush's recompute.
    mock_retry.assert_called_once()
    assert isinstance(mock_retry.call_args.kwargs["exc"], ConnectionError)
    mock_dispatch.assert_not_called()
