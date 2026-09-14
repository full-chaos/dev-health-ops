"""Unit tests for ``schedule_or_coalesce()`` (CHAOS-2699).

CHAOS-5702 deleted the Valkey debounce/coalesce path this function used to
attempt before dispatching -- guard+pending-blob bookkeeping that fed a
Celery flush task retired under CHAOS-3093, so it never dispatched anything
of its own regardless of Valkey's health. ``schedule_or_coalesce`` now has
exactly one path: plan + dispatch + persist this one batch's scope,
unconditionally. These tests cover that live path and pin the absence of
any remaining Valkey dependency.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

import dev_health_ops.external_ingest.recompute as recompute_mod
from dev_health_ops.external_ingest.recompute import schedule_or_coalesce

ORG = "org-1"
SYSTEM = "github"
INSTANCE = "acme/api"


def test_dispatches_synchronously_with_the_full_batch_scope() -> None:
    with patch(
        "dev_health_ops.external_ingest.recompute.dispatch_and_persist_scope"
    ) as mock_dispatch:
        schedule_or_coalesce(
            org_id=ORG,
            source_system=SYSTEM,
            source_instance=INSTANCE,
            ingestion_id="ing-1",
            repo_ids={"repo-b", "repo-a"},
            team_ids={"team-a"},
            window_start=datetime(2026, 6, 25, tzinfo=timezone.utc),
            window_end=datetime(2026, 6, 25, 12, tzinfo=timezone.utc),
            record_kinds={"pull_request.v1", "work_item.v1"},
        )

    mock_dispatch.assert_called_once()
    call_kwargs = mock_dispatch.call_args.kwargs
    assert call_kwargs["org_id"] == ORG
    assert call_kwargs["source_system"] == SYSTEM
    assert call_kwargs["source_instance"] == INSTANCE
    assert call_kwargs["ingestion_ids"] == ["ing-1"]
    assert call_kwargs["repo_ids"] == ["repo-a", "repo-b"]
    assert call_kwargs["team_ids"] == ["team-a"]
    assert call_kwargs["record_kinds"] == ["pull_request.v1", "work_item.v1"]
    assert call_kwargs["window_start"] == datetime(2026, 6, 25, tzinfo=timezone.utc)
    assert call_kwargs["window_end"] == datetime(2026, 6, 25, 12, tzinfo=timezone.utc)


def test_dispatch_failure_propagates_to_the_caller() -> None:
    """No try/except left inside ``schedule_or_coalesce`` itself -- the
    best-effort guarantee (a recompute-dispatch problem must never fail
    ingestion) lives one layer up, in the worker caller, exactly as it did
    for this function's Valkey-unavailable path before the deletion."""
    with patch(
        "dev_health_ops.external_ingest.recompute.dispatch_and_persist_scope",
        side_effect=RuntimeError("boom"),
    ):
        try:
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
        except RuntimeError as exc:
            assert str(exc) == "boom"
        else:
            raise AssertionError("expected RuntimeError to propagate")


def test_no_valkey_dependency_dispatches_identically_regardless_of_redis_url(
    monkeypatch,
) -> None:
    """CHAOS-5702: the function must not look at ``REDIS_URL`` or Valkey at
    all anymore -- it dispatches the identical scope whether or not a
    Valkey URL is configured, and the Valkey client probe it used to call
    is gone from the module outright."""
    assert not hasattr(recompute_mod, "_get_redis_client")

    calls: list[dict] = []

    def _fake_dispatch(**kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(recompute_mod, "dispatch_and_persist_scope", _fake_dispatch)

    def _call() -> None:
        schedule_or_coalesce(
            org_id=ORG,
            source_system=SYSTEM,
            source_instance=INSTANCE,
            ingestion_id="ing-1",
            repo_ids={"repo-a"},
            team_ids=set(),
            window_start=datetime(2026, 6, 25, tzinfo=timezone.utc),
            window_end=datetime(2026, 6, 25, 12, tzinfo=timezone.utc),
            record_kinds={"pull_request.v1"},
        )

    monkeypatch.delenv("REDIS_URL", raising=False)
    _call()

    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    _call()

    assert len(calls) == 2
    assert calls[0] == calls[1]
