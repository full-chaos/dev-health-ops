"""Tests for workers/feature_flag_sync.py's LaunchDarkly cutover (CHAOS-2761).

``_sync_launchdarkly_feature_flags`` now fetches flags/audit-log through the
canonical ``providers/launchdarkly/client.py::LaunchDarklyClient`` (not the
frozen ``connectors/launchdarkly.py::LaunchDarklyConnector``), and drains real
per-request actuals from both ``LaunchDarklyClient`` and
``LaunchDarklyCodeReferencesClient`` into
``result["observations"]["provider_usage"]``. This module tests that wiring
at the seam (fake clients standing in for the real HTTP-backed ones -- the
clients' own internal per-request counting is covered by
tests/test_launchdarkly.py) with no network calls.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from dev_health_ops.exceptions import RateLimitException
from dev_health_ops.metrics.job_work_items import read_work_item_partial_observations
from dev_health_ops.workers.feature_flag_sync import _sync_launchdarkly_feature_flags

_SINK_PATCH = "dev_health_ops.metrics.sinks.clickhouse.ClickHouseMetricsSink"
_BUILDER_PATCH = "dev_health_ops.work_graph.builder.WorkGraphBuilder"
_CLIENT_PATCH = "dev_health_ops.providers.launchdarkly.client.LaunchDarklyClient"
_CODE_REFS_PATCH = (
    "dev_health_ops.providers.launchdarkly.code_refs.LaunchDarklyCodeReferencesClient"
)


def _credentials() -> dict[str, Any]:
    return {"api_key": "ld-key", "project_key": "proj"}


def _make_fake_client(
    usage_observations: list[dict[str, Any]],
    *,
    flags: list[dict] | None = None,
    events: list[dict] | None = None,
    raise_on: str | None = None,
):
    """Build a fake stand-in for LaunchDarklyClient (flags + audit_log)."""

    class _FakeClient:
        def __init__(self, *, api_key: str, project_key: str | None = None) -> None:
            self.api_key = api_key
            self.project_key = project_key

        async def __aenter__(self) -> _FakeClient:
            return self

        async def __aexit__(self, *exc_info: object) -> None:
            return None

        async def get_flags(self, project_key: str | None = None) -> list[dict]:
            if raise_on == "get_flags":
                raise RateLimitException(
                    "LaunchDarkly rate limit exceeded", retry_after_seconds=30
                )
            return flags or []

        async def get_audit_log(self, since=None, limit: int = 1000) -> list[dict]:
            if raise_on == "get_audit_log":
                raise RateLimitException(
                    "LaunchDarkly rate limit exceeded", retry_after_seconds=30
                )
            return events or []

        def drain_usage_observations(self) -> list[dict[str, Any]]:
            return usage_observations

    return _FakeClient


def _make_fake_code_refs_client(
    usage_observations: list[dict[str, Any]],
    *,
    refs: list[Any] | None = None,
    raise_exc: Exception | None = None,
):
    """Build a fake stand-in for LaunchDarklyCodeReferencesClient."""

    class _FakeCodeRefsClient:
        def __init__(self, *, api_key: str) -> None:
            self.api_key = api_key

        async def __aenter__(self) -> _FakeCodeRefsClient:
            return self

        async def __aexit__(self, *exc_info: object) -> None:
            return None

        async def list_default_branch_references(
            self, *, project_key: str, flag_key: str | None = None
        ) -> list[Any]:
            if raise_exc is not None:
                raise raise_exc
            return refs or []

        def drain_usage_observations(self) -> list[dict[str, Any]]:
            return usage_observations

    return _FakeCodeRefsClient


def _stub_sink_and_builder():
    """Patch the sink/graph-builder dependencies so the sync's persistence
    step never touches a real ClickHouse instance."""
    sink = MagicMock()
    sink.query_dicts.return_value = []
    sink_cls = MagicMock(return_value=sink)
    builder_cls = MagicMock(return_value=MagicMock())
    return patch(_SINK_PATCH, sink_cls), patch(_BUILDER_PATCH, builder_cls)


def test_sync_uses_canonical_client_and_threads_provider_usage(monkeypatch) -> None:
    """The 3 currently-emitted LD route families (flags, audit_log, code_refs)
    all drain into result['observations']['provider_usage']."""
    flags_usage = [
        {
            "transport": "rest",
            "route_family": "flags",
            "dimension": "rest_core",
            "request_count": 1,
        },
        {
            "transport": "rest",
            "route_family": "audit_log",
            "dimension": "rest_core",
            "request_count": 1,
        },
    ]
    code_refs_usage = [
        {
            "transport": "rest",
            "route_family": "code_refs",
            "dimension": "rest_core",
            "request_count": 1,
        }
    ]

    monkeypatch.setattr(_CLIENT_PATCH, _make_fake_client(flags_usage))
    monkeypatch.setattr(_CODE_REFS_PATCH, _make_fake_code_refs_client(code_refs_usage))

    sink_patch, builder_patch = _stub_sink_and_builder()
    with sink_patch, builder_patch:
        result = _sync_launchdarkly_feature_flags(
            db_url="clickhouse://localhost/default",
            org_id="org-1",
            credentials=_credentials(),
            sync_options={"project_key": "proj"},
            since_dt=None,
        )

    assert result["observations"]["provider_usage"] == flags_usage + code_refs_usage


def test_sync_omits_observations_key_when_nothing_drained(monkeypatch) -> None:
    """No fabricated empty observations dict when both clients drain nothing."""
    monkeypatch.setattr(_CLIENT_PATCH, _make_fake_client([]))
    monkeypatch.setattr(_CODE_REFS_PATCH, _make_fake_code_refs_client([]))

    sink_patch, builder_patch = _stub_sink_and_builder()
    with sink_patch, builder_patch:
        result = _sync_launchdarkly_feature_flags(
            db_url="clickhouse://localhost/default",
            org_id="org-1",
            credentials=_credentials(),
            sync_options={"project_key": "proj"},
            since_dt=None,
        )

    assert "observations" not in result


def test_rate_limit_during_audit_log_preserves_partial_flags_usage(
    monkeypatch,
) -> None:
    """CHAOS-2754 contract, reused verbatim for LaunchDarkly: a mid-sync
    RateLimitException still carries whatever actuals were recorded before the
    raise (here, the flags fetch succeeded before audit_log hit a limit), so
    the worker deferral path can persist them without ever calling the
    provider again."""
    flags_usage = [
        {
            "transport": "rest",
            "route_family": "flags",
            "dimension": "rest_core",
            "request_count": 1,
        }
    ]
    monkeypatch.setattr(
        _CLIENT_PATCH,
        _make_fake_client(flags_usage, raise_on="get_audit_log"),
    )

    with pytest.raises(RateLimitException) as excinfo:
        _sync_launchdarkly_feature_flags(
            db_url="clickhouse://localhost/default",
            org_id="org-1",
            credentials=_credentials(),
            sync_options={"project_key": "proj"},
            since_dt=None,
        )

    observations = read_work_item_partial_observations(excinfo.value)
    assert observations is not None
    assert observations["provider_usage"] == flags_usage


def test_code_references_failure_still_drains_its_own_usage(monkeypatch) -> None:
    """code_refs failures stay tolerated (existing behavior, unchanged), but
    any requests that DID complete before the failure must still count as
    real actuals rather than being silently discarded."""
    from dev_health_ops.connectors.exceptions import APIException

    flags_usage = [
        {
            "transport": "rest",
            "route_family": "flags",
            "dimension": "rest_core",
            "request_count": 1,
        }
    ]
    code_refs_usage = [
        {
            "transport": "rest",
            "route_family": "code_refs",
            "dimension": "rest_core",
            "request_count": 1,
        }
    ]

    monkeypatch.setattr(_CLIENT_PATCH, _make_fake_client(flags_usage))
    monkeypatch.setattr(
        _CODE_REFS_PATCH,
        _make_fake_code_refs_client(
            code_refs_usage,
            raise_exc=APIException("LaunchDarkly code references server error: 500"),
        ),
    )

    sink_patch, builder_patch = _stub_sink_and_builder()
    with sink_patch, builder_patch:
        result = _sync_launchdarkly_feature_flags(
            db_url="clickhouse://localhost/default",
            org_id="org-1",
            credentials=_credentials(),
            sync_options={"project_key": "proj"},
            since_dt=None,
        )

    assert result["code_references_error"] is not None
    assert result["observations"]["provider_usage"] == flags_usage + code_refs_usage
