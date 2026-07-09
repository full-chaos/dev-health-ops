"""Tests for workers/feature_flag_sync.py's LaunchDarkly (CHAOS-2761) and
GitLab (CHAOS-2785) cutovers.

``_sync_launchdarkly_feature_flags`` fetches flags/audit-log through the
canonical ``providers/launchdarkly/client.py::LaunchDarklyClient`` (not the
frozen ``connectors/launchdarkly.py::LaunchDarklyConnector``), and drains real
per-request actuals from both ``LaunchDarklyClient`` and
``LaunchDarklyCodeReferencesClient`` into
``result["observations"]["provider_usage"]``.

``_sync_gitlab_feature_flags`` fetches flags/project-name through the
canonical ``providers/gitlab/feature_flags.py::GitLabFeatureFlagsClient`` (not
the frozen ``connectors/gitlab.py::GitLabConnector``), draining the same
shape of actuals into ``result["observations"]["provider_usage"]``.

This module tests that wiring at the seam (fake clients standing in for the
real HTTP-backed ones -- the clients' own internal per-request counting is
covered by tests/test_launchdarkly.py and
tests/test_gitlab_feature_flags_client.py) with no network calls.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from dev_health_ops.exceptions import RateLimitException
from dev_health_ops.metrics.job_work_items import read_work_item_partial_observations
from dev_health_ops.workers.feature_flag_sync import (
    _sync_gitlab_feature_flags,
    _sync_launchdarkly_feature_flags,
)

_SINK_PATCH = "dev_health_ops.metrics.sinks.clickhouse.ClickHouseMetricsSink"
_BUILDER_PATCH = "dev_health_ops.work_graph.builder.WorkGraphBuilder"
_CLIENT_PATCH = "dev_health_ops.providers.launchdarkly.client.LaunchDarklyClient"
_CODE_REFS_PATCH = (
    "dev_health_ops.providers.launchdarkly.code_refs.LaunchDarklyCodeReferencesClient"
)
_GITLAB_CLIENT_PATCH = (
    "dev_health_ops.providers.gitlab.feature_flags.GitLabFeatureFlagsClient"
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
            self._drained = False

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
            # Mirrors UsageRecorder.drain() clearing its state: a second call
            # after an earlier successful drain returns [] rather than
            # re-emitting the same observations (double-counting).
            if self._drained:
                return []
            self._drained = True
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
            self._drained = False

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
            # Mirrors UsageRecorder.drain() clearing its state: a second call
            # after an earlier successful drain returns [] rather than
            # re-emitting the same observations (double-counting).
            if self._drained:
                return []
            self._drained = True
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


def test_sink_write_failure_preserves_all_actuals_gathered_so_far(monkeypatch) -> None:
    """CHAOS-2761 review finding (HIGH): before the fix, only a raise DURING
    the flags/audit_log fetch attached partial observations -- a failure
    anywhere downstream (code_refs, normalization, ClickHouse/WorkGraph
    writes) reached run_sync_unit bare, silently dropping real request counts
    that had already been recorded and drained. Here the flags/audit_log AND
    code_refs fetches both succeed (and drain usage) before a ClickHouse sink
    write fails; both actuals must still survive on the raised exception."""
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
    monkeypatch.setattr(_CODE_REFS_PATCH, _make_fake_code_refs_client(code_refs_usage))

    sink = MagicMock()
    sink.query_dicts.return_value = []
    sink.write_feature_flags.side_effect = RuntimeError("clickhouse write failed")
    sink_cls = MagicMock(return_value=sink)
    builder_cls = MagicMock(return_value=MagicMock())

    with patch(_SINK_PATCH, sink_cls), patch(_BUILDER_PATCH, builder_cls):
        with pytest.raises(RuntimeError) as excinfo:
            _sync_launchdarkly_feature_flags(
                db_url="clickhouse://localhost/default",
                org_id="org-1",
                credentials=_credentials(),
                sync_options={"project_key": "proj"},
                since_dt=None,
            )

    observations = read_work_item_partial_observations(excinfo.value)
    assert observations is not None
    assert observations["provider_usage"] == flags_usage + code_refs_usage
    # The finally block must still have closed the sink/builder even though
    # the write raised.
    sink.close.assert_called_once()


# ---------------------------------------------------------------------------
# GitLab (CHAOS-2785)
# ---------------------------------------------------------------------------


def _gitlab_credentials() -> dict[str, Any]:
    return {"token": "gl-token", "url": "https://gitlab.example.com"}


def _make_fake_gitlab_client(
    usage_observations: list[dict[str, Any]],
    *,
    flags: list[dict] | None = None,
    project_name: str = "group/project",
    raise_on: str | None = None,
):
    """Build a fake stand-in for GitLabFeatureFlagsClient."""

    class _FakeGitLabClient:
        def __init__(self, *, private_token: str, base_url: str) -> None:
            self.private_token = private_token
            self.base_url = base_url
            self._drained = False

        async def __aenter__(self) -> _FakeGitLabClient:
            return self

        async def __aexit__(self, *exc_info: object) -> None:
            return None

        async def get_feature_flags(self, project_id_or_path: object) -> list[dict]:
            if raise_on == "get_feature_flags":
                raise RateLimitException(
                    "GitLab rate limit exceeded", retry_after_seconds=30
                )
            return flags or []

        async def get_project_name(self, project_id_or_path: object) -> str:
            if raise_on == "get_project_name":
                raise RateLimitException(
                    "GitLab rate limit exceeded", retry_after_seconds=30
                )
            return project_name

        def drain_usage_observations(self) -> list[dict[str, Any]]:
            # Mirrors UsageRecorder.drain() clearing its state: a second call
            # after an earlier successful drain returns [] rather than
            # re-emitting the same observations (double-counting).
            if self._drained:
                return []
            self._drained = True
            return usage_observations

    return _FakeGitLabClient


def test_gitlab_sync_uses_canonical_client_and_threads_provider_usage(
    monkeypatch,
) -> None:
    """Feature-flag + project-name fetches drain into
    result['observations']['provider_usage']."""
    usage = [
        {
            "transport": "rest",
            "route_family": "project",
            "dimension": "rest_core",
            "request_count": 2,
        }
    ]
    monkeypatch.setattr(
        _GITLAB_CLIENT_PATCH,
        _make_fake_gitlab_client(
            usage, flags=[{"name": "awesome_feature", "active": True}]
        ),
    )

    sink_patch, builder_patch = _stub_sink_and_builder()
    with sink_patch, builder_patch:
        result = _sync_gitlab_feature_flags(
            db_url="clickhouse://localhost/default",
            org_id="org-1",
            credentials=_gitlab_credentials(),
            sync_options={"project_id": "group/project"},
        )

    assert result["flags_synced"] == 1
    assert result["project_key"] == "group/project"
    assert result["observations"]["provider_usage"] == usage


def test_gitlab_sync_honors_stored_gitlab_url_credential_key(monkeypatch) -> None:
    """CHAOS-2785 review finding: the canonical credential resolver
    (credentials/resolver.py::gitlab_credentials_from_mapping) resolves a
    self-hosted base URL from the stored credentials mapping's ``gitlab_url``
    key (before ``url``/``base_url``), and every other GitLab dataset path
    goes through that resolver. The feature-flags sync builds its own
    ``GitLabFeatureFlagsClient`` directly instead, so it must honor the same
    key/precedence or a self-hosted org's token is sent to gitlab.com."""
    constructed: list[tuple[str, str]] = []

    class _RecordingFakeClient:
        def __init__(self, *, private_token: str, base_url: str) -> None:
            constructed.append((private_token, base_url))

        async def __aenter__(self) -> _RecordingFakeClient:
            return self

        async def __aexit__(self, *exc_info: object) -> None:
            return None

        async def get_feature_flags(self, project_id_or_path: object) -> list[dict]:
            return []

        async def get_project_name(self, project_id_or_path: object) -> str:
            return "group/project"

        def drain_usage_observations(self) -> list[dict[str, Any]]:
            return []

    monkeypatch.setattr(_GITLAB_CLIENT_PATCH, _RecordingFakeClient)

    sink_patch, builder_patch = _stub_sink_and_builder()
    with sink_patch, builder_patch:
        _sync_gitlab_feature_flags(
            db_url="clickhouse://localhost/default",
            org_id="org-1",
            credentials={
                "token": "self-hosted-token",
                "gitlab_url": "https://gitlab.example.com",
                # A stale/incorrect "url" must NOT win over "gitlab_url" --
                # matches the canonical resolver's precedence.
                "url": "https://gitlab.com",
            },
            sync_options={"project_id": "group/project"},
        )

    assert constructed == [("self-hosted-token", "https://gitlab.example.com")]


def test_gitlab_sync_omits_observations_key_when_nothing_drained(monkeypatch) -> None:
    """No fabricated empty observations dict when the client drains nothing."""
    monkeypatch.setattr(_GITLAB_CLIENT_PATCH, _make_fake_gitlab_client([]))

    sink_patch, builder_patch = _stub_sink_and_builder()
    with sink_patch, builder_patch:
        result = _sync_gitlab_feature_flags(
            db_url="clickhouse://localhost/default",
            org_id="org-1",
            credentials=_gitlab_credentials(),
            sync_options={"project_id": "group/project"},
        )

    assert "observations" not in result


def test_gitlab_rate_limit_during_fetch_preserves_partial_observations(
    monkeypatch,
) -> None:
    """CHAOS-2754 contract, reused verbatim for GitLab: a mid-sync
    RateLimitException still carries whatever actuals were recorded before
    the raise, so the worker deferral path can persist them without ever
    calling the provider again."""
    usage = [
        {
            "transport": "rest",
            "route_family": "project",
            "dimension": "rest_core",
            "request_count": 1,
        }
    ]
    monkeypatch.setattr(
        _GITLAB_CLIENT_PATCH,
        _make_fake_gitlab_client(usage, raise_on="get_project_name"),
    )

    with pytest.raises(RateLimitException) as excinfo:
        _sync_gitlab_feature_flags(
            db_url="clickhouse://localhost/default",
            org_id="org-1",
            credentials=_gitlab_credentials(),
            sync_options={"project_id": "group/project"},
        )

    observations = read_work_item_partial_observations(excinfo.value)
    assert observations is not None
    assert observations["provider_usage"] == usage


def test_gitlab_sink_write_failure_preserves_actuals_gathered_so_far(
    monkeypatch,
) -> None:
    """Mirrors test_sink_write_failure_preserves_all_actuals_gathered_so_far
    for LaunchDarkly: a downstream ClickHouse write failure (not a raise
    during the fetch itself) must not silently drop already-drained actuals."""
    usage = [
        {
            "transport": "rest",
            "route_family": "project",
            "dimension": "rest_core",
            "request_count": 2,
        }
    ]
    monkeypatch.setattr(
        _GITLAB_CLIENT_PATCH,
        _make_fake_gitlab_client(
            usage, flags=[{"name": "awesome_feature", "active": True}]
        ),
    )

    sink = MagicMock()
    sink.query_dicts.return_value = []
    sink.write_feature_flags.side_effect = RuntimeError("clickhouse write failed")
    sink_cls = MagicMock(return_value=sink)
    builder_cls = MagicMock(return_value=MagicMock())

    with patch(_SINK_PATCH, sink_cls), patch(_BUILDER_PATCH, builder_cls):
        with pytest.raises(RuntimeError) as excinfo:
            _sync_gitlab_feature_flags(
                db_url="clickhouse://localhost/default",
                org_id="org-1",
                credentials=_gitlab_credentials(),
                sync_options={"project_id": "group/project"},
            )

    observations = read_work_item_partial_observations(excinfo.value)
    assert observations is not None
    assert observations["provider_usage"] == usage
    sink.close.assert_called_once()


def test_gitlab_sync_requires_token_and_project() -> None:
    with pytest.raises(ValueError, match="token and project_id"):
        _sync_gitlab_feature_flags(
            db_url="clickhouse://localhost/default",
            org_id="org-1",
            credentials={},
            sync_options={},
        )
