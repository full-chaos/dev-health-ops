"""Tests for providers/gitlab/code_client.py (CHAOS-2773 CS10).

``GitLabCodeClient`` is the GitLab-wave pathfinder: the first canonical code
client built on the shared ``providers/_http.py::InstrumentedRESTCore``
(CHAOS-2773 CS1). These are PARITY tests proving it reproduces
``GitLabConnector.get_security_alerts``'s (``connectors/gitlab.py``, FROZEN)
behavior for vulnerability findings + dependency-scan alerts: auth header,
401/403/404/429 handling, identical ``SecurityAlertData`` field mapping, and
the pre-existing single-page-per-endpoint quirk. httpx is mocked at the
transport layer via ``httpx.MockTransport`` (the pattern already established
by ``tests/providers/test_http_core.py``) -- no live network, matching the
offline local gate.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from typing import Any, cast

import httpx
import pytest

from dev_health_ops.exceptions import (
    AuthenticationException,
    NotFoundException,
    RateLimitException,
)
from dev_health_ops.providers.gitlab.code_client import GitLabCodeClient
from dev_health_ops.sync.budget_types import BudgetDimension

_PROJECT_PATH = "/api/v4/projects/42"
_FINDINGS_PATH = "/api/v4/projects/42/vulnerability_findings"
_DEPENDENCIES_PATH = "/api/v4/projects/42/dependencies"

_PROJECT_RESPONSE = {"id": 42, "path_with_namespace": "group/project"}


def _json_response(
    status_code: int, body: object, headers: dict[str, str] | None = None
) -> httpx.Response:
    return httpx.Response(status_code, json=body, headers=headers or {})


def _empty_response(
    status_code: int, headers: dict[str, str] | None = None
) -> httpx.Response:
    return httpx.Response(status_code, headers=headers or {})


def _router_transport(
    routes: dict[str, list[httpx.Response] | httpx.Response],
) -> tuple[httpx.MockTransport, dict[str, int]]:
    """Route requests by URL path, queuing a list of responses per path (one
    consumed per hit, the last repeats) or a single fixed response."""
    calls: dict[str, int] = {}
    captured_headers: dict[str, httpx.Headers] = {}
    captured_urls: dict[str, httpx.URL] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        calls[path] = calls.get(path, 0) + 1
        captured_headers[path] = request.headers
        captured_urls[path] = request.url
        entry = routes[path]
        if isinstance(entry, list):
            idx = min(calls[path] - 1, len(entry) - 1)
            return entry[idx]
        return entry

    transport = httpx.MockTransport(handler)
    transport.calls = calls  # type: ignore[attr-defined]
    transport.captured_headers = captured_headers  # type: ignore[attr-defined]
    transport.captured_urls = captured_urls  # type: ignore[attr-defined]
    return transport, calls


def _client(transport: httpx.MockTransport, **overrides: object) -> GitLabCodeClient:
    kwargs: dict[str, object] = {
        "private_token": "test-token",
        "transport": transport,
    }
    kwargs.update(overrides)
    return GitLabCodeClient(**kwargs)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Auth header + project resolution
# ---------------------------------------------------------------------------


class TestAuthAndProjectResolution:
    @pytest.mark.asyncio
    async def test_private_token_header_sent_on_every_request(self) -> None:
        transport, _ = _router_transport(
            {
                _PROJECT_PATH: _json_response(200, _PROJECT_RESPONSE),
                _FINDINGS_PATH: _json_response(200, []),
                _DEPENDENCIES_PATH: _json_response(200, []),
            }
        )
        client = _client(transport, private_token="secret-tok")

        await client.get_security_alerts(42)

        for path in (_PROJECT_PATH, _FINDINGS_PATH, _DEPENDENCIES_PATH):
            assert transport.captured_headers[path]["PRIVATE-TOKEN"] == "secret-tok"  # type: ignore[attr-defined]

    @pytest.mark.asyncio
    async def test_resolves_numeric_project_id_before_fetching(self) -> None:
        """Mirrors the connector's ``self.gitlab.projects.get(project_identifier)
        .id`` resolution -- the id/path passed in is resolved via ONE GET
        /projects/{id_or_path} first, and the RETURNED numeric id is what the
        two endpoint calls use."""
        transport, calls = _router_transport(
            {
                "/api/v4/projects/group/project": _json_response(200, {"id": 42}),
                _FINDINGS_PATH: _json_response(200, []),
                _DEPENDENCIES_PATH: _json_response(200, []),
            }
        )
        client = _client(transport)

        await client.get_security_alerts("group/project")

        assert calls["/api/v4/projects/group/project"] == 1
        assert calls[_FINDINGS_PATH] == 1
        assert calls[_DEPENDENCIES_PATH] == 1

    @pytest.mark.asyncio
    async def test_401_on_project_resolution_propagates(self) -> None:
        transport, _ = _router_transport({_PROJECT_PATH: _empty_response(401)})
        client = _client(transport)

        with pytest.raises(AuthenticationException):
            await client.get_security_alerts(42)


# ---------------------------------------------------------------------------
# Field mapping parity (connectors/gitlab.py::GitLabConnector.get_security_alerts)
# ---------------------------------------------------------------------------


class TestFieldMappingParity:
    @pytest.mark.asyncio
    async def test_vulnerability_finding_field_mapping(self) -> None:
        finding = {
            "id": 101,
            "severity": "high",
            "state": "detected",
            "name": "SQL Injection",
            "created_at": "2026-01-15T10:30:00.000Z",
            "identifiers": [
                {"type": "other", "name": "ignored"},
                {"type": "cve", "name": "CVE-2026-1234"},
            ],
            "links": {"url": "https://gitlab.example.com/vuln/101"},
        }
        transport, _ = _router_transport(
            {
                _PROJECT_PATH: _json_response(200, _PROJECT_RESPONSE),
                _FINDINGS_PATH: _json_response(200, [finding]),
                _DEPENDENCIES_PATH: _json_response(200, []),
            }
        )
        client = _client(transport)

        alerts = await client.get_security_alerts(42)

        assert len(alerts) == 1
        alert = alerts[0]
        assert alert.alert_id == "gitlab_vuln:101"
        assert alert.source == "gitlab_vulnerability"
        assert alert.severity == "high"
        assert alert.state == "detected"
        assert alert.package_name is None
        assert alert.cve_id == "CVE-2026-1234"
        assert alert.url == "https://gitlab.example.com/vuln/101"
        assert alert.title == "SQL Injection"
        assert alert.description is None
        assert alert.created_at == datetime(2026, 1, 15, 10, 30, tzinfo=timezone.utc)
        assert alert.fixed_at is None
        assert alert.dismissed_at is None

    @pytest.mark.asyncio
    async def test_vulnerability_finding_missing_created_at_and_cve(self) -> None:
        finding = {"id": 202, "severity": "low", "state": "resolved", "name": "XSS"}
        transport, _ = _router_transport(
            {
                _PROJECT_PATH: _json_response(200, _PROJECT_RESPONSE),
                _FINDINGS_PATH: _json_response(200, [finding]),
                _DEPENDENCIES_PATH: _json_response(200, []),
            }
        )
        client = _client(transport)

        alerts = await client.get_security_alerts(42)

        assert len(alerts) == 1
        assert alerts[0].created_at is None
        assert alerts[0].cve_id is None
        assert alerts[0].url is None

    @pytest.mark.asyncio
    async def test_dependency_alert_field_mapping(self) -> None:
        dependency = {
            "name": "lodash",
            "vulnerabilities": [
                {
                    "id": 303,
                    "severity": "critical",
                    "url": "https://gitlab.example.com/vuln/303",
                    "name": "Prototype Pollution",
                }
            ],
        }
        before = datetime.now(timezone.utc)
        transport, _ = _router_transport(
            {
                _PROJECT_PATH: _json_response(200, _PROJECT_RESPONSE),
                _FINDINGS_PATH: _json_response(200, []),
                _DEPENDENCIES_PATH: _json_response(200, [dependency]),
            }
        )
        client = _client(transport)

        alerts = await client.get_security_alerts(42)
        after = datetime.now(timezone.utc)

        assert len(alerts) == 1
        alert = alerts[0]
        assert alert.alert_id == "gitlab_dep:303"
        assert alert.source == "gitlab_dependency"
        assert alert.severity == "critical"
        assert alert.state is None
        assert alert.package_name == "lodash"
        assert alert.cve_id is None
        assert alert.url == "https://gitlab.example.com/vuln/303"
        assert alert.title == "Prototype Pollution"
        assert alert.description is None
        # Dependency alerts carry no per-vulnerability timestamp upstream --
        # the connector's own placeholder (datetime.now(timezone.utc)),
        # reproduced here byte-for-byte, not something this migration invents.
        assert alert.created_at is not None
        assert before <= alert.created_at <= after
        assert alert.fixed_at is None
        assert alert.dismissed_at is None

    @pytest.mark.asyncio
    async def test_dependency_with_no_vulnerabilities_yields_no_alerts(self) -> None:
        dependency = {"name": "safe-package", "vulnerabilities": []}
        transport, _ = _router_transport(
            {
                _PROJECT_PATH: _json_response(200, _PROJECT_RESPONSE),
                _FINDINGS_PATH: _json_response(200, []),
                _DEPENDENCIES_PATH: _json_response(200, [dependency]),
            }
        )
        client = _client(transport)

        alerts = await client.get_security_alerts(42)

        assert alerts == []

    @pytest.mark.asyncio
    async def test_max_alerts_truncates_combined_results(self) -> None:
        findings = [{"id": i, "severity": "low", "state": "detected"} for i in range(3)]
        transport, _ = _router_transport(
            {
                _PROJECT_PATH: _json_response(200, _PROJECT_RESPONSE),
                _FINDINGS_PATH: _json_response(200, findings),
                _DEPENDENCIES_PATH: _json_response(200, []),
            }
        )
        client = _client(transport)

        alerts = await client.get_security_alerts(42, max_alerts=2)

        assert len(alerts) == 2


# ---------------------------------------------------------------------------
# Best-effort suppression: plain 403 / 404 degrade to empty per-endpoint
# ---------------------------------------------------------------------------


class TestBestEffortSuppression:
    @pytest.mark.asyncio
    async def test_plain_403_on_findings_degrades_to_empty_but_dependencies_still_fetched(
        self,
    ) -> None:
        dependency = {
            "name": "pkg",
            "vulnerabilities": [{"id": 1, "severity": "high", "name": "CVE"}],
        }
        transport, calls = _router_transport(
            {
                _PROJECT_PATH: _json_response(200, _PROJECT_RESPONSE),
                _FINDINGS_PATH: _empty_response(403, {}),
                _DEPENDENCIES_PATH: _json_response(200, [dependency]),
            }
        )
        client = _client(transport)

        alerts = await client.get_security_alerts(42)

        assert len(alerts) == 1
        assert alerts[0].source == "gitlab_dependency"
        # A plain 403 must not consume the retry budget -- exactly one
        # request against the forbidden endpoint.
        assert calls[_FINDINGS_PATH] == 1

    @pytest.mark.asyncio
    async def test_404_on_dependencies_degrades_to_empty(self) -> None:
        finding = {"id": 1, "severity": "high", "state": "detected"}
        transport, _ = _router_transport(
            {
                _PROJECT_PATH: _json_response(200, _PROJECT_RESPONSE),
                _FINDINGS_PATH: _json_response(200, [finding]),
                _DEPENDENCIES_PATH: _empty_response(404),
            }
        )
        client = _client(transport)

        alerts = await client.get_security_alerts(42)

        assert len(alerts) == 1
        assert alerts[0].source == "gitlab_vulnerability"

    @pytest.mark.asyncio
    async def test_both_endpoints_forbidden_yields_empty_list_no_raise(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        transport, _ = _router_transport(
            {
                _PROJECT_PATH: _json_response(200, _PROJECT_RESPONSE),
                _FINDINGS_PATH: _empty_response(403),
                _DEPENDENCIES_PATH: _empty_response(404),
            }
        )
        client = _client(transport)

        with caplog.at_level("WARNING"):
            assert await client.get_security_alerts(42) == []
        messages = [record.message for record in caplog.records]
        assert any(
            "provider=gitlab" in message
            and "project_id=42" in message
            and "endpoint=vulnerability_findings" in message
            for message in messages
        )
        assert any(
            "provider=gitlab" in message
            and "project_id=42" in message
            and "endpoint=dependencies" in message
            for message in messages
        )


# ---------------------------------------------------------------------------
# Rate limiting: 429 always, header-qualified 403 -- never a plain 403
# ---------------------------------------------------------------------------


class TestRateLimiting:
    @pytest.mark.asyncio
    async def test_429_on_findings_raises_rate_limit_after_exhausting_retries(
        self,
    ) -> None:
        transport, calls = _router_transport(
            {
                _PROJECT_PATH: _json_response(200, _PROJECT_RESPONSE),
                _FINDINGS_PATH: _empty_response(429, {"Retry-After": "0"}),
                _DEPENDENCIES_PATH: _json_response(200, []),
            }
        )
        client = _client(transport, max_retries=2)

        with pytest.raises(RateLimitException) as excinfo:
            await client.get_security_alerts(42)

        assert calls[_FINDINGS_PATH] == 2
        signal = excinfo.value.signal
        assert signal is not None
        assert signal.provider == "gitlab"
        assert signal.reason == "primary"
        assert signal.dimension is BudgetDimension.REST_CORE
        assert excinfo.value.retry_after_seconds == 0.0

    @pytest.mark.asyncio
    async def test_header_qualified_403_on_dependencies_raises_rate_limit(self) -> None:
        """A 403 carrying rate-limit headers is retried in place like a 429,
        then classified as RateLimitException on exhaustion -- NOT the
        non-retryable/suppressible plain-403 path."""
        transport, calls = _router_transport(
            {
                _PROJECT_PATH: _json_response(200, _PROJECT_RESPONSE),
                _FINDINGS_PATH: _json_response(200, []),
                _DEPENDENCIES_PATH: _empty_response(403, {"RateLimit-Remaining": "0"}),
            }
        )
        client = _client(transport, max_retries=2)

        with pytest.raises(RateLimitException) as excinfo:
            await client.get_security_alerts(42)

        assert calls[_DEPENDENCIES_PATH] == 2
        assert excinfo.value.signal is not None
        assert excinfo.value.signal.reason == "secondary"

    @pytest.mark.asyncio
    async def test_header_qualified_403_then_recovers_is_not_suppressed_as_forbidden(
        self,
    ) -> None:
        finding = {"id": 1, "severity": "high", "state": "detected"}
        throttled = _empty_response(403, {"Retry-After": "0"})
        ok = _json_response(200, [finding])
        transport, calls = _router_transport(
            {
                _PROJECT_PATH: _json_response(200, _PROJECT_RESPONSE),
                _FINDINGS_PATH: [throttled, ok],
                _DEPENDENCIES_PATH: _json_response(200, []),
            }
        )
        client = _client(transport)

        alerts = await client.get_security_alerts(42)

        assert len(alerts) == 1
        assert calls[_FINDINGS_PATH] == 2

    @pytest.mark.asyncio
    async def test_429_retry_after_http_date_derives_seconds(self) -> None:
        future = datetime.now(timezone.utc) + timedelta(seconds=120)
        http_date = format_datetime(future, usegmt=True)
        transport, _ = _router_transport(
            {
                _PROJECT_PATH: _json_response(200, _PROJECT_RESPONSE),
                _FINDINGS_PATH: _empty_response(429, {"Retry-After": http_date}),
                _DEPENDENCIES_PATH: _json_response(200, []),
            }
        )
        client = _client(transport, max_retries=1)

        with pytest.raises(RateLimitException) as excinfo:
            await client.get_security_alerts(42)

        retry_after = excinfo.value.retry_after_seconds
        assert retry_after is not None
        assert 100 <= retry_after <= 120

    @pytest.mark.asyncio
    async def test_429_falls_back_to_rate_limit_reset_header(self) -> None:
        reset_epoch = int(time.time()) + 300
        transport, _ = _router_transport(
            {
                _PROJECT_PATH: _json_response(200, _PROJECT_RESPONSE),
                _FINDINGS_PATH: _empty_response(
                    429, {"RateLimit-Reset": str(reset_epoch)}
                ),
                _DEPENDENCIES_PATH: _json_response(200, []),
            }
        )
        client = _client(transport, max_retries=1)

        with pytest.raises(RateLimitException) as excinfo:
            await client.get_security_alerts(42)

        retry_after = excinfo.value.retry_after_seconds
        assert retry_after is not None
        assert 290 <= retry_after <= 300


# ---------------------------------------------------------------------------
# Single-page parity quirk (connectors/utils/rest.py::get_vulnerability_findings
# / get_dependencies never loop on page/X-Next-Page)
# ---------------------------------------------------------------------------


class TestSinglePageParity:
    @pytest.mark.asyncio
    async def test_findings_endpoint_never_paginates_even_with_next_page_header(
        self,
    ) -> None:
        finding = {"id": 1, "severity": "high", "state": "detected"}
        page = _json_response(200, [finding], headers={"X-Next-Page": "2"})
        transport, calls = _router_transport(
            {
                _PROJECT_PATH: _json_response(200, _PROJECT_RESPONSE),
                _FINDINGS_PATH: page,
                _DEPENDENCIES_PATH: _json_response(200, []),
            }
        )
        client = _client(transport)

        alerts = await client.get_security_alerts(42)

        assert len(alerts) == 1
        assert calls[_FINDINGS_PATH] == 1

    @pytest.mark.asyncio
    async def test_dependencies_endpoint_never_paginates_even_with_next_page_header(
        self,
    ) -> None:
        dependency = {
            "name": "pkg",
            "vulnerabilities": [{"id": 1, "severity": "high", "name": "CVE"}],
        }
        page = _json_response(200, [dependency], headers={"X-Next-Page": "2"})
        transport, calls = _router_transport(
            {
                _PROJECT_PATH: _json_response(200, _PROJECT_RESPONSE),
                _FINDINGS_PATH: _json_response(200, []),
                _DEPENDENCIES_PATH: page,
            }
        )
        client = _client(transport)

        alerts = await client.get_security_alerts(42)

        assert len(alerts) == 1
        assert calls[_DEPENDENCIES_PATH] == 1


# ---------------------------------------------------------------------------
# Usage recording (CHAOS-2754) + the "security:" prefix short-circuit
# ---------------------------------------------------------------------------


class TestUsageRecording:
    @pytest.mark.asyncio
    async def test_all_three_requests_resolve_to_security_route_family(self) -> None:
        transport, _ = _router_transport(
            {
                _PROJECT_PATH: _json_response(200, _PROJECT_RESPONSE),
                _FINDINGS_PATH: _json_response(200, []),
                _DEPENDENCIES_PATH: _json_response(200, []),
            }
        )
        client = _client(transport)

        await client.get_security_alerts(42)
        observations = client.drain_usage_observations()

        assert len(observations) == 1
        observation = observations[0]
        assert observation["route_family"] == "security"
        assert observation["dimension"] == "rest_core"
        assert observation["transport"] == "rest"
        # Project resolve + vulnerability_findings + dependencies == 3 real
        # physical requests, all bucketed under the SAME family because every
        # operation label this client emits carries the "security:" prefix.
        assert observation["request_count"] == 3
        assert observation["latest_status"] == 200

    @pytest.mark.asyncio
    async def test_drain_clears_observations(self) -> None:
        transport, _ = _router_transport(
            {
                _PROJECT_PATH: _json_response(200, _PROJECT_RESPONSE),
                _FINDINGS_PATH: _json_response(200, []),
                _DEPENDENCIES_PATH: _json_response(200, []),
            }
        )
        client = _client(transport)

        await client.get_security_alerts(42)
        client.drain_usage_observations()

        assert client.drain_usage_observations() == []


_PIPELINES_PATH = "/api/v4/projects/42/pipelines"
_RELEASES_PATH = "/api/v4/projects/42/releases"
_DEPLOYMENTS_PATH = "/api/v4/projects/42/deployments"
_DEPLOYMENT_MRS_PATH = "/api/v4/projects/42/repository/commits/abc123/merge_requests"


class TestPipelinesParity:
    @pytest.mark.asyncio
    async def test_pipeline_request_params_mapping_and_usage_family(self) -> None:
        pipeline = {
            "id": 11,
            "status": "success",
            "created_at": "2026-02-01T00:00:00Z",
            "started_at": "2026-02-01T00:01:00Z",
            "finished_at": "2026-02-01T00:05:00Z",
        }
        transport, calls = _router_transport(
            {_PIPELINES_PATH: _json_response(200, [pipeline])}
        )
        client = _client(transport)

        pipelines = await client.get_pipelines(42, max_pipelines=10)

        assert calls[_PIPELINES_PATH] == 1
        assert pipelines[0].pipeline_id == "11"
        assert pipelines[0].status == "success"
        assert pipelines[0].created_at == datetime(
            2026, 2, 1, 0, 0, tzinfo=timezone.utc
        )
        assert pipelines[0].started_at == datetime(
            2026, 2, 1, 0, 1, tzinfo=timezone.utc
        )
        query = dict(transport.captured_urls[_PIPELINES_PATH].params)  # type: ignore[attr-defined]
        assert query == {
            "order_by": "updated_at",
            "sort": "desc",
            "page": "1",
            "per_page": "100",
        }
        observations = client.drain_usage_observations()
        assert observations[0]["route_family"] == "pipelines"
        assert observations[0]["request_count"] == 1

    @pytest.mark.asyncio
    async def test_pipeline_max_over_one_page_follows_x_next_page_until_cap(
        self,
    ) -> None:
        page_one = [{"id": i, "created_at": "2026-02-01T00:00:00Z"} for i in range(100)]
        page_two = [{"id": 100, "created_at": "2026-02-01T00:00:00Z"}]
        transport, calls = _router_transport(
            {
                _PIPELINES_PATH: [
                    _json_response(200, page_one, headers={"X-Next-Page": "2"}),
                    _json_response(200, page_two),
                ]
            }
        )
        client = _client(transport)

        pipelines = await client.get_pipelines(42, max_pipelines=101)

        assert len(pipelines) == 101
        assert calls[_PIPELINES_PATH] == 2

    @pytest.mark.asyncio
    async def test_pipeline_hard_cap_does_not_chase_next_page(self) -> None:
        page_one = [{"id": i, "created_at": "2026-02-01T00:00:00Z"} for i in range(100)]
        transport, calls = _router_transport(
            {
                _PIPELINES_PATH: _json_response(
                    200, page_one, headers={"X-Next-Page": "2"}
                )
            }
        )
        client = _client(transport)

        pipelines = await client.get_pipelines(42, max_pipelines=100)

        assert len(pipelines) == 100
        assert calls[_PIPELINES_PATH] == 1


class TestDeploymentsParity:
    @pytest.mark.asyncio
    async def test_deployments_request_params_mapping_and_usage_family(self) -> None:
        deployment = {
            "id": 501,
            "iid": 7,
            "status": "success",
            "environment": {"name": "production"},
            "created_at": "2026-03-01T10:00:00Z",
            "finished_at": "2026-03-01T10:05:00Z",
            "sha": "abc123",
            "ref": "v1.2.3",
        }
        transport, calls = _router_transport(
            {
                _RELEASES_PATH: _json_response(200, [{"tag_name": "v1.2.3"}]),
                _DEPLOYMENTS_PATH: _json_response(200, [deployment]),
                _DEPLOYMENT_MRS_PATH: _json_response(200, []),
            }
        )
        client = _client(transport)

        releases = await client.get_deployment_releases(42, per_page=10)
        deployments = await client.get_deployments(42, max_deployments=10)
        mrs = await client.get_deployment_merge_requests(42, "abc123")

        assert releases == [{"tag_name": "v1.2.3"}]
        assert mrs == []
        assert calls[_RELEASES_PATH] == 1
        assert calls[_DEPLOYMENTS_PATH] == 1
        assert calls[_DEPLOYMENT_MRS_PATH] == 1
        assert deployments[0].deployment_id == "501"
        assert deployments[0].deployment_iid == 7
        assert deployments[0].status == "success"
        assert deployments[0].environment == "production"
        assert deployments[0].created_at == datetime(
            2026, 3, 1, 10, 0, tzinfo=timezone.utc
        )
        assert deployments[0].finished_at == datetime(
            2026, 3, 1, 10, 5, tzinfo=timezone.utc
        )
        assert dict(transport.captured_urls[_DEPLOYMENTS_PATH].params) == {  # type: ignore[attr-defined]
            "order_by": "created_at",
            "sort": "desc",
            "page": "1",
            "per_page": "10",
        }
        observations = client.drain_usage_observations()
        assert observations[0]["route_family"] == "deployments"
        assert observations[0]["request_count"] == 3

    @pytest.mark.asyncio
    async def test_deployments_endpoint_keeps_frozen_single_page_behavior(self) -> None:
        first_page = [{"id": 1, "created_at": "2026-03-01T10:00:00Z"}]
        transport, calls = _router_transport(
            {
                _DEPLOYMENTS_PATH: _json_response(
                    200, first_page, headers={"X-Next-Page": "2"}
                )
            }
        )
        client = _client(transport)

        deployments = await client.get_deployments(42, max_deployments=10)

        assert len(deployments) == 1
        assert calls[_DEPLOYMENTS_PATH] == 1


_TEST_REPORT_PATH = "/api/v4/projects/42/pipelines/11/test_report"
_PIPELINE_JOBS_PATH = "/api/v4/projects/42/pipelines/11/jobs"
_ARTIFACT_PATH = "/api/v4/projects/42/jobs/99/artifacts"
_ARTIFACT_REDIRECT_PATH = "/artifact.zip"


class TestTestsFamilyParity:
    """CHAOS-2773 CS12: GitLab ``tests`` family (pipeline listing scoped to
    the test-report scan, native JUnit test_report, pipeline jobs, job
    artifact download) -- parity with the legacy
    ``connectors/utils/rest.py::GitLabRESTClient.get_pipeline_test_report``
    / ``get_list`` / ``download_job_artifacts`` (FROZEN) and the
    python-gitlab ``gl_project.pipelines.list()`` call both used to ride,
    now on ``GitLabCodeClient`` (built on the shared
    ``InstrumentedRESTCore``). Every request carries the ``tests:`` family
    prefix so tests usage is distinct from base CI/CD pipeline usage."""

    @pytest.mark.asyncio
    async def test_iter_pipelines_since_request_params_and_usage_family(
        self,
    ) -> None:
        pipeline = {
            "id": 11,
            "ref": "main",
            "created_at": "2026-02-01T00:00:00Z",
        }
        transport, calls = _router_transport(
            {_PIPELINES_PATH: _json_response(200, [pipeline])}
        )
        client = _client(transport)
        since = datetime(2026, 1, 1, tzinfo=timezone.utc)

        pipelines = await client.iter_pipelines_since(42, since=since)

        assert pipelines == [pipeline]
        assert calls[_PIPELINES_PATH] == 1
        query = dict(transport.captured_urls[_PIPELINES_PATH].params)  # type: ignore[attr-defined]
        assert query == {
            "order_by": "updated_at",
            "sort": "desc",
            "updated_after": "2026-01-01T00:00:00+00:00",
            "page": "1",
            "per_page": "100",
        }
        observations = client.drain_usage_observations()
        assert observations[0]["route_family"] == "tests"

    @pytest.mark.asyncio
    async def test_iter_pipelines_since_omits_updated_after_when_since_none(
        self,
    ) -> None:
        transport, calls = _router_transport({_PIPELINES_PATH: _json_response(200, [])})
        client = _client(transport)

        await client.iter_pipelines_since(42)

        query = dict(transport.captured_urls[_PIPELINES_PATH].params)  # type: ignore[attr-defined]
        assert "updated_after" not in query
        assert calls[_PIPELINES_PATH] == 1

    @pytest.mark.asyncio
    async def test_get_pipeline_test_report_returns_parsed_json(self) -> None:
        report_body = {"test_suites": [{"name": "unit", "total_count": 3}]}
        transport, calls = _router_transport(
            {_TEST_REPORT_PATH: _json_response(200, report_body)}
        )
        client = _client(transport)

        report = await client.get_pipeline_test_report(42, 11)

        assert report == report_body
        assert calls[_TEST_REPORT_PATH] == 1
        observations = client.drain_usage_observations()
        assert observations[0]["route_family"] == "tests"

    @pytest.mark.asyncio
    async def test_get_pipeline_test_report_404_propagates(self) -> None:
        transport, _ = _router_transport({_TEST_REPORT_PATH: _empty_response(404)})
        client = _client(transport)

        with pytest.raises(NotFoundException):
            await client.get_pipeline_test_report(42, 11)

    @pytest.mark.asyncio
    async def test_iter_pipeline_jobs_single_page_no_include_retried(
        self,
    ) -> None:
        jobs_page = [{"id": i, "name": f"job-{i}"} for i in range(100)]
        transport, calls = _router_transport(
            {
                _PIPELINE_JOBS_PATH: _json_response(
                    200, jobs_page, headers={"X-Next-Page": "2"}
                )
            }
        )
        client = _client(transport)

        jobs = await client.iter_pipeline_jobs(42, 11)

        assert len(jobs) == 100
        assert calls[_PIPELINE_JOBS_PATH] == 1
        query = dict(transport.captured_urls[_PIPELINE_JOBS_PATH].params)  # type: ignore[attr-defined]
        assert "include_retried" not in query

    @pytest.mark.asyncio
    async def test_download_job_artifact_returns_bytes_on_200(self) -> None:
        transport, calls = _router_transport(
            {_ARTIFACT_PATH: httpx.Response(200, content=b"zip-bytes", headers={})}
        )
        client = _client(transport)

        data = await client.download_job_artifact(42, 99)

        assert data == b"zip-bytes"
        assert calls[_ARTIFACT_PATH] == 1
        observations = client.drain_usage_observations()
        assert observations[0]["route_family"] == "tests"

    @pytest.mark.asyncio
    async def test_download_job_artifact_follows_presigned_redirect(self) -> None:
        transport, calls = _router_transport(
            {
                _ARTIFACT_PATH: httpx.Response(
                    302,
                    headers={"Location": "https://storage.example/artifact.zip"},
                ),
                _ARTIFACT_REDIRECT_PATH: httpx.Response(
                    200,
                    content=b"redirected-zip-bytes",
                    headers={},
                ),
            }
        )
        client = _client(transport)

        data = await client.download_job_artifact(42, 99)

        assert data == b"redirected-zip-bytes"
        assert calls[_ARTIFACT_PATH] == 1
        assert calls[_ARTIFACT_REDIRECT_PATH] == 1
        captured_headers = cast(Any, transport).captured_headers
        assert "PRIVATE-TOKEN" not in captured_headers[_ARTIFACT_REDIRECT_PATH]
        observations = client.drain_usage_observations()
        assert len(observations) == 1
        assert observations[0]["route_family"] == "tests"
        assert observations[0]["request_count"] == 2

    @pytest.mark.asyncio
    async def test_download_job_artifact_returns_empty_on_404(self) -> None:
        transport, _ = _router_transport({_ARTIFACT_PATH: _empty_response(404)})
        client = _client(transport)

        data = await client.download_job_artifact(42, 99)

        assert data == b""

    @pytest.mark.asyncio
    async def test_download_job_artifact_discards_over_byte_cap(self) -> None:
        transport, _ = _router_transport(
            {_ARTIFACT_PATH: httpx.Response(200, content=b"x" * 100, headers={})}
        )
        client = _client(transport)

        data = await client.download_job_artifact(42, 99, max_bytes=10)

        assert data == b""


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


class TestLifecycle:
    @pytest.mark.asyncio
    async def test_context_manager_closes_underlying_client(self) -> None:
        transport, _ = _router_transport(
            {
                _PROJECT_PATH: _json_response(200, _PROJECT_RESPONSE),
                _FINDINGS_PATH: _json_response(200, []),
                _DEPENDENCIES_PATH: _json_response(200, []),
            }
        )
        async with _client(transport) as client:
            await client.get_security_alerts(42)
