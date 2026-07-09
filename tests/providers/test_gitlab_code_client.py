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
from urllib.parse import quote

import httpx
import pytest

from dev_health_ops.exceptions import (
    APIException,
    AuthenticationException,
    NotFoundException,
    RateLimitException,
)
from dev_health_ops.providers.gitlab.code_client import GitLabCodeClient
from dev_health_ops.sync.budget_types import BudgetDimension

_PROJECT_PATH = "/api/v4/projects/42"
_FINDINGS_PATH = "/api/v4/projects/42/vulnerability_findings"
_DEPENDENCIES_PATH = "/api/v4/projects/42/dependencies"
_PROJECTS_PATH = "/api/v4/projects"

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
    # Keyed by the same DECODED ``request.url.path`` the router matches on --
    # ``.path`` itself decodes a percent-encoded '/' (%2F) back to a literal
    # '/', so it cannot prove wire-level path-segment encoding happened;
    # ``.raw_path`` retains the percent-encoding actually sent on the wire
    # (see ``TestProjectIdPathEncoding``).
    captured_raw_paths: dict[str, bytes] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        calls[path] = calls.get(path, 0) + 1
        captured_headers[path] = request.headers
        captured_urls[path] = request.url
        captured_raw_paths[path] = request.url.raw_path
        entry = routes[path]
        if isinstance(entry, list):
            idx = min(calls[path] - 1, len(entry) - 1)
            return entry[idx]
        return entry

    transport = httpx.MockTransport(handler)
    transport.calls = calls  # type: ignore[attr-defined]
    transport.captured_headers = captured_headers  # type: ignore[attr-defined]
    transport.captured_urls = captured_urls  # type: ignore[attr-defined]
    transport.captured_raw_paths = captured_raw_paths  # type: ignore[attr-defined]
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


class TestProjectMetadataAndDiscovery:
    @pytest.mark.asyncio
    async def test_get_project_maps_metadata_and_records_project_usage(self) -> None:
        project_path = "/api/v4/projects/group/project"
        transport, _ = _router_transport(
            {
                project_path: _json_response(
                    200,
                    {
                        "id": "42",
                        "name": "project",
                        "path_with_namespace": "group/project",
                        "web_url": "https://gitlab.example.com/group/project",
                        "default_branch": "trunk",
                    },
                )
            }
        )
        client = _client(transport, base_url="https://gitlab.example.com/")

        project = await client.get_project("group/project")

        assert project.id == 42
        assert project.name == "project"
        assert project.path_with_namespace == "group/project"
        assert project.web_url == "https://gitlab.example.com/group/project"
        assert project.default_branch == "trunk"
        assert (
            b"/api/v4/projects/group%2Fproject"
            in transport.captured_raw_paths[  # type: ignore[attr-defined]
                project_path
            ]
        )
        observations = client.drain_usage_observations()
        assert len(observations) == 1
        assert observations[0]["route_family"] == "project"
        assert observations[0]["request_count"] == 1

    @pytest.mark.asyncio
    async def test_list_projects_paginates_membership_and_records_per_page_usage(
        self,
    ) -> None:
        transport, calls = _router_transport(
            {
                _PROJECTS_PATH: [
                    _json_response(
                        200,
                        [
                            {
                                "id": 1,
                                "name": "alpha",
                                "path_with_namespace": "group/alpha",
                                "default_branch": "main",
                                "web_url": "https://gitlab.example.com/group/alpha",
                            },
                            {
                                "id": "2",
                                "name": "beta",
                                "path_with_namespace": "group/beta",
                                "default_branch": "develop",
                                "web_url": "https://gitlab.example.com/group/beta",
                            },
                        ],
                        headers={"X-Next-Page": "2"},
                    ),
                    _json_response(
                        200,
                        [
                            {
                                "id": 3,
                                "name": "gamma",
                                "path_with_namespace": "group/gamma",
                                "default_branch": None,
                                "web_url": "https://gitlab.example.com/group/gamma",
                            }
                        ],
                    ),
                ]
            }
        )
        client = _client(transport)

        repos = await client.list_projects(
            membership=True,
            pattern="group/*a*",
            max_projects=3,
            per_page=2,
        )

        assert [repo.full_name for repo in repos] == [
            "group/alpha",
            "group/beta",
            "group/gamma",
        ]
        assert repos[1].id == 2
        assert repos[2].default_branch == "main"
        assert calls[_PROJECTS_PATH] == 2
        first_url = transport.captured_urls[_PROJECTS_PATH]  # type: ignore[attr-defined]
        assert first_url.params["membership"] == "true"
        assert first_url.params["per_page"] == "2"
        observations = client.drain_usage_observations()
        assert len(observations) == 1
        assert observations[0]["route_family"] == "project"
        assert observations[0]["request_count"] == 2


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
_COMMITS_PATH = "/api/v4/projects/42/repository/commits"
_COMMIT_DETAIL_PATH = "/api/v4/projects/42/repository/commits/abc123"


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


class TestCommitsParity:
    @pytest.mark.asyncio
    async def test_commit_list_params_mapping_and_usage_family(self) -> None:
        commit = {
            "id": "abc123",
            "message": "ship it",
            "author_name": "Ada",
            "authored_date": "2026-01-10T00:00:00Z",
            "committer_name": "Grace",
            "committed_date": "2026-01-10T00:01:00Z",
            "parent_ids": ["parent1"],
        }
        transport, calls = _router_transport(
            {_COMMITS_PATH: _json_response(200, [commit])}
        )
        client = _client(transport)
        since = datetime(2026, 1, 1, tzinfo=timezone.utc)
        until = datetime(2026, 1, 31, tzinfo=timezone.utc)

        commits = await client.get_commits(42, max_commits=10, since=since, until=until)

        assert calls[_COMMITS_PATH] == 1
        assert commits[0].commit_id == "abc123"
        assert commits[0].message == "ship it"
        assert commits[0].author_name == "Ada"
        assert commits[0].committer_name == "Grace"
        assert commits[0].parent_ids == ("parent1",)
        query = dict(transport.captured_urls[_COMMITS_PATH].params)  # type: ignore[attr-defined]
        assert query == {
            "since": "2026-01-01T00:00:00Z",
            "until": "2026-01-31T00:00:00Z",
            "page": "1",
            "per_page": "10",
        }
        observations = client.drain_usage_observations()
        assert observations[0]["route_family"] == "project"
        assert observations[0]["request_count"] == 1

    @pytest.mark.asyncio
    async def test_commit_list_paginates_until_cap(self) -> None:
        page_one = [
            {"id": f"sha-{i}", "committed_date": "2026-01-10T00:00:00Z"}
            for i in range(100)
        ]
        page_two = [{"id": "sha-100", "committed_date": "2026-01-10T00:00:00Z"}]
        transport, calls = _router_transport(
            {
                _COMMITS_PATH: [
                    _json_response(200, page_one, headers={"X-Next-Page": "2"}),
                    _json_response(200, page_two),
                ]
            }
        )
        client = _client(transport)

        commits = await client.get_commits(42, max_commits=101)

        assert len(commits) == 101
        assert calls[_COMMITS_PATH] == 2

    @pytest.mark.asyncio
    async def test_latest_commit_sha_sends_ref_and_until_window(self) -> None:
        commit = {"id": "abc123", "committed_date": "2026-01-10T00:00:00Z"}
        transport, calls = _router_transport(
            {_COMMITS_PATH: _json_response(200, [commit])}
        )
        client = _client(transport)
        until = datetime(2026, 1, 31, tzinfo=timezone.utc)

        sha = await client.get_latest_commit_sha(42, ref="main", until=until)

        assert sha == "abc123"
        assert calls[_COMMITS_PATH] == 1
        query = dict(transport.captured_urls[_COMMITS_PATH].params)  # type: ignore[attr-defined]
        assert query == {
            "ref_name": "main",
            "until": "2026-01-31T00:00:00Z",
            "page": "1",
            "per_page": "1",
        }
        observations = client.drain_usage_observations()
        assert observations[0]["route_family"] == "project"
        assert observations[0]["request_count"] == 1

    @pytest.mark.asyncio
    async def test_commit_stats_maps_aggregate_stats_and_usage_family(self) -> None:
        detail = {"id": "abc123", "stats": {"additions": 12, "deletions": 3}}
        transport, calls = _router_transport(
            {_COMMIT_DETAIL_PATH: _json_response(200, detail)}
        )
        client = _client(transport)

        stats = await client.get_commit_stats(42, "abc123")

        assert calls[_COMMIT_DETAIL_PATH] == 1
        assert stats.commit_id == "abc123"
        assert stats.additions == 12
        assert stats.deletions == 3
        observations = client.drain_usage_observations()
        assert observations[0]["route_family"] == "project"
        assert observations[0]["request_count"] == 1


# ---------------------------------------------------------------------------
# Project-id path-segment encoding (CHAOS-2814 security review): get_commits
# / get_commit_stats must percent-encode a caller-supplied project_id/path
# into ONE opaque URL path segment -- never literal additional path
# structure or query metacharacters. httpx's ``request.url.path`` decodes a
# percent-encoded '/' (%2F) back to a literal '/' for display, so these
# tests assert on ``request.url.raw_path`` (the actual wire bytes) via
# ``transport.captured_raw_paths`` -- ``.path`` alone cannot prove encoding
# occurred.
# ---------------------------------------------------------------------------


class TestProjectIdPathEncoding:
    @pytest.mark.asyncio
    async def test_commits_encodes_project_id_containing_slash(self) -> None:
        commit = {"id": "abc123", "committed_date": "2026-01-10T00:00:00Z"}
        raw_project_id = "group/project"
        decoded_path = "/api/v4/projects/group/project/repository/commits"
        transport, calls = _router_transport(
            {decoded_path: _json_response(200, [commit])}
        )
        client = _client(transport)

        commits = await client.get_commits(raw_project_id, max_commits=10)

        assert calls[decoded_path] == 1
        assert commits[0].commit_id == "abc123"
        raw_path = transport.captured_raw_paths[decoded_path]  # type: ignore[attr-defined]
        assert (
            raw_path.split(b"?")[0]
            == (
                f"/api/v4/projects/{quote(raw_project_id, safe='')}/repository/commits"
            ).encode()
        )

    @pytest.mark.asyncio
    async def test_commits_encodes_project_id_containing_query_metacharacter(
        self,
    ) -> None:
        commit = {"id": "abc123", "committed_date": "2026-01-10T00:00:00Z"}
        raw_project_id = "42?evil=1"
        decoded_path = "/api/v4/projects/42?evil=1/repository/commits"
        transport, calls = _router_transport(
            {decoded_path: _json_response(200, [commit])}
        )
        client = _client(transport)

        commits = await client.get_commits(raw_project_id, max_commits=10)

        assert calls[decoded_path] == 1
        assert commits[0].commit_id == "abc123"
        raw_path = transport.captured_raw_paths[decoded_path]  # type: ignore[attr-defined]
        assert (
            raw_path.split(b"?", 1)[0]
            == (
                f"/api/v4/projects/{quote(raw_project_id, safe='')}/repository/commits"
            ).encode()
        )
        # The '?' must have been escaped INTO the project-id segment, not
        # parsed as the start of the query string -- only the real per_page/
        # page params the client adds should appear as query parameters.
        assert dict(transport.captured_urls[decoded_path].params) == {  # type: ignore[attr-defined]
            "page": "1",
            "per_page": "10",
        }

    @pytest.mark.asyncio
    async def test_commits_encodes_project_id_containing_path_traversal(self) -> None:
        commit = {"id": "abc123", "committed_date": "2026-01-10T00:00:00Z"}
        raw_project_id = "../../etc/passwd"
        decoded_path = "/api/v4/projects/../../etc/passwd/repository/commits"
        transport, calls = _router_transport(
            {decoded_path: _json_response(200, [commit])}
        )
        client = _client(transport)

        commits = await client.get_commits(raw_project_id, max_commits=10)

        assert calls[decoded_path] == 1
        assert commits[0].commit_id == "abc123"
        raw_path = transport.captured_raw_paths[decoded_path]  # type: ignore[attr-defined]
        expected_segment = quote(raw_project_id, safe="")
        assert (
            raw_path.split(b"?")[0]
            == (f"/api/v4/projects/{expected_segment}/repository/commits").encode()
        )
        # Every literal '/' inside the traversal payload must be escaped --
        # the wire path carries exactly the client's own two structural
        # slashes ("/api/v4/projects/" .. "/repository/commits"), never the
        # attacker-controlled ones.
        assert b"..%2F" in raw_path
        assert b"/../" not in raw_path

    @pytest.mark.asyncio
    async def test_commits_numeric_project_id_path_is_unencoded(self) -> None:
        """Regression: a plain numeric project_id must still reach the wire
        byte-for-byte unencoded (digits are unreserved per RFC 3986)."""
        commit = {"id": "abc123", "committed_date": "2026-01-10T00:00:00Z"}
        transport, calls = _router_transport(
            {_COMMITS_PATH: _json_response(200, [commit])}
        )
        client = _client(transport)

        await client.get_commits(42, max_commits=10)

        assert calls[_COMMITS_PATH] == 1
        raw_path = transport.captured_raw_paths[_COMMITS_PATH]  # type: ignore[attr-defined]
        assert raw_path.split(b"?")[0] == _COMMITS_PATH.encode()

    @pytest.mark.asyncio
    async def test_commit_stats_encodes_project_id_containing_slash(self) -> None:
        detail = {"id": "abc123", "stats": {"additions": 1, "deletions": 2}}
        raw_project_id = "group/project"
        decoded_path = "/api/v4/projects/group/project/repository/commits/abc123"
        transport, calls = _router_transport(
            {decoded_path: _json_response(200, detail)}
        )
        client = _client(transport)

        stats = await client.get_commit_stats(raw_project_id, "abc123")

        assert calls[decoded_path] == 1
        assert stats.additions == 1
        assert stats.deletions == 2
        raw_path = transport.captured_raw_paths[decoded_path]  # type: ignore[attr-defined]
        assert (
            raw_path
            == (
                f"/api/v4/projects/{quote(raw_project_id, safe='')}"
                "/repository/commits/abc123"
            ).encode()
        )

    @pytest.mark.asyncio
    async def test_commit_stats_encodes_project_id_containing_path_traversal(
        self,
    ) -> None:
        detail = {"id": "abc123", "stats": {"additions": 1, "deletions": 2}}
        raw_project_id = "../../etc/passwd"
        decoded_path = "/api/v4/projects/../../etc/passwd/repository/commits/abc123"
        transport, calls = _router_transport(
            {decoded_path: _json_response(200, detail)}
        )
        client = _client(transport)

        stats = await client.get_commit_stats(raw_project_id, "abc123")

        assert calls[decoded_path] == 1
        assert stats.additions == 1
        raw_path = transport.captured_raw_paths[decoded_path]  # type: ignore[attr-defined]
        expected_segment = quote(raw_project_id, safe="")
        assert (
            raw_path
            == (
                f"/api/v4/projects/{expected_segment}/repository/commits/abc123"
            ).encode()
        )
        assert b"/../" not in raw_path

    @pytest.mark.asyncio
    async def test_commit_stats_numeric_project_id_path_is_unencoded(self) -> None:
        """Regression: a plain numeric project_id must still reach the wire
        byte-for-byte unencoded (digits are unreserved per RFC 3986)."""
        detail = {"id": "abc123", "stats": {"additions": 12, "deletions": 3}}
        transport, calls = _router_transport(
            {_COMMIT_DETAIL_PATH: _json_response(200, detail)}
        )
        client = _client(transport)

        stats = await client.get_commit_stats(42, "abc123")

        assert calls[_COMMIT_DETAIL_PATH] == 1
        assert stats.additions == 12
        raw_path = transport.captured_raw_paths[_COMMIT_DETAIL_PATH]  # type: ignore[attr-defined]
        assert raw_path == _COMMIT_DETAIL_PATH.encode()


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


# ---------------------------------------------------------------------------
# ``files`` dataset (CHAOS-2815/CS14): batched GraphQL blob-content fetch --
# parity with the legacy ``GitLabConnector.get_file_contents`` /
# ``_graphql_blobs`` (``connectors/gitlab.py``, FROZEN).
# ---------------------------------------------------------------------------

_GRAPHQL_PATH = "/api/graphql"


def _graphql_response(data: dict) -> httpx.Response:
    return httpx.Response(200, json={"data": data}, headers={})


class TestFileContentsParity:
    @pytest.mark.asyncio
    async def test_size_pass_then_text_pass_filters_oversized(self) -> None:
        responses = [
            _graphql_response(
                {
                    "project": {
                        "repository": {
                            "blobs": {
                                "nodes": [
                                    {"path": "src/a.py", "rawSize": 7},
                                    {"path": "big.py", "rawSize": 2_000_000},
                                ]
                            }
                        }
                    }
                }
            ),
            _graphql_response(
                {
                    "project": {
                        "repository": {
                            "blobs": {
                                "nodes": [
                                    {"path": "src/a.py", "rawTextBlob": "x = 1\n"},
                                ]
                            }
                        }
                    }
                }
            ),
        ]
        transport, calls = _router_transport({_GRAPHQL_PATH: responses})
        client = _client(transport, private_token="secret-tok")

        result = await client.get_file_contents(
            "group/project", ["src/a.py", "big.py"], ref="main"
        )

        assert result == {"src/a.py": "x = 1\n"}
        assert calls[_GRAPHQL_PATH] == 2
        assert (
            transport.captured_headers[_GRAPHQL_PATH]["PRIVATE-TOKEN"]  # type: ignore[attr-defined]
            == "secret-tok"
        )
        observations = client.drain_usage_observations()
        assert observations[0]["route_family"] == "project"
        assert observations[0]["request_count"] == 2

    @pytest.mark.asyncio
    async def test_no_size_pass_when_max_bytes_disabled(self) -> None:
        transport, calls = _router_transport(
            {
                _GRAPHQL_PATH: _graphql_response(
                    {
                        "project": {
                            "repository": {
                                "blobs": {
                                    "nodes": [{"path": "a.py", "rawTextBlob": "a"}]
                                }
                            }
                        }
                    }
                )
            }
        )
        client = _client(transport)

        result = await client.get_file_contents(
            "group/project", ["a.py"], ref="main", max_bytes=None
        )

        assert result == {"a.py": "a"}
        assert calls[_GRAPHQL_PATH] == 1

    @pytest.mark.asyncio
    async def test_empty_paths_makes_no_request(self) -> None:
        transport, calls = _router_transport({})
        client = _client(transport)

        result = await client.get_file_contents("group/project", [], ref="main")

        assert result == {}
        assert calls == {}

    @pytest.mark.asyncio
    async def test_graphql_errors_degrade_to_empty_for_that_chunk(self) -> None:
        """A GraphQL ``errors`` payload on the text pass is per-chunk
        resilience, not a hard failure -- mirrors the legacy connector's own
        ``except Exception`` degrade-and-continue contract."""
        transport, _ = _router_transport(
            {
                _GRAPHQL_PATH: httpx.Response(
                    200, json={"errors": [{"message": "boom"}]}, headers={}
                )
            }
        )
        client = _client(transport, max_retries=1)

        result = await client.get_file_contents(
            "group/project", ["a.py"], ref="main", max_bytes=None
        )

        assert result == {}

    @pytest.mark.asyncio
    async def test_graphql_blobs_raises_api_exception_on_errors(self) -> None:
        """The private ``_graphql_blobs`` helper itself DOES raise on a
        GraphQL ``errors`` payload -- ``get_file_contents`` is the layer that
        chooses to degrade per-chunk."""
        transport, _ = _router_transport(
            {
                _GRAPHQL_PATH: httpx.Response(
                    200, json={"errors": [{"message": "boom"}]}, headers={}
                )
            }
        )
        client = _client(transport, max_retries=1)

        with pytest.raises(APIException):
            await client._graphql_blobs(
                "group/project", "main", ["a.py"], "path rawTextBlob"
            )

    @pytest.mark.asyncio
    async def test_429_raises_rate_limit_exception(self) -> None:
        transport, calls = _router_transport(
            {_GRAPHQL_PATH: _empty_response(429, {"Retry-After": "0"})}
        )
        client = _client(transport, max_retries=2)

        with pytest.raises(RateLimitException) as excinfo:
            await client.get_file_contents(
                "group/project", ["a.py"], ref="main", max_bytes=None
            )

        assert calls[_GRAPHQL_PATH] == 2
        assert excinfo.value.signal is not None
        assert excinfo.value.signal.dimension is BudgetDimension.REST_CORE


# ---------------------------------------------------------------------------
# ``blame`` dataset (CHAOS-2815/CS14): parity with the legacy
# ``GitLabRESTClient.get_file_blame`` (``connectors/utils/rest.py``, FROZEN).
# ---------------------------------------------------------------------------


class TestFileBlameParity:
    @pytest.mark.asyncio
    async def test_returns_normalized_blame_ranges_and_usage_family(self) -> None:
        blame_path = "/api/v4/projects/42/repository/files/app.py/blame"
        transport, calls = _router_transport(
            {
                blame_path: _json_response(
                    200,
                    [
                        {
                            "commit": {
                                "id": "sha1",
                                "author_name": "Ada",
                                "author_email": "ada@example.com",
                            },
                            "lines": ["x = 1", "y = 2"],
                        },
                        {
                            "commit": {
                                "id": "sha2",
                                "author_name": "Grace",
                                "author_email": "grace@example.com",
                            },
                            "lines": ["z = 3"],
                        },
                    ],
                )
            }
        )
        client = _client(transport)

        blame = await client.get_file_blame(42, "app.py", ref="main")

        assert blame.file_path == "app.py"
        assert [
            (rng.starting_line, rng.ending_line, rng.commit_sha, rng.author)
            for rng in blame.ranges
        ] == [(1, 2, "sha1", "Ada"), (3, 3, "sha2", "Grace")]
        assert blame.ranges[0].author_email == "ada@example.com"
        assert [rng.lines for rng in blame.ranges] == [("x = 1", "y = 2"), ("z = 3",)]
        assert calls[blame_path] == 1
        query = dict(transport.captured_urls[blame_path].params)  # type: ignore[attr-defined]
        assert query == {"ref": "main"}
        observations = client.drain_usage_observations()
        assert observations[0]["route_family"] == "project"
        assert observations[0]["request_count"] == 1

    @pytest.mark.asyncio
    async def test_404_propagates(self) -> None:
        blame_path = "/api/v4/projects/42/repository/files/missing.py/blame"
        transport, _ = _router_transport({blame_path: _empty_response(404)})
        client = _client(transport)

        with pytest.raises(NotFoundException):
            await client.get_file_blame(42, "missing.py", ref="main")

    @pytest.mark.asyncio
    async def test_429_raises_rate_limit_exception(self) -> None:
        blame_path = "/api/v4/projects/42/repository/files/app.py/blame"
        transport, calls = _router_transport(
            {blame_path: _empty_response(429, {"Retry-After": "0"})}
        )
        client = _client(transport, max_retries=2)

        with pytest.raises(RateLimitException):
            await client.get_file_blame(42, "app.py", ref="main")

        assert calls[blame_path] == 2

    @pytest.mark.asyncio
    async def test_encodes_file_path_containing_slash(self) -> None:
        raw_file_path = "src/nested/app.py"
        decoded_path = "/api/v4/projects/42/repository/files/src/nested/app.py/blame"
        transport, calls = _router_transport({decoded_path: _json_response(200, [])})
        client = _client(transport)

        blame = await client.get_file_blame(42, raw_file_path, ref="HEAD")

        assert blame.file_path == raw_file_path
        assert blame.ranges == ()
        assert calls[decoded_path] == 1
        raw_path = transport.captured_raw_paths[decoded_path]  # type: ignore[attr-defined]
        expected_segment = quote(raw_file_path, safe="")
        assert (
            raw_path.split(b"?")[0]
            == f"/api/v4/projects/42/repository/files/{expected_segment}/blame".encode()
        )


# ---------------------------------------------------------------------------
# ``merge_requests`` / ``notes`` datasets (CHAOS-2816/CS15): parity with the
# legacy GitLab REST client MR list, MR commits, approvals, and notes helpers.
# ---------------------------------------------------------------------------


class TestMergeRequestsAndNotesParity:
    @pytest.mark.asyncio
    async def test_iter_merge_requests_uses_expected_query_and_usage_family(
        self,
    ) -> None:
        mr_path = "/api/v4/projects/42/merge_requests"
        mr = {
            "id": "1001",
            "iid": "7",
            "title": "Ship it",
            "updated_at": "2026-01-02T03:04:05Z",
        }
        transport, calls = _router_transport({mr_path: _json_response(200, [mr])})
        client = _client(transport)

        result = await client.iter_merge_requests(42, per_page=50)

        assert result == [mr]
        assert calls[mr_path] == 1
        query = dict(cast(Any, transport).captured_urls[mr_path].params)
        assert query == {
            "state": "all",
            "order_by": "updated_at",
            "sort": "desc",
            "page": "1",
            "per_page": "50",
        }
        observations = client.drain_usage_observations()
        assert observations[0]["route_family"] == "merge_requests"
        assert observations[0]["request_count"] == 1

    @pytest.mark.asyncio
    async def test_get_merge_requests_page_uses_expected_page_query(self) -> None:
        mr_path = "/api/v4/projects/42/merge_requests"
        mr = {"iid": "7", "title": "Ship it"}
        transport, calls = _router_transport({mr_path: _json_response(200, [mr])})
        client = _client(transport)

        result = await client.get_merge_requests_page(42, page=3, per_page=50)

        assert result == [mr]
        assert calls[mr_path] == 1
        query = dict(cast(Any, transport).captured_urls[mr_path].params)
        assert query == {
            "state": "all",
            "order_by": "updated_at",
            "sort": "desc",
            "page": "3",
            "per_page": "50",
        }
        observations = client.drain_usage_observations()
        assert observations[0]["route_family"] == "merge_requests"
        assert observations[0]["request_count"] == 1

    @pytest.mark.asyncio
    async def test_iter_merge_requests_exhausts_empty_last_page(self) -> None:
        mr_path = "/api/v4/projects/42/merge_requests"
        transport, calls = _router_transport(
            {
                mr_path: [
                    _json_response(200, [{"iid": 1}], {"X-Next-Page": "2"}),
                    _json_response(200, []),
                ]
            }
        )
        client = _client(transport)

        result = await client.iter_merge_requests(42, per_page=1)

        assert result == [{"iid": 1}]
        assert calls[mr_path] == 2
        observations = client.drain_usage_observations()
        assert observations[0]["request_count"] == 2

    @pytest.mark.asyncio
    async def test_iter_merge_requests_empty_first_page_records_one_request(
        self,
    ) -> None:
        mr_path = "/api/v4/projects/42/merge_requests"
        transport, calls = _router_transport({mr_path: _json_response(200, [])})
        client = _client(transport)

        result = await client.iter_merge_requests(42)

        assert result == []
        assert calls[mr_path] == 1
        observations = client.drain_usage_observations()
        assert observations[0]["request_count"] == 1

    @pytest.mark.asyncio
    async def test_iter_merge_requests_item_cap_uses_min_per_page(self) -> None:
        mr_path = "/api/v4/projects/42/merge_requests"
        transport, calls = _router_transport(
            {mr_path: _json_response(200, [{"iid": 1}, {"iid": 2}])}
        )
        client = _client(transport)

        result = await client.iter_merge_requests(42, max_items=2, per_page=100)

        assert result == [{"iid": 1}, {"iid": 2}]
        assert calls[mr_path] == 1
        query = dict(cast(Any, transport).captured_urls[mr_path].params)
        assert query["per_page"] == "2"

    @pytest.mark.asyncio
    async def test_iter_merge_requests_failure_preserves_partial_observations(
        self,
    ) -> None:
        mr_path = "/api/v4/projects/42/merge_requests"
        transport, calls = _router_transport(
            {
                mr_path: [
                    _json_response(200, [{"iid": 1}], {"X-Next-Page": "2"}),
                    _empty_response(500),
                ]
            }
        )
        client = _client(transport, max_retries=1)

        with pytest.raises(APIException):
            await client.iter_merge_requests(42, per_page=1)

        assert calls[mr_path] == 2
        observations = client.drain_usage_observations()
        assert observations[0]["route_family"] == "merge_requests"
        assert observations[0]["request_count"] == 2

    @pytest.mark.asyncio
    async def test_iter_merge_requests_encodes_project_path_segment(self) -> None:
        project_id = "group/sub.project?x=1"
        decoded_path = f"/api/v4/projects/{project_id}/merge_requests"
        transport, calls = _router_transport({decoded_path: _json_response(200, [])})
        client = _client(transport)

        result = await client.iter_merge_requests(project_id)

        assert result == []
        assert calls[decoded_path] == 1
        raw_path = cast(Any, transport).captured_raw_paths[decoded_path]
        assert raw_path.split(b"?")[0] == (
            f"/api/v4/projects/{quote(project_id, safe='')}/merge_requests".encode()
        )

    @pytest.mark.asyncio
    async def test_iter_mr_commits_coerces_int_and_str_ids_in_path(self) -> None:
        commits_path = "/api/v4/projects/42/merge_requests/7/commits"
        commit = {"id": "abc123", "created_at": "2026-01-02T03:04:05Z"}
        transport, calls = _router_transport(
            {commits_path: _json_response(200, [commit])}
        )
        client = _client(transport)

        result = await client.iter_mr_commits("42", 7)

        assert result == [commit]
        assert calls[commits_path] == 1
        observations = client.drain_usage_observations()
        assert observations[0]["route_family"] == "merge_requests"

    @pytest.mark.asyncio
    async def test_iter_mr_notes_exhausts_pages_and_records_notes_family(self) -> None:
        notes_path = "/api/v4/projects/42/merge_requests/7/notes"
        notes = [
            {"id": 1, "created_at": "2026-01-02T03:04:05Z"},
            {"id": 2, "created_at": "2026-01-02T03:05:05Z"},
        ]
        transport, calls = _router_transport(
            {
                notes_path: [
                    _json_response(200, [notes[0]], {"X-Next-Page": "2"}),
                    _json_response(200, [notes[1]]),
                ]
            }
        )
        client = _client(transport)

        result = await client.iter_mr_notes(42, "7", per_page=2)

        assert result == notes
        assert calls[notes_path] == 2
        query = dict(cast(Any, transport).captured_urls[notes_path].params)
        assert query == {
            "sort": "asc",
            "order_by": "created_at",
            "page": "2",
            "per_page": "2",
        }
        observations = client.drain_usage_observations()
        assert observations[0]["route_family"] == "notes"
        assert observations[0]["request_count"] == 2

    @pytest.mark.asyncio
    async def test_get_mr_approvals_uses_single_request(self) -> None:
        approvals_path = "/api/v4/projects/42/merge_requests/7/approvals"
        approvals = {"approved_by": [{"user": {"id": "5", "username": "ada"}}]}
        transport, calls = _router_transport(
            {approvals_path: _json_response(200, approvals)}
        )
        client = _client(transport)

        result = await client.get_mr_approvals(42, 7)

        assert result == approvals
        assert calls[approvals_path] == 1
        observations = client.drain_usage_observations()
        assert observations[0]["route_family"] == "merge_requests"
        assert observations[0]["request_count"] == 1
