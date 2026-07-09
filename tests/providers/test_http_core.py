"""Tests for providers/_http.py::InstrumentedRESTCore (CHAOS-2773 CS1).

Covers the shared transport primitive every canonical code client (CS3+)
will compose: recorder-per-physical-attempt (including retries),
Retry-After-honoring backoff, exhaustion -> canonical RateLimitException +
RateLimitSignal, the ``classify_error``/``is_retryable_status`` extension
points, both paginators (including the documented edge cases: missing Link
header, empty GitLab X-Next-Page, empty first page, hard page caps), and
GHE / self-hosted-GitLab base-URL joining.
"""

from __future__ import annotations

import time
from unittest.mock import AsyncMock

import httpx
import pytest

from dev_health_ops.exceptions import (
    APIException,
    AuthenticationException,
    NotFoundException,
    RateLimitException,
)
from dev_health_ops.providers._http import (
    GITHUB_DEFAULT_BASE_URL,
    GITHUB_DIAGNOSTIC_HEADER_NAMES,
    GITLAB_DEFAULT_BASE_URL,
    GITLAB_DIAGNOSTIC_HEADER_NAMES,
    InstrumentedRESTCore,
    github_rest_base_url,
    gitlab_rest_base_url,
)
from dev_health_ops.providers.usage import OperationResolver, UsageRouteFamily
from dev_health_ops.sync.budget_types import BudgetDimension

_GITHUB_RESOLVER = OperationResolver(
    families=(UsageRouteFamily("git", BudgetDimension.REST_CORE),),
    defaults=(("rest", "unclassified_default", BudgetDimension.REST_CORE),),
)


def _core(**overrides: object) -> InstrumentedRESTCore:
    kwargs: dict[str, object] = {
        "base_url": "https://api.github.com",
        "provider": "github",
        "resolver": _GITHUB_RESOLVER,
        "max_retries": 3,
    }
    kwargs.update(overrides)
    return InstrumentedRESTCore(**kwargs)  # type: ignore[arg-type]


def _handler_sequence(responses: list[httpx.Response]) -> httpx.MockTransport:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        response = responses[min(calls["n"], len(responses) - 1)]
        calls["n"] += 1
        return response

    transport = httpx.MockTransport(handler)
    transport.calls = calls  # type: ignore[attr-defined]
    return transport


# ---------------------------------------------------------------------------
# Base-URL joining (GHE / self-hosted GitLab)
# ---------------------------------------------------------------------------


class TestBaseUrlJoining:
    def test_github_default_base_url(self) -> None:
        assert github_rest_base_url(None) == GITHUB_DEFAULT_BASE_URL

    def test_github_empty_string_falls_back_to_default(self) -> None:
        assert github_rest_base_url("") == GITHUB_DEFAULT_BASE_URL

    def test_github_ghe_base_url_joined_as_is(self) -> None:
        # GHE's REST base already carries /api/v3 -- no path rewriting.
        assert (
            github_rest_base_url("https://ghe.example.com/api/v3")
            == "https://ghe.example.com/api/v3"
        )

    def test_github_ghe_base_url_trailing_slash_stripped(self) -> None:
        assert (
            github_rest_base_url("https://ghe.example.com/api/v3/")
            == "https://ghe.example.com/api/v3"
        )

    def test_gitlab_default_base_url_gets_api_v4_suffix(self) -> None:
        assert gitlab_rest_base_url(None) == f"{GITLAB_DEFAULT_BASE_URL}/api/v4"

    def test_gitlab_self_hosted_base_url_gets_api_v4_suffix(self) -> None:
        assert (
            gitlab_rest_base_url("https://gitlab.example.com")
            == "https://gitlab.example.com/api/v4"
        )

    def test_gitlab_trailing_slash_stripped_before_suffix(self) -> None:
        assert (
            gitlab_rest_base_url("https://gitlab.example.com/")
            == "https://gitlab.example.com/api/v4"
        )

    @pytest.mark.asyncio
    async def test_ghe_base_url_actually_used_end_to_end(self) -> None:
        """Proves the join isn't just a string helper -- a real request
        constructed against a GHE-joined base actually hits that host/path."""
        seen: dict[str, str] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["url"] = str(request.url)
            return httpx.Response(200, json={"ok": True})

        core = _core(
            base_url=github_rest_base_url("https://ghe.example.com/api/v3"),
            transport=httpx.MockTransport(handler),
        )
        await core.request(
            "GET", "/repos/acme/widgets", operation="git:GET /repos/acme/widgets"
        )

        assert seen["url"] == "https://ghe.example.com/api/v3/repos/acme/widgets"

    @pytest.mark.asyncio
    async def test_gitlab_self_hosted_base_url_actually_used_end_to_end(self) -> None:
        seen: dict[str, str] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["url"] = str(request.url)
            return httpx.Response(200, json={"id": 1})

        core = _core(
            base_url=gitlab_rest_base_url("https://gitlab.example.com"),
            provider="gitlab",
            transport=httpx.MockTransport(handler),
        )
        await core.request("GET", "/projects/1", operation="project:GET /projects/:id")

        assert seen["url"] == "https://gitlab.example.com/api/v4/projects/1"


# ---------------------------------------------------------------------------
# request() -- retries, usage recording, exhaustion
# ---------------------------------------------------------------------------


class TestRequestRetriesAndUsage:
    @pytest.mark.asyncio
    async def test_success_records_one_observation(self) -> None:
        core = _core(
            transport=httpx.MockTransport(lambda r: httpx.Response(200, json=[]))
        )
        response = await core.request(
            "GET", "/repos/a/b", operation="git:GET /repos/a/b"
        )

        assert response.status_code == 200
        observations = core.drain_usage_observations()
        assert len(observations) == 1
        assert observations[0]["route_family"] == "git"
        assert observations[0]["request_count"] == 1

    @pytest.mark.asyncio
    async def test_retried_attempts_each_record_one_observation(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "dev_health_ops.providers._http.asyncio.sleep", AsyncMock(return_value=None)
        )
        transport = _handler_sequence(
            [
                httpx.Response(429, headers={"Retry-After": "1"}),
                httpx.Response(429, headers={"Retry-After": "1"}),
                httpx.Response(200, json=[1, 2]),
            ]
        )
        core = _core(transport=transport, max_retries=5)
        response = await core.request(
            "GET", "/repos/a/b", operation="git:GET /repos/a/b"
        )

        assert response.status_code == 200
        observations = core.drain_usage_observations()
        assert len(observations) == 1
        # Every PHYSICAL round trip -- both failed 429 attempts AND the
        # final success -- is recorded (CHAOS-2754: real request counts).
        assert observations[0]["request_count"] == 3

    @pytest.mark.asyncio
    async def test_retry_after_header_honored(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sleep_calls: list[float] = []

        async def _fake_sleep(seconds: float) -> None:
            sleep_calls.append(seconds)

        monkeypatch.setattr("dev_health_ops.providers._http.asyncio.sleep", _fake_sleep)
        transport = _handler_sequence(
            [
                httpx.Response(429, headers={"Retry-After": "17"}),
                httpx.Response(200, json=[]),
            ]
        )
        core = _core(transport=transport, max_retries=3)
        await core.request("GET", "/repos/a/b", operation="git:GET /repos/a/b")

        assert sleep_calls == [17.0]

    @pytest.mark.asyncio
    async def test_429_exhaustion_raises_rate_limit_with_signal(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "dev_health_ops.providers._http.asyncio.sleep", AsyncMock(return_value=None)
        )
        reset_epoch = int(time.time()) + 120
        transport = httpx.MockTransport(
            lambda r: httpx.Response(
                429,
                headers={"Retry-After": "5", "X-RateLimit-Reset": str(reset_epoch)},
            )
        )
        core = _core(transport=transport, max_retries=3)

        with pytest.raises(RateLimitException) as excinfo:
            await core.request("GET", "/repos/a/b", operation="git:GET /repos/a/b")

        assert excinfo.value.retry_after_seconds == pytest.approx(5.0)
        signal = excinfo.value.signal
        assert signal is not None
        assert signal.provider == "github"
        assert signal.reason == "primary"
        assert signal.dimension is BudgetDimension.REST_CORE
        # integration_id/route_family are worker-boundary enrichment -- the
        # client never populates them.
        assert signal.integration_id is None
        assert signal.route_family is None
        assert signal.reset_at is not None
        assert abs(signal.reset_at.timestamp() - reset_epoch) < 2

        # Every attempt -- including the ones that ultimately raised --
        # still recorded a usage observation.
        observations = core.drain_usage_observations()
        assert observations[0]["request_count"] == 3

    @pytest.mark.asyncio
    async def test_reset_header_name_override_for_gitlab_style_headers(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A future GitLab code client overrides reset_header_name to
        # "RateLimit-Reset" (GitLab's header, unlike GitHub's
        # "X-RateLimit-Reset") -- the default must not silently ignore it.
        monkeypatch.setattr(
            "dev_health_ops.providers._http.asyncio.sleep", AsyncMock(return_value=None)
        )
        reset_epoch = int(time.time()) + 90
        transport = httpx.MockTransport(
            lambda r: httpx.Response(429, headers={"RateLimit-Reset": str(reset_epoch)})
        )
        core = _core(
            transport=transport,
            provider="gitlab",
            reset_header_name="RateLimit-Reset",
            max_retries=1,
        )

        with pytest.raises(RateLimitException) as excinfo:
            await core.request(
                "GET", "/projects/1", operation="project:GET /projects/1"
            )

        signal = excinfo.value.signal
        assert signal is not None
        assert signal.provider == "gitlab"
        assert signal.reset_at is not None
        assert abs(signal.reset_at.timestamp() - reset_epoch) < 2
        # The worker-visible delay must ALSO come from the reset header
        # (codex HIGH): the deferral path plans not_before from
        # retry_after_seconds, not signal.reset_at.
        assert excinfo.value.retry_after_seconds is not None
        assert 85 <= excinfo.value.retry_after_seconds <= 90

    @pytest.mark.asyncio
    async def test_reset_only_429_derives_retry_after_from_reset_header(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Codex HIGH (PR #1149): a terminal 429 carrying ONLY the provider's
        reset header (no Retry-After) must surface retry_after_seconds
        derived from the reset delta -- workers/sync_units.py plans the
        deferral's not_before from exc.retry_after_seconds, so leaving it
        None wakes the unit on the 60s default instead of the provider's
        real reset window."""
        monkeypatch.setattr(
            "dev_health_ops.providers._http.asyncio.sleep", AsyncMock(return_value=None)
        )
        reset_epoch = int(time.time()) + 1800  # 30-minute primary-limit window
        transport = httpx.MockTransport(
            lambda r: httpx.Response(
                429, headers={"X-RateLimit-Reset": str(reset_epoch)}
            )
        )
        core = _core(transport=transport, max_retries=2)

        with pytest.raises(RateLimitException) as excinfo:
            await core.request("GET", "/repos/a/b", operation="git:GET /repos/a/b")

        retry_after = excinfo.value.retry_after_seconds
        assert retry_after is not None
        assert 1790 <= retry_after <= 1800
        signal = excinfo.value.signal
        assert signal is not None
        assert signal.retry_after_seconds == retry_after
        assert signal.reset_at is not None
        assert abs(signal.reset_at.timestamp() - reset_epoch) < 2

    @pytest.mark.asyncio
    async def test_reset_only_429_retry_sleep_uses_reset_delta(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The in-place retry sleep derives from the reset header too (same
        # shared resolution), not the generic exponential default.
        sleep_calls: list[float] = []

        async def _fake_sleep(seconds: float) -> None:
            sleep_calls.append(seconds)

        monkeypatch.setattr("dev_health_ops.providers._http.asyncio.sleep", _fake_sleep)
        reset_epoch = int(time.time()) + 40
        transport = _handler_sequence(
            [
                httpx.Response(429, headers={"X-RateLimit-Reset": str(reset_epoch)}),
                httpx.Response(200, json=[]),
            ]
        )
        core = _core(transport=transport, max_retries=3)
        await core.request("GET", "/repos/a/b", operation="git:GET /repos/a/b")

        assert len(sleep_calls) == 1
        assert 35 <= sleep_calls[0] <= 40

    @pytest.mark.asyncio
    async def test_5xx_exhaustion_raises_api_exception_not_rate_limit(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "dev_health_ops.providers._http.asyncio.sleep", AsyncMock(return_value=None)
        )
        transport = httpx.MockTransport(
            lambda r: httpx.Response(503, text="unavailable")
        )
        core = _core(transport=transport, max_retries=3)

        with pytest.raises(APIException) as excinfo:
            await core.request("GET", "/repos/a/b", operation="git:GET /repos/a/b")

        assert not isinstance(excinfo.value, RateLimitException)

    @pytest.mark.asyncio
    async def test_5xx_then_success_returns_response(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "dev_health_ops.providers._http.asyncio.sleep", AsyncMock(return_value=None)
        )
        transport = _handler_sequence(
            [httpx.Response(502), httpx.Response(200, json=[1])]
        )
        core = _core(transport=transport, max_retries=3)
        response = await core.request(
            "GET", "/repos/a/b", operation="git:GET /repos/a/b"
        )
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_network_error_retries_then_succeeds(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "dev_health_ops.providers._http.asyncio.sleep", AsyncMock(return_value=None)
        )
        calls = {"n": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            if calls["n"] == 1:
                raise httpx.ConnectError("boom", request=request)
            return httpx.Response(200, json=[])

        core = _core(transport=httpx.MockTransport(handler), max_retries=3)
        response = await core.request(
            "GET", "/repos/a/b", operation="git:GET /repos/a/b"
        )

        assert response.status_code == 200
        assert calls["n"] == 2
        # A network-level failure never produced a response -- no usage
        # observation for that attempt (nothing to record).
        observations = core.drain_usage_observations()
        assert observations[0]["request_count"] == 1

    @pytest.mark.asyncio
    async def test_network_error_exhaustion_raises_api_exception(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "dev_health_ops.providers._http.asyncio.sleep", AsyncMock(return_value=None)
        )

        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("boom", request=request)

        core = _core(transport=httpx.MockTransport(handler), max_retries=2)
        with pytest.raises(APIException):
            await core.request("GET", "/repos/a/b", operation="git:GET /repos/a/b")

    @pytest.mark.asyncio
    async def test_404_raises_not_found_without_retry(self) -> None:
        calls = {"n": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            return httpx.Response(404)

        core = _core(transport=httpx.MockTransport(handler), max_retries=3)
        with pytest.raises(NotFoundException):
            await core.request(
                "GET", "/repos/missing", operation="git:GET /repos/missing"
            )
        assert calls["n"] == 1

    @pytest.mark.asyncio
    async def test_401_raises_authentication_exception(self) -> None:
        core = _core(transport=httpx.MockTransport(lambda r: httpx.Response(401)))
        with pytest.raises(AuthenticationException):
            await core.request("GET", "/repos/a/b", operation="git:GET /repos/a/b")


# ---------------------------------------------------------------------------
# classify_error extension point
# ---------------------------------------------------------------------------


class TestClassifyErrorHook:
    @pytest.mark.asyncio
    async def test_classify_error_hook_raises_takes_precedence(self) -> None:
        def classify(response: httpx.Response, operation: str) -> None:
            if response.status_code == 403:
                raise RateLimitException(
                    "classified as secondary rate limit", retry_after_seconds=9.0
                )

        core = _core(
            transport=httpx.MockTransport(lambda r: httpx.Response(403)),
            classify_error=classify,
        )
        with pytest.raises(RateLimitException) as excinfo:
            await core.request("GET", "/repos/a/b", operation="git:GET /repos/a/b")
        assert excinfo.value.retry_after_seconds == 9.0

    @pytest.mark.asyncio
    async def test_classify_error_hook_returning_none_falls_through_to_default(
        self,
    ) -> None:
        calls = {"n": 0}

        def classify(response: httpx.Response, operation: str) -> None:
            calls["n"] += 1
            return None

        core = _core(
            transport=httpx.MockTransport(lambda r: httpx.Response(403)),
            classify_error=classify,
        )
        with pytest.raises(AuthenticationException):
            await core.request("GET", "/repos/a/b", operation="git:GET /repos/a/b")
        assert calls["n"] == 1

    @pytest.mark.asyncio
    async def test_custom_is_retryable_status_allows_403_retry(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Mirrors what a future GitLab code client needs: a header-qualified
        # 403 must be retryable, not just 429/5xx.
        monkeypatch.setattr(
            "dev_health_ops.providers._http.asyncio.sleep", AsyncMock(return_value=None)
        )
        transport = _handler_sequence(
            [
                httpx.Response(403, headers={"Retry-After": "2"}),
                httpx.Response(200, json=[]),
            ]
        )
        core = _core(
            transport=transport,
            max_retries=3,
            is_retryable_status=lambda r: (
                r.status_code in {403, 429, 500, 502, 503, 504}
            ),
        )
        response = await core.request(
            "GET", "/repos/a/b", operation="git:GET /repos/a/b"
        )
        assert response.status_code == 200


# ---------------------------------------------------------------------------
# paginate_link_header (GitHub)
# ---------------------------------------------------------------------------


class TestPaginateLinkHeader:
    @pytest.mark.asyncio
    async def test_single_page_no_link_header(self) -> None:
        core = _core(
            transport=httpx.MockTransport(
                lambda r: httpx.Response(200, json=[{"id": 1}, {"id": 2}])
            )
        )
        items = await core.paginate_link_header(
            "/repos/a/b/commits", operation="git:GET commits"
        )
        assert items == [{"id": 1}, {"id": 2}]

    @pytest.mark.asyncio
    async def test_follows_link_header_across_pages(self) -> None:
        page1 = httpx.Response(
            200,
            json=[{"id": 1}],
            headers={
                "Link": '<https://api.github.com/repos/a/b/commits?page=2>; rel="next"'
            },
        )
        page2 = httpx.Response(200, json=[{"id": 2}])
        transport = _handler_sequence([page1, page2])
        core = _core(transport=transport)
        items = await core.paginate_link_header(
            "/repos/a/b/commits", operation="git:GET commits"
        )
        assert items == [{"id": 1}, {"id": 2}]
        assert transport.calls["n"] == 2  # type: ignore[attr-defined]

    @pytest.mark.asyncio
    async def test_link_header_with_multiple_rels_extracts_next_only(self) -> None:
        page1 = httpx.Response(
            200,
            json=[{"id": 1}],
            headers={
                "Link": (
                    '<https://api.github.com/repos/a/b/commits?page=1>; rel="prev", '
                    '<https://api.github.com/repos/a/b/commits?page=2>; rel="next", '
                    '<https://api.github.com/repos/a/b/commits?page=9>; rel="last"'
                )
            },
        )
        page2 = httpx.Response(200, json=[{"id": 2}])
        transport = _handler_sequence([page1, page2])
        core = _core(transport=transport)
        items = await core.paginate_link_header(
            "/repos/a/b/commits", operation="git:GET commits"
        )
        assert [item["id"] for item in items] == [1, 2]

    @pytest.mark.asyncio
    async def test_data_key_envelope_extraction(self) -> None:
        core = _core(
            transport=httpx.MockTransport(
                lambda r: httpx.Response(200, json={"workflow_runs": [{"id": 1}]})
            )
        )
        items = await core.paginate_link_header(
            "/repos/a/b/actions/runs",
            operation="cicd:GET runs",
            data_key="workflow_runs",
        )
        assert items == [{"id": 1}]

    @pytest.mark.asyncio
    async def test_hard_page_cap_stops_a_looping_link_header(self) -> None:
        # A misbehaving/looping Link header (always points to "next") must
        # not spin forever.
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json=[{"id": 1}],
                headers={"Link": f'<{request.url}>; rel="next"'},
            )

        core = _core(transport=httpx.MockTransport(handler))
        items = await core.paginate_link_header(
            "/repos/a/b/commits", operation="git:GET commits", max_pages=3
        )
        assert len(items) == 3


# ---------------------------------------------------------------------------
# paginate_page_param (GitLab)
# ---------------------------------------------------------------------------


class TestPaginatePageParam:
    @pytest.mark.asyncio
    async def test_empty_first_page_stops_immediately(self) -> None:
        transport = _handler_sequence([httpx.Response(200, json=[])])
        core = _core(
            transport=transport, base_url="https://gitlab.com/api/v4", provider="gitlab"
        )
        items = await core.paginate_page_param(
            "/projects/1/repository/commits", operation="project:GET commits"
        )
        assert items == []
        assert transport.calls["n"] == 1  # type: ignore[attr-defined]

    @pytest.mark.asyncio
    async def test_follows_x_next_page_header(self) -> None:
        page1 = httpx.Response(
            200, json=[{"id": 1}, {"id": 2}], headers={"X-Next-Page": "2"}
        )
        page2 = httpx.Response(200, json=[{"id": 3}])
        transport = _handler_sequence([page1, page2])
        core = _core(
            transport=transport, base_url="https://gitlab.com/api/v4", provider="gitlab"
        )
        items = await core.paginate_page_param(
            "/projects/1/repository/commits",
            operation="project:GET commits",
            per_page=2,
        )
        assert [item["id"] for item in items] == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_empty_x_next_page_header_stops_via_item_count_heuristic(
        self,
    ) -> None:
        # GitLab sends X-Next-Page as a PRESENT but EMPTY header on the last
        # page -- must not attempt int("") and must fall back to the
        # item-count heuristic (a short page == last page).
        page1 = httpx.Response(200, json=[{"id": 1}], headers={"X-Next-Page": ""})
        transport = _handler_sequence([page1])
        core = _core(
            transport=transport, base_url="https://gitlab.com/api/v4", provider="gitlab"
        )
        items = await core.paginate_page_param(
            "/projects/1/repository/commits",
            operation="project:GET commits",
            per_page=100,
        )
        assert [item["id"] for item in items] == [1]
        assert transport.calls["n"] == 1  # type: ignore[attr-defined]

    @pytest.mark.asyncio
    async def test_missing_x_next_page_falls_back_to_item_count_heuristic(self) -> None:
        full_page = httpx.Response(200, json=[{"id": 1}, {"id": 2}])
        short_page = httpx.Response(200, json=[{"id": 3}])
        transport = _handler_sequence([full_page, short_page])
        core = _core(
            transport=transport, base_url="https://gitlab.com/api/v4", provider="gitlab"
        )
        items = await core.paginate_page_param(
            "/projects/1/repository/commits",
            operation="project:GET commits",
            per_page=2,
        )
        assert [item["id"] for item in items] == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_hard_page_cap_stops_a_looping_cursor(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=[{"id": 1}], headers={"X-Next-Page": "2"})

        core = _core(
            transport=httpx.MockTransport(handler),
            base_url="https://gitlab.com/api/v4",
            provider="gitlab",
        )
        items = await core.paginate_page_param(
            "/projects/1/repository/commits",
            operation="project:GET commits",
            per_page=1,
            max_pages=4,
        )
        assert len(items) == 4


# ---------------------------------------------------------------------------
# Diagnostic-header parity with the existing work-client recorders
# (codex MED-1 on PR #1149)
# ---------------------------------------------------------------------------


class TestDiagnosticHeaderParity:
    def test_github_tuple_matches_work_client_recorder_exactly(self) -> None:
        from dev_health_ops.providers.github.client import (
            _DIAGNOSTIC_HEADER_NAMES as GITHUB_CLIENT_NAMES,
        )

        assert set(GITHUB_DIAGNOSTIC_HEADER_NAMES) == set(GITHUB_CLIENT_NAMES)

    def test_gitlab_tuple_matches_work_client_recorder_exactly(self) -> None:
        from dev_health_ops.providers.gitlab.client import (
            _DIAGNOSTIC_HEADER_NAMES as GITLAB_CLIENT_NAMES,
        )

        assert set(GITLAB_DIAGNOSTIC_HEADER_NAMES) == set(GITLAB_CLIENT_NAMES)

    @pytest.mark.asyncio
    async def test_github_recorded_headers_match_work_client_shape(self) -> None:
        """A core configured with the GitHub tuple must record EXACTLY the
        headers the existing GitHubWorkClient recorder preserves -- request
        id and accepted-permissions included, token/Authorization and junk
        excluded."""
        response_headers = {
            "Authorization": "Bearer ghs_secret",
            "X-RateLimit-Limit": "5000",
            "X-RateLimit-Remaining": "4990",
            "X-RateLimit-Reset": "12345",
            "X-RateLimit-Used": "10",
            "X-RateLimit-Resource": "core",
            "Retry-After": "3",
            "X-GitHub-Request-Id": "ABCD:1234",
            "X-Accepted-GitHub-Permissions": "contents=read",
            "X-Totally-Unrelated": "junk",
        }
        core = _core(
            transport=httpx.MockTransport(
                lambda r: httpx.Response(200, json=[], headers=response_headers)
            ),
            diagnostic_header_names=GITHUB_DIAGNOSTIC_HEADER_NAMES,
        )
        await core.request("GET", "/repos/a/b", operation="git:GET /repos/a/b")

        observation = core.drain_usage_observations()[0]
        assert observation["latest_headers"] == {
            "x-ratelimit-limit": "5000",
            "x-ratelimit-remaining": "4990",
            "x-ratelimit-reset": "12345",
            "x-ratelimit-used": "10",
            "x-ratelimit-resource": "core",
            "retry-after": "3",
            "x-github-request-id": "ABCD:1234",
            "x-accepted-github-permissions": "contents=read",
        }
        # rate_limit fields carry the GitHub vocabulary incl. used/resource.
        assert observation["rate_limit"]["used"] == "10"
        assert observation["rate_limit"]["resource"] == "core"

    @pytest.mark.asyncio
    async def test_gitlab_recorded_headers_match_work_client_shape(self) -> None:
        response_headers = {
            "PRIVATE-TOKEN": "glpat-secret",
            "RateLimit-Limit": "600",
            "RateLimit-Remaining": "599",
            "RateLimit-Reset": "1712345",
            "Retry-After": "7",
            "X-Request-Id": "req-42",
            "X-Runtime": "0.123",
            "X-Totally-Unrelated": "junk",
        }
        core = _core(
            transport=httpx.MockTransport(
                lambda r: httpx.Response(200, json=[], headers=response_headers)
            ),
            provider="gitlab",
            base_url="https://gitlab.com/api/v4",
            diagnostic_header_names=GITLAB_DIAGNOSTIC_HEADER_NAMES,
        )
        await core.request("GET", "/projects/1", operation="project:GET /projects/1")

        observation = core.drain_usage_observations()[0]
        assert observation["latest_headers"] == {
            "ratelimit-limit": "600",
            "ratelimit-remaining": "599",
            "ratelimit-reset": "1712345",
            "retry-after": "7",
            "x-request-id": "req-42",
            "x-runtime": "0.123",
        }

    @pytest.mark.asyncio
    async def test_default_set_never_records_auth_headers(self) -> None:
        core = _core(
            transport=httpx.MockTransport(
                lambda r: httpx.Response(
                    200,
                    json=[],
                    headers={
                        "Authorization": "Bearer secret",
                        "X-RateLimit-Remaining": "1",
                    },
                )
            )
        )
        await core.request("GET", "/repos/a/b", operation="git:GET /repos/a/b")
        observation = core.drain_usage_observations()[0]
        assert "authorization" not in {k.lower() for k in observation["latest_headers"]}


# ---------------------------------------------------------------------------
# Redirect policy (codex MED-2 on PR #1149)
# ---------------------------------------------------------------------------


class TestRedirectPolicy:
    @pytest.mark.asyncio
    async def test_301_default_raises_terminal_api_error(self) -> None:
        calls = {"n": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            return httpx.Response(
                301, headers={"Location": "https://api.github.com/repos/new/name"}
            )

        core = _core(transport=httpx.MockTransport(handler), max_retries=3)
        with pytest.raises(APIException) as excinfo:
            await core.request("GET", "/repos/a/b", operation="git:GET /repos/a/b")

        message = str(excinfo.value)
        assert "redirect" in message
        assert "301" in message
        assert "https://api.github.com/repos/new/name" in message
        assert "raw_redirect" in message
        # Never retried, and not misclassified as a rate limit.
        assert calls["n"] == 1
        assert not isinstance(excinfo.value, RateLimitException)
        # The single physical attempt was still recorded.
        assert core.drain_usage_observations()[0]["request_count"] == 1

    @pytest.mark.asyncio
    async def test_302_default_raises_terminal_api_error(self) -> None:
        core = _core(
            transport=httpx.MockTransport(
                lambda r: httpx.Response(
                    302, headers={"Location": "https://cdn.example/x"}
                )
            )
        )
        with pytest.raises(APIException) as excinfo:
            await core.request(
                "GET",
                "/repos/a/b/actions/artifacts/1/zip",
                operation="tests:GET artifact",
            )
        assert "302" in str(excinfo.value)

    @pytest.mark.asyncio
    async def test_raw_redirect_opt_in_returns_the_302_response(self) -> None:
        """The CS5 artifact-zip contract: the caller reads Location off the
        302 itself and follows it WITHOUT forwarding Authorization (see
        connectors/github.py::download_artifact_zip) -- so the opt-in must
        hand back the raw redirect response, recorded as one physical
        attempt, never retried and never classified as an error."""
        presigned = "https://productionresults.blob.core.windows.net/artifact?sig=x"
        calls = {"n": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            return httpx.Response(302, headers={"Location": presigned})

        core = _core(transport=httpx.MockTransport(handler), max_retries=3)
        response = await core.request(
            "GET",
            "/repos/a/b/actions/artifacts/1/zip",
            operation="tests:GET artifact zip",
            raw_redirect=True,
        )

        assert response.status_code == 302
        assert response.headers["Location"] == presigned
        assert calls["n"] == 1
        assert core.drain_usage_observations()[0]["request_count"] == 1

    @pytest.mark.asyncio
    async def test_raw_redirect_does_not_leak_into_success_path(self) -> None:
        # raw_redirect=True must not change 2xx handling.
        core = _core(
            transport=httpx.MockTransport(lambda r: httpx.Response(200, json=[1]))
        )
        response = await core.request(
            "GET", "/repos/a/b", operation="git:GET /repos/a/b", raw_redirect=True
        )
        assert response.status_code == 200
        assert response.json() == [1]


# ---------------------------------------------------------------------------
# Unauthenticated follow-up hop (codex re-pass MED on PR #1149): the second
# half of the two-hop artifact pattern -- follow the 302 Location WITHOUT
# the client's default Authorization header, still recorded.
# ---------------------------------------------------------------------------


class TestUnauthenticatedFollow:
    @pytest.mark.asyncio
    async def test_two_hop_artifact_pattern_strips_auth_and_records_both(self) -> None:
        """The codex-required end-to-end: obtain a 302 via raw_redirect=True,
        follow the returned absolute Location via request_unauthenticated,
        assert the second request carries NO Authorization header (while the
        first hop DID -- proving default headers exist and are stripped only
        on the bare path) AND both physical attempts are recorded, one
        each."""
        presigned = "https://productionresults.blob.core.windows.net/artifact?sig=x"
        seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            if request.url.host == "api.github.com":
                return httpx.Response(302, headers={"Location": presigned})
            return httpx.Response(200, content=b"zip-bytes")

        core = _core(
            transport=httpx.MockTransport(handler),
            headers={
                "Authorization": "Bearer ghs_token",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )

        first = await core.request(
            "GET",
            "/repos/a/b/actions/artifacts/1/zip",
            operation="git:GET artifact zip",
            raw_redirect=True,
        )
        assert first.status_code == 302
        follow = await core.request_unauthenticated(
            first.headers["Location"], operation="GET artifact zip follow"
        )
        assert follow.status_code == 200
        assert follow.content == b"zip-bytes"

        assert len(seen) == 2
        # Hop 1 (authenticated API call) carried the client defaults...
        assert seen[0].headers.get("Authorization") == "Bearer ghs_token"
        # ...hop 2 (pre-signed URL) carried NONE of them.
        assert "authorization" not in seen[1].headers
        assert "x-github-api-version" not in seen[1].headers
        assert seen[1].url == httpx.URL(presigned)

        # Both physical attempts recorded, one each: hop 1 resolves via the
        # "git:" prefix, hop 2 (unprefixed) via the transport default -- two
        # distinct observations with request_count == 1 apiece.
        observations = {
            obs["route_family"]: obs for obs in core.drain_usage_observations()
        }
        assert observations["git"]["request_count"] == 1
        assert observations["unclassified_default"]["request_count"] == 1

    @pytest.mark.asyncio
    async def test_explicit_per_request_headers_are_the_only_extras_sent(self) -> None:
        seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(200, content=b"")

        core = _core(
            transport=httpx.MockTransport(handler),
            headers={"Authorization": "Bearer secret"},
        )
        await core.request_unauthenticated(
            "https://cdn.example/blob",
            operation="GET follow",
            headers={"Accept": "application/octet-stream"},
        )
        assert "authorization" not in seen[0].headers
        assert seen[0].headers["Accept"] == "application/octet-stream"

    @pytest.mark.asyncio
    async def test_relative_url_is_rejected(self) -> None:
        core = _core(transport=httpx.MockTransport(lambda r: httpx.Response(200)))
        with pytest.raises(ValueError, match="absolute"):
            await core.request_unauthenticated(
                "/repos/a/b/zipball", operation="GET follow"
            )

    @pytest.mark.asyncio
    async def test_response_returned_as_is_without_status_classification(self) -> None:
        # The pre-signed host is a different error domain: a 404/410 there is
        # the code client's convenience-empty decision (CS5), never this
        # transport's -- so non-2xx must come back unraised (but recorded).
        core = _core(transport=httpx.MockTransport(lambda r: httpx.Response(410)))
        response = await core.request_unauthenticated(
            "https://cdn.example/expired", operation="GET follow"
        )
        assert response.status_code == 410
        assert core.drain_usage_observations()[0]["request_count"] == 1

    @pytest.mark.asyncio
    async def test_network_error_raises_api_exception(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("boom", request=request)

        core = _core(transport=httpx.MockTransport(handler))
        with pytest.raises(APIException):
            await core.request_unauthenticated(
                "https://cdn.example/blob", operation="GET follow"
            )

    @pytest.mark.asyncio
    async def test_close_closes_both_clients(self) -> None:
        core = _core(transport=httpx.MockTransport(lambda r: httpx.Response(200)))
        await core.request("GET", "/repos/a/b", operation="git:GET /repos/a/b")
        await core.request_unauthenticated(
            "https://cdn.example/blob", operation="GET follow"
        )
        assert core._client is not None
        assert core._bare_client is not None
        await core.close()
        assert core._client is None
        assert core._bare_client is None
