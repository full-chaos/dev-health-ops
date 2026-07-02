"""Tests for providers/gitlab/feature_flags.py (CHAOS-2785).

``GitLabFeatureFlagsClient`` is the canonical migration target for
``connectors/gitlab.py``'s ``GitLabConnector.get_feature_flags`` /
``get_project_name`` (frozen, un-instrumented). These tests pin the same
403-is-non-retryable / 429-is-retryable behavior as
``tests/test_gitlab_connector.py::TestGitLabFeatureFlags403`` and additionally
cover pagination edges and real per-request usage recording through the
shared CHAOS-2754 recorder -- mirroring
``tests/test_launchdarkly.py::TestLaunchDarklyClientUsageRecording``.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from dev_health_ops.connectors.exceptions import (
    APIException,
    AuthenticationException,
)
from dev_health_ops.exceptions import RateLimitException as RootRateLimitException
from dev_health_ops.providers.gitlab.feature_flags import (
    GitLabFeatureFlagsClient,
    _raise_for_status,
)
from dev_health_ops.sync.budget_types import BudgetDimension


def _response(
    status_code: int,
    *,
    headers: dict[str, str] | None = None,
    text: str = "",
    json_body: object = None,
    url: str = "https://gitlab.com/api/v4/projects/1/feature_flags",
) -> MagicMock:
    response = MagicMock()
    response.status_code = status_code
    response.headers = headers or {}
    response.text = text
    response.url = url
    if json_body is not None:
        response.json.return_value = json_body
    return response


# ---------------------------------------------------------------------------
# _raise_for_status
# ---------------------------------------------------------------------------


class TestRaiseForStatus:
    def test_401_raises_auth(self) -> None:
        with pytest.raises(AuthenticationException):
            _raise_for_status(_response(401))

    def test_403_raises_auth_non_retryable(self) -> None:
        """Parity pin: a PLAIN GitLab 403 (no rate-limit headers) =
        permission/feature-disabled, never the rate limit (429 is), so it
        must classify as AuthenticationException -- mirrors
        tests/test_gitlab_connector.py::TestGitLabFeatureFlags403.
        """
        with pytest.raises(AuthenticationException, match="forbidden"):
            _raise_for_status(_response(403, text="Feature Flags disabled"))

    def test_403_with_retry_after_header_raises_rate_limit(self) -> None:
        """A header-qualified 403 (some self-managed instances front a
        throttle with 403 instead of 429) must classify as RateLimitException
        -- not the non-retryable AuthenticationException -- so the worker
        deferral path engages. Mirrors
        providers/gitlab/client.py::_maybe_raise_gitlab_rate_limit's 403
        qualification via the shared
        providers._ratelimit.gitlab_403_is_rate_limited predicate."""
        with pytest.raises(RootRateLimitException) as excinfo:
            _raise_for_status(_response(403, headers={"Retry-After": "9"}))
        signal = excinfo.value.signal
        assert signal is not None
        assert signal.provider == "gitlab"
        assert signal.reason == "secondary"
        assert signal.dimension is BudgetDimension.REST_CORE
        assert excinfo.value.retry_after_seconds == 9.0

    def test_403_with_rate_limit_remaining_zero_raises_rate_limit(self) -> None:
        """Same qualification, driven by RateLimit-Remaining: 0 instead of
        Retry-After -- matches providers/gitlab/client.py's OR condition."""
        with pytest.raises(RootRateLimitException) as excinfo:
            _raise_for_status(_response(403, headers={"RateLimit-Remaining": "0"}))
        assert excinfo.value.signal is not None
        assert excinfo.value.signal.reason == "secondary"

    def test_403_with_nonzero_remaining_and_no_retry_after_is_not_rate_limited(
        self,
    ) -> None:
        """A 403 with a present-but-nonzero RateLimit-Remaining and no
        Retry-After is NOT rate-limit-qualified -- stays a plain permission
        403 (AuthenticationException)."""
        with pytest.raises(AuthenticationException):
            _raise_for_status(_response(403, headers={"RateLimit-Remaining": "42"}))

    def test_403_with_http_date_retry_after_derives_seconds_via_shared_parser(
        self,
    ) -> None:
        """The delay must come from providers._ratelimit.
        gitlab_resolve_retry_after_seconds (HTTP-date-aware), not a local
        numeric-only parser that would silently drop an HTTP-date
        Retry-After and leave retry_after_seconds as None."""
        future = datetime.now(timezone.utc) + timedelta(seconds=120)
        http_date = format_datetime(future, usegmt=True)

        with pytest.raises(RootRateLimitException) as excinfo:
            _raise_for_status(_response(403, headers={"Retry-After": http_date}))

        retry_after = excinfo.value.retry_after_seconds
        assert retry_after is not None
        assert 100 <= retry_after <= 120

    def test_403_with_no_retry_after_derives_seconds_from_rate_limit_reset(
        self,
    ) -> None:
        """No Retry-After header at all -- retry_after_seconds must be
        derived from RateLimit-Reset (epoch seconds), matching
        providers/gitlab/client.py::_maybe_raise_gitlab_rate_limit's
        fallback, instead of surfacing None (which
        workers/sync_units.py would otherwise treat as "no signal" and plan
        the deferral's re-enqueue from a much shorter default)."""
        reset_epoch = int(time.time()) + 300

        with pytest.raises(RootRateLimitException) as excinfo:
            _raise_for_status(
                _response(
                    403,
                    headers={
                        "RateLimit-Remaining": "0",
                        "RateLimit-Reset": str(reset_epoch),
                    },
                )
            )

        retry_after = excinfo.value.retry_after_seconds
        assert retry_after is not None
        assert 290 <= retry_after <= 300

    def test_429_raises_rate_limit_with_signal(self) -> None:
        with pytest.raises(RootRateLimitException) as excinfo:
            _raise_for_status(
                _response(429, headers={"Retry-After": "12", "RateLimit-Reset": "0"})
            )
        signal = excinfo.value.signal
        assert signal is not None
        assert signal.provider == "gitlab"
        assert signal.reason == "primary"
        assert signal.dimension is BudgetDimension.REST_CORE
        assert excinfo.value.retry_after_seconds == 12.0

    def test_404_raises_api_exception(self) -> None:
        with pytest.raises(APIException, match="not found"):
            _raise_for_status(_response(404))

    def test_500_raises_api_exception(self) -> None:
        with pytest.raises(APIException, match="server error"):
            _raise_for_status(_response(500, text="boom"))

    def test_200_passes(self) -> None:
        _raise_for_status(_response(200))


# ---------------------------------------------------------------------------
# get_feature_flags
# ---------------------------------------------------------------------------


class TestGetFeatureFlags:
    @pytest.fixture
    def client(self) -> GitLabFeatureFlagsClient:
        return GitLabFeatureFlagsClient(private_token="test-token")

    @pytest.mark.asyncio
    async def test_single_page(self, client: GitLabFeatureFlagsClient) -> None:
        page = _response(
            200,
            headers={},
            json_body=[{"name": "flag-a"}, {"name": "flag-b"}],
        )
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.is_closed = False
        mock_client.request = AsyncMock(return_value=page)
        client._client = mock_client

        flags = await client.get_feature_flags("group/project")

        assert [f["name"] for f in flags] == ["flag-a", "flag-b"]
        mock_client.request.assert_called_once_with(
            "GET",
            "/projects/group%2Fproject/feature_flags",
            params={"page": 1, "per_page": 100},
        )

    @pytest.mark.asyncio
    async def test_empty_page_stops_immediately(
        self, client: GitLabFeatureFlagsClient
    ) -> None:
        page = _response(200, json_body=[])
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.is_closed = False
        mock_client.request = AsyncMock(return_value=page)
        client._client = mock_client

        flags = await client.get_feature_flags(42)

        assert flags == []
        mock_client.request.assert_called_once()

    @pytest.mark.asyncio
    async def test_paginates_via_x_next_page_header(
        self, client: GitLabFeatureFlagsClient
    ) -> None:
        page1 = _response(
            200,
            headers={"X-Next-Page": "2"},
            json_body=[{"name": f"flag-{i}"} for i in range(2)],
        )
        page2 = _response(200, headers={}, json_body=[{"name": "flag-last"}])

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.is_closed = False
        mock_client.request = AsyncMock(side_effect=[page1, page2])
        client._client = mock_client

        flags = await client.get_feature_flags("group/project", per_page=2)

        assert len(flags) == 3
        assert mock_client.request.await_count == 2
        second_call = mock_client.request.call_args_list[1]
        assert second_call.kwargs["params"]["page"] == 2

    @pytest.mark.asyncio
    async def test_pagination_falls_back_to_item_count_when_next_page_absent(
        self, client: GitLabFeatureFlagsClient
    ) -> None:
        """When X-Next-Page is absent (older/mocked GitLab), a full page
        implies another page exists; a short page means the last page."""
        full_page = _response(
            200, headers={}, json_body=[{"name": f"flag-{i}"} for i in range(2)]
        )
        short_page = _response(200, headers={}, json_body=[{"name": "flag-last"}])

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.is_closed = False
        mock_client.request = AsyncMock(side_effect=[full_page, short_page])
        client._client = mock_client

        flags = await client.get_feature_flags("group/project", per_page=2)

        assert len(flags) == 3
        assert mock_client.request.await_count == 2

    @pytest.mark.asyncio
    async def test_hard_page_cap_stops_a_looping_cursor(
        self, client: GitLabFeatureFlagsClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import dev_health_ops.providers.gitlab.feature_flags as feature_flags_module

        monkeypatch.setattr(feature_flags_module, "_MAX_PAGES", 3)

        # Always a full page with X-Next-Page pointing at itself -- a
        # misbehaving cursor that never terminates on its own.
        looping_page = _response(
            200,
            headers={"X-Next-Page": "1"},
            json_body=[{"name": "flag-a"}, {"name": "flag-b"}],
        )
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.is_closed = False
        mock_client.request = AsyncMock(return_value=looping_page)
        client._client = mock_client

        flags = await client.get_feature_flags("group/project", per_page=2)

        assert mock_client.request.await_count == 3
        assert len(flags) == 6

    @pytest.mark.asyncio
    async def test_403_is_non_retryable(self, client: GitLabFeatureFlagsClient) -> None:
        """Parity pin (tests/test_gitlab_connector.py::
        TestGitLabFeatureFlags403::test_forbidden_raises_non_retryable_
        authentication_exception): a 403 must not consume the retry budget --
        exactly one request is made before the non-retryable exception is
        raised."""
        forbidden = _response(403, text="Feature Flags disabled")
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.is_closed = False
        mock_client.request = AsyncMock(return_value=forbidden)
        client._client = mock_client

        with pytest.raises(AuthenticationException):
            await client.get_feature_flags("group/project")

        assert mock_client.request.await_count == 1

    @pytest.mark.asyncio
    async def test_retried_429_then_success_counts_every_attempt(
        self, client: GitLabFeatureFlagsClient
    ) -> None:
        throttled = _response(429, headers={"Retry-After": "0"})
        ok = _response(200, json_body=[{"name": "flag-a"}])

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.is_closed = False
        mock_client.request = AsyncMock(side_effect=[throttled, ok])
        client._client = mock_client

        flags = await client.get_feature_flags("group/project")

        assert len(flags) == 1
        observations = client.drain_usage_observations()
        assert len(observations) == 1
        assert observations[0]["route_family"] == "project"
        assert observations[0]["dimension"] == "rest_core"
        assert observations[0]["request_count"] == 2, (
            "the retried 429 attempt AND the eventual success are both real "
            "requests against the provider"
        )
        assert observations[0]["latest_status"] == 200

    @pytest.mark.asyncio
    async def test_header_qualified_403_is_retried_then_recovers(
        self, client: GitLabFeatureFlagsClient
    ) -> None:
        """A 403 carrying Retry-After is retried in place (like a 429), not
        escalated to AuthenticationException on the first attempt."""
        throttled_403 = _response(403, headers={"Retry-After": "0"})
        ok = _response(200, json_body=[{"name": "flag-a"}])

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.is_closed = False
        mock_client.request = AsyncMock(side_effect=[throttled_403, ok])
        client._client = mock_client

        flags = await client.get_feature_flags("group/project")

        assert len(flags) == 1
        assert mock_client.request.await_count == 2
        observations = client.drain_usage_observations()
        assert observations[0]["request_count"] == 2

    @pytest.mark.asyncio
    async def test_exhausted_header_qualified_403_retries_raise_rate_limit(
        self, client: GitLabFeatureFlagsClient
    ) -> None:
        client.max_retries = 2
        throttled_403 = _response(403, headers={"RateLimit-Remaining": "0"})
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.is_closed = False
        mock_client.request = AsyncMock(return_value=throttled_403)
        client._client = mock_client

        with pytest.raises(RootRateLimitException) as excinfo:
            await client.get_feature_flags("group/project")

        assert mock_client.request.await_count == 2
        assert excinfo.value.signal is not None
        assert excinfo.value.signal.reason == "secondary"

    @pytest.mark.asyncio
    async def test_exhausted_header_qualified_403_derives_seconds_from_reset(
        self, client: GitLabFeatureFlagsClient
    ) -> None:
        """No Retry-After anywhere, only RateLimit-Remaining: 0 +
        RateLimit-Reset -- the exhausted-retries RateLimitException must
        still carry a derived retry_after_seconds (not None), matching
        providers/gitlab/client.py::_maybe_raise_gitlab_rate_limit. Uses
        max_retries=1 (single attempt = immediately exhausted) so the test
        doesn't actually sleep the ~300s the derived delay implies -- that
        derivation is exactly what's under test, not something to wait out."""
        client.max_retries = 1
        reset_epoch = int(time.time()) + 300
        throttled_403 = _response(
            403,
            headers={
                "RateLimit-Remaining": "0",
                "RateLimit-Reset": str(reset_epoch),
            },
        )
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.is_closed = False
        mock_client.request = AsyncMock(return_value=throttled_403)
        client._client = mock_client

        with pytest.raises(RootRateLimitException) as excinfo:
            await client.get_feature_flags("group/project")

        assert mock_client.request.await_count == 1
        retry_after = excinfo.value.retry_after_seconds
        assert retry_after is not None
        assert 290 <= retry_after <= 300

    @pytest.mark.asyncio
    async def test_exhausted_429_retries_raise_rate_limit_exception(
        self, client: GitLabFeatureFlagsClient
    ) -> None:
        client.max_retries = 2
        throttled = _response(429, headers={"Retry-After": "0"})
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.is_closed = False
        mock_client.request = AsyncMock(return_value=throttled)
        client._client = mock_client

        with pytest.raises(RootRateLimitException):
            await client.get_feature_flags("group/project")

        assert mock_client.request.await_count == 2
        observations = client.drain_usage_observations()
        assert observations[0]["request_count"] == 2

    @pytest.mark.asyncio
    async def test_5xx_exhausted_retries_raise_api_exception(
        self, client: GitLabFeatureFlagsClient
    ) -> None:
        client.max_retries = 2
        server_error = _response(500, text="boom")
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.is_closed = False
        mock_client.request = AsyncMock(return_value=server_error)
        client._client = mock_client

        with pytest.raises(APIException, match="server error"):
            await client.get_feature_flags("group/project")

        assert mock_client.request.await_count == 2


# ---------------------------------------------------------------------------
# get_project_name
# ---------------------------------------------------------------------------


class TestGetProjectName:
    @pytest.fixture
    def client(self) -> GitLabFeatureFlagsClient:
        return GitLabFeatureFlagsClient(private_token="test-token")

    @pytest.mark.asyncio
    async def test_returns_path_with_namespace(
        self, client: GitLabFeatureFlagsClient
    ) -> None:
        response = _response(200, json_body={"path_with_namespace": "group/project"})
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.is_closed = False
        mock_client.request = AsyncMock(return_value=response)
        client._client = mock_client

        name = await client.get_project_name("group/project")

        assert name == "group/project"
        mock_client.request.assert_called_once_with(
            "GET", "/projects/group%2Fproject", params=None
        )

    @pytest.mark.asyncio
    async def test_falls_back_to_path(self, client: GitLabFeatureFlagsClient) -> None:
        response = _response(200, json_body={"path": "project"})
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.is_closed = False
        mock_client.request = AsyncMock(return_value=response)
        client._client = mock_client

        assert await client.get_project_name(42) == "project"

    @pytest.mark.asyncio
    async def test_usage_recorded_under_project_family(
        self, client: GitLabFeatureFlagsClient
    ) -> None:
        response = _response(200, json_body={"path_with_namespace": "group/project"})
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.is_closed = False
        mock_client.request = AsyncMock(return_value=response)
        client._client = mock_client

        await client.get_project_name("group/project")

        observations = client.drain_usage_observations()
        assert len(observations) == 1
        assert observations[0]["route_family"] == "project"
        assert observations[0]["dimension"] == "rest_core"


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


class TestLifecycle:
    @pytest.mark.asyncio
    async def test_close_closes_client(self) -> None:
        client = GitLabFeatureFlagsClient(private_token="test-token")
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.is_closed = False
        client._client = mock_client

        await client.close()

        mock_client.aclose.assert_called_once()

    @pytest.mark.asyncio
    async def test_context_manager(self) -> None:
        async with GitLabFeatureFlagsClient(private_token="tok") as client:
            assert client.private_token == "tok"
