"""Tests for LaunchDarkly connector and processor normalization."""

import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from dev_health_ops.connectors.exceptions import (
    APIException,
    AuthenticationException,
    RateLimitException,
)
from dev_health_ops.connectors.launchdarkly import (
    LaunchDarklyConnector,
    _parse_rate_limit_remaining,
    _parse_retry_after,
    _raise_for_status,
)
from dev_health_ops.exceptions import RateLimitException as RootRateLimitException
from dev_health_ops.processors.launchdarkly import (
    _EVENT_KIND_MAP,
    _parse_iso,
    normalize_audit_events,
    normalize_flags,
)
from dev_health_ops.providers.launchdarkly.client import LaunchDarklyClient
from dev_health_ops.providers.launchdarkly.code_refs import (
    LD_CODE_REFERENCE_CONFIDENCE,
    LaunchDarklyCodeReference,
    LaunchDarklyCodeReferencesClient,
    build_code_reference_links,
    index_repo_rows,
    parse_code_reference_repositories,
)
from dev_health_ops.work_graph.ids import (
    generate_feature_flag_id,
    generate_file_id,
    generate_pr_id,
)

# ---------------------------------------------------------------------------
# Connector helpers
# ---------------------------------------------------------------------------


class TestParseRateLimitRemaining:
    def test_valid_header(self):
        response = MagicMock()
        response.headers = {"X-RateLimit-Route-Remaining": "42"}
        assert _parse_rate_limit_remaining(response) == 42

    def test_missing_header(self):
        response = MagicMock()
        response.headers = {}
        assert _parse_rate_limit_remaining(response) is None

    def test_non_numeric_header(self):
        response = MagicMock()
        response.headers = {"X-RateLimit-Route-Remaining": "abc"}
        assert _parse_rate_limit_remaining(response) is None


class TestParseRetryAfter:
    def test_valid_header(self):
        response = MagicMock()
        response.headers = {"Retry-After": "30"}
        assert _parse_retry_after(response) == 30.0

    def test_minimum_clamp(self):
        response = MagicMock()
        response.headers = {"Retry-After": "0.1"}
        assert _parse_retry_after(response) == 1.0

    def test_missing_header(self):
        response = MagicMock()
        response.headers = {}
        assert _parse_retry_after(response) is None


class TestRaiseForStatus:
    def test_401_raises_auth(self):
        response = MagicMock()
        response.status_code = 401
        with pytest.raises(AuthenticationException):
            _raise_for_status(response)

    def test_429_raises_rate_limit(self):
        response = MagicMock()
        response.status_code = 429
        response.headers = {"Retry-After": "5"}
        with pytest.raises(RateLimitException):
            _raise_for_status(response)

    def test_500_raises_api(self):
        response = MagicMock()
        response.status_code = 500
        response.text = "Internal Server Error"
        with pytest.raises(APIException, match="server error"):
            _raise_for_status(response)

    def test_200_passes(self):
        response = MagicMock()
        response.status_code = 200
        _raise_for_status(response)


# ---------------------------------------------------------------------------
# Connector async tests
# ---------------------------------------------------------------------------


class TestLaunchDarklyConnector:
    @pytest.fixture
    def connector(self):
        return LaunchDarklyConnector(
            api_key="test-api-key",
            project_key="default",
        )

    @pytest.mark.asyncio
    async def test_get_flags_returns_items(self, connector):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"X-RateLimit-Route-Remaining": "100"}
        mock_response.json.return_value = {
            "totalCount": 2,
            "items": [
                {"key": "flag-1", "name": "Flag One", "kind": "boolean"},
                {"key": "flag-2", "name": "Flag Two", "kind": "multivariate"},
            ],
        }

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.is_closed = False
        mock_client.request = AsyncMock(return_value=mock_response)
        connector._client = mock_client

        flags = await connector.get_flags()
        assert len(flags) == 2
        assert flags[0]["key"] == "flag-1"
        mock_client.request.assert_called_once_with(
            "GET", "/flags/default", params={"limit": 50, "offset": 0}
        )

    @pytest.mark.asyncio
    async def test_get_flags_requires_project_key(self):
        connector = LaunchDarklyConnector(api_key="key")
        with pytest.raises(ValueError, match="project_key is required"):
            await connector.get_flags()

    @pytest.mark.asyncio
    async def test_get_audit_log_with_since(self, connector):
        since = datetime(2025, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
        expected_ms = int(since.timestamp() * 1000)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {}
        mock_response.json.return_value = {"items": [{"_id": "abc"}]}

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.is_closed = False
        mock_client.request = AsyncMock(return_value=mock_response)
        connector._client = mock_client

        events = await connector.get_audit_log(since=since)
        assert len(events) == 1

        call_kwargs = mock_client.request.call_args
        params = call_kwargs.kwargs.get("params") or call_kwargs[1].get("params")
        assert params["after"] == expected_ms
        assert params["limit"] == 20

    @pytest.mark.asyncio
    async def test_get_audit_log_clamps_limit_to_api_max(self, connector):
        # LaunchDarkly returns HTTP 400 if `limit` exceeds 20; the connector
        # must clamp any larger caller value to the API's supported range.
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {}
        mock_response.json.return_value = {"items": []}

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.is_closed = False
        mock_client.request = AsyncMock(return_value=mock_response)
        connector._client = mock_client

        await connector.get_audit_log(limit=200)

        call_kwargs = mock_client.request.call_args
        params = call_kwargs.kwargs.get("params") or call_kwargs[1].get("params")
        assert params["limit"] == 20

    @pytest.mark.asyncio
    async def test_get_audit_log_paginates_via_next_link(self, connector):
        # LaunchDarkly returns at most 20 entries per page; the connector must
        # follow _links.next (with the /api/v2 prefix stripped) to assemble the
        # full history.
        def _page(items: list[dict], next_href: str | None = None) -> MagicMock:
            resp = MagicMock()
            resp.status_code = 200
            resp.headers = {}
            body: dict[str, object] = {"items": items}
            if next_href is not None:
                body["_links"] = {"next": {"href": next_href}}
            resp.json.return_value = body
            return resp

        page1 = _page(
            [{"_id": f"a{i}"} for i in range(20)],
            "/api/v2/auditlog?limit=20&before=2000",
        )
        page2 = _page(
            [{"_id": f"b{i}"} for i in range(20)],
            "/api/v2/auditlog?limit=20&before=1000",
        )
        page3 = _page([{"_id": f"c{i}"} for i in range(5)])

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.is_closed = False
        mock_client.request = AsyncMock(side_effect=[page1, page2, page3])
        connector._client = mock_client

        events = await connector.get_audit_log(limit=1000)

        assert len(events) == 45
        assert mock_client.request.await_count == 3
        # First request caps the page at 20 entries.
        first_call = mock_client.request.call_args_list[0]
        first_params = first_call.kwargs.get("params") or first_call[1].get("params")
        assert first_params["limit"] == 20
        # Pagination follows _links.next with the /api/v2 prefix stripped.
        assert mock_client.request.call_args_list[1].args[1] == (
            "/auditlog?limit=20&before=2000"
        )

    @pytest.mark.asyncio
    async def test_close_closes_client(self, connector):
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.is_closed = False
        connector._client = mock_client

        await connector.close()
        mock_client.aclose.assert_called_once()

    @pytest.mark.asyncio
    async def test_context_manager(self):
        async with LaunchDarklyConnector(api_key="key", project_key="p") as conn:
            assert conn.api_key == "key"


class TestLaunchDarklyClientUsageRecording:
    """CHAOS-2761: providers/launchdarkly/client.py is the canonical migration
    target for the frozen connector's flag/audit-log fetch logic. It must
    record REAL per-request counts (not abstract per-call units -- the
    codex-flagged CHAOS-2759 contract) through the shared CHAOS-2754 recorder,
    while keeping identical retry/429 behavior to the connector it replaces.
    """

    @pytest.fixture
    def client(self):
        return LaunchDarklyClient(api_key="test-api-key", project_key="default")

    @pytest.mark.asyncio
    async def test_get_flags_records_one_request_per_page(self, client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"X-RateLimit-Route-Remaining": "100"}
        mock_response.json.return_value = {
            "totalCount": 2,
            "items": [
                {"key": "flag-1", "name": "Flag One", "kind": "boolean"},
                {"key": "flag-2", "name": "Flag Two", "kind": "multivariate"},
            ],
        }

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.is_closed = False
        mock_client.request = AsyncMock(return_value=mock_response)
        client._client = mock_client

        flags = await client.get_flags()

        assert len(flags) == 2
        observations = client.drain_usage_observations()
        assert len(observations) == 1
        assert observations[0]["route_family"] == "flags"
        assert observations[0]["dimension"] == "rest_core"
        assert observations[0]["request_count"] == 1
        assert observations[0]["rate_limit"]["remaining"] == "100"

    @pytest.mark.asyncio
    async def test_get_audit_log_pagination_counts_each_page_as_a_real_request(
        self, client
    ):
        # Mirrors TestLaunchDarklyConnector.test_get_audit_log_paginates_via_
        # next_link: 3 pages -> 3 real HTTP requests, all keyed under the SAME
        # audit_log route family (never one "logical fetch" unit).
        def _page(items: list[dict], next_href: str | None = None) -> MagicMock:
            resp = MagicMock()
            resp.status_code = 200
            resp.headers = {}
            body: dict[str, object] = {"items": items}
            if next_href is not None:
                body["_links"] = {"next": {"href": next_href}}
            resp.json.return_value = body
            return resp

        page1 = _page(
            [{"_id": f"a{i}"} for i in range(20)],
            "/api/v2/auditlog?limit=20&before=2000",
        )
        page2 = _page(
            [{"_id": f"b{i}"} for i in range(20)],
            "/api/v2/auditlog?limit=20&before=1000",
        )
        page3 = _page([{"_id": f"c{i}"} for i in range(5)])

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.is_closed = False
        mock_client.request = AsyncMock(side_effect=[page1, page2, page3])
        client._client = mock_client

        events = await client.get_audit_log(limit=1000)

        assert len(events) == 45
        observations = client.drain_usage_observations()
        assert len(observations) == 1
        assert observations[0]["route_family"] == "audit_log"
        assert observations[0]["request_count"] == 3, (
            "three real HTTP requests were made (one per page); this must "
            "reflect a REAL request count, not an abstract per-call unit"
        )

    @pytest.mark.asyncio
    async def test_retried_429_counts_every_attempt_as_a_real_request(self, client):
        throttled = MagicMock()
        throttled.status_code = 429
        throttled.headers = {"Retry-After": "0"}
        throttled.text = ""

        ok = MagicMock()
        ok.status_code = 200
        ok.headers = {}
        ok.json.return_value = {"totalCount": 0, "items": []}

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.is_closed = False
        mock_client.request = AsyncMock(side_effect=[throttled, ok])
        client._client = mock_client

        await client.get_flags()

        observations = client.drain_usage_observations()
        assert len(observations) == 1
        assert observations[0]["request_count"] == 2, (
            "the retried 429 attempt AND the eventual success are both real "
            "requests against the provider -- neither is free"
        )
        assert observations[0]["latest_status"] == 200

    @pytest.mark.asyncio
    async def test_rate_limit_exception_carries_canonical_signal(self, client):
        # CHAOS-2761 requirement: the same canonical exception + RateLimitSignal
        # the frozen connector already raised (CHAOS-2753 normalization) --
        # this is a migration, not a new signal shape.
        throttled = MagicMock()
        throttled.status_code = 429
        throttled.headers = {"Retry-After": "7", "X-RateLimit-Reset": "0"}
        throttled.text = ""
        throttled.url = None

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.is_closed = False
        mock_client.request = AsyncMock(return_value=throttled)
        client._client = mock_client
        client.max_retries = 1

        with pytest.raises(RootRateLimitException) as excinfo:
            await client.get_flags()

        assert excinfo.value.signal is not None
        assert excinfo.value.signal.provider == "launchdarkly"
        assert excinfo.value.signal.reason == "primary"
        assert excinfo.value.retry_after_seconds == 7.0
        # Exactly one real request was recorded before giving up.
        observations = client.drain_usage_observations()
        assert observations[0]["request_count"] == 1


class TestLaunchDarklyCodeReferences:
    @pytest.mark.asyncio
    async def test_list_default_branch_references_uses_project_filter(self):
        response = MagicMock()
        response.status_code = 200
        response.headers = {}
        response.json.return_value = {"items": []}

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.is_closed = False
        mock_client.request = AsyncMock(return_value=response)

        client = LaunchDarklyCodeReferencesClient(api_key="key")
        client._client = mock_client

        refs = await client.list_default_branch_references(project_key="web")

        assert refs == []
        mock_client.request.assert_called_once_with(
            "GET",
            "/code-refs/repositories",
            params={"withReferencesForDefaultBranch": "1", "projKey": "web"},
        )

    @pytest.mark.asyncio
    async def test_list_default_branch_references_records_usage(self):
        """CHAOS-2761: code_refs was already canonical but never wired to the
        shared CHAOS-2754 recorder -- close that gap."""
        response = MagicMock()
        response.status_code = 200
        response.headers = {"X-RateLimit-Route-Remaining": "50"}
        response.json.return_value = {"items": []}

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.is_closed = False
        mock_client.request = AsyncMock(return_value=response)

        client = LaunchDarklyCodeReferencesClient(api_key="key")
        client._client = mock_client

        await client.list_default_branch_references(project_key="web")

        observations = client.drain_usage_observations()
        assert len(observations) == 1
        assert observations[0]["route_family"] == "code_refs"
        assert observations[0]["dimension"] == "rest_core"
        assert observations[0]["request_count"] == 1
        assert observations[0]["rate_limit"]["remaining"] == "50"

    def test_parse_code_reference_repositories_flattens_hunks(self):
        refs = parse_code_reference_repositories(
            {
                "items": [
                    {
                        "name": "dev-health",
                        "sourceLink": "https://github.com/full-chaos/dev-health",
                        "defaultBranch": "main",
                        "branches": [
                            {
                                "name": "main",
                                "head": "abc123",
                                "references": [
                                    {
                                        "path": "/main/web/src/checkout.ts",
                                        "hunks": [
                                            {
                                                "startingLineNumber": 42,
                                                "lines": "variation('checkout-v2')",
                                                "projKey": "web",
                                                "flagKey": "checkout-v2",
                                                "aliases": ["checkoutV2"],
                                            }
                                        ],
                                    }
                                ],
                            }
                        ],
                    }
                ]
            }
        )

        assert refs == [
            LaunchDarklyCodeReference(
                flag_key="checkout-v2",
                project_key="web",
                repo_name="dev-health",
                repo_source_link="https://github.com/full-chaos/dev-health",
                branch_name="main",
                branch_head="abc123",
                file_path="web/src/checkout.ts",
                starting_line_number=42,
                lines="variation('checkout-v2')",
                aliases=("checkoutV2",),
            )
        ]

    def test_build_code_reference_links_emits_native_file_and_pr_artifacts(self):
        repo_id = "11111111-1111-1111-1111-111111111111"
        ref = LaunchDarklyCodeReference(
            flag_key="checkout-v2",
            project_key="web",
            repo_name="dev-health",
            repo_source_link="https://github.com/full-chaos/dev-health",
            branch_name="main",
            branch_head="abc123",
            file_path="web/src/checkout.ts",
            starting_line_number=42,
            lines="variation('checkout-v2')",
        )
        repo_index = index_repo_rows([{"id": repo_id, "repo": "full-chaos/dev-health"}])
        pr_id = generate_pr_id(uuid.UUID(repo_id), 17)

        links, edges = build_code_reference_links(
            [ref],
            org_id="org-1",
            repo_index=repo_index,
            pr_ids_by_repo_path={(repo_id, "web/src/checkout.ts"): {pr_id}},
            now=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

        assert {(link.target_type, link.target_id) for link in links} == {
            ("file", generate_file_id(uuid.UUID(repo_id), "web/src/checkout.ts")),
            ("pr", pr_id),
        }
        assert all(link.org_id == "org-1" for link in links)
        assert all(link.link_source == "native" for link in links)
        assert all(link.link_type == "code_reference" for link in links)
        assert all(link.confidence == LD_CODE_REFERENCE_CONFIDENCE for link in links)

        flag_id = generate_feature_flag_id(
            "org-1", "launchdarkly", "web", "checkout-v2"
        )
        assert edges == [
            {
                "flag_id": flag_id,
                "target_type": "file",
                "target_id": generate_file_id(
                    uuid.UUID(repo_id), "web/src/checkout.ts"
                ),
                "repo_id": uuid.UUID(repo_id),
                "evidence": "ld_code_ref:dev-health:main:web/src/checkout.ts:L42",
            },
            {
                "flag_id": flag_id,
                "target_type": "pr",
                "target_id": pr_id,
                "repo_id": uuid.UUID(repo_id),
                "evidence": "ld_code_ref:dev-health:main:web/src/checkout.ts:L42",
            },
        ]

    def test_source_link_wins_over_bare_repo_name_collision(self):
        wrong_repo_id = "11111111-1111-1111-1111-111111111111"
        correct_repo_id = "22222222-2222-2222-2222-222222222222"
        ref = LaunchDarklyCodeReference(
            flag_key="checkout-v2",
            project_key="web",
            repo_name="dev-health",
            repo_source_link="https://github.com/full-chaos/dev-health",
            branch_name="main",
            branch_head="abc123",
            file_path="web/src/checkout.ts",
            starting_line_number=42,
            lines="variation('checkout-v2')",
        )
        repo_index = index_repo_rows(
            [
                {"id": wrong_repo_id, "repo": "other/dev-health"},
                {"id": correct_repo_id, "repo": "full-chaos/dev-health"},
            ]
        )

        links, edges = build_code_reference_links(
            [ref],
            org_id="org-1",
            repo_index=repo_index,
            pr_ids_by_repo_path={},
            now=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

        assert [(link.target_type, link.target_id) for link in links] == [
            (
                "file",
                generate_file_id(uuid.UUID(correct_repo_id), "web/src/checkout.ts"),
            )
        ]
        assert edges[0]["repo_id"] == uuid.UUID(correct_repo_id)


# ---------------------------------------------------------------------------
# Processor: _parse_iso
# ---------------------------------------------------------------------------


class TestParseIso:
    def test_epoch_ms(self):
        result = _parse_iso(1705312800000)
        assert result is not None
        assert result.tzinfo == timezone.utc

    def test_iso_string(self):
        result = _parse_iso("2025-01-15T10:00:00Z")
        assert result is not None
        assert result.year == 2025

    def test_none(self):
        assert _parse_iso(None) is None

    def test_invalid(self):
        assert _parse_iso("not-a-date") is None


# ---------------------------------------------------------------------------
# Processor: normalize_flags
# ---------------------------------------------------------------------------


class TestNormalizeFlags:
    def test_basic_flag(self):
        flags = [
            {
                "key": "new-checkout",
                "name": "New Checkout Flow",
                "kind": "boolean",
                "tags": ["frontend", "checkout"],
                "creationDate": 1705312800000,
                "_projectKey": "my-project",
                "environments": {
                    "production": {"on": True},
                },
            }
        ]
        records = normalize_flags(flags, org_id="org-1")
        assert len(records) == 1
        rec = records[0]
        assert rec.org_id == "org-1"
        assert rec.flag_key == "new-checkout"
        assert rec.flag_type == "boolean"
        assert rec.provider == "launchdarkly"
        assert rec.project_key == "my-project"
        assert rec.created_at is not None

    def test_inactive_flag(self):
        flags = [
            {
                "key": "old-feature",
                "name": "Old Feature",
                "kind": "boolean",
                "environments": {
                    "production": {"on": False},
                },
            }
        ]
        records = normalize_flags(flags, org_id="org-1")
        assert records[0].flag_key == "old-feature"

    def test_empty_environments(self):
        flags = [{"key": "bare", "environments": {}}]
        records = normalize_flags(flags, org_id="org-1")
        assert records[0].flag_key == "bare"

    def test_empty_input(self):
        assert normalize_flags([], org_id="org-1") == []


# ---------------------------------------------------------------------------
# Processor: normalize_audit_events
# ---------------------------------------------------------------------------


class TestNormalizeAuditEvents:
    def _make_entry(self, kind="createFlag", flag_key="my-flag", entry_id="evt-1"):
        return {
            "_id": entry_id,
            "kind": kind,
            "date": 1705312800000,
            "description": f"Test {kind}",
            "member": {"email": "dev@example.com", "_id": "user-1"},
            "target": {"resources": [f"proj/default:env/production:flag/{flag_key}"]},
            "name": flag_key,
        }

    def test_create_flag_event(self):
        events = [self._make_entry("createFlag")]
        records = normalize_audit_events(events, org_id="org-1")
        assert len(records) == 1
        rec = records[0]
        assert rec.event_type == "create"
        assert rec.flag_key == "my-flag"
        assert rec.actor_type == "dev@example.com"
        assert rec.source_event_id == "evt-1"
        assert rec.dedupe_key == "evt-1"

    @pytest.mark.parametrize(
        "ld_kind,expected",
        [
            ("createFlag", "create"),
            ("updateFlag", "update"),
            ("toggleFlag", "toggle"),
            ("updateFlagVariations", "rule"),
            ("updateFlagDefaultRule", "rollout"),
        ],
    )
    def test_event_kind_mapping(self, ld_kind, expected):
        events = [self._make_entry(ld_kind)]
        records = normalize_audit_events(events, org_id="org-1")
        assert records[0].event_type == expected

    def test_unknown_kind_passes_through(self):
        events = [self._make_entry("deleteFlag")]
        records = normalize_audit_events(events, org_id="org-1")
        assert records[0].event_type == "deleteFlag"

    def test_flag_key_from_target_resources(self):
        entry = {
            "_id": "e1",
            "kind": "updateFlag",
            "target": {"resources": ["proj/default:env/prod:flag/checkout-v2"]},
            "member": {},
        }
        records = normalize_audit_events([entry], org_id="org-1")
        assert records[0].flag_key == "checkout-v2"

    def test_flag_key_fallback_to_name(self):
        entry = {
            "_id": "e2",
            "kind": "updateFlag",
            "target": {"resources": []},
            "name": "fallback-flag",
            "member": {},
        }
        records = normalize_audit_events([entry], org_id="org-1")
        assert records[0].flag_key == "fallback-flag"

    def test_actor_fallback_to_member_id(self):
        entry = {
            "_id": "e3",
            "kind": "createFlag",
            "member": {"_id": "user-99"},
            "target": {},
        }
        records = normalize_audit_events([entry], org_id="org-1")
        assert records[0].actor_type == "user-99"

    def test_empty_input(self):
        assert normalize_audit_events([], org_id="org-1") == []

    def test_event_kind_map_completeness(self):
        expected_keys = {
            "createFlag",
            "updateFlag",
            "toggleFlag",
            "updateFlagVariations",
            "updateFlagDefaultRule",
        }
        assert set(_EVENT_KIND_MAP.keys()) == expected_keys
