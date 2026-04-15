"""Tests for LaunchDarkly connector and processor normalization."""

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
from dev_health_ops.processors.launchdarkly import (
    _EVENT_KIND_MAP,
    _parse_iso,
    normalize_audit_events,
    normalize_flags,
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
            "items": [
                {"key": "flag-1", "name": "Flag One", "kind": "boolean"},
                {"key": "flag-2", "name": "Flag Two", "kind": "multivariate"},
            ]
        }

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.is_closed = False
        mock_client.request = AsyncMock(return_value=mock_response)
        connector._client = mock_client

        flags = await connector.get_flags()
        assert len(flags) == 2
        assert flags[0]["key"] == "flag-1"
        mock_client.request.assert_called_once_with(
            "GET", "/flags/default", params=None
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
