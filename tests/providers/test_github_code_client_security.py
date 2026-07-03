"""Parity tests for providers/github/code_client.py::GitHubCodeClient
(CHAOS-2773 CS3).

Proves the httpx-based ``GitHubCodeClient`` reproduces
``connectors/github.py``'s frozen security-alert fetch (``get_dependabot_alerts``
/ ``get_code_scanning_alerts`` / ``get_security_advisories`` and their shared
``_get_security_alert_page`` pager) byte-for-byte: Link-header pagination,
403/404 degrade-to-empty on these optional endpoints, 429 -> RateLimitException,
and identical ``SecurityAlertData`` field mapping. All HTTP is mocked at the
transport layer (``httpx.MockTransport``) -- no live network (offline gate).
"""

from __future__ import annotations

import time

import httpx
import pytest

from dev_health_ops.exceptions import RateLimitException
from dev_health_ops.providers.github.client import GitHubAuth
from dev_health_ops.providers.github.code_client import GitHubCodeClient


def _client(
    transport: httpx.AsyncBaseTransport, *, token: str = "ghp_test_token"
) -> GitHubCodeClient:
    return GitHubCodeClient(auth=GitHubAuth(token=token), transport=transport)


def _handler_sequence(responses: list[httpx.Response]) -> httpx.MockTransport:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        response = responses[min(calls["n"], len(responses) - 1)]
        calls["n"] += 1
        return response

    transport = httpx.MockTransport(handler)
    transport.calls = calls  # type: ignore[attr-defined]
    return transport


def _mock_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    """No-op ``asyncio.sleep`` so retry backoff (429 / rate-limited 403) does
    not actually block the test."""
    from unittest.mock import AsyncMock

    monkeypatch.setattr(
        "dev_health_ops.providers._http.asyncio.sleep",
        AsyncMock(return_value=None),
    )


# ---------------------------------------------------------------------------
# Auth / headers
# ---------------------------------------------------------------------------


class TestAuthHeaders:
    @pytest.mark.asyncio
    async def test_sends_token_and_accept_headers(self) -> None:
        seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(200, json=[])

        client = _client(httpx.MockTransport(handler), token="unit-test-pat")
        await client.get_dependabot_alerts("acme", "widgets")

        assert seen[0].headers["Authorization"] == "token unit-test-pat"
        assert seen[0].headers["Accept"] == "application/vnd.github+json"
        await client.close()

    def test_missing_token_raises(self) -> None:
        with pytest.raises(ValueError, match="resolved token"):
            GitHubCodeClient(auth=GitHubAuth(token=None))


# ---------------------------------------------------------------------------
# Pagination via Link header (rel="next")
# ---------------------------------------------------------------------------


class TestPagination:
    @pytest.mark.asyncio
    async def test_follows_link_header_across_pages(self) -> None:
        page1 = httpx.Response(
            200,
            json=[{"number": 1, "state": "open", "html_url": "u1"}],
            headers={
                "Link": (
                    "<https://api.github.com/repos/acme/widgets/dependabot/alerts"
                    '?page=2>; rel="next"'
                )
            },
        )
        page2 = httpx.Response(
            200, json=[{"number": 2, "state": "open", "html_url": "u2"}]
        )
        transport = _handler_sequence([page1, page2])
        client = _client(transport)

        alerts = await client.get_dependabot_alerts("acme", "widgets")

        assert [a.alert_id for a in alerts] == ["dependabot:1", "dependabot:2"]
        assert transport.calls["n"] == 2  # type: ignore[attr-defined]
        await client.close()

    @pytest.mark.asyncio
    async def test_first_request_applies_params_followups_use_absolute_url(
        self,
    ) -> None:
        seen: list[httpx.Request] = []
        page1 = httpx.Response(
            200,
            json=[{"number": 1, "state": "open"}],
            headers={
                "Link": (
                    "<https://api.github.com/repos/acme/widgets/dependabot/alerts"
                    '?page=2>; rel="next"'
                )
            },
        )
        page2 = httpx.Response(200, json=[{"number": 2, "state": "open"}])

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return page1 if len(seen) == 1 else page2

        client = _client(httpx.MockTransport(handler))
        await client.get_dependabot_alerts("acme", "widgets", state="open")

        assert seen[0].url.params["state"] == "open"
        assert seen[0].url.params["per_page"] == "100"
        # The second (absolute next-url) request carries no re-applied params
        # beyond what the Link header itself encoded.
        assert "state" not in seen[1].url.params
        await client.close()

    @pytest.mark.asyncio
    async def test_no_link_header_single_page(self) -> None:
        client = _client(
            httpx.MockTransport(
                lambda r: httpx.Response(200, json=[{"number": 1, "state": "open"}])
            )
        )
        alerts = await client.get_code_scanning_alerts("acme", "widgets")
        assert len(alerts) == 1
        await client.close()

    @pytest.mark.asyncio
    async def test_max_alerts_truncates_after_full_pagination(self) -> None:
        page1 = httpx.Response(
            200,
            json=[{"number": 1, "state": "open"}, {"number": 2, "state": "open"}],
            headers={
                "Link": (
                    "<https://api.github.com/repos/acme/widgets/dependabot/alerts"
                    '?page=2>; rel="next"'
                )
            },
        )
        page2 = httpx.Response(200, json=[{"number": 3, "state": "open"}])
        transport = _handler_sequence([page1, page2])
        client = _client(transport)

        alerts = await client.get_dependabot_alerts("acme", "widgets", max_alerts=2)

        assert [a.alert_id for a in alerts] == ["dependabot:1", "dependabot:2"]
        # Pagination still ran to completion (matches the connector, which
        # fetches all pages before the alert-building loop truncates).
        assert transport.calls["n"] == 2  # type: ignore[attr-defined]
        await client.close()


# ---------------------------------------------------------------------------
# 403 / 404 degrade-to-empty (optional endpoints)
# ---------------------------------------------------------------------------


class TestDegradeToEmpty:
    @pytest.mark.asyncio
    async def test_permission_403_degrades_to_empty(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        client = _client(
            httpx.MockTransport(
                lambda r: httpx.Response(
                    403, json={"message": "Resource not accessible"}
                )
            )
        )
        with caplog.at_level("WARNING"):
            alerts = await client.get_dependabot_alerts("acme", "widgets")
        assert alerts == []
        # Diagnosability parity: a permission/SSO 403 must be logged (safe
        # headers only) before degrading -- never silently drop all alerts.
        assert any("non-rate-limit 403" in r.message for r in caplog.records)
        await client.close()

    @pytest.mark.asyncio
    async def test_401_degrades_to_empty_with_log(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        client = _client(
            httpx.MockTransport(
                lambda r: httpx.Response(401, json={"message": "Bad credentials"})
            )
        )
        with caplog.at_level("WARNING"):
            alerts = await client.get_dependabot_alerts("acme", "widgets")
        assert alerts == []
        assert any("401" in r.message for r in caplog.records)
        await client.close()

    @pytest.mark.asyncio
    async def test_404_degrades_to_empty(self) -> None:
        client = _client(
            httpx.MockTransport(
                lambda r: httpx.Response(404, json={"message": "Not Found"})
            )
        )
        alerts = await client.get_security_advisories("acme", "widgets")
        assert alerts == []
        await client.close()

    @pytest.mark.asyncio
    async def test_mid_pagination_403_discards_partial_results(self) -> None:
        # Matches connectors/github.py::_get_security_alert_page exactly: a
        # non-rate-limited 403 on page 2 returns [] -- NOT the items already
        # gathered from page 1.
        page1 = httpx.Response(
            200,
            json=[{"number": 1, "state": "open"}],
            headers={
                "Link": (
                    "<https://api.github.com/repos/acme/widgets/code-scanning/alerts"
                    '?page=2>; rel="next"'
                )
            },
        )
        page2 = httpx.Response(403, json={"message": "Resource not accessible"})
        transport = _handler_sequence([page1, page2])
        client = _client(transport)

        alerts = await client.get_code_scanning_alerts("acme", "widgets")

        assert alerts == []
        await client.close()


# ---------------------------------------------------------------------------
# 429 -> RateLimitException / rate-limited 403 -> RateLimitException
# ---------------------------------------------------------------------------


class TestRateLimit:
    @pytest.mark.asyncio
    async def test_429_exhaustion_raises_rate_limit_exception(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from unittest.mock import AsyncMock

        monkeypatch.setattr(
            "dev_health_ops.providers._http.asyncio.sleep", AsyncMock(return_value=None)
        )
        client = _client(
            httpx.MockTransport(
                lambda r: httpx.Response(429, headers={"Retry-After": "5"})
            )
        )
        with pytest.raises(RateLimitException) as excinfo:
            await client.get_dependabot_alerts("acme", "widgets")
        assert excinfo.value.retry_after_seconds == pytest.approx(5.0)
        await client.close()

    @pytest.mark.asyncio
    async def test_primary_rate_limit_403_raises_rate_limit_exception(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _mock_sleep(monkeypatch)
        reset_epoch = int(time.time()) + 120
        client = _client(
            httpx.MockTransport(
                lambda r: httpx.Response(
                    403,
                    headers={
                        "X-RateLimit-Remaining": "0",
                        "X-RateLimit-Reset": str(reset_epoch),
                    },
                    json={"message": "API rate limit exceeded"},
                )
            )
        )
        with pytest.raises(RateLimitException) as excinfo:
            await client.get_dependabot_alerts("acme", "widgets")
        signal = excinfo.value.signal
        assert signal is not None
        assert signal.provider == "github"
        assert signal.reason == "primary"
        await client.close()

    @pytest.mark.asyncio
    async def test_secondary_abuse_403_raises_rate_limit_exception(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _mock_sleep(monkeypatch)
        client = _client(
            httpx.MockTransport(
                lambda r: httpx.Response(
                    403,
                    headers={"Retry-After": "30"},
                    json={"message": "You have exceeded a secondary rate limit."},
                )
            )
        )
        with pytest.raises(RateLimitException) as excinfo:
            await client.get_code_scanning_alerts("acme", "widgets")
        assert excinfo.value.retry_after_seconds == pytest.approx(30.0)
        assert excinfo.value.signal is not None
        assert excinfo.value.signal.reason == "secondary"
        await client.close()

    @pytest.mark.asyncio
    async def test_rate_limited_403_then_200_retries_and_succeeds(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Finding-1 parity fix: a transient secondary/abuse 403 is RETRIED
        # (mirrors the connector's retry_with_backoff), not abandoned on the
        # first response -- a 403-then-200 sequence yields the 200 payload.
        _mock_sleep(monkeypatch)
        limited = httpx.Response(
            403,
            headers={"Retry-After": "0"},
            json={"message": "You have exceeded a secondary rate limit."},
        )
        ok = httpx.Response(200, json=[{"number": 5, "state": "open"}])
        transport = _handler_sequence([limited, ok])
        client = _client(transport)

        alerts = await client.get_code_scanning_alerts("acme", "widgets")

        assert [a.alert_id for a in alerts] == ["code_scanning:5"]
        assert transport.calls["n"] == 2  # type: ignore[attr-defined]
        await client.close()

    @pytest.mark.asyncio
    async def test_usage_observations_key_under_security_route_family(self) -> None:
        client = _client(httpx.MockTransport(lambda r: httpx.Response(200, json=[])))
        await client.get_dependabot_alerts("acme", "widgets")
        observations = client.drain_usage_observations()
        assert len(observations) == 1
        assert observations[0]["route_family"] == "security"
        assert observations[0]["request_count"] == 1
        await client.close()


# ---------------------------------------------------------------------------
# Field mapping parity (connectors/github.py::get_*_alerts byte-for-byte)
# ---------------------------------------------------------------------------


class TestFieldMapping:
    @pytest.mark.asyncio
    async def test_dependabot_alert_field_mapping(self) -> None:
        payload = [
            {
                "number": 42,
                "state": "open",
                "html_url": "https://github.com/acme/widgets/security/dependabot/42",
                "created_at": "2024-01-01T00:00:00Z",
                "fixed_at": None,
                "dismissed_at": None,
                "security_advisory": {
                    "severity": "high",
                    "cve_id": "CVE-2024-1234",
                    "summary": "Vulnerable dependency",
                    "description": "Full description",
                },
                "dependency": {"package": {"name": "left-pad"}},
            }
        ]
        client = _client(
            httpx.MockTransport(lambda r: httpx.Response(200, json=payload))
        )
        alerts = await client.get_dependabot_alerts("acme", "widgets")
        await client.close()

        assert len(alerts) == 1
        alert = alerts[0]
        assert alert.alert_id == "dependabot:42"
        assert alert.source == "dependabot"
        assert alert.severity == "high"
        assert alert.state == "open"
        assert alert.package_name == "left-pad"
        assert alert.cve_id == "CVE-2024-1234"
        assert alert.url == "https://github.com/acme/widgets/security/dependabot/42"
        assert alert.title == "Vulnerable dependency"
        assert alert.description == "Full description"
        assert alert.created_at is not None
        assert alert.fixed_at is None
        assert alert.dismissed_at is None

    @pytest.mark.asyncio
    async def test_code_scanning_alert_field_mapping(self) -> None:
        payload = [
            {
                "number": 7,
                "state": "open",
                "html_url": "https://github.com/acme/widgets/security/code-scanning/7",
                "created_at": "2024-02-02T00:00:00Z",
                "dismissed_at": "2024-02-03T00:00:00Z",
                "rule": {"severity": "error", "description": "SQL injection"},
                "most_recent_instance": {"message": {"text": "Tainted input"}},
            }
        ]
        client = _client(
            httpx.MockTransport(lambda r: httpx.Response(200, json=payload))
        )
        alerts = await client.get_code_scanning_alerts("acme", "widgets")
        await client.close()

        assert len(alerts) == 1
        alert = alerts[0]
        assert alert.alert_id == "code_scanning:7"
        assert alert.source == "code_scanning"
        assert alert.severity == "error"
        assert alert.state == "open"
        assert alert.package_name is None
        assert alert.cve_id is None
        assert alert.title == "SQL injection"
        assert alert.description == "Tainted input"
        assert alert.fixed_at is None
        assert alert.dismissed_at is not None

    @pytest.mark.asyncio
    async def test_security_advisory_field_mapping(self) -> None:
        payload = [
            {
                "ghsa_id": "GHSA-xxxx-yyyy-zzzz",
                "state": "published",
                "severity": "critical",
                "cve_id": "CVE-2024-9999",
                "html_url": "https://github.com/acme/widgets/security/advisories/GHSA-xxxx",
                "summary": "Critical advisory",
                "description": "Advisory description",
                "created_at": "2024-03-03T00:00:00Z",
            }
        ]
        client = _client(
            httpx.MockTransport(lambda r: httpx.Response(200, json=payload))
        )
        alerts = await client.get_security_advisories("acme", "widgets")
        await client.close()

        assert len(alerts) == 1
        alert = alerts[0]
        assert alert.alert_id == "advisory:GHSA-xxxx-yyyy-zzzz"
        assert alert.source == "advisory"
        assert alert.severity == "critical"
        assert alert.state == "published"
        assert alert.package_name is None
        assert alert.cve_id == "CVE-2024-9999"
        assert alert.title == "Critical advisory"
        assert alert.description == "Advisory description"
        assert alert.fixed_at is None
        assert alert.dismissed_at is None
