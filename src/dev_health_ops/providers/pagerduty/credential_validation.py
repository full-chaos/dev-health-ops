"""Non-mutating live validation for PagerDuty sync credentials."""

from __future__ import annotations

from collections.abc import Awaitable
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

import httpx

from dev_health_ops.credentials.types import PagerDutyCredentials
from dev_health_ops.providers.pagerduty.client import pagerduty_base_url

_PAGERDUTY_ACCEPT = "application/vnd.pagerduty+json;version=2"
_TOKEN_URL = "https://identity.pagerduty.com/oauth/token"


class PagerDutyCredentialValidationError(Exception):
    """A safe, typed validation failure that never includes secret material."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class ValidatedPagerDutyCredential:
    """An ephemeral validated bearer/access descriptor for the next lifecycle step."""

    auth_mode: str
    access_token: str | None
    granted_scopes: frozenset[str]
    account_id: str
    account_display: str
    subdomain: str


async def validate_pagerduty_credential(
    credentials: PagerDutyCredentials,
    *,
    required_scopes: frozenset[str],
    transport: httpx.AsyncBaseTransport | None = None,
) -> ValidatedPagerDutyCredential:
    """Validate one persisted credential with no database or descriptor mutation.

    Every mode proves usable read access through ``/services?limit=1``. OAuth
    modes also prove the persisted/token-exchange scope grant covers the selected
    datasets. PagerDuty API tokens do not expose OAuth scope claims, so their
    bounded service read is the authoritative live capability proof.
    """
    async with httpx.AsyncClient(transport=transport, follow_redirects=True) as client:
        match credentials.auth_mode:
            case "api_token":
                api_token = _required_secret(credentials.api_token, "missing_api_token")
                response = await _validate_read(
                    client,
                    region=credentials.region,
                    authorization=f"Token token={api_token}",
                )
                account_id, account_display, subdomain = _account_identity(response)
                return ValidatedPagerDutyCredential(
                    auth_mode="api_token",
                    access_token=None,
                    granted_scopes=frozenset(),
                    account_id=account_id,
                    account_display=account_display,
                    subdomain=subdomain,
                )
            case "oauth":
                _require_scopes(credentials.granted_scopes, required_scopes)
                access_token = _required_secret(
                    credentials.access_token, "missing_access_token"
                )
                response = await _validate_read(
                    client,
                    region=credentials.region,
                    authorization=f"Bearer {access_token}",
                )
                account_id, account_display, subdomain = _account_identity(response)
                return ValidatedPagerDutyCredential(
                    auth_mode="oauth",
                    access_token=access_token,
                    granted_scopes=frozenset(credentials.granted_scopes),
                    account_id=account_id,
                    account_display=account_display,
                    subdomain=subdomain,
                )
            case "client_credentials":
                access_token, granted_scopes = await _exchange_client_credentials(
                    client, credentials, required_scopes
                )
                response = await _validate_read(
                    client,
                    region=credentials.region,
                    authorization=f"Bearer {access_token}",
                )
                account_id, account_display, subdomain = _account_identity(response)
                return ValidatedPagerDutyCredential(
                    auth_mode="client_credentials",
                    access_token=access_token,
                    granted_scopes=granted_scopes,
                    account_id=account_id,
                    account_display=account_display,
                    subdomain=subdomain,
                )
            case _:
                raise PagerDutyCredentialValidationError("unsupported_auth_mode")


async def _exchange_client_credentials(
    client: httpx.AsyncClient,
    credentials: PagerDutyCredentials,
    required_scopes: frozenset[str],
) -> tuple[str, frozenset[str]]:
    response = await _request(
        client.post(
            _TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": _required_secret(
                    credentials.client_id, "missing_client_id"
                ),
                "client_secret": _required_secret(
                    credentials.client_secret, "missing_client_secret"
                ),
                "scope": " ".join(sorted(required_scopes)),
                "subdomain": _required_secret(
                    credentials.subdomain, "missing_subdomain"
                ),
                "region": credentials.region,
            },
        )
    )
    try:
        payload = response.json()
    except ValueError as exc:
        raise PagerDutyCredentialValidationError("invalid_token_response") from exc
    if not isinstance(payload, dict):
        raise PagerDutyCredentialValidationError("invalid_token_response")
    access_token = payload.get("access_token")
    scope_value = payload.get("scope", "")
    if not isinstance(access_token, str) or not access_token:
        raise PagerDutyCredentialValidationError("invalid_token_response")
    if not isinstance(scope_value, str):
        raise PagerDutyCredentialValidationError("invalid_token_response")
    granted_scopes = frozenset(scope_value.split())
    _require_scopes(granted_scopes, required_scopes)
    return access_token, granted_scopes


async def _validate_read(
    client: httpx.AsyncClient, *, region: str, authorization: str
) -> httpx.Response:
    return await _request(
        client.get(
            f"{pagerduty_base_url(region=region)}/services",
            params={"limit": "1"},
            headers={
                "Accept": _PAGERDUTY_ACCEPT,
                "Authorization": authorization,
            },
        )
    )


def _account_identity(response: httpx.Response) -> tuple[str, str, str]:
    try:
        payload = response.json()
    except ValueError as exc:
        raise PagerDutyCredentialValidationError("missing_account_identity") from exc
    if not isinstance(payload, dict):
        raise PagerDutyCredentialValidationError("missing_account_identity")
    services = payload.get("services")
    if not isinstance(services, list) or not services:
        raise PagerDutyCredentialValidationError("missing_account_identity")
    service = services[0]
    if not isinstance(service, dict):
        raise PagerDutyCredentialValidationError("missing_account_identity")
    account = service.get("account")
    if isinstance(account, dict):
        account_id = _nonempty_string(account.get("id"))
        subdomain = _nonempty_string(account.get("subdomain"))
        account_display = _nonempty_string(account.get("name"))
        if account_id is not None and subdomain is not None:
            return account_id, account_display or subdomain, subdomain
    subdomain = _subdomain_from_service_url(service.get("html_url"))
    if subdomain is None:
        raise PagerDutyCredentialValidationError("missing_account_identity")
    return subdomain, subdomain, subdomain


def _nonempty_string(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    return normalized or None


def _subdomain_from_service_url(value: Any) -> str | None:
    url = _nonempty_string(value)
    if url is None:
        return None
    hostname = urlparse(url).hostname
    if hostname is None:
        return None
    labels = hostname.lower().split(".")
    if len(labels) < 3 or labels[-2:] != ["pagerduty", "com"]:
        return None
    subdomain = labels[0]
    return subdomain if subdomain != "api" else None


async def _request(response_request: Awaitable[httpx.Response]) -> httpx.Response:
    try:
        response = await response_request
        response.raise_for_status()
    except httpx.HTTPError as exc:
        raise PagerDutyCredentialValidationError("live_read_failed") from exc
    return response


def _required_secret(value: str | None, code: str) -> str:
    if not value:
        raise PagerDutyCredentialValidationError(code)
    return value


def _require_scopes(
    granted_scopes: tuple[str, ...] | frozenset[str], required_scopes: frozenset[str]
) -> None:
    if required_scopes.difference(granted_scopes):
        raise PagerDutyCredentialValidationError("missing_required_scopes")
