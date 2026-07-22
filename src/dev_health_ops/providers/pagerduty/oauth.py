"""Configuration-driven PagerDuty OAuth helpers; no app registration occurs here."""

import base64
import hashlib
import os
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Final
from urllib.parse import urlencode

import httpx
from pydantic import BaseModel, ConfigDict

READ_SCOPES = frozenset(
    {
        "Incidents.read",
        "Services.read",
        "Escalation_policies.read",
        "Schedules.read",
        "Oncalls.read",
        "Users.read",
        "Teams.read",
    }
)
DATASET_SCOPES = {
    "incidents": frozenset({"Incidents.read"}),
    "services": frozenset({"Services.read"}),
    "business_services": frozenset({"Services.read"}),
    "escalation_policies": frozenset({"Escalation_policies.read"}),
    "schedules": frozenset({"Schedules.read"}),
    "oncalls": frozenset({"Oncalls.read"}),
    "users": frozenset({"Users.read"}),
    "teams": frozenset({"Teams.read"}),
}
DATASET_OAUTH_FAMILIES = {
    "incidents": "incidents",
    "services": "services",
    "business-services": "business_services",
    "escalation-policies": "escalation_policies",
    "schedules": "schedules",
    "on-calls": "oncalls",
    "users": "users",
    "teams": "teams",
    "incident-alerts": "incidents",
    "incident-log-entries": "incidents",
    "incident-notes": "incidents",
}
DEFAULT_RENEWAL_WINDOW: Final = timedelta(minutes=5)

_HTTP_TIMEOUT: Final = httpx.Timeout(10.0)


class OAuthTokens(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)
    access_token: str
    refresh_token: str | None = None
    expires_at: datetime
    granted_scopes: frozenset[str] = frozenset()


@dataclass(frozen=True, slots=True)
class PagerDutyOAuthConfig:
    client_id: str
    client_secret: str | None
    redirect_uri: str
    authorization_url: str = "https://identity.pagerduty.com/oauth/authorize"
    token_url: str = "https://identity.pagerduty.com/oauth/token"
    revoke_url: str = "https://identity.pagerduty.com/oauth/revoke"

    @classmethod
    def from_env(cls) -> "PagerDutyOAuthConfig | None":
        """Build the registered PagerDuty app config from ``PAGER_DUTY_*`` env vars.

        Returns ``None`` when ``PAGER_DUTY_CLIENT_ID`` is unset. ``client_secret``
        may be absent for public PKCE clients; ``redirect_uri`` is unused by the
        client-credentials (self-hosted) flow and defaults to empty.
        """
        client_id = os.getenv("PAGER_DUTY_CLIENT_ID")
        if not client_id:
            return None
        return cls(
            client_id=client_id,
            client_secret=os.getenv("PAGER_DUTY_SECRET"),
            redirect_uri=os.getenv("PAGER_DUTY_REDIRECT_URI", ""),
        )


@dataclass(frozen=True, slots=True)
class AuthorizationRequest:
    url: str
    state: str
    nonce: str
    code_verifier: str


class OAuthCallbackValidationError(ValueError):
    """Raised when an OAuth callback cannot be tied to its authorization request."""


def pagerduty_oauth_family(dataset_key: str) -> str:
    """Normalize a sync-registry dataset key to its PagerDuty OAuth family."""
    return DATASET_OAUTH_FAMILIES.get(dataset_key, dataset_key)


def required_read_scopes(enabled_datasets: set[str]) -> frozenset[str]:
    return frozenset().union(
        *(
            DATASET_SCOPES.get(pagerduty_oauth_family(dataset), frozenset())
            for dataset in enabled_datasets
        )
    )


def missing_read_scopes(
    enabled_datasets: set[str], granted_scopes: set[str]
) -> frozenset[str]:
    return required_read_scopes(enabled_datasets).difference(granted_scopes)


def build_authorization_request(config: PagerDutyOAuthConfig) -> AuthorizationRequest:
    verifier = secrets.token_urlsafe(64)
    challenge = (
        base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest())
        .decode()
        .rstrip("=")
    )
    state, nonce = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
    params = {
        "response_type": "code",
        "client_id": config.client_id,
        "redirect_uri": config.redirect_uri,
        "scope": " ".join(sorted(READ_SCOPES)),
        "state": state,
        "nonce": nonce,
        "code_challenge": challenge,
        "code_challenge_method": "S256",
    }
    return AuthorizationRequest(
        f"{config.authorization_url}?{urlencode(params)}", state, nonce, verifier
    )


def validate_callback(*, code: str, error: str | None = None) -> str:
    """Reject an error/denied callback or an empty code before code exchange.

    State/CSRF binding is enforced by the server-side authorization-request
    store (one-time consume keyed by the state hash); the PKCE ``code_verifier``
    is held server-side and never travels through the browser, so it is never
    accepted from the caller here.
    """
    if error:
        raise OAuthCallbackValidationError(
            f"PagerDuty OAuth callback returned an error: {error}"
        )
    if not code:
        raise OAuthCallbackValidationError("PagerDuty OAuth callback code is required")
    return code


async def exchange_code(
    config: PagerDutyOAuthConfig, *, code: str, code_verifier: str
) -> OAuthTokens:
    async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
        response = await client.post(
            config.token_url,
            data={
                "grant_type": "authorization_code",
                "client_id": config.client_id,
                "client_secret": config.client_secret or "",
                "redirect_uri": config.redirect_uri,
                "code": code,
                "code_verifier": code_verifier,
            },
        )
    response.raise_for_status()
    return _tokens(response.json())


async def refresh_tokens(
    config: PagerDutyOAuthConfig, refresh_token: str
) -> OAuthTokens:
    async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
        response = await client.post(
            config.token_url,
            data={
                "grant_type": "refresh_token",
                "client_id": config.client_id,
                "client_secret": config.client_secret or "",
                "refresh_token": refresh_token,
            },
        )
    response.raise_for_status()
    return _tokens(response.json())


async def client_credentials(
    config: PagerDutyOAuthConfig, *, scopes: set[str], subdomain: str, region: str
) -> OAuthTokens:
    async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
        response = await client.post(
            config.token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": config.client_id,
                "client_secret": config.client_secret or "",
                "scope": " ".join(sorted(scopes)),
                "subdomain": subdomain,
                "region": region,
            },
        )
    response.raise_for_status()
    return _tokens(response.json())


async def revoke_token(config: PagerDutyOAuthConfig, token: str) -> None:
    """Best-effort OAuth revocation request; callers own local credential removal."""
    async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
        response = await client.post(
            config.revoke_url,
            data={"token": token, "client_id": config.client_id},
        )
    response.raise_for_status()


def _tokens(payload: dict[str, object]) -> OAuthTokens:
    expires_in = payload.get("expires_in", 3600)
    seconds = int(expires_in) if isinstance(expires_in, int | str) else 3600
    return OAuthTokens(
        access_token=str(payload["access_token"]),
        refresh_token=str(payload["refresh_token"])
        if payload.get("refresh_token")
        else None,
        expires_at=datetime.now(UTC) + timedelta(seconds=seconds),
        granted_scopes=frozenset(str(payload.get("scope", "")).split()),
    )
