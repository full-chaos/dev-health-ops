from __future__ import annotations

from dataclasses import dataclass

from dev_health_ops.credentials.types import PagerDutyCredentials
from dev_health_ops.providers.pagerduty.auth import (
    ApiTokenAuth,
    OAuthBearerAuth,
    PagerDutyAuth,
)


@dataclass(frozen=True, slots=True)
class PagerDutyWebhookWorkerContext:
    org_id: str
    binding_id: str
    provider_instance_id: str
    credential_id: str


@dataclass(frozen=True, slots=True)
class PagerDutyWebhookAuth:
    auth: PagerDutyAuth
    region: str


def build_pagerduty_webhook_auth(
    credentials: PagerDutyCredentials,
) -> PagerDutyWebhookAuth:
    match credentials.auth_mode:
        case "api_token":
            api_token = credentials.api_token
            if not api_token:
                raise RuntimeError("pagerduty webhook credential has no API token")
            return PagerDutyWebhookAuth(
                auth=ApiTokenAuth(api_token), region=credentials.region
            )
        case "oauth" | "client_credentials":
            access_token = credentials.access_token
            if not access_token:
                raise RuntimeError("pagerduty webhook credential has no access token")
            return PagerDutyWebhookAuth(
                auth=OAuthBearerAuth(access_token), region=credentials.region
            )
        case _:
            raise RuntimeError(
                "pagerduty webhook credential has an unsupported auth mode"
            )
