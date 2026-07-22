from __future__ import annotations

from dataclasses import asdict
from functools import partial

import anyio
import httpx
import pytest

from dev_health_ops.credentials.types import CredentialSource, PagerDutyCredentials
from dev_health_ops.providers.pagerduty.credential_validation import (
    PagerDutyCredentialValidationError,
    validate_pagerduty_credential,
)


def test_validate_api_token_uses_one_least_privileged_service_read() -> None:
    # Given: an API-token credential and a transport that records its request.
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "services": [
                    {
                        "account": {
                            "id": "acme",
                            "name": "Acme Operations",
                            "subdomain": "acme",
                        }
                    }
                ]
            },
        )

    credential = PagerDutyCredentials(
        source=CredentialSource.DATABASE,
        auth_mode="api_token",
        api_token="api-token",
    )

    # When: the credential is live-validated.
    validated = anyio.run(
        partial(
            validate_pagerduty_credential,
            credential,
            required_scopes=frozenset({"Services.read"}),
            transport=httpx.MockTransport(handler),
        )
    )

    # Then: exactly the bounded services read validates the token.
    assert validated.auth_mode == "api_token"
    assert len(requests) == 1
    assert requests[0].url.path == "/services"
    assert requests[0].url.params == httpx.QueryParams("limit=1")
    assert requests[0].headers["Authorization"] == "Token token=api-token"


def test_validate_oauth_derives_canonical_account_identity_from_service_response() -> (
    None
):
    # Given: caller metadata that disagrees with the provider's authenticated service.
    credential = PagerDutyCredentials(
        source=CredentialSource.DATABASE,
        auth_mode="oauth",
        access_token="oauth-token",
        granted_scopes=("Services.read",),
        subdomain="caller-controlled-label",
    )

    # When: PagerDuty returns an account identity attached to a readable service.
    validated = anyio.run(
        partial(
            validate_pagerduty_credential,
            credential,
            required_scopes=frozenset({"Services.read"}),
            transport=httpx.MockTransport(
                lambda _: httpx.Response(
                    200,
                    json={
                        "services": [
                            {
                                "id": "P123",
                                "account": {
                                    "id": "acme",
                                    "name": "Acme Operations",
                                    "subdomain": "acme",
                                },
                            }
                        ]
                    },
                )
            ),
        )
    )

    # Then: only the live PagerDuty identity is returned for persistence.
    assert validated.account_id == "acme"
    assert validated.account_display == "Acme Operations"
    assert validated.subdomain == "acme"


def test_validate_rejects_empty_service_response_without_account_identity() -> None:
    # Given: an OAuth credential with valid granted scopes.
    credential = PagerDutyCredentials(
        source=CredentialSource.DATABASE,
        auth_mode="oauth",
        access_token="oauth-token",
        granted_scopes=("Services.read",),
    )

    # When: the live capability check contains no service to prove its account.
    with pytest.raises(PagerDutyCredentialValidationError) as error:
        anyio.run(
            partial(
                validate_pagerduty_credential,
                credential,
                required_scopes=frozenset({"Services.read"}),
                transport=httpx.MockTransport(
                    lambda _: httpx.Response(200, json={"services": []})
                ),
            )
        )

    # Then: the candidate cannot be connected without a verified account identity.
    assert error.value.code == "missing_account_identity"


def test_validate_oauth_rejects_missing_required_scopes_without_request() -> None:
    # Given: an OAuth credential whose persisted grant lacks a selected dataset scope.
    credential = PagerDutyCredentials(
        source=CredentialSource.DATABASE,
        auth_mode="oauth",
        access_token="oauth-token",
        granted_scopes=("Users.read",),
    )

    # When: validation is requested for the operational scope set.
    with pytest.raises(PagerDutyCredentialValidationError) as error:
        anyio.run(
            partial(
                validate_pagerduty_credential,
                credential,
                required_scopes=frozenset({"Services.read"}),
                transport=httpx.MockTransport(
                    lambda _: (_ for _ in ()).throw(AssertionError("must not request"))
                ),
            ),
        )

    # Then: the typed scope failure stops before the provider call.
    assert error.value.code == "missing_required_scopes"


def test_validate_client_credentials_exchanges_then_reads_without_mutation() -> None:
    # Given: a client-credentials descriptor and an immutable pre-validation snapshot.
    requests: list[httpx.Request] = []
    credential = PagerDutyCredentials(
        source=CredentialSource.DATABASE,
        auth_mode="client_credentials",
        client_id="client-id",
        client_secret="client-secret",
        subdomain="acme",
    )
    before = asdict(credential)

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/oauth/token":
            return httpx.Response(
                200,
                json={
                    "access_token": "machine-token",
                    "expires_in": 3600,
                    "scope": "Services.read Users.read",
                },
            )
        return httpx.Response(
            200,
            json={
                "services": [
                    {
                        "account": {
                            "id": "acme",
                            "name": "Acme Operations",
                            "subdomain": "acme",
                        }
                    }
                ]
            },
        )

    # When: validation obtains a machine token and validates its read access.
    validated = anyio.run(
        partial(
            validate_pagerduty_credential,
            credential,
            required_scopes=frozenset({"Services.read", "Users.read"}),
            transport=httpx.MockTransport(handler),
        )
    )

    # Then: both requests use the least-privileged read and no descriptor state changed.
    assert validated.access_token == "machine-token"
    assert [request.url.path for request in requests] == ["/oauth/token", "/services"]
    assert requests[1].url.params == httpx.QueryParams("limit=1")
    assert asdict(credential) == before
