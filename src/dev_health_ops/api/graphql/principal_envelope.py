"""Effective-principal envelope issuance (CHAOS-4366 Wave 0, plan §3/§7 D1).

chris's ruling (2026-08-27, CHAOS-4379, now Done): ``query-api`` trusts a
short-lived, audience-bound, SIGNED envelope issued by the Python edge,
rather than independently re-deriving auth state from Postgres/Valkey.
Reproducing the FULL `graphql/authz.py` + `graphql/app.py` + `services/auth.py`
contract in Go before it has proven anything else was judged unnecessary
risk (plan §3).

This module is the ISSUER half, on the Python edge, where all of that
contract already lives and is already tested
(``AuthService.authenticate_access_token`` -- disabled-user, token-version
revocation; ``services/permissions.get_user_permissions`` -- impersonation-
aware permission set; ``services/licensing.resolve_org_tier`` -- tier
fallback). The Go VERIFIER half (in ``query-api``, using dev-health-go's
extracted JWKS verification mechanisms) is a separate deliverable, blocked
on the dev-health-go module (CHAOS-4377) landing its ``authverify``
primitives; :func:`build_envelope_jwks` below is this module's half of that
contract -- the public key material a JWKS-based Go verifier will fetch.

Claim schema is versioned (``v``) from day one: plan §7 open decision 1
requires the envelope to reproduce disabled/token-version/org-switch/
impersonation/tier-fallback semantics, and those semantics WILL evolve before
query-api ever verifies a real request. A verifier that doesn't check ``v``
would silently accept a schema it wasn't written against.

Deliberately SHORT-LIVED (default 60s, not the 60-minute user-facing access
token TTL): this envelope is minted fresh, per decision, immediately before
a request is (eventually) proxied/compared -- it is not a bearer credential
a client holds, so there is no reason to give it session-length life. A
stolen envelope is worthless within a minute.

Deliberately a SEPARATE signing key from ``AuthService``'s user-facing
HS256 access-token secret: this envelope crosses a language and process
boundary (Python edge -> Go query-api) via a JWKS-style public key, matching
the pattern ``acr/internal/auth`` already uses for its own JWKS/web-assertion
verification (plan §3) -- an asymmetric key lets query-api verify without
ever holding a Python-edge secret capable of forging a USER session token.
"""

from __future__ import annotations

import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey, RSAPublicKey
from jwt.algorithms import RSAAlgorithm

from dev_health_ops.api.services.auth import get_impersonation_context
from dev_health_ops.api.services.permissions import get_user_permissions
from dev_health_ops.telemetry_metrics import (
    build_counter,
    load_otel_meter,
    load_prometheus,
)

if TYPE_CHECKING:
    from dev_health_ops.api.services.auth import AuthenticatedUser
    from dev_health_ops.licensing.types import LicenseTier

logger = logging.getLogger(__name__)

__all__ = [
    "ENVELOPE_CLAIM_SCHEMA_VERSION",
    "ENVELOPE_ALGORITHM",
    "ENVELOPE_DEFAULT_TTL_SECONDS",
    "EffectivePrincipalEnvelopeClaims",
    "issue_effective_principal_envelope",
    "build_envelope_jwks",
    "EnvelopeSigningKeyError",
]

#: Bump whenever a claim is added, removed, or its meaning changes. A
#: verifier MUST reject an envelope whose ``v`` it was not written to handle
#: -- see the module docstring.
ENVELOPE_CLAIM_SCHEMA_VERSION = 1

ENVELOPE_ALGORITHM = "RS256"
ENVELOPE_DEFAULT_TTL_SECONDS = 60
ENVELOPE_ISSUER = os.getenv("GO_API_ENVELOPE_ISSUER", "dev-health-ops-edge")
ENVELOPE_AUDIENCE = os.getenv("GO_API_ENVELOPE_AUDIENCE", "query-api")
#: Stable identifier for the active signing key, carried in the JWT header
#: (``kid``) so a JWKS with multiple keys (mid-rotation) resolves correctly.
ENVELOPE_KEY_ID = os.getenv("GO_API_ENVELOPE_KEY_ID", "go-api-envelope-2026-08")

_prometheus: Any = load_prometheus()
_meter: Any = load_otel_meter(__name__)

_ENVELOPE_ISSUED_TOTAL = build_counter(
    "devhealth_go_api_envelope_issued_total",
    "Effective-principal envelopes issued by the Python edge, by outcome",
    ["outcome"],
    meter=_meter,
    prometheus=_prometheus,
)


class EnvelopeSigningKeyError(RuntimeError):
    """Raised when the envelope signing key is missing or malformed."""


def _get_envelope_signing_private_key_pem() -> str:
    pem = os.getenv("GO_API_ENVELOPE_PRIVATE_KEY")
    if not pem:
        raise EnvelopeSigningKeyError(
            "GO_API_ENVELOPE_PRIVATE_KEY is required and must be set in the "
            "environment (PEM-encoded RSA private key)."
        )
    return pem


def _load_private_key(pem: str) -> RSAPrivateKey:
    try:
        key = serialization.load_pem_private_key(pem.encode("utf-8"), password=None)
    except ValueError as exc:
        raise EnvelopeSigningKeyError(
            f"GO_API_ENVELOPE_PRIVATE_KEY is not a valid PEM private key: {exc}"
        ) from exc
    if not isinstance(key, RSAPrivateKey):
        raise EnvelopeSigningKeyError(
            "GO_API_ENVELOPE_PRIVATE_KEY must be an RSA private key "
            f"(RS256), got {type(key).__name__}"
        )
    return key


@dataclass(frozen=True, slots=True)
class EffectivePrincipalEnvelopeClaims:
    """The versioned claim schema. Field-for-field, this reproduces the
    ``graphql/authz.py`` + ``graphql/app.py`` + ``services/auth.py`` contract
    plan §3 requires: user identity, org, permissions, superuser state,
    active impersonation, licensed features/tier, token_version, and the
    disabled-user semantics (a disabled/deactivated user never reaches this
    function -- see :func:`issue_effective_principal_envelope`'s docstring --
    so there is no separate ``disabled`` claim; absence of a valid envelope
    IS the disabled signal, matching how ``authenticate_access_token``
    already treats a deactivated user as unauthenticated, not as
    authenticated-but-flagged).
    """

    v: int
    sub: str
    org_id: str
    role: str
    is_superuser: bool
    is_superuser_verified: bool
    permissions: list[str]
    token_version: int
    tier: str
    licensed_features: list[str]
    impersonated_by: str | None = None
    impersonation_active: bool = False
    iss: str = field(default=ENVELOPE_ISSUER)
    aud: str = field(default=ENVELOPE_AUDIENCE)


def issue_effective_principal_envelope(
    user: AuthenticatedUser,
    *,
    tier: LicenseTier,
    licensed_features: list[str],
    ttl_seconds: int = ENVELOPE_DEFAULT_TTL_SECONDS,
    audience: str = ENVELOPE_AUDIENCE,
) -> str:
    """Mint a signed, short-lived effective-principal envelope.

    Callers MUST pass a ``user`` that already survived
    ``AuthService.authenticate_access_token`` (DB-backed disabled-user and
    token-version-revocation checks) for THIS request -- this function does
    not re-check the database. That mirrors the existing contract: nothing
    downstream of ``authenticate_access_token`` re-validates disabled/token-
    version state either (see ``graphql/app.py``'s per-request auth flow).

    ``permissions`` and impersonation state are read via
    ``services.permissions.get_user_permissions``/``get_impersonation_context``,
    the same impersonation-aware logic every other authorization check in
    this codebase uses -- not reimplemented here.
    """
    if ttl_seconds <= 0:
        raise ValueError("ttl_seconds must be positive")

    try:
        private_key = _load_private_key(_get_envelope_signing_private_key_pem())
    except EnvelopeSigningKeyError:
        _ENVELOPE_ISSUED_TOTAL.labels(outcome="key_error").inc()
        raise

    impersonation = get_impersonation_context()
    impersonation_active = bool(impersonation is not None and impersonation.is_active)

    claims = EffectivePrincipalEnvelopeClaims(
        v=ENVELOPE_CLAIM_SCHEMA_VERSION,
        sub=user.user_id,
        org_id=user.org_id,
        role=user.role,
        is_superuser=user.is_superuser,
        is_superuser_verified=user.is_superuser_verified,
        permissions=sorted(get_user_permissions(user)),
        token_version=user.token_version or 0,
        tier=tier.value,
        licensed_features=sorted(licensed_features),
        impersonated_by=user.impersonated_by,
        impersonation_active=impersonation_active,
        aud=audience,
    )

    now = int(time.time())
    payload: dict[str, Any] = {
        "v": claims.v,
        "sub": claims.sub,
        "org_id": claims.org_id,
        "role": claims.role,
        "is_superuser": claims.is_superuser,
        "is_superuser_verified": claims.is_superuser_verified,
        "permissions": claims.permissions,
        "token_version": claims.token_version,
        "tier": claims.tier,
        "licensed_features": claims.licensed_features,
        "impersonated_by": claims.impersonated_by,
        "impersonation_active": claims.impersonation_active,
        "iss": claims.iss,
        "aud": claims.aud,
        "iat": now,
        "exp": now + ttl_seconds,
        "jti": str(uuid.uuid4()),
    }

    token = jwt.encode(
        payload,
        private_key,
        algorithm=ENVELOPE_ALGORITHM,
        headers={"kid": ENVELOPE_KEY_ID},
    )
    _ENVELOPE_ISSUED_TOTAL.labels(outcome="issued").inc()
    return token


def build_envelope_jwks() -> dict[str, Any]:
    """Return the JWKS document a Go verifier fetches to check the envelope
    signature. Contains only the PUBLIC key -- never the private key.
    """
    try:
        private_key = _load_private_key(_get_envelope_signing_private_key_pem())
    except EnvelopeSigningKeyError:
        _ENVELOPE_ISSUED_TOTAL.labels(outcome="key_error").inc()
        raise

    public_key: RSAPublicKey = private_key.public_key()
    jwk_json = RSAAlgorithm.to_jwk(public_key)
    import json

    jwk = json.loads(jwk_json)
    jwk["kid"] = ENVELOPE_KEY_ID
    jwk["use"] = "sig"
    jwk["alg"] = ENVELOPE_ALGORITHM
    return {"keys": [jwk]}
