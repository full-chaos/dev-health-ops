"""Tests for the effective-principal envelope issuer (CHAOS-4366 Wave 0,
plan §3/§7 D1 -- signed short-lived envelope, chris's ruling on CHAOS-4379).

Verifies the envelope end-to-end the way a Go verifier eventually will:
mint with the private key, fetch the JWKS, reconstruct the public key from
it, and check the signature -- not just "encode didn't raise".

Ed25519/EdDSA, not RS256 (reconciled 2026-08-27 per CHAOS-4377 -- see
principal_envelope's module docstring): matches dev-health-go's
authverify.Ed25519JWKSVerifier, which is Ed25519-only.
"""

from __future__ import annotations

import time

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
)
from jwt.algorithms import OKPAlgorithm

from dev_health_ops.api.graphql import principal_envelope
from dev_health_ops.api.services.auth import (
    AuthenticatedUser,
    set_impersonation_context,
)
from dev_health_ops.licensing.types import LicenseTier


def _generate_test_key_pem() -> str:
    key = Ed25519PrivateKey.generate()
    return key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    ).decode("utf-8")


@pytest.fixture
def signing_key(monkeypatch: pytest.MonkeyPatch) -> str:
    pem = _generate_test_key_pem()
    monkeypatch.setenv("GO_API_ENVELOPE_PRIVATE_KEY", pem)
    return pem


def _sample_user(**overrides: object) -> AuthenticatedUser:
    defaults: dict[str, object] = dict(
        user_id="11111111-1111-4111-8111-111111111111",
        email="dev@example.com",
        org_id="org-1",
        role="admin",
        is_superuser=False,
        is_superuser_verified=False,
        token_version=3,
    )
    defaults.update(overrides)
    return AuthenticatedUser(**defaults)  # type: ignore[arg-type]


def _public_key_from_jwks(token: str, jwks: dict) -> Ed25519PublicKey:
    header = jwt.get_unverified_header(token)
    matching = [k for k in jwks["keys"] if k["kid"] == header["kid"]]
    assert matching, f"no JWKS key matches kid={header['kid']!r}"
    key = OKPAlgorithm.from_jwk(matching[0])
    assert isinstance(key, Ed25519PublicKey)
    return key


def _verify_with_jwks(token: str, jwks: dict) -> dict:
    return jwt.decode(
        token,
        key=_public_key_from_jwks(token, jwks),
        algorithms=[principal_envelope.ENVELOPE_ALGORITHM],
        audience=principal_envelope.ENVELOPE_AUDIENCE,
        issuer=principal_envelope.ENVELOPE_ISSUER,
    )


def test_issued_envelope_verifies_against_its_own_jwks(signing_key: str) -> None:
    user = _sample_user()
    token = principal_envelope.issue_effective_principal_envelope(
        user,
        tier=LicenseTier.TEAM,
        licensed_features=["ai_review", "work_graph"],
    )
    jwks = principal_envelope.build_envelope_jwks()

    claims = _verify_with_jwks(token, jwks)
    assert claims["v"] == principal_envelope.ENVELOPE_CLAIM_SCHEMA_VERSION
    assert claims["sub"] == user.user_id
    assert claims["org_id"] == "org-1"
    assert claims["role"] == "admin"
    assert claims["is_superuser"] is False
    assert claims["token_version"] == 3
    assert claims["tier"] == "team"
    assert claims["licensed_features"] == ["ai_review", "work_graph"]
    assert claims["impersonation_active"] is False
    assert claims["impersonated_by"] is None
    # admin role permissions, cumulative through viewer/member/admin.
    assert "org:write" in claims["permissions"]
    assert "analytics:read" in claims["permissions"]


def test_envelope_is_short_lived_by_default(signing_key: str) -> None:
    user = _sample_user()
    before = int(time.time())
    token = principal_envelope.issue_effective_principal_envelope(
        user, tier=LicenseTier.COMMUNITY, licensed_features=[]
    )
    jwks = principal_envelope.build_envelope_jwks()
    claims = _verify_with_jwks(token, jwks)

    ttl = claims["exp"] - claims["iat"]
    assert ttl == principal_envelope.ENVELOPE_DEFAULT_TTL_SECONDS
    assert claims["iat"] >= before
    # Far shorter than the 60-minute user-facing access token TTL.
    assert ttl <= 300


def test_envelope_expires_and_is_rejected_after_ttl(signing_key: str) -> None:
    user = _sample_user()
    token = principal_envelope.issue_effective_principal_envelope(
        user, tier=LicenseTier.COMMUNITY, licensed_features=[], ttl_seconds=1
    )
    jwks = principal_envelope.build_envelope_jwks()

    time.sleep(2)
    with pytest.raises(jwt.ExpiredSignatureError):
        jwt.decode(
            token,
            key=_public_key_from_jwks(token, jwks),
            algorithms=[principal_envelope.ENVELOPE_ALGORITHM],
            audience=principal_envelope.ENVELOPE_AUDIENCE,
            issuer=principal_envelope.ENVELOPE_ISSUER,
        )


def test_envelope_rejects_wrong_audience(signing_key: str) -> None:
    """A verifier pinned to aud='query-api' must reject an envelope minted
    for a different audience -- audience-binding is the whole point."""
    user = _sample_user()
    token = principal_envelope.issue_effective_principal_envelope(
        user,
        tier=LicenseTier.COMMUNITY,
        licensed_features=[],
        audience="some-other-service",
    )
    jwks = principal_envelope.build_envelope_jwks()

    with pytest.raises(jwt.InvalidAudienceError):
        jwt.decode(
            token,
            key=_public_key_from_jwks(token, jwks),
            algorithms=[principal_envelope.ENVELOPE_ALGORITHM],
            audience=principal_envelope.ENVELOPE_AUDIENCE,
            issuer=principal_envelope.ENVELOPE_ISSUER,
        )


def test_envelope_reflects_active_impersonation(signing_key: str) -> None:
    """Every identity claim (sub/org_id/role) must be the TARGET's, not the
    real admin's -- planted-defect shape: real and target differ on every
    field, so a claim that silently fell back to the real admin's value
    (the bug this guards) cannot hide behind values that coincide."""
    real_admin_id = "22222222-2222-4222-8222-222222222222"
    user = _sample_user(
        user_id=real_admin_id,
        org_id="org-real-admin",
        role="admin",
        impersonated_by=None,
    )
    token_ctx = set_impersonation_context(
        target_user_id="33333333-3333-4333-8333-333333333333",
        target_org_id="org-target",
        target_role="viewer",
        real_user_id=real_admin_id,
    )
    try:
        token = principal_envelope.issue_effective_principal_envelope(
            user, tier=LicenseTier.COMMUNITY, licensed_features=[]
        )
    finally:
        from dev_health_ops.api.services.auth import _impersonation_ctx

        _impersonation_ctx.reset(token_ctx)

    jwks = principal_envelope.build_envelope_jwks()
    claims = _verify_with_jwks(token, jwks)

    assert claims["impersonation_active"] is True
    assert claims["impersonated_by"] == real_admin_id
    # The envelope's identity is the TARGET being impersonated, not the real
    # admin -- a Go consumer authorizes/scopes the request by these claims.
    assert claims["sub"] == "33333333-3333-4333-8333-333333333333"
    assert claims["org_id"] == "org-target"
    assert claims["role"] == "viewer"
    # Impersonating: permissions come from the TARGET role (viewer), not the
    # underlying admin user's own role -- matches services.permissions.
    assert "org:write" not in claims["permissions"]
    assert "analytics:read" in claims["permissions"]


def test_missing_signing_key_raises_and_counts_key_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GO_API_ENVELOPE_PRIVATE_KEY", raising=False)
    user = _sample_user()
    with pytest.raises(principal_envelope.EnvelopeSigningKeyError):
        principal_envelope.issue_effective_principal_envelope(
            user, tier=LicenseTier.COMMUNITY, licensed_features=[]
        )


def test_jwks_never_contains_private_key_material(signing_key: str) -> None:
    jwks = principal_envelope.build_envelope_jwks()
    serialized = str(jwks)
    assert "PRIVATE KEY" not in serialized
    assert "-----BEGIN" not in serialized
    # OKP JWK private-key-only field must not be present.
    assert "d" not in jwks["keys"][0]
    assert jwks["keys"][0]["kty"] == "OKP"
    assert jwks["keys"][0]["crv"] == "Ed25519"


def test_ttl_must_be_positive(signing_key: str) -> None:
    user = _sample_user()
    with pytest.raises(ValueError, match="ttl_seconds must be positive"):
        principal_envelope.issue_effective_principal_envelope(
            user, tier=LicenseTier.COMMUNITY, licensed_features=[], ttl_seconds=0
        )
