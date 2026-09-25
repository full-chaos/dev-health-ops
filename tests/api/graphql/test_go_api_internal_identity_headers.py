"""CHAOS-6144 P3: the internal identity headers the Python edge sends to
query-api's internal listener state the SAME identity the signed
effective-principal envelope carries, from the SAME derivation
(``effective_principal_identity``), for every principal shape."""

from __future__ import annotations

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

from dev_health_ops.api.graphql import go_api_dispatcher, principal_envelope
from dev_health_ops.api.services.auth import (
    AuthenticatedUser,
    _impersonation_ctx,
    set_impersonation_context,
)
from dev_health_ops.licensing.types import LicenseTier


@pytest.fixture
def signing_key(monkeypatch: pytest.MonkeyPatch) -> None:
    pem = (
        Ed25519PrivateKey.generate()
        .private_bytes(
            encoding=Encoding.PEM,
            format=PrivateFormat.PKCS8,
            encryption_algorithm=NoEncryption(),
        )
        .decode("utf-8")
    )
    monkeypatch.setenv("GO_API_ENVELOPE_PRIVATE_KEY", pem)


def _user(**overrides: object) -> AuthenticatedUser:
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


def _claims(user: AuthenticatedUser) -> dict:
    token = principal_envelope.issue_effective_principal_envelope(
        user, tier=LicenseTier.TEAM, licensed_features=[]
    )
    jwks = principal_envelope.build_envelope_jwks()
    header = jwt.get_unverified_header(token)
    (jwk,) = [k for k in jwks["keys"] if k["kid"] == header["kid"]]
    key = OKPAlgorithm.from_jwk(jwk)
    assert isinstance(key, Ed25519PublicKey)
    return jwt.decode(
        token,
        key=key,
        algorithms=[principal_envelope.ENVELOPE_ALGORITHM],
        audience=principal_envelope.ENVELOPE_AUDIENCE,
        issuer=principal_envelope.ENVELOPE_ISSUER,
    )


def _flag(value: bool) -> str:
    return "true" if value else "false"


@pytest.mark.parametrize("impersonating", [False, True])
@pytest.mark.parametrize("superuser", [False, True])
@pytest.mark.parametrize("role", ["viewer", "admin", "owner"])
def test_headers_equal_the_envelope_claims_for_every_principal_shape(
    signing_key: None, role: str, superuser: bool, impersonating: bool
) -> None:
    user = _user(role=role, is_superuser=superuser, org_id="org-real")
    token_ctx = None
    if impersonating:
        token_ctx = set_impersonation_context(
            target_user_id="33333333-3333-4333-8333-333333333333",
            target_org_id="org-target",
            target_role="viewer",
            real_user_id=user.user_id,
        )
    try:
        headers = go_api_dispatcher._internal_identity_headers(user)
        claims = _claims(user)
    finally:
        if token_ctx is not None:
            _impersonation_ctx.reset(token_ctx)

    assert headers == {
        "X-DH-Internal-Org-Id": claims["org_id"],
        "X-DH-Internal-Role": claims["role"],
        "X-DH-Internal-Superuser": _flag(claims["is_superuser"]),
        "X-DH-Internal-Impersonation-Active": _flag(claims["impersonation_active"]),
    }
    if impersonating:
        # The TARGET's identity, never the real admin's (planted-defect shape:
        # real and target differ on every field).
        assert headers["X-DH-Internal-Org-Id"] == "org-target"
        assert headers["X-DH-Internal-Role"] == "viewer"
        assert headers["X-DH-Internal-Impersonation-Active"] == "true"


def test_the_flags_are_exactly_true_or_false_never_python_spelling() -> None:
    """query-api refuses a flag that is not exactly ``true`` or ``false``:
    ``str(True)`` would be ``True``."""
    for superuser in (True, False):
        headers = go_api_dispatcher._internal_identity_headers(
            _user(is_superuser=superuser)
        )
        assert headers["X-DH-Internal-Superuser"] == _flag(superuser)
        assert set(headers.values()) - {"org-1", "admin"} <= {"true", "false"}


def test_the_header_names_are_the_query_api_contract() -> None:
    assert set(go_api_dispatcher._internal_identity_headers(_user())) == {
        "X-DH-Internal-Org-Id",
        "X-DH-Internal-Role",
        "X-DH-Internal-Superuser",
        "X-DH-Internal-Impersonation-Active",
    }


def test_the_header_names_equal_the_go_constants_query_api_reads() -> None:
    """Cross-language drift guard: the names are a contract with query-api's
    ``internalidentity`` package (which reads them) and the ingress (which strips
    them); read the Go source, not a copy of it."""
    import re
    from pathlib import Path

    source = (
        Path(__file__).resolve().parents[3]
        / "internal"
        / "queryapi"
        / "internalidentity"
        / "internalidentity.go"
    ).read_text()
    go_names = set(re.findall(r'Header\w+\s*=\s*"(X-DH-Internal-[A-Za-z-]+)"', source))
    assert len(go_names) == 4, go_names
    assert set(go_dispatcher_headers()) == go_names


def go_dispatcher_headers() -> dict[str, str]:
    return go_api_dispatcher._internal_identity_headers(_user())
