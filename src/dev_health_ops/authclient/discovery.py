"""Python client for the ``discovery.v1`` wire contract.

Mirrors ``internal/auth/contracts/discovery.go``. The schema pins the shape of
the document; the two rules it cannot express live here, and *trusted_issuer*
is a required argument so a caller cannot skip them:

* the document's ``issuer`` must equal the issuer the verifier is configured to
  trust, byte for byte (OpenID Connect Discovery section 4.3);
* ``jwks_uri`` must equal ``issuer`` + ``/.well-known/jwks.json``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Final

from dev_health_ops.authclient.contracts import ContractError, validate

SURFACE: Final = "discovery.v1"
JWKS_PATH: Final = "/.well-known/jwks.json"
PLATFORM_TOKEN_KEY_USE: Final = "platform_token"


@dataclass(frozen=True, slots=True)
class Discovery:
    issuer: str
    jwks_uri: str
    id_token_signing_alg_values_supported: tuple[str, ...]
    key_use: str


def _refuse_duplicate_members(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Reject a repeated member: parsers keep the last, hiding the earlier value."""
    seen: set[str] = set()
    for key, _ in pairs:
        if key in seen:
            raise ContractError(
                f"{SURFACE}: duplicate object member {key!r}. Parsers keep the "
                "last value, so an earlier one would never reach validation"
            )
        seen.add(key)
    return dict(pairs)


def parse_discovery(raw: bytes | str, trusted_issuer: str) -> Discovery:
    """Validate a discovery document fetched for *trusted_issuer*."""
    try:
        document = json.loads(raw, object_pairs_hook=_refuse_duplicate_members)
    except json.JSONDecodeError as exc:
        raise ContractError(f"{SURFACE}: document is not valid JSON: {exc}") from exc
    validate(SURFACE, document)

    issuer: str = document["issuer"]
    jwks_uri: str = document["jwks_uri"]
    if issuer != trusted_issuer:
        raise ContractError(
            f"{SURFACE}: document issuer {issuer!r} is not the trusted issuer "
            f"{trusted_issuer!r}; refusing rather than following a jwks_uri that "
            "a different issuer published"
        )
    if jwks_uri != issuer + JWKS_PATH:
        raise ContractError(
            f"{SURFACE}: jwks_uri {jwks_uri!r} is not {issuer + JWKS_PATH!r}; "
            "the issuer disagrees with its own endpoint URL"
        )
    return Discovery(
        issuer=issuer,
        jwks_uri=jwks_uri,
        id_token_signing_alg_values_supported=tuple(
            document["id_token_signing_alg_values_supported"]
        ),
        key_use=document["dev_health_key_use"],
    )
