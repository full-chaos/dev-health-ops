"""Safe-scope credential fingerprinting for run-auth freezing (CHAOS-2755).

A sync run's auth is resolved once at planning and stamped onto the ``SyncRun``
row (``credential_id`` + ``credential_fingerprint`` + ``auth_source``). Every
later phase — reference discovery, BudgetGuard, unit execution — reads the
run-stamped credential instead of re-resolving the mutable
``Integration.credential_id``, so a mid-run credential edit can no longer produce
a mixed-auth run.

The stamped ``credential_fingerprint`` is a **content witness**: it detects an
*in-place secret edit* (same credential id, rotated secret bytes) while never
persisting raw secret material. It is deliberately a NEW, independent helper —
it does NOT reuse the per-provider ``providers/*/budget.py`` fingerprints, whose
output feeds ``BudgetBucketKey`` identity (budget-bucket keying is owned by the
reservation machinery and must not change). This module derives the same
safe-scope *approach* but is scoped only to run-auth stamping/verification.

Safety contract:
  * Secret-bearing fields (tokens, API keys, private keys) are only ever
    SHA-256 hashed into ``<field>_sha256`` markers — never stored verbatim.
  * The returned fingerprint is a single SHA-256 hex digest of the safe scope,
    so the persisted column carries no plaintext identifier or secret byte.
  * Never persist the full-payload secret hash used by the runtime cache
    (``workers/sync_bootstrap.py`` ``_credential_fingerprint``) — that hashes
    the *entire* decrypted payload and is a per-process cache key, not a durable
    witness.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping

__all__ = [
    "AUTH_SOURCE_ENVIRONMENT",
    "AUTH_SOURCE_INTEGRATION_CREDENTIAL",
    "credential_fingerprint",
    "safe_credential_scope",
]

# ``SyncRun.auth_source`` discriminator values. A NULL ``auth_source`` marks a
# legacy (pre-migration) or in-flight-at-deploy run that was never stamped; such
# runs fall back to the mutable ``Integration.credential_id`` resolution path.
AUTH_SOURCE_INTEGRATION_CREDENTIAL = "integration_credential"
AUTH_SOURCE_ENVIRONMENT = "environment"

# Non-secret identifiers that scope a credential to a host/tenant/app identity.
# Persisting them (indirectly, via the final digest) is safe — none is a secret.
_IDENTIFIER_KEYS: tuple[str, ...] = (
    "app_id",
    "installation_id",
    "email",
    "cloud_id",
    "cloudId",
    "client_id",
    "clientId",
    "user_id",
    "username",
    "group_id",
    "project_id",
    "project_key",
    "environment",
    "schema_version",
    "organization_id",
    "workspace_id",
    "team_id",
    "oauth_binding_id",
)

# Secret-bearing fields. Each present value is SHA-256 hashed into a
# ``<key>_sha256`` marker so an in-place secret edit changes the fingerprint,
# while the raw secret is never included in the scope.
_SECRET_KEYS: tuple[str, ...] = (
    "token",
    "private_token",
    "access_token",
    "accessToken",
    "refresh_token",
    "refreshToken",
    "api_token",
    "apiToken",
    "api_key",
    "apiKey",
    "private_key",
    "privateKey",
    "client_secret",
    "clientSecret",
)


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _normalize_base_url(value: str) -> str:
    return value.strip().rstrip("/")


def _fallback_scope(
    *, credential_id: str | None, integration_id: str
) -> dict[str, object]:
    return {
        "credential_id": credential_id or "env",
        "integration_id": integration_id,
    }


def safe_credential_scope(
    credentials: object,
    *,
    credential_id: str | None,
    integration_id: str,
) -> dict[str, object]:
    """Return a provider-agnostic, secret-free scope of a decrypted credential.

    Identifiers are copied verbatim; secret-bearing fields are SHA-256 hashed.
    Falls back to ``{credential_id, integration_id}`` when the credentials are
    not a mapping or expose no recognizable field, so the witness is always
    deterministic and non-empty.
    """
    if not isinstance(credentials, Mapping):
        return _fallback_scope(
            credential_id=credential_id, integration_id=integration_id
        )

    scope: dict[str, object] = {}
    for key in _IDENTIFIER_KEYS:
        value = credentials.get(key)
        if value is not None:
            scope[key] = value

    base_url = credentials.get("base_url") or credentials.get("baseUrl")
    if base_url is not None:
        scope["base_url"] = _normalize_base_url(str(base_url))

    for key in _SECRET_KEYS:
        value = credentials.get(key)
        if value:
            scope[f"{key}_sha256"] = _sha256(str(value))

    if not scope:
        return _fallback_scope(
            credential_id=credential_id, integration_id=integration_id
        )
    return scope


def credential_fingerprint(
    credentials: object,
    *,
    credential_id: str | None,
    integration_id: str,
) -> str:
    """Return a stable SHA-256 hex digest of the credential's safe scope.

    The same decrypted credential content yields the same digest at plan time
    and at every later read; a rotated secret (same credential id) yields a
    different digest, which is how the run-auth freeze detects an in-place edit.
    """
    scope = safe_credential_scope(
        credentials,
        credential_id=credential_id,
        integration_id=integration_id,
    )
    payload = json.dumps(scope, sort_keys=True, default=str, separators=(",", ":"))
    return _sha256(payload)
