"""Parsers for ``session.v1``, ``refresh-request.v1``, ``refresh-response.v1`` and
``auth-outcome.v1`` (CHAOS-10155).

Each parser takes the RAW bytes so a duplicate object member can be refused
before the decode discards it, then validates against the schema, then applies
the rules a schema cannot state (integer token form, calendar day, UTC range,
cross-field time order). The Go twin is ``internal/auth/contracts/session.go``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Final

from dev_health_ops.authclient.contracts import (
    ContractError,
    parse_rfc3339,
    require_wire_int,
    violations,
)
from dev_health_ops.authclient.principal import INT64_MAX

SESSION: Final = "session.v1"
REFRESH_REQUEST: Final = "refresh-request.v1"
REFRESH_RESPONSE: Final = "refresh-response.v1"
AUTH_OUTCOME: Final = "auth-outcome.v1"

#: Mirror of the schema enum; ``test_auth_outcome_vocabulary_matches_the_schema``
#: keeps the two equal.
AUTH_OUTCOME_SUCCESS_REASONS: Final = frozenset({"accepted", "refresh_grace_replay"})
AUTH_OUTCOME_REASON_CODES: Final = frozenset(
    {
        "accepted", "refresh_grace_replay",
        "malformed", "bad_signature", "audience", "issuer", "expired", "not_yet_valid",
        "missing_sub", "missing_type", "type_mismatch", "refresh_unknown", "refresh_expired",
        "refresh_reused", "session_revoked", "session_expired", "principal_not_found",
        "principal_deactivated", "revision_stale", "invalid_credentials", "account_locked",
        "email_unverified", "email_collision", "rate_limited",
        "store_unavailable", "dependency_unavailable",
    }
)  # fmt: skip


def _refuse_duplicates_for(surface: str):
    def hook(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        seen: set[str] = set()
        for key, _ in pairs:
            if key in seen:
                raise ContractError(
                    f"{surface}: duplicate object member {key!r}. Parsers keep the "
                    "last value, so an earlier one would never reach validation"
                )
            seen.add(key)
        return dict(pairs)

    return hook


def _load(surface: str, raw: bytes | str) -> dict[str, Any]:
    if isinstance(raw, bytes):
        # json.loads(bytes) auto-detects UTF-16/32 and swallows a UTF-8 BOM;
        # Go refuses all of them, so the clients agree on plain UTF-8 only.
        try:
            raw = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ContractError(f"{surface}: document is not UTF-8: {exc}") from exc
    try:
        document = json.loads(raw, object_pairs_hook=_refuse_duplicates_for(surface))
    except json.JSONDecodeError as exc:
        raise ContractError(f"{surface}: document is not valid JSON: {exc}") from exc
    found = violations(surface, document)
    if found:
        # The validators' messages repeat the rejected value, which on the
        # refresh surfaces is a credential: report location and keyword only.
        joined = "; ".join(f"{v.instance_location} violates {v.keyword}" for v in found)
        raise ContractError(f"{surface}: {joined}")
    return document


@dataclass(frozen=True, slots=True)
class Session:
    session_id: str
    principal_id: str
    revision: int
    auth_time: datetime
    amr: tuple[str, ...]
    created_at: datetime
    expires_at: datetime
    revoked_at: datetime | None

    def is_live(self, now: datetime) -> bool:
        return self.revoked_at is None and now < self.expires_at


def parse_session(raw: bytes | str) -> Session:
    document = _load(SESSION, raw)
    revision = require_wire_int(document, "revision", SESSION)
    assert revision is not None  # required by the schema
    if revision > INT64_MAX:
        raise ContractError(
            f"{SESSION}: /revision is outside the int64 range Go can represent"
        )
    auth_time = parse_rfc3339(document["auth_time"], SESSION, "auth_time")
    created_at = parse_rfc3339(document["created_at"], SESSION, "created_at")
    expires_at = parse_rfc3339(document["expires_at"], SESSION, "expires_at")
    revoked_raw = document["revoked_at"]
    revoked_at = (
        None
        if revoked_raw is None
        else parse_rfc3339(revoked_raw, SESSION, "revoked_at")
    )
    if revoked_at is not None and revoked_at < created_at:
        raise ContractError(f"{SESSION}: revoked_at precedes created_at")
    if expires_at <= created_at:
        raise ContractError(f"{SESSION}: expires_at is not after created_at")
    if auth_time > created_at:
        raise ContractError(f"{SESSION}: auth_time is after created_at")
    return Session(
        session_id=document["session_id"],
        principal_id=document["principal_id"],
        revision=revision,
        auth_time=auth_time,
        amr=tuple(document["amr"]),
        created_at=created_at,
        expires_at=expires_at,
        revoked_at=revoked_at,
    )


@dataclass(frozen=True, slots=True)
class RefreshRequest:
    refresh_credential: str = field(repr=False)


def parse_refresh_request(raw: bytes | str) -> RefreshRequest:
    document = _load(REFRESH_REQUEST, raw)
    return RefreshRequest(refresh_credential=document["refresh_credential"])


@dataclass(frozen=True, slots=True)
class RefreshResponse:
    access_token: str = field(repr=False)
    token_type: str
    expires_in: int
    refresh_credential: str = field(repr=False)
    refresh_expires_in: int
    session_id: str


def parse_refresh_response(raw: bytes | str) -> RefreshResponse:
    document = _load(REFRESH_RESPONSE, raw)
    expires_in = require_wire_int(document, "expires_in", REFRESH_RESPONSE)
    refresh_expires_in = require_wire_int(
        document, "refresh_expires_in", REFRESH_RESPONSE
    )
    assert expires_in is not None and refresh_expires_in is not None
    return RefreshResponse(
        access_token=document["access_token"],
        token_type=document["token_type"],
        expires_in=expires_in,
        refresh_credential=document["refresh_credential"],
        refresh_expires_in=refresh_expires_in,
        session_id=document["session_id"],
    )


@dataclass(frozen=True, slots=True)
class AuthOutcome:
    operation: str
    result: str
    reason_code: str
    request_id: str
    occurred_at: datetime
    session_id: str | None


def parse_auth_outcome(raw: bytes | str) -> AuthOutcome:
    document = _load(AUTH_OUTCOME, raw)
    return AuthOutcome(
        operation=document["operation"],
        result=document["result"],
        reason_code=document["reason_code"],
        request_id=document["request_id"],
        occurred_at=parse_rfc3339(document["occurred_at"], AUTH_OUTCOME, "occurred_at"),
        session_id=document.get("session_id"),
    )
