"""Parsers for ``session.v1``, ``refresh-request.v1``, ``refresh-response.v1`` and
``auth-outcome.v1`` (CHAOS-10155).

Each parser takes the RAW bytes so a duplicate object member can be refused
before the decode discards it, then validates against the schema, then applies
the rules a schema cannot state (integer token form, calendar day, UTC range,
cross-field time order). The Go twin is ``internal/auth/contracts/session.go``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Final

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
    raise NotImplementedError


@dataclass(frozen=True, slots=True)
class RefreshRequest:
    refresh_credential: str = field(repr=False)


def parse_refresh_request(raw: bytes | str) -> RefreshRequest:
    raise NotImplementedError


@dataclass(frozen=True, slots=True)
class RefreshResponse:
    access_token: str = field(repr=False)
    token_type: str
    expires_in: int
    refresh_credential: str = field(repr=False)
    refresh_expires_in: int
    session_id: str


def parse_refresh_response(raw: bytes | str) -> RefreshResponse:
    raise NotImplementedError


@dataclass(frozen=True, slots=True)
class AuthOutcome:
    operation: str
    result: str
    reason_code: str
    request_id: str
    occurred_at: datetime
    session_id: str | None


def parse_auth_outcome(raw: bytes | str) -> AuthOutcome:
    raise NotImplementedError
