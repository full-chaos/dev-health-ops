"""Unit tests for the customer-push ingest-auth models (CHAOS-2696)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from dev_health_ops.models.ingest_auth import (
    TOKEN_PREFIX,
    IngestSource,
    IngestSourceMode,
    IngestToken,
    hash_ingest_token,
)
from dev_health_ops.models.ingest_auth import generate_ingest_token as _generate_token

NOW = datetime(2026, 7, 1, tzinfo=timezone.utc)


def test_generate_ingest_token_has_expected_prefix_and_length():
    token = _generate_token()
    assert token.startswith(TOKEN_PREFIX)
    # secrets.token_urlsafe(32) -> 43 chars, plus the "fcpush_" prefix.
    assert len(token) == len(TOKEN_PREFIX) + 43


def test_generate_ingest_token_is_random():
    assert _generate_token() != _generate_token()


def test_hash_ingest_token_round_trip_is_deterministic():
    token = _generate_token()
    assert hash_ingest_token(token) == hash_ingest_token(token)


def test_hash_ingest_token_differs_for_different_tokens():
    assert hash_ingest_token(_generate_token()) != hash_ingest_token(_generate_token())


def test_hash_ingest_token_never_contains_the_raw_token():
    token = _generate_token()
    digest = hash_ingest_token(token)
    assert token not in digest
    assert len(digest) == 64  # sha256 hex digest


@pytest.mark.parametrize(
    "revoked_at, expires_at, expected",
    [
        (None, None, True),
        (None, NOW + timedelta(hours=1), True),
        (None, NOW - timedelta(hours=1), False),
        (NOW - timedelta(minutes=1), None, False),
        (NOW - timedelta(minutes=1), NOW + timedelta(hours=1), False),
        (NOW - timedelta(minutes=1), NOW - timedelta(hours=1), False),
    ],
)
def test_is_valid_matrix(revoked_at, expires_at, expected):
    token = IngestToken(
        org_id="org-1",
        name="t",
        token_hash="hash",
        token_prefix="fcpush_abcd",
        scopes=["schema:read"],
        revoked_at=revoked_at,
        expires_at=expires_at,
    )
    assert token.is_valid(NOW) is expected


@pytest.mark.parametrize(
    "mode, enabled, expected",
    [
        (IngestSourceMode.CUSTOMER_PUSH.value, True, True),
        (IngestSourceMode.CUSTOMER_PUSH.value, False, False),
        (IngestSourceMode.FULLCHAOS_SYNC.value, True, False),
        (IngestSourceMode.DISABLED.value, True, False),
        (IngestSourceMode.DISABLED.value, False, False),
    ],
)
def test_is_write_eligible_matrix(mode, enabled, expected):
    source = IngestSource(
        org_id="org-1",
        system="github",
        instance="acme/api",
        mode=mode,
        enabled=enabled,
    )
    assert source.is_write_eligible() is expected
