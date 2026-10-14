from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from dev_health_ops.models.internal_service_credential import (
    InternalServiceCredential,
)

NOW = datetime(2026, 10, 14, 12, 0, tzinfo=timezone.utc)


def _credential(**fields) -> InternalServiceCredential:
    credential, _token = InternalServiceCredential.issue(
        service_name="worker-operator",
        scopes=["workers:operate"],
        created_by_user_id=None,
    )
    for key, value in fields.items():
        setattr(credential, key, value)
    return credential


@pytest.mark.parametrize(
    ("fields", "expected"),
    [
        ({}, True),
        ({"expires_at": NOW + timedelta(hours=1)}, True),
        ({"expires_at": NOW}, True),
        ({"expires_at": NOW - timedelta(seconds=1)}, False),
        ({"revoked_at": NOW - timedelta(seconds=1)}, False),
        (
            {
                "revoked_at": NOW - timedelta(seconds=1),
                "expires_at": NOW + timedelta(hours=1),
            },
            False,
        ),
    ],
)
def test_public_metadata_serves_is_valid(fields, expected):
    metadata = _credential(**fields).public_metadata(NOW)
    assert metadata["is_valid"] is expected
    assert "token_hash" not in metadata
    assert "token" not in metadata


def test_public_metadata_defaults_now_to_the_clock():
    expired = _credential(expires_at=datetime(2000, 1, 1, tzinfo=timezone.utc))
    assert expired.public_metadata()["is_valid"] is False
