"""Verify SSO secrets are encrypted at rest using Fernet."""
from __future__ import annotations

import os

os.environ.setdefault("SETTINGS_ENCRYPTION_KEY", "test-encryption-key")

from dev_health_ops.api.services.settings import decrypt_value, encrypt_value


def test_encrypt_decrypt_round_trip():
    secret = "oidc-client-secret-value"
    encrypted = encrypt_value(secret)
    assert encrypted != secret
    assert decrypt_value(encrypted) == secret


def test_encrypt_produces_unique_ciphertext():
    """Fernet includes a timestamp, so same plaintext -> different ciphertext."""
    secret = "my-secret"
    e1 = encrypt_value(secret)
    e2 = encrypt_value(secret)
    assert e1 != e2
    assert decrypt_value(e1) == secret
    assert decrypt_value(e2) == secret


def test_decrypt_secret_helper_fallback():
    """The _decrypt_secret helper falls back to raw value for pre-encryption data."""
    from dev_health_ops.api.auth.router import _decrypt_secret

    # Encrypted value round-trips
    encrypted = encrypt_value("real-secret")
    assert _decrypt_secret({"client_secret": encrypted}, "client_secret") == "real-secret"

    # Legacy plaintext value passes through (fallback)
    assert _decrypt_secret({"client_secret": "plaintext"}, "client_secret") == "plaintext"

    # Missing key returns default
    assert _decrypt_secret({}, "client_secret") == ""
    assert _decrypt_secret(None, "client_secret") == ""
