"""Encryption utilities — pure, framework-agnostic.

Contains shared encrypt/decrypt helpers that can be used by the API layer,
Celery workers, and any other module without creating circular imports.

Nothing here imports from dev_health_ops.api.*.
"""

from __future__ import annotations

import base64
import hashlib
import logging
import os
from hmac import compare_digest

from cryptography.fernet import Fernet, InvalidToken

logger = logging.getLogger(__name__)

KEY_VERSION_PREFIX = "v1:"
PBKDF2_ITERATIONS = 600_000

# Backwards-compatibility default only. Production deployments should set an
# explicit, stable SETTINGS_ENCRYPTION_SALT before writing v1 ciphertexts.
DEFAULT_SETTINGS_ENCRYPTION_SALT = "dev-health-ops-settings-encryption-v1"


def _derive_key(secret: str) -> bytes:
    """Derive a Fernet-compatible v1 key with PBKDF2-HMAC-SHA256."""
    salt = os.getenv(
        "SETTINGS_ENCRYPTION_SALT",
        DEFAULT_SETTINGS_ENCRYPTION_SALT,
    ).encode()
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        secret.encode(),
        salt,
        PBKDF2_ITERATIONS,
        dklen=32,
    )
    return base64.urlsafe_b64encode(digest)


def _derive_legacy_key(secret: str) -> bytes:
    """Derive the legacy v0 Fernet key from raw SHA-256 for old rows."""
    digest = hashlib.sha256(secret.encode()).digest()
    return base64.urlsafe_b64encode(digest)


def _get_encryption_secret() -> str:
    """Get the encryption secret from environment."""
    secret = os.getenv("SETTINGS_ENCRYPTION_KEY")
    if not secret:
        raise RuntimeError(
            "SETTINGS_ENCRYPTION_KEY environment variable is required for encryption"
        )
    return secret


def _get_encryption_key() -> bytes:
    """Get the current v1 encryption key from environment."""
    return _derive_key(_get_encryption_secret())


def _get_legacy_encryption_key() -> bytes:
    """Get the legacy v0 encryption key from environment."""
    return _derive_legacy_key(_get_encryption_secret())


def is_v1_ciphertext(value: str | None) -> bool:
    """Return True when a value has the current v1 ciphertext prefix."""
    return bool(value) and value.startswith(KEY_VERSION_PREFIX)


def encrypt_value(plaintext: str) -> str:
    """Encrypt a plaintext string and return version-prefixed ciphertext."""
    key = _get_encryption_key()
    f = Fernet(key)
    return f"{KEY_VERSION_PREFIX}{f.encrypt(plaintext.encode()).decode()}"


def decrypt_value(ciphertext: str) -> str:
    """Decrypt v1 or legacy v0 ciphertext and return plaintext."""
    if ciphertext.startswith(KEY_VERSION_PREFIX):
        key = _get_encryption_key()
        token = ciphertext.removeprefix(KEY_VERSION_PREFIX)
    elif ciphertext.startswith("v") and ":" in ciphertext:
        # NOTE: deliberately do not log the parsed version prefix. CodeQL
        # (py/clear-text-logging-sensitive-data) flags any logging of values
        # derived from ciphertext, even when the derived value is only a
        # short non-secret tag like "v2". The exception message below is
        # sufficient for operators; full ciphertext is never persisted to logs.
        raise ValueError("Decryption failed - unsupported ciphertext version")
    else:
        key = _get_legacy_encryption_key()
        token = ciphertext

    f = Fernet(key)
    try:
        return f.decrypt(token.encode()).decode()
    except InvalidToken:
        logger.error("Failed to decrypt value - invalid token or wrong key")
        raise ValueError("Decryption failed - check SETTINGS_ENCRYPTION_KEY")


def reencrypt_legacy_value(ciphertext: str) -> str:
    """Return v1 ciphertext for a legacy v0 value; leave v1 values unchanged."""
    if is_v1_ciphertext(ciphertext):
        return ciphertext
    plaintext = decrypt_value(ciphertext)
    new_ciphertext = encrypt_value(plaintext)
    if compare_digest(new_ciphertext, ciphertext):
        raise ValueError("Re-encryption failed to produce a new ciphertext version")
    return new_ciphertext
