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

from cryptography.fernet import Fernet, InvalidToken

logger = logging.getLogger(__name__)


def _derive_key(secret: str) -> bytes:
    """Derive a Fernet-compatible key from a secret string using SHA-256."""
    digest = hashlib.sha256(secret.encode()).digest()
    return base64.urlsafe_b64encode(digest)


def _get_encryption_key() -> bytes:
    """Get the encryption key from environment, deriving it if needed."""
    secret = os.getenv("SETTINGS_ENCRYPTION_KEY")
    if not secret:
        raise RuntimeError(
            "SETTINGS_ENCRYPTION_KEY environment variable is required for encryption"
        )
    return _derive_key(secret)


def encrypt_value(plaintext: str) -> str:
    """Encrypt a plaintext string and return base64-encoded ciphertext."""
    key = _get_encryption_key()
    f = Fernet(key)
    return f.encrypt(plaintext.encode()).decode()


def decrypt_value(ciphertext: str) -> str:
    """Decrypt a base64-encoded ciphertext and return plaintext."""
    key = _get_encryption_key()
    f = Fernet(key)
    try:
        return f.decrypt(ciphertext.encode()).decode()
    except InvalidToken:
        logger.error("Failed to decrypt value - invalid token or wrong key")
        raise ValueError("Decryption failed - check SETTINGS_ENCRYPTION_KEY")
