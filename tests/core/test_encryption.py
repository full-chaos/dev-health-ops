from __future__ import annotations

import base64
import hashlib

import pytest
from cryptography.fernet import Fernet

from dev_health_ops.core import encryption


def _set_key(monkeypatch: pytest.MonkeyPatch, key: str = "test-key") -> None:
    monkeypatch.setenv("SETTINGS_ENCRYPTION_KEY", key)
    monkeypatch.setenv("SETTINGS_ENCRYPTION_SALT", "test-salt")


def _legacy_ciphertext(secret: str, plaintext: str) -> str:
    digest = hashlib.sha256(secret.encode()).digest()
    key = base64.urlsafe_b64encode(digest)
    return Fernet(key).encrypt(plaintext.encode()).decode()


def test_round_trip_encrypts_and_decrypts(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_key(monkeypatch)

    ciphertext = encryption.encrypt_value("super-secret")

    assert ciphertext != "super-secret"
    assert encryption.decrypt_value(ciphertext) == "super-secret"


def test_pbkdf2_key_derivation_is_deterministic(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_key(monkeypatch)

    assert encryption._derive_key("same-secret") == encryption._derive_key("same-secret")
    assert encryption._derive_key("same-secret") != encryption._derive_legacy_key(
        "same-secret"
    )


def test_legacy_v0_ciphertext_still_decrypts(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_key(monkeypatch, "legacy-secret")
    ciphertext = _legacy_ciphertext("legacy-secret", "old-row-value")

    assert not ciphertext.startswith(encryption.KEY_VERSION_PREFIX)
    assert encryption.decrypt_value(ciphertext) == "old-row-value"


def test_v1_ciphertext_gets_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_key(monkeypatch)

    assert encryption.encrypt_value("new-row-value").startswith(
        encryption.KEY_VERSION_PREFIX
    )


def test_wrong_key_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_key(monkeypatch, "correct-key")
    ciphertext = encryption.encrypt_value("secret")

    _set_key(monkeypatch, "wrong-key")

    with pytest.raises(ValueError, match="Decryption failed"):
        encryption.decrypt_value(ciphertext)


def test_malformed_ciphertext_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_key(monkeypatch)

    with pytest.raises(ValueError, match="Decryption failed"):
        encryption.decrypt_value(f"{encryption.KEY_VERSION_PREFIX}not-fernet")


def test_missing_env_var_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SETTINGS_ENCRYPTION_KEY", raising=False)

    with pytest.raises(RuntimeError, match="SETTINGS_ENCRYPTION_KEY"):
        encryption.encrypt_value("secret")


def test_salt_change_cannot_decrypt(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_key(monkeypatch)
    ciphertext = encryption.encrypt_value("salt-bound-secret")

    monkeypatch.setenv("SETTINGS_ENCRYPTION_SALT", "different-salt")

    with pytest.raises(ValueError, match="Decryption failed"):
        encryption.decrypt_value(ciphertext)


def test_reencrypt_legacy_value_returns_v1(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_key(monkeypatch, "legacy-secret")
    legacy = _legacy_ciphertext("legacy-secret", "old-row-value")

    upgraded = encryption.reencrypt_legacy_value(legacy)

    assert upgraded.startswith(encryption.KEY_VERSION_PREFIX)
    assert encryption.decrypt_value(upgraded) == "old-row-value"
