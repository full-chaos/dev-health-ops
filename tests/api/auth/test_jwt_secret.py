from __future__ import annotations

import hashlib

import pytest

from dev_health_ops.api.services.auth import _get_jwt_secret


def _clear_secret_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in (
        "JWT_SECRET_KEY",
        "SETTINGS_ENCRYPTION_KEY",
        "ENVIRONMENT",
        "ENV",
        "RAILWAY_ENVIRONMENT",
        "FLY_APP_NAME",
        "RENDER_SERVICE_ID",
        "KUBERNETES_SERVICE_HOST",
    ):
        monkeypatch.delenv(var, raising=False)


def test_jwt_secret_key_env_var_is_used_when_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_secret_env(monkeypatch)
    jwt_secret = "this-is-a-very-secure-secret-key-12345"
    monkeypatch.setenv("JWT_SECRET_KEY", jwt_secret)

    assert _get_jwt_secret() == jwt_secret


def test_fallback_to_settings_encryption_key_works_in_dev(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_secret_env(monkeypatch)
    encryption_key = "local-dev-encryption-key"
    monkeypatch.setenv("SETTINGS_ENCRYPTION_KEY", encryption_key)
    monkeypatch.setenv("ENVIRONMENT", "development")

    expected = hashlib.sha256(encryption_key.encode()).hexdigest()
    assert _get_jwt_secret() == expected


def test_insecure_default_raises_runtime_error_in_production(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_secret_env(monkeypatch)
    monkeypatch.setenv("ENVIRONMENT", "production")

    with pytest.raises(
        RuntimeError,
        match="JWT_SECRET_KEY must be explicitly set in production environments",
    ):
        _get_jwt_secret()


def test_short_secret_raises_value_error(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_secret_env(monkeypatch)
    monkeypatch.setenv("JWT_SECRET_KEY", "too-short")

    with pytest.raises(ValueError, match="JWT secret must be at least 32 characters"):
        _get_jwt_secret()
