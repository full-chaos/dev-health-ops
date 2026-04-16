from __future__ import annotations

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


def test_missing_jwt_secret_raises_runtime_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """JWT_SECRET_KEY absent in ANY environment must fail closed."""
    _clear_secret_env(monkeypatch)

    with pytest.raises(RuntimeError, match="JWT_SECRET_KEY"):
        _get_jwt_secret()


def test_missing_jwt_secret_with_settings_encryption_key_still_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SETTINGS_ENCRYPTION_KEY must NOT be used as a fallback."""
    _clear_secret_env(monkeypatch)
    monkeypatch.setenv("SETTINGS_ENCRYPTION_KEY", "some-dev-key")
    monkeypatch.setenv("ENVIRONMENT", "development")

    with pytest.raises(RuntimeError, match="JWT_SECRET_KEY"):
        _get_jwt_secret()


def test_short_secret_raises_value_error(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_secret_env(monkeypatch)
    monkeypatch.setenv("JWT_SECRET_KEY", "too-short")

    with pytest.raises(ValueError, match="JWT secret must be at least 32 characters"):
        _get_jwt_secret()
