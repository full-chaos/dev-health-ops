"""Tests for providers/utils.py env parsing helpers."""

from __future__ import annotations

import pytest

from dev_health_ops.providers.utils import env_flag, env_int


class TestEnvFlag:
    def test_unset_returns_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("TEST_FLAG", raising=False)
        assert env_flag("TEST_FLAG", True) is True
        assert env_flag("TEST_FLAG", False) is False

    @pytest.mark.parametrize("value", ["1", "true", "True", "YES", "on", "ON"])
    def test_truthy_values(self, monkeypatch: pytest.MonkeyPatch, value: str) -> None:
        monkeypatch.setenv("TEST_FLAG", value)
        assert env_flag("TEST_FLAG", False) is True

    @pytest.mark.parametrize("value", ["0", "false", "FALSE", "no", "NO", "off", "Off"])
    def test_falsy_values(self, monkeypatch: pytest.MonkeyPatch, value: str) -> None:
        monkeypatch.setenv("TEST_FLAG", value)
        assert env_flag("TEST_FLAG", True) is False

    def test_unknown_value_returns_default(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("TEST_FLAG", "maybe")
        assert env_flag("TEST_FLAG", True) is True
        assert env_flag("TEST_FLAG", False) is False

    def test_whitespace_trimmed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TEST_FLAG", "  true  ")
        assert env_flag("TEST_FLAG", False) is True


class TestEnvInt:
    def test_unset_returns_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("TEST_INT", raising=False)
        assert env_int("TEST_INT", 7) == 7

    def test_valid_int(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TEST_INT", "42")
        assert env_int("TEST_INT", 0) == 42

    def test_invalid_returns_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TEST_INT", "notanumber")
        assert env_int("TEST_INT", 99) == 99

    def test_empty_returns_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TEST_INT", "")
        assert env_int("TEST_INT", 99) == 99

    def test_negative_allowed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TEST_INT", "-5")
        assert env_int("TEST_INT", 0) == -5
