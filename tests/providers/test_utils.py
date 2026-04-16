"""Tests for providers/utils.py env parsing helpers."""

from __future__ import annotations

import pytest

from dev_health_ops.providers.utils import EnvSpec, env_flag, env_int, read_env_spec


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


class TestReadEnvSpec:
    def test_all_required_present(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MY_TOKEN", "abc")
        monkeypatch.setenv("MY_URL", "https://x")
        spec = EnvSpec(
            required={"token": "MY_TOKEN", "url": "MY_URL"},
            optional={},
            missing_error="MY_TOKEN and MY_URL are required",
        )
        assert read_env_spec(spec) == {"token": "abc", "url": "https://x"}

    def test_required_missing_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("MY_TOKEN", raising=False)
        spec = EnvSpec(
            required={"token": "MY_TOKEN"},
            optional={},
            missing_error="Token required (set MY_TOKEN)",
        )
        with pytest.raises(ValueError, match="Token required"):
            read_env_spec(spec)

    def test_required_empty_string_raises(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("MY_TOKEN", "")
        spec = EnvSpec(
            required={"token": "MY_TOKEN"},
            optional={},
            missing_error="Token required",
        )
        with pytest.raises(ValueError, match="Token required"):
            read_env_spec(spec)

    def test_optional_with_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MY_TOKEN", "abc")
        monkeypatch.delenv("MY_URL", raising=False)
        spec = EnvSpec(
            required={"token": "MY_TOKEN"},
            optional={"url": ("MY_URL", "https://default.example")},
            missing_error="required",
        )
        assert read_env_spec(spec) == {
            "token": "abc",
            "url": "https://default.example",
        }

    def test_optional_none_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MY_TOKEN", "abc")
        monkeypatch.delenv("MY_URL", raising=False)
        spec = EnvSpec(
            required={"token": "MY_TOKEN"},
            optional={"url": ("MY_URL", None)},
            missing_error="required",
        )
        assert read_env_spec(spec) == {"token": "abc", "url": None}

    def test_multiple_required_missing_lists_all(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("A", raising=False)
        monkeypatch.delenv("B", raising=False)
        spec = EnvSpec(
            required={"a": "A", "b": "B"},
            optional={},
            missing_error="A and B are required",
        )
        with pytest.raises(ValueError, match="A and B are required"):
            read_env_spec(spec)
