from __future__ import annotations

from dev_health_ops.api.integrations.github_app_config import github_app_private_key

_ESCAPED = "line-one\\nline-two\\nline-three\\n"
_REAL = "line-one\nline-two\nline-three\n"


def test_inline_escaped_newlines_are_normalized(monkeypatch):
    monkeypatch.delenv("GITHUB_APP_PRIVATE_KEY_PATH", raising=False)
    monkeypatch.setenv("GITHUB_APP_PRIVATE_KEY", _ESCAPED)
    assert github_app_private_key() == _REAL


def test_inline_real_newlines_are_unchanged(monkeypatch):
    monkeypatch.delenv("GITHUB_APP_PRIVATE_KEY_PATH", raising=False)
    monkeypatch.setenv("GITHUB_APP_PRIVATE_KEY", _REAL)
    assert github_app_private_key() == _REAL


def test_path_is_read_when_inline_absent(monkeypatch, tmp_path):
    monkeypatch.delenv("GITHUB_APP_PRIVATE_KEY", raising=False)
    pem = tmp_path / "app.pem"
    pem.write_text(_REAL, encoding="utf-8")
    monkeypatch.setenv("GITHUB_APP_PRIVATE_KEY_PATH", str(pem))
    assert github_app_private_key() == _REAL


def test_none_when_unconfigured(monkeypatch):
    monkeypatch.delenv("GITHUB_APP_PRIVATE_KEY", raising=False)
    monkeypatch.delenv("GITHUB_APP_PRIVATE_KEY_PATH", raising=False)
    assert github_app_private_key() is None
