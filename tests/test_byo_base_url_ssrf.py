from __future__ import annotations

import asyncio
import os
import socket
import sys
import types
import uuid
from contextlib import contextmanager
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dev_health_ops.api.admin.llm_settings import (
    LLMSettingsAccessError,
    upsert_llm_settings,
)
from dev_health_ops.api.admin.schemas import LLMSettingsUpsert
from dev_health_ops.llm import credentials as creds
from dev_health_ops.llm.credentials import validate_llm_base_url
from dev_health_ops.llm.providers._http import (
    make_hardened_async_httpx_client,
    make_hardened_httpx_client,
)

os.environ.setdefault("SETTINGS_ENCRYPTION_KEY", "test-encryption-key")


def _addrinfo(address: str):
    family = socket.AF_INET6 if ":" in address else socket.AF_INET
    sockaddr = (address, 443, 0, 0) if family == socket.AF_INET6 else (address, 443)
    return (family, socket.SOCK_STREAM, 6, "", sockaddr)


@pytest.fixture(autouse=True)
def hermetic_dns(monkeypatch: pytest.MonkeyPatch):
    answers = {
        "api.openai.com": ["104.18.33.45"],
        "api.anthropic.com": ["160.79.104.10"],
        "my-gateway.example.com": ["93.184.216.34"],
        "localhost": ["127.0.0.1", "::1"],
        "127.0.0.1": ["127.0.0.1"],
        "::1": ["::1"],
        "2130706433": ["127.0.0.1"],
        "127.1": ["127.0.0.1"],
        "0x7f.0.0.1": ["127.0.0.1"],
        "mixed.example.com": ["93.184.216.34", "10.0.0.5"],
        "evil.example": ["93.184.216.34"],
    }

    def fake_getaddrinfo(host: str, port: int | None, *args, **kwargs):
        if host not in answers:
            raise socket.gaierror(f"test DNS has no answer for {host}")
        return [_addrinfo(address) for address in answers[host]]

    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)


@pytest.mark.parametrize(
    "url",
    [
        "",
        None,
        "https://api.openai.com/v1",
        "https://my-gateway.example.com/v1",
    ],
)
def test_ssrf_shape_accepts_valid_base_urls(url: str | None):
    ok, err = validate_llm_base_url(url)
    assert ok is True
    assert err is None


@pytest.mark.parametrize(
    "url",
    [
        "http://api.openai.com",
        "https://api.openai.com@169.254.169.254/v1",
        "https://169.254.169.254/",
        "https://[::ffff:169.254.169.254]/",
        "https://localhost/",
        "https://[::1]/",
        "http://localhost/",
        "http://localhost:1234/v1",
        "http://127.0.0.1:8000",
        "https://10.0.0.5/",
        "https://192.168.1.1/",
        "https://2130706433/",
        "https://127.1/",
        "https://0x7f.0.0.1/",
        "https://mixed.example.com/v1",
        "ftp://api.openai.com/v1",
        "not-a-url",
    ],
)
def test_ssrf_shape_rejects_unsafe_base_urls(url: str):
    ok, err = validate_llm_base_url(url)
    assert ok is False
    assert err


def _patch_org(settings: dict[str, str]):
    return patch.object(creds, "_load_org_llm_settings", lambda org_id: dict(settings))


class _AuditSession:
    def __init__(self) -> None:
        self.added: list[Any] = []
        self.flushed = False

    def add(self, entry: object) -> None:
        self.added.append(entry)

    def flush(self) -> None:
        self.flushed = True


def test_org_disallowed_base_url_falls_back_and_audits(
    monkeypatch: pytest.MonkeyPatch,
):
    org_id = str(uuid.uuid4())
    audit_session = _AuditSession()

    @contextmanager
    def fake_session():
        yield audit_session

    monkeypatch.setattr("dev_health_ops.db.get_postgres_session_sync", fake_session)
    org = {
        "provider": "openai",
        "api_key": "sk-org",
        "base_url": "http://evil.example/v1",
    }
    with _patch_org(org):
        assert creds.resolve_usable_org_llm_provider(org_id=org_id) == ""
        assert creds._resolve_org_byo_credentials("openai", org_id) is None
        assert creds.org_byo_provider_matches("openai", org_id) is False

    assert audit_session.flushed is True
    assert audit_session.added
    entry = audit_session.added[0]
    assert entry.org_id == uuid.UUID(org_id)
    assert entry.resource_id == "llm.base_url"
    assert entry.status == "failure"
    assert entry.changes["provider"] == "openai"
    assert entry.changes["base_url"] == "http://evil.example/v1"


def test_org_public_https_base_url_is_usable():
    org = {
        "provider": "openai",
        "api_key": "sk-org",
        "base_url": "https://my-gateway.example.com/v1",
    }
    with _patch_org(org):
        assert (
            creds.resolve_usable_org_llm_provider(org_id=str(uuid.uuid4())) == "openai"
        )
        resolved = creds._resolve_org_byo_credentials("openai", str(uuid.uuid4()))
        assert resolved is not None
        assert resolved.base_url == "https://my-gateway.example.com/v1"
        assert creds.org_byo_provider_matches("openai", str(uuid.uuid4())) is True


def test_org_local_loopback_base_url_falls_back():
    org = {"provider": "ollama", "base_url": "http://localhost:11434/v1"}
    with _patch_org(org):
        assert creds.resolve_usable_org_llm_provider(org_id=str(uuid.uuid4())) == ""
        assert creds._resolve_org_byo_credentials("ollama", str(uuid.uuid4())) is None


@pytest.mark.parametrize(
    "module_name,class_name,kwargs",
    [
        (
            "dev_health_ops.llm.providers.local",
            "LocalProvider",
            {"base_url": "https://api.openai.com/v1"},
        ),
        ("dev_health_ops.llm.providers.gemini", "GeminiProvider", {"api_key": "test"}),
        ("dev_health_ops.llm.providers.qwen", "QwenProvider", {"api_key": "test"}),
    ],
)
def test_openai_compatible_providers_use_hardened_http_client(
    monkeypatch: pytest.MonkeyPatch,
    module_name: str,
    class_name: str,
    kwargs: dict[str, str],
):
    captured: dict[str, object] = {}
    sentinel_http_client = object()

    class FakeAsyncOpenAI:
        def __init__(self, **client_kwargs: object) -> None:
            captured.update(client_kwargs)

    monkeypatch.setitem(
        sys.modules, "openai", types.SimpleNamespace(AsyncOpenAI=FakeAsyncOpenAI)
    )
    monkeypatch.setattr(
        "dev_health_ops.llm.providers.local.make_hardened_async_httpx_client",
        lambda: sentinel_http_client,
    )

    module = __import__(module_name, fromlist=[class_name])
    provider = getattr(module, class_name)(**kwargs)
    provider._get_client()

    assert captured["http_client"] is sentinel_http_client
    assert captured["max_retries"] == 0


def test_hardened_http_client_factory_disables_redirects_and_env_proxies():
    async_client = make_hardened_async_httpx_client()
    sync_client = make_hardened_httpx_client()
    try:
        assert async_client.follow_redirects is False
        assert async_client.trust_env is False
        assert sync_client.follow_redirects is False
        assert sync_client.trust_env is False
    finally:
        asyncio.run(async_client.aclose())
        sync_client.close()


def test_upsert_rejects_unsafe_base_url():
    svc = MagicMock()
    svc.set = AsyncMock()
    payload = LLMSettingsUpsert(
        provider="openai",
        model="gpt-4o-mini",
        api_key="sk-test",
        base_url="http://169.254.169.254/latest",
    )
    with pytest.raises(LLMSettingsAccessError) as exc:
        asyncio.run(upsert_llm_settings(svc, payload))
    assert exc.value.status_code == 400
    assert exc.value.detail["error"] == "invalid_base_url"
    svc.set.assert_not_called()


def test_upsert_accepts_public_https_base_url():
    svc = MagicMock()
    svc.set = AsyncMock()
    svc.get = AsyncMock(return_value=None)
    payload = LLMSettingsUpsert(
        provider="openai",
        model="gpt-4o-mini",
        api_key="sk-test",
        base_url="https://api.openai.com/v1",
    )
    asyncio.run(upsert_llm_settings(svc, payload))
    assert svc.set.await_count >= 1
