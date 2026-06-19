"""CHAOS-2552: BYO LLM base_url allowlist validation (anti-SSRF).

Per the locked owner decision this is an ALLOWLIST of approved provider
gateways + default SDK URLs (NOT IP-deny). Verifies:
- validate_llm_base_url accepts approved gateways and default local endpoints,
  rejects everything else (other hosts, private IPs, cloud metadata, non-http).
- Runtime: an org BYO config with a disallowed base_url is ignored (provider,
  credential, and model resolution all fall back to the platform default).
- Persist: upsert_llm_settings rejects a disallowed base_url with a 400 before
  persisting anything.
"""

from __future__ import annotations

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dev_health_ops.api.admin.llm_settings import (
    LLMSettingsAccessError,
    upsert_llm_settings,
)
from dev_health_ops.api.admin.schemas import LLMSettingsUpsert
from dev_health_ops.llm import credentials as creds
from dev_health_ops.llm.credentials import validate_llm_base_url

os.environ.setdefault("SETTINGS_ENCRYPTION_KEY", "test-encryption-key")


# ---------------------------------------------------------------------------
# validate_llm_base_url
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "url",
    [
        "",
        None,
        "https://api.openai.com/v1",
        "https://api.anthropic.com",
        "https://generativelanguage.googleapis.com/v1beta/openai",
        "https://dashscope.aliyuncs.com/compatible-mode/v1",
        "https://dashscope-intl.aliyuncs.com/compatible-mode/v1",
        "https://dashscope-us.aliyuncs.com/compatible-mode/v1",
        "http://localhost:11434/v1",
        "http://localhost:1234/v1",
        "http://127.0.0.1:8000/v1",
    ],
)
def test_allowed_base_urls(url):
    ok, err = validate_llm_base_url(url)
    assert ok is True
    assert err is None


@pytest.mark.parametrize(
    "url",
    [
        "http://evil.example/v1",
        "https://api.openai.com.evil.example/v1",  # lookalike host, not exact
        "https://openai.com/v1",  # close but not the api host
        "http://10.0.0.5:11434/v1",  # private network IP, not allowlisted
        "http://192.168.1.10:1234/v1",  # private network IP
        "http://169.254.169.254/latest/meta-data/",  # cloud metadata endpoint
        "ftp://api.openai.com/v1",  # non-http scheme
        "file:///etc/passwd",  # non-http scheme
        "not-a-url",
    ],
)
def test_rejected_base_urls(url):
    ok, err = validate_llm_base_url(url)
    assert ok is False
    assert err


# ---------------------------------------------------------------------------
# Runtime: org BYO with a disallowed base_url falls back to the platform
# ---------------------------------------------------------------------------


def _patch_org(settings: dict[str, str]):
    return patch.object(creds, "_load_org_llm_settings", lambda org_id: dict(settings))


def test_org_disallowed_base_url_falls_back_everywhere():
    org = {
        "provider": "openai",
        "api_key": "sk-org",
        "base_url": "http://evil.example/v1",
    }
    with _patch_org(org):
        # Provider, credential, and model source all decline -> platform default.
        assert creds.resolve_usable_org_llm_provider(org_id="org-1") == ""
        assert creds._resolve_org_byo_credentials("openai", "org-1") is None
        assert creds.org_byo_provider_matches("openai", "org-1") is False


def test_org_allowed_base_url_is_usable():
    org = {
        "provider": "openai",
        "api_key": "sk-org",
        "base_url": "https://api.openai.com/v1",
    }
    with _patch_org(org):
        assert creds.resolve_usable_org_llm_provider(org_id="org-1") == "openai"
        resolved = creds._resolve_org_byo_credentials("openai", "org-1")
        assert resolved is not None
        assert resolved.base_url == "https://api.openai.com/v1"
        assert creds.org_byo_provider_matches("openai", "org-1") is True


def test_org_local_loopback_base_url_is_usable():
    org = {"provider": "ollama", "base_url": "http://localhost:11434/v1"}
    with _patch_org(org):
        assert creds.resolve_usable_org_llm_provider(org_id="org-1") == "ollama"
        assert creds._resolve_org_byo_credentials("ollama", "org-1") is not None


# ---------------------------------------------------------------------------
# Persist: upsert rejects a disallowed base_url before storing anything
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upsert_rejects_disallowed_base_url():
    svc = MagicMock()
    svc.set = AsyncMock()
    payload = LLMSettingsUpsert(
        provider="openai",
        model="gpt-4o-mini",
        api_key="sk-test",
        base_url="http://169.254.169.254/latest",
    )
    with pytest.raises(LLMSettingsAccessError) as exc:
        await upsert_llm_settings(svc, payload)
    assert exc.value.status_code == 400
    assert exc.value.detail["error"] == "invalid_base_url"
    # Must reject BEFORE persisting anything.
    svc.set.assert_not_called()


@pytest.mark.asyncio
async def test_upsert_accepts_allowed_base_url():
    svc = MagicMock()
    svc.set = AsyncMock()
    svc.get = AsyncMock(return_value=None)
    payload = LLMSettingsUpsert(
        provider="openai",
        model="gpt-4o-mini",
        api_key="sk-test",
        base_url="https://api.openai.com/v1",
    )
    # Should not raise; persists via svc.set.
    await upsert_llm_settings(svc, payload)
    assert svc.set.await_count >= 1
