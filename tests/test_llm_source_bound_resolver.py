"""Source-bound LLM resolver + platform fallback + credential isolation.

CHAOS-2550. Verifies the locked owner decisions:

* Org BYO (when configured AND complete) wins over the platform/env default
  (fixes the env-before-org precedence flagged in the CHAOS-2549 review).
* Platform default (existing ``LLM_*``/provider env) is used when an org has no
  BYO configured (decision #2: reuse existing env, no ``PLATFORM_LLM_*``).
* An invalid/incomplete org BYO config does NOT crash: it logs a WARNING and
  falls back to the platform default (decision #1: "silent-but-missing >
  crashing"). Only a truly-no-usable-config-anywhere case fails closed.
* Credential isolation: api_key and base_url are never mixed across sources.
  A platform key must never reach an org-provided base_url and vice versa.
* ``mock``/``none`` stay explicit-only; an org cannot select them.
"""

from __future__ import annotations

import logging
import os
from unittest.mock import patch

import pytest

from dev_health_ops.llm import LLMAuthError, get_provider, is_llm_available
from dev_health_ops.llm import credentials as creds
from dev_health_ops.llm.providers import resolve_provider_name
from dev_health_ops.llm.providers.openai import OpenAIProvider


def _patch_org(settings_by_org: dict[str, dict[str, str]]):
    """Patch the org LLM settings loader so tests are DB-free and hermetic."""
    return patch.object(
        creds,
        "_load_org_llm_settings",
        lambda org_id: dict(settings_by_org.get(org_id or "", {})),
    )


def test_org_byo_provider_beats_platform_env_in_auto():
    """Regression (CHAOS-2549 review / CHAOS-2550): with a global provider env
    configured, an org with complete BYO settings must resolve to its OWN
    provider and credentials, not the platform default."""
    org = {
        "org-1": {
            "provider": "openai",
            "api_key": "sk-org",
            "base_url": "https://api.openai.com/v1",
        }
    }
    # Platform default is a DIFFERENT provider (anthropic) — org must still win.
    with patch.dict(
        os.environ, {"ANTHROPIC_API_KEY": "sk-platform-anthropic"}, clear=True
    ):
        with _patch_org(org):
            assert resolve_provider_name("auto", org_id="org-1") == "openai"
            provider = get_provider("auto", org_id="org-1")

    assert isinstance(provider, OpenAIProvider)
    assert provider._impl.cfg.api_key == "sk-org"
    assert provider._impl.cfg.base_url == "https://api.openai.com/v1"


def test_platform_env_used_when_org_absent():
    """Org with no BYO settings falls through to the platform/env default."""
    with patch.dict(
        os.environ,
        {
            "OPENAI_API_KEY": "sk-platform",
            "OPENAI_BASE_URL": "https://platform.invalid/v1",
        },
        clear=True,
    ):
        with _patch_org({}):
            assert resolve_provider_name("auto", org_id="org-1") == "openai"
            provider = get_provider("auto", org_id="org-1")

    assert isinstance(provider, OpenAIProvider)
    assert provider._impl.cfg.api_key == "sk-platform"
    assert provider._impl.cfg.base_url == "https://platform.invalid/v1"


def test_org_credentials_never_mix_with_platform_env():
    """Credential isolation: an org-sourced provider uses ONLY the org key and
    org base_url; the platform env values must never leak in."""
    org = {
        "org-1": {
            "provider": "openai",
            "api_key": "sk-org",
            "base_url": "https://api.openai.com/v1",
        }
    }
    with patch.dict(
        os.environ,
        {
            "OPENAI_API_KEY": "sk-platform",
            "OPENAI_BASE_URL": "https://platform.invalid/v1",
        },
        clear=True,
    ):
        with _patch_org(org):
            provider = get_provider("auto", org_id="org-1")

    assert isinstance(provider, OpenAIProvider)
    assert provider._impl.cfg.api_key == "sk-org"
    assert provider._impl.cfg.base_url == "https://api.openai.com/v1"
    # Platform values must never leak into the org-sourced provider.
    assert provider._impl.cfg.api_key != "sk-platform"
    assert provider._impl.cfg.base_url != "https://platform.invalid/v1"


def test_incomplete_org_falls_back_to_platform_without_mixing(caplog):
    """Decision #1 + isolation: an org that configured a base_url but no key
    must NOT pair its base_url with the platform key (the classic
    credential-leak/SSRF shape). It warns and falls back ENTIRELY to platform.
    """
    org = {
        "org-1": {
            "provider": "openai",
            # NOTE: org base_url present but NO api_key -> incomplete.
            "base_url": "https://org-gateway.invalid/v1",
        }
    }
    with patch.dict(
        os.environ,
        {
            "OPENAI_API_KEY": "sk-platform",
            "OPENAI_BASE_URL": "https://platform.invalid/v1",
        },
        clear=True,
    ):
        with _patch_org(org):
            with caplog.at_level(
                logging.WARNING, logger="dev_health_ops.llm.credentials"
            ):
                provider = get_provider("auto", org_id="org-1")

    assert isinstance(provider, OpenAIProvider)
    # Falls back entirely to platform: env key AND env base_url together.
    assert provider._impl.cfg.api_key == "sk-platform"
    assert provider._impl.cfg.base_url == "https://platform.invalid/v1"
    # The org base_url must never be paired with the platform key.
    assert provider._impl.cfg.base_url != "https://org-gateway.invalid/v1"
    assert any("incomplete" in r.getMessage() for r in caplog.records)


def test_incomplete_org_without_platform_fails_closed():
    """Truly-no-usable-config-anywhere is the only case that fails closed:
    incomplete org BYO (key-required provider, no key) AND no platform env."""
    org = {"org-1": {"provider": "anthropic", "base_url": "https://org.invalid"}}
    with patch.dict(os.environ, {}, clear=True):
        with _patch_org(org):
            assert is_llm_available("auto", org_id="org-1") is False
            with pytest.raises(LLMAuthError):
                resolve_provider_name("auto", org_id="org-1")


@pytest.mark.parametrize("sneaky", ["mock", "none"])
def test_org_cannot_select_mock_or_none_provider(sneaky):
    """mock/none stay explicit-only; an org's stored settings cannot select
    them (would otherwise let stored config silently disable real LLM use)."""
    org = {"org-1": {"provider": sneaky}}
    with patch.dict(os.environ, {}, clear=True):
        with _patch_org(org):
            assert creds.resolve_usable_org_llm_provider(org_id="org-1") == ""
            with pytest.raises(LLMAuthError):
                resolve_provider_name("auto", org_id="org-1")


def test_per_call_credentials_override_org_and_env():
    """Per-call credentials are their own isolated source and override both org
    BYO and platform env."""
    org = {
        "org-1": {
            "provider": "openai",
            "api_key": "sk-org",
            "base_url": "https://api.openai.com/v1",
        }
    }
    with patch.dict(
        os.environ,
        {
            "OPENAI_API_KEY": "sk-platform",
            "OPENAI_BASE_URL": "https://platform.invalid/v1",
        },
        clear=True,
    ):
        with _patch_org(org):
            provider = get_provider(
                "openai",
                org_id="org-1",
                api_key="sk-inline",
                base_url="https://inline.invalid/v1",
            )

    assert isinstance(provider, OpenAIProvider)
    assert provider._impl.cfg.api_key == "sk-inline"
    assert provider._impl.cfg.base_url == "https://inline.invalid/v1"


def test_org_local_provider_usable_with_base_url_only():
    """A self-hosted org provider (no API key required) is usable with just a
    base_url, and wins over an empty platform default."""
    org = {"org-1": {"provider": "local", "base_url": "http://localhost:11434/v1"}}
    with patch.dict(os.environ, {}, clear=True):
        with _patch_org(org):
            assert resolve_provider_name("auto", org_id="org-1") == "local"
            assert is_llm_available("auto", org_id="org-1") is True


def test_resolve_llm_credentials_is_source_bound_for_org():
    """Unit-level isolation guarantee on the credential resolver itself: an org
    provider resolves to org creds only; a provider the org did NOT configure
    resolves to platform/env creds only."""
    org = {
        "org-1": {
            "provider": "openai",
            "api_key": "sk-org",
            "base_url": "https://api.openai.com/v1",
        }
    }
    with patch.dict(
        os.environ,
        {
            "OPENAI_API_KEY": "sk-platform",
            "OPENAI_BASE_URL": "https://platform.invalid/v1",
            "ANTHROPIC_API_KEY": "sk-platform-anthropic",
        },
        clear=True,
    ):
        with _patch_org(org):
            # Provider the org configured -> org creds only.
            openai_creds = creds.resolve_llm_credentials("openai", org_id="org-1")
            # Provider the org did NOT configure -> platform env creds only.
            anthropic_creds = creds.resolve_llm_credentials("anthropic", org_id="org-1")

    assert openai_creds.api_key == "sk-org"
    assert openai_creds.base_url == "https://api.openai.com/v1"
    assert anthropic_creds.api_key == "sk-platform-anthropic"
    # The org's openai base_url must never bleed into the anthropic resolution.
    assert anthropic_creds.base_url != "https://api.openai.com/v1"


def test_org_byo_model_is_source_bound_not_platform_env():
    """Review finding (CHAOS-2550): when org BYO wins, the model must come from
    the org (or provider default), NOT a platform env model var. Otherwise an
    org gateway receives a platform-configured model name (wrong routing)."""
    org = {
        "org-1": {
            "provider": "openai",
            "api_key": "sk-org",
            "base_url": "https://api.openai.com/v1",
            "model": "org-model",
        }
    }
    with patch.dict(
        os.environ,
        {"LLM_MODEL_OPENAI": "platform-model", "LLM_MODEL": "platform-global"},
        clear=True,
    ):
        with _patch_org(org):
            provider = get_provider("auto", org_id="org-1")

    assert isinstance(provider, OpenAIProvider)
    assert provider._impl.cfg.api_key == "sk-org"
    assert provider._impl.cfg.base_url == "https://api.openai.com/v1"
    # The model must be the org model, not the platform env model.
    assert provider._impl.cfg.model == "org-model"
    assert provider._impl.cfg.model != "platform-model"


@pytest.mark.parametrize("disable_value", ["none", "mock"])
def test_env_provider_disable_beats_org_byo(disable_value):
    """Review finding (CHAOS-2550): an operator kill-switch / explicit override
    via LLM_PROVIDER=none (maintenance disable) or mock (fixtures) must take
    precedence over org BYO for auto callers; org config must not silently
    re-enable a disabled platform."""
    org = {
        "org-1": {
            "provider": "openai",
            "api_key": "sk-org",
            "base_url": "https://api.openai.com/v1",
        }
    }
    with patch.dict(os.environ, {"LLM_PROVIDER": disable_value}, clear=True):
        with _patch_org(org):
            assert resolve_provider_name("auto", org_id="org-1") == disable_value
            if disable_value == "none":
                # none is an explicit disable: not "available" for real work.
                assert is_llm_available("auto", org_id="org-1") is False


def test_unrecognized_org_provider_falls_back_not_crashes():
    """Oracle finding (CHAOS-2550): an org that stored an unrecognized provider
    name (e.g. a typo) must warn and fall back to the platform default, NOT
    crash with an unknown-provider error (decision #1)."""
    org = {"org-1": {"provider": "bogus-typo", "api_key": "sk-org"}}
    with patch.dict(
        os.environ,
        {
            "OPENAI_API_KEY": "sk-platform",
            "OPENAI_BASE_URL": "https://platform.invalid/v1",
        },
        clear=True,
    ):
        with _patch_org(org):
            assert creds.resolve_usable_org_llm_provider(org_id="org-1") == ""
            # Falls back to the platform-detected provider, not the bogus org one.
            assert resolve_provider_name("auto", org_id="org-1") == "openai"
            provider = get_provider("auto", org_id="org-1")
    assert isinstance(provider, OpenAIProvider)
    assert provider._impl.cfg.api_key == "sk-platform"
    assert provider._impl.cfg.base_url == "https://platform.invalid/v1"


def test_unrecognized_org_provider_without_platform_fails_closed():
    """Oracle finding (CHAOS-2550): unrecognized org provider with no platform
    default is the no-usable-config-anywhere case -> fails closed / unavailable
    (never resolves the bogus provider)."""
    org = {"org-1": {"provider": "bogus-typo", "api_key": "sk-org"}}
    with patch.dict(os.environ, {}, clear=True):
        with _patch_org(org):
            assert is_llm_available("auto", org_id="org-1") is False
            with pytest.raises(LLMAuthError):
                resolve_provider_name("auto", org_id="org-1")


def test_org_provider_without_credential_material_falls_back_consistently():
    """Oracle finding (CHAOS-2550): an org that names a no-key provider (ollama)
    with NO api_key and NO base_url is not an active BYO config. Provider AND
    credential resolution must BOTH fall back to platform and stay in agreement
    (no provider-from-org / creds-from-platform split)."""
    org = {"org-1": {"provider": "ollama"}}  # no api_key, no base_url
    with patch.dict(
        os.environ,
        {
            "OPENAI_API_KEY": "sk-platform",
            "OPENAI_BASE_URL": "https://platform.invalid/v1",
        },
        clear=True,
    ):
        with _patch_org(org):
            # Org provider selection declines (no material) ...
            assert creds.resolve_usable_org_llm_provider(org_id="org-1") == ""
            # ... so the platform default is used for BOTH provider and creds.
            assert resolve_provider_name("auto", org_id="org-1") == "openai"
            provider = get_provider("auto", org_id="org-1")
    assert isinstance(provider, OpenAIProvider)
    assert provider._impl.cfg.api_key == "sk-platform"
    assert provider._impl.cfg.base_url == "https://platform.invalid/v1"
