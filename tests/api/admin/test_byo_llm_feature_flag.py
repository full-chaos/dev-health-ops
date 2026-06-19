"""CHAOS-2551: byo_llm feature flag + admin gate + runtime gate.

Verifies:
- The byo_llm flag is registered at TEAM tier in the canonical registry.
- The admin gate (require_byo_llm_access) enforces BOTH tier and the byo_llm
  flag, with distinct errors: feature_not_licensed (tier) vs feature_not_enabled
  (flag disabled). When the flag is not registered (minimal/legacy DB) it falls
  back to the prior tier-only behavior.
- The runtime gate (_org_byo_llm_enabled / _load_org_llm_settings) ignores
  stored org BYO settings when the flag is disabled for the org, and is
  graceful (does not gate) when the flag is unregistered/unverifiable.
"""

from __future__ import annotations

import contextlib
import os
import uuid
from pathlib import Path
from unittest.mock import patch

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.admin.llm_settings import (
    LLMSettingsAccessError,
    require_byo_llm_access,
)
from dev_health_ops.api.services import licensing as licensing_module
from dev_health_ops.licensing.registry import get_features_for_tier
from dev_health_ops.licensing.types import LicenseTier
from dev_health_ops.llm import credentials as creds
from dev_health_ops.models.git import Base
from dev_health_ops.models.licensing import FeatureFlag, OrgFeatureOverride, OrgLicense
from dev_health_ops.models.users import Organization, User
from tests._helpers import tables_of

os.environ.setdefault("SETTINGS_ENCRYPTION_KEY", "test-encryption-key")

_TABLES = tables_of(User, Organization, OrgLicense, FeatureFlag, OrgFeatureOverride)


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


def test_byo_llm_registered_at_team_tier():
    community = get_features_for_tier(LicenseTier.COMMUNITY)
    team = get_features_for_tier(LicenseTier.TEAM)
    enterprise = get_features_for_tier(LicenseTier.ENTERPRISE)
    assert community.get("byo_llm") is False
    assert team.get("byo_llm") is True
    assert enterprise.get("byo_llm") is True


# ---------------------------------------------------------------------------
# Runtime gate: _apply_byo_llm_flag_gate / _load_org_llm_settings
# ---------------------------------------------------------------------------


from dev_health_ops.llm.errors import LLMAuthError  # noqa: E402


def _patch_flag_state(state_or_exc):
    def _fake(_session, _org_id):
        if isinstance(state_or_exc, Exception):
            raise state_or_exc
        return state_or_exc

    return patch.object(licensing_module, "byo_llm_flag_state", _fake)


_SETTINGS = {"provider": "openai", "api_key": "sk-org"}


def test_flag_gate_returns_settings_when_enabled():
    with _patch_flag_state("enabled"):
        out = creds._apply_byo_llm_flag_gate(object(), str(uuid.uuid4()), _SETTINGS)
    assert out == _SETTINGS


def test_flag_gate_returns_settings_when_unregistered():
    with _patch_flag_state("unregistered"):
        out = creds._apply_byo_llm_flag_gate(object(), str(uuid.uuid4()), _SETTINGS)
    assert out == _SETTINGS


def test_flag_gate_returns_empty_when_disabled():
    with _patch_flag_state("disabled"):
        out = creds._apply_byo_llm_flag_gate(object(), str(uuid.uuid4()), _SETTINGS)
    assert out == {}


def test_flag_gate_raises_on_lookup_error_for_byo_org():
    # Data-residency: a BYO-configured org must NOT be silently rerouted to the
    # platform when the flag lookup fails -> raise (fail loudly), not return {}.
    with _patch_flag_state(RuntimeError("licensing store down")):
        with pytest.raises(LLMAuthError):
            creds._apply_byo_llm_flag_gate(object(), str(uuid.uuid4()), _SETTINGS)


class _FakeRow:
    def __init__(self, key, value, is_encrypted=False):
        self.key = key
        self.value = value
        self.is_encrypted = is_encrypted


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def scalars(self):
        return self

    def all(self):
        return self._rows


class _FakeSession:
    def __init__(self, rows):
        self._rows = rows

    def execute(self, *_a, **_k):
        return _FakeResult(self._rows)


def _patch_session(rows):
    @contextlib.contextmanager
    def _cm():
        yield _FakeSession(rows)

    return patch("dev_health_ops.db.get_postgres_session_sync", _cm)


def test_load_settings_byo_org_flag_enabled_returns_settings():
    rows = [_FakeRow("provider", "openai"), _FakeRow("api_key", "sk-org")]
    with _patch_session(rows), _patch_flag_state("enabled"):
        out = creds._load_org_llm_settings(str(uuid.uuid4()))
    assert out == {"provider": "openai", "api_key": "sk-org"}


def test_load_settings_byo_org_flag_disabled_returns_empty():
    rows = [_FakeRow("provider", "openai"), _FakeRow("api_key", "sk-org")]
    with _patch_session(rows), _patch_flag_state("disabled"):
        out = creds._load_org_llm_settings(str(uuid.uuid4()))
    assert out == {}


def test_load_settings_byo_org_flag_error_raises_no_silent_reroute():
    # codex finding: a BYO-configured org hitting a flag-lookup error must raise
    # (no silent reroute to the platform LLM), NOT return {}.
    rows = [_FakeRow("provider", "openai"), _FakeRow("api_key", "sk-org")]
    with _patch_session(rows), _patch_flag_state(RuntimeError("store down")):
        with pytest.raises(LLMAuthError):
            creds._load_org_llm_settings(str(uuid.uuid4()))


def test_load_settings_non_byo_org_flag_error_returns_empty():
    # An org with NO BYO settings is unaffected by a flag-lookup error: there is
    # nothing to gate and no residency concern, so it returns {} (no raise) and
    # never even performs the flag lookup.
    with _patch_session([]), _patch_flag_state(RuntimeError("store down")):
        out = creds._load_org_llm_settings(str(uuid.uuid4()))
    assert out == {}


# ---------------------------------------------------------------------------
# Admin gate: require_byo_llm_access
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def session_maker(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db_path = tmp_path / "byo-llm-flag.db"
    async_url = f"sqlite+aiosqlite:///{db_path}"
    monkeypatch.setenv("POSTGRES_URI", f"sqlite:///{db_path}")
    engine = create_async_engine(async_url)
    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(sync_conn, tables=_TABLES)
        )
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


async def _seed(
    session_maker,
    *,
    tier: str = "team",
    flag_enabled: bool | None = True,
    override_enabled: bool | None = None,
) -> str:
    """Seed an org (+license) and optionally the byo_llm flag/override.

    flag_enabled=None omits the FeatureFlag row entirely (unregistered).
    """
    org_id = uuid.uuid4()
    async with session_maker() as session:
        session.add_all(
            [
                Organization(id=org_id, slug=f"{tier}-org", name="Org", tier=tier),
                OrgLicense(org_id=org_id, tier=tier),
            ]
        )
        if flag_enabled is not None:
            flag = FeatureFlag(
                key="byo_llm",
                name="BYO LLM",
                category="analytics",
                min_tier="team",
                is_enabled=flag_enabled,
            )
            session.add(flag)
            await session.flush()
            if override_enabled is not None:
                session.add(
                    OrgFeatureOverride(
                        org_id=org_id,
                        feature_id=flag.id,
                        is_enabled=override_enabled,
                    )
                )
        await session.commit()
    return str(org_id)


@pytest.mark.asyncio
async def test_admin_gate_allows_team_with_flag_enabled(session_maker):
    org_id = await _seed(session_maker, tier="team", flag_enabled=True)
    async with session_maker() as session:
        await require_byo_llm_access(session, org_id)  # must not raise


@pytest.mark.asyncio
async def test_admin_gate_blocks_when_flag_globally_disabled(session_maker):
    org_id = await _seed(session_maker, tier="team", flag_enabled=False)
    async with session_maker() as session:
        with pytest.raises(LLMSettingsAccessError) as exc:
            await require_byo_llm_access(session, org_id)
    assert exc.value.status_code == 403
    assert exc.value.detail["error"] == "feature_not_enabled"


@pytest.mark.asyncio
async def test_admin_gate_blocks_when_org_override_disables(session_maker):
    org_id = await _seed(
        session_maker, tier="team", flag_enabled=True, override_enabled=False
    )
    async with session_maker() as session:
        with pytest.raises(LLMSettingsAccessError) as exc:
            await require_byo_llm_access(session, org_id)
    assert exc.value.status_code == 403
    assert exc.value.detail["error"] == "feature_not_enabled"


@pytest.mark.asyncio
async def test_admin_gate_blocks_community_tier_as_not_licensed(session_maker):
    org_id = await _seed(session_maker, tier="community", flag_enabled=True)
    async with session_maker() as session:
        with pytest.raises(LLMSettingsAccessError) as exc:
            await require_byo_llm_access(session, org_id)
    assert exc.value.status_code == 402
    assert exc.value.detail["error"] == "feature_not_licensed"


@pytest.mark.asyncio
async def test_admin_gate_falls_back_to_tier_when_flag_unregistered(session_maker):
    # No FeatureFlag row at all: behaves like the prior tier-only gate.
    org_id = await _seed(session_maker, tier="team", flag_enabled=None)
    async with session_maker() as session:
        await require_byo_llm_access(session, org_id)  # must not raise


@pytest.mark.asyncio
async def test_admin_gate_fails_closed_when_flag_lookup_errors(session_maker):
    # codex finding: the admin gate must NOT swallow a genuine flag-lookup
    # error and then allow the request. A real error propagates -> the request
    # is denied (fail closed), even for a TEAM-tier org.
    org_id = await _seed(session_maker, tier="team", flag_enabled=True)
    with patch(
        "dev_health_ops.api.admin.llm_settings.byo_llm_flag_state",
        side_effect=RuntimeError("licensing store unavailable"),
    ):
        async with session_maker() as session:
            with pytest.raises(RuntimeError):
                await require_byo_llm_access(session, org_id)


@pytest.mark.asyncio
async def test_admin_gate_allows_cleanup_when_flag_disabled(session_maker):
    # codex finding: a kill switch must stop reads/writes/runtime use but must
    # NOT trap stored secrets. The DELETE path passes allow_disabled_flag=True
    # so an admin can clean up BYO settings even when the flag is disabled.
    org_id = await _seed(session_maker, tier="team", flag_enabled=False)
    async with session_maker() as session:
        # Default (PUT/GET) still denied ...
        with pytest.raises(LLMSettingsAccessError) as exc:
            await require_byo_llm_access(session, org_id)
        assert exc.value.detail["error"] == "feature_not_enabled"
    async with session_maker() as session:
        # ... but cleanup (DELETE) is allowed.
        await require_byo_llm_access(session, org_id, allow_disabled_flag=True)
