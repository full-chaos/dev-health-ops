from __future__ import annotations

import json
from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from dev_health_ops.api.services.configuration.generic import SettingsService
from dev_health_ops.llm.agent.roles import (
    ROLE_CERTIFICATION_PROFILE_VERSION,
    AgentRole,
    RoleCertificationProfile,
    RoleCertificationRecord,
    RoleCertificationState,
    SettingsRoleCertificationStore,
)
from dev_health_ops.models.git import Base
from dev_health_ops.models.settings import Setting
from tests._helpers import tables_of

_TABLES = tables_of(Setting)


class _FakeSettingsService:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    async def get(self, key: str, category: str, default=None):
        del category
        return self.values.get(key, default)

    async def set(self, key: str, value: str, *, category: str, description: str):
        del category, description
        self.values[key] = value


def _record(
    role: AgentRole = AgentRole.LEGACY_AGENT,
    *,
    state: RoleCertificationState = RoleCertificationState.COMPATIBLE,
    certification_key: str = "key-01",
) -> RoleCertificationRecord:
    return RoleCertificationRecord(
        role=role,
        certification_key=certification_key,
        readiness_version="ask-dev-agent-v3",
        checked_at="2026-08-02T00:00:00+00:00",
        state=state,
        safe_error_code=None
        if state is RoleCertificationState.COMPATIBLE
        else "output_exhausted",
    )


def test_is_current_requires_compatible_state_and_matching_key() -> None:
    compatible = _record(
        state=RoleCertificationState.COMPATIBLE, certification_key="k1"
    )
    assert compatible.is_current(certification_key="k1")
    assert not compatible.is_current(certification_key="k2")

    incompatible = _record(
        state=RoleCertificationState.INCOMPATIBLE, certification_key="k1"
    )
    assert not incompatible.is_current(certification_key="k1")


def test_with_record_does_not_clobber_other_roles() -> None:
    profile = RoleCertificationProfile()
    legacy = _record(AgentRole.LEGACY_AGENT)
    profile = profile.with_record(legacy)
    intent = _record(AgentRole.INTENT_CLASSIFICATION, certification_key="k-intent")
    profile = profile.with_record(intent)

    assert profile.for_role(AgentRole.LEGACY_AGENT) == legacy
    assert profile.for_role(AgentRole.INTENT_CLASSIFICATION) == intent
    assert profile.for_role(AgentRole.ANSWER_FRAME_NARRATIVE) is None


@pytest.mark.asyncio
async def test_store_round_trips_a_profile() -> None:
    settings = _FakeSettingsService()
    store = SettingsRoleCertificationStore(settings)  # type: ignore[arg-type]

    empty = await store.load()
    assert empty.records == {}

    profile = RoleCertificationProfile().with_record(_record(AgentRole.LEGACY_AGENT))
    await store.save(profile)

    reloaded = await store.load()
    assert reloaded.for_role(AgentRole.LEGACY_AGENT) == _record(AgentRole.LEGACY_AGENT)


@pytest.mark.asyncio
async def test_save_record_writes_only_that_roles_own_key() -> None:
    """The storage-level guarantee behind the sibling-survival fix: writing
    one role never touches another role's settings row at all."""

    settings = _FakeSettingsService()
    store = SettingsRoleCertificationStore(settings)  # type: ignore[arg-type]

    await store.save_record(_record(AgentRole.LEGACY_AGENT))

    assert set(settings.values) == {"ask_dev_role_certification_profile:legacy_agent"}


@pytest.mark.asyncio
async def test_concurrent_writes_to_different_roles_both_survive_in_memory() -> None:
    """The lost-update reproduction, in-memory: two independent stores
    (simulating two concurrent request handlers) each load a snapshot BEFORE
    either commits, then each certifies a DIFFERENT role. Under the old
    whole-envelope design, whichever save() landed second would overwrite
    the sibling role's row with a blob that never knew about it. Per-role
    storage makes this safe by construction: reproduced here directly
    against the store (not through RoleReadinessService, since that no
    longer even performs a read before writing)."""

    settings = _FakeSettingsService()
    store_a = SettingsRoleCertificationStore(settings)  # type: ignore[arg-type]
    store_b = SettingsRoleCertificationStore(settings)  # type: ignore[arg-type]

    # Both "sessions" observe the same empty starting state.
    profile_a = await store_a.load()
    profile_b = await store_b.load()
    assert profile_a.records == {} and profile_b.records == {}

    legacy = _record(AgentRole.LEGACY_AGENT, certification_key="k-legacy")
    intent = _record(AgentRole.INTENT_CLASSIFICATION, certification_key="k-intent")

    # A certifies and "commits" first...
    await store_a.save(profile_a.with_record(legacy))
    # ...then B certifies a DIFFERENT role from its stale pre-A snapshot and
    # "commits" second.
    await store_b.save(profile_b.with_record(intent))

    reloaded = await SettingsRoleCertificationStore(settings).load()  # type: ignore[arg-type]
    assert reloaded.for_role(AgentRole.LEGACY_AGENT) == legacy
    assert reloaded.for_role(AgentRole.INTENT_CLASSIFICATION) == intent


@pytest.mark.asyncio
async def test_concurrent_writes_to_different_roles_both_survive_across_db_sessions(
    tmp_path: Path,
) -> None:
    """The same barrier, against a real database with two independent
    SQLAlchemy sessions/transactions -- the two-session proof, not just an
    in-memory stand-in. Interleaves the sessions so B's read genuinely
    precedes A's commit, then commits B AFTER A, and asserts both roles'
    records are present regardless of that commit order."""

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'roles-barrier.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(
            lambda sync_connection: Base.metadata.create_all(
                sync_connection, tables=_TABLES
            )
        )
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    org_id = "org-roles-barrier"
    legacy = _record(AgentRole.LEGACY_AGENT, certification_key="k-legacy")
    intent = _record(AgentRole.INTENT_CLASSIFICATION, certification_key="k-intent")

    async with maker() as session_a, maker() as session_b:
        store_a = SettingsRoleCertificationStore(SettingsService(session_a, org_id))
        store_b = SettingsRoleCertificationStore(SettingsService(session_b, org_id))

        # Both sessions observe the pre-write state before either writes.
        await store_a.load()
        await store_b.load()

        # Session B's write and commit happen BEFORE session A's, deliberately
        # the reverse of certification order below -- proving the fix does
        # not depend on commit order.
        await store_b.save_record(intent)
        await session_b.commit()

        await store_a.save_record(legacy)
        await session_a.commit()

    async with maker() as verify_session:
        reloaded = await SettingsRoleCertificationStore(
            SettingsService(verify_session, org_id)
        ).load()

    assert reloaded.for_role(AgentRole.LEGACY_AGENT) == legacy
    assert reloaded.for_role(AgentRole.INTENT_CLASSIFICATION) == intent
    assert reloaded.for_role(AgentRole.ANSWER_FRAME_NARRATIVE) is None

    await engine.dispose()


@pytest.mark.asyncio
async def test_store_never_reads_the_legacy_binary_readiness_key() -> None:
    """CHAOS-3285 backward compatibility: a pre-existing single-role
    AgentReadinessRecord stored under the OLD binary key must never be
    interpreted as a certification for any role by the NEW per-role store --
    the new store reads a different key entirely."""

    settings = _FakeSettingsService()
    settings.values["ask_dev_agent_readiness"] = json.dumps(
        {
            "fingerprint": "old-fingerprint",
            "readiness_version": "ask-dev-agent-v3",
            "checked_at": "2026-08-01T00:00:00+00:00",
            "outcome": "ready",
            "safe_error_code": None,
        }
    )

    store = SettingsRoleCertificationStore(settings)  # type: ignore[arg-type]
    profile = await store.load()

    assert profile.records == {}
    for role in AgentRole:
        assert profile.for_role(role) is None


@pytest.mark.asyncio
async def test_store_never_trusts_a_mismatched_envelope_version() -> None:
    settings = _FakeSettingsService()
    settings.values["ask_dev_role_certification_profile:legacy_agent"] = json.dumps(
        {
            "version": "some-future-version",
            "record": {
                "role": "legacy_agent",
                "certification_key": "k1",
                "readiness_version": "ask-dev-agent-v3",
                "checked_at": "2026-08-01T00:00:00+00:00",
                "state": "compatible",
                "safe_error_code": None,
            },
        }
    )
    store = SettingsRoleCertificationStore(settings)  # type: ignore[arg-type]

    profile = await store.load()

    assert profile.records == {}


@pytest.mark.asyncio
async def test_store_never_trusts_a_role_mismatched_row() -> None:
    """Defense in depth: a row found under role R's own key must actually
    describe role R. A hand-edited or corrupted mismatch is never trusted."""

    settings = _FakeSettingsService()
    settings.values["ask_dev_role_certification_profile:legacy_agent"] = json.dumps(
        {
            "version": ROLE_CERTIFICATION_PROFILE_VERSION,
            "record": {
                "role": "intent_classification",
                "certification_key": "k1",
                "readiness_version": "ask-dev-agent-v3",
                "checked_at": "2026-08-01T00:00:00+00:00",
                "state": "compatible",
                "safe_error_code": None,
            },
        }
    )
    store = SettingsRoleCertificationStore(settings)  # type: ignore[arg-type]

    profile = await store.load()

    assert profile.for_role(AgentRole.LEGACY_AGENT) is None


def test_profile_version_is_pinned() -> None:
    assert ROLE_CERTIFICATION_PROFILE_VERSION == "ask-dev-role-certification.v1"
