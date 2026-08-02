from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

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


@pytest_asyncio.fixture
async def session_maker(
    tmp_path: Path,
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    """A real aiosqlite-backed session factory. save_record() now performs a
    dialect-aware atomic upsert (INSERT ... ON CONFLICT DO UPDATE) directly
    against the settings table, so its own tests need a real SQLAlchemy
    session/engine -- a plain dict-backed fake can no longer stand in for
    the write path (it still works fine for read-only tests of load())."""

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'roles.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(
            lambda sync_connection: Base.metadata.create_all(
                sync_connection, tables=_TABLES
            )
        )
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        yield maker
    finally:
        await engine.dispose()


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
async def test_store_round_trips_a_profile(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    async with session_maker() as session:
        store = SettingsRoleCertificationStore(SettingsService(session, "org-01"))

        empty = await store.load()
        assert empty.records == {}

        profile = RoleCertificationProfile().with_record(
            _record(AgentRole.LEGACY_AGENT)
        )
        await store.save(profile)
        await session.commit()

    async with session_maker() as verify_session:
        reloaded = await SettingsRoleCertificationStore(
            SettingsService(verify_session, "org-01")
        ).load()
    assert reloaded.for_role(AgentRole.LEGACY_AGENT) == _record(AgentRole.LEGACY_AGENT)


@pytest.mark.asyncio
async def test_save_record_writes_only_that_roles_own_key(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    """The storage-level guarantee behind the sibling-survival fix: writing
    one role never touches another role's settings row at all."""

    async with session_maker() as session:
        store = SettingsRoleCertificationStore(SettingsService(session, "org-01"))
        await store.save_record(_record(AgentRole.LEGACY_AGENT))
        await session.commit()

    async with session_maker() as verify_session:
        rows = (
            (
                await verify_session.execute(
                    select(Setting.key).where(Setting.org_id == "org-01")
                )
            )
            .scalars()
            .all()
        )
    assert set(rows) == {"ask_dev_role_certification_profile:legacy_agent"}


@pytest.mark.asyncio
async def test_concurrent_writes_to_different_roles_both_survive_across_db_sessions(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    """The two-session sibling-role barrier: two independent SQLAlchemy
    sessions/transactions each observe the pre-write state before either
    writes, then each certifies a DIFFERENT role. Under the old
    whole-envelope design, whichever save() landed second would overwrite
    the sibling role's row with a blob that never knew about it. Per-role
    storage makes this safe by construction. Session B's write and commit
    happen BEFORE session A's -- deliberately the reverse of certification
    order below -- proving the fix does not depend on commit order."""

    org_id = "org-roles-barrier"
    legacy = _record(AgentRole.LEGACY_AGENT, certification_key="k-legacy")
    intent = _record(AgentRole.INTENT_CLASSIFICATION, certification_key="k-intent")

    async with session_maker() as session_a, session_maker() as session_b:
        store_a = SettingsRoleCertificationStore(SettingsService(session_a, org_id))
        store_b = SettingsRoleCertificationStore(SettingsService(session_b, org_id))

        # Both sessions observe the pre-write state before either writes.
        await store_a.load()
        await store_b.load()

        await store_b.save_record(intent)
        await session_b.commit()

        await store_a.save_record(legacy)
        await session_a.commit()

    async with session_maker() as verify_session:
        reloaded = await SettingsRoleCertificationStore(
            SettingsService(verify_session, org_id)
        ).load()

    assert reloaded.for_role(AgentRole.LEGACY_AGENT) == legacy
    assert reloaded.for_role(AgentRole.INTENT_CLASSIFICATION) == intent
    assert reloaded.for_role(AgentRole.ANSWER_FRAME_NARRATIVE) is None


@pytest.mark.asyncio
async def test_concurrent_first_writes_to_the_same_role_do_not_raise(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    """CHAOS-3285 round 2 (Codex MEDIUM): two sessions truly racing a
    first-ever certification of the SAME role -- via asyncio.gather, so
    both sessions' own internal "does a row exist yet" checks and their
    INSERT attempts are genuinely concurrent, not sequential. Before the
    atomic-upsert fix (a bare select-then-insert), both sessions could
    observe "no row yet" and both attempt INSERT; the loser's flush raised
    IntegrityError, and -- caught without a rollback/savepoint by a caller
    (platform_ask_dev.py / settings.py's best-effort role-certify try/except)
    -- poisoned that session for any later query in the same transaction,
    including an unrelated earlier write (the binary readiness certify()
    call each of those routes runs just before). The single-statement
    ``INSERT ... ON CONFLICT DO UPDATE`` this fix uses is atomic at the
    database level: there is no window in which two overlapping statements
    can both decide "insert" and conflict, so this must complete without
    raising regardless of how the two sessions' internal operations
    interleave. The exact winner between two truly concurrent writers is
    not asserted here (it is whichever committed last, at the database's
    discretion) -- only that a value from one of the two writers, not a
    corrupted mix, is what a later reader observes.
    """

    org_id = "org-same-role-race"
    key = "k-shared"
    compatible = _record(
        AgentRole.LEGACY_AGENT,
        state=RoleCertificationState.COMPATIBLE,
        certification_key=key,
    )
    incompatible = _record(
        AgentRole.LEGACY_AGENT,
        state=RoleCertificationState.INCOMPATIBLE,
        certification_key=key,
    )

    async def certify(record: RoleCertificationRecord) -> None:
        async with session_maker() as session:
            store = SettingsRoleCertificationStore(SettingsService(session, org_id))
            await store.load()
            await store.save_record(record)
            await session.commit()

    # Both writers' load()/save_record()/commit() sequences run concurrently
    # under the same event loop -- asyncio.gather does not serialize them
    # the way two sequential `await` calls in one coroutine would.
    await asyncio.gather(certify(compatible), certify(incompatible))

    async with session_maker() as verify_session:
        reloaded = await SettingsRoleCertificationStore(
            SettingsService(verify_session, org_id)
        ).load()

    winner = reloaded.for_role(AgentRole.LEGACY_AGENT)
    assert winner in (compatible, incompatible)


@pytest.mark.asyncio
async def test_sequential_writes_to_the_same_role_apply_last_commit_wins(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    """Stated winner policy, verified deterministically: when session B's
    write and commit happen strictly AFTER session A's commit, B's
    (opposing) verdict is what a later reader observes -- last COMMIT wins,
    not "the first insert wins" and not "an arbitrary/undefined result"."""

    org_id = "org-same-role-sequential"
    key = "k-shared"
    compatible = _record(
        AgentRole.LEGACY_AGENT,
        state=RoleCertificationState.COMPATIBLE,
        certification_key=key,
    )
    incompatible = _record(
        AgentRole.LEGACY_AGENT,
        state=RoleCertificationState.INCOMPATIBLE,
        certification_key=key,
    )

    async with session_maker() as session_a:
        store_a = SettingsRoleCertificationStore(SettingsService(session_a, org_id))
        await store_a.save_record(compatible)
        await session_a.commit()

    async with session_maker() as session_b:
        store_b = SettingsRoleCertificationStore(SettingsService(session_b, org_id))
        await store_b.save_record(incompatible)
        await session_b.commit()

    async with session_maker() as verify_session:
        reloaded = await SettingsRoleCertificationStore(
            SettingsService(verify_session, org_id)
        ).load()

    assert reloaded.for_role(AgentRole.LEGACY_AGENT) == incompatible


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


@pytest.mark.asyncio
async def test_savepoint_wrapped_failure_does_not_lose_an_earlier_write(
    session_maker: async_sessionmaker[AsyncSession],
) -> None:
    """CHAOS-3285 round 2 (Codex MEDIUM): direct proof of the SAVEPOINT
    pattern platform_ask_dev.py and settings.py now wrap the role-certify
    call in. A `try/except Exception: log` around a DB operation WITHOUT a
    rollback or savepoint leaves the session's transaction unable to accept
    further operations; the request's own session dependency then rolls
    back the WHOLE transaction on its eventual commit, silently discarding
    an earlier write already flushed into the same transaction (e.g. the
    binary readiness certify() call each route runs just before). Wrapping
    the risky operation in ``session.begin_nested()`` confines that damage
    to a SAVEPOINT: on failure, only the nested operation rolls back: the
    earlier write and the session's usability for the rest of the request
    both survive.
    """

    org_id = "org-savepoint-proof"
    earlier_write = _record(AgentRole.LEGACY_AGENT, certification_key="k1")

    async with session_maker() as session:
        store = SettingsRoleCertificationStore(SettingsService(session, org_id))
        # Stand-in for "the binary readiness certify() call that already
        # ran, and must survive, before the role-certify attempt below."
        await store.save_record(earlier_write)

        with pytest.raises(RuntimeError, match="simulated"):
            async with session.begin_nested():
                # Stand-in for an unexpected failure during the role-certify
                # attempt -- a bare `except Exception` around this, with no
                # savepoint, is exactly what Codex round 2 flagged.
                raise RuntimeError("simulated unexpected DB/provider failure")

        # The session must still be usable here -- a poisoned session (the
        # pre-fix shape) would raise PendingRollbackError on this next query
        # instead of returning the earlier write untouched.
        reloaded = await store.load()
        assert reloaded.for_role(AgentRole.LEGACY_AGENT) == earlier_write

        await session.commit()

    async with session_maker() as verify_session:
        final = await SettingsRoleCertificationStore(
            SettingsService(verify_session, org_id)
        ).load()
    assert final.for_role(AgentRole.LEGACY_AGENT) == earlier_write
