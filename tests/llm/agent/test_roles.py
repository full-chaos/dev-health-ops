from __future__ import annotations

import json

import pytest

from dev_health_ops.llm.agent.roles import (
    ROLE_CERTIFICATION_PROFILE_VERSION,
    AgentRole,
    RoleCertificationProfile,
    RoleCertificationRecord,
    RoleCertificationState,
    SettingsRoleCertificationStore,
)


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
    settings.values["ask_dev_role_certification_profile"] = json.dumps(
        {"version": "some-future-version", "records": {}}
    )
    store = SettingsRoleCertificationStore(settings)  # type: ignore[arg-type]

    profile = await store.load()

    assert profile.records == {}


def test_profile_version_is_pinned() -> None:
    assert ROLE_CERTIFICATION_PROFILE_VERSION == "ask-dev-role-certification.v1"
