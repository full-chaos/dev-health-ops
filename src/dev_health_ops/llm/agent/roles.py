"""Per-role capability certification model for Ask Dev provider readiness.

CHAOS-3285: a provider/model may be certified for one Ask Dev role and not
another (a small strict-schema call may fit a fixed output budget while the
full tool-registry agent loop exhausts it). This module is the role-aware
successor to the single binary ``AgentReadinessRecord`` in ``readiness.py``.

Backward compatibility / invalidation semantics (explicit, per CHAOS-3285
plan R5): the per-role profile is persisted under a **new** settings key
(``ROLE_CERTIFICATION_SETTING_KEY`` / ``PLATFORM_ROLE_CERTIFICATION_SETTING_KEY``),
never the legacy ``ask_dev_agent_readiness`` / ``platform_ask_dev_agent_readiness``
keys the old binary store still owns. The new store never reads the old key,
so an old binary record can never be interpreted as a certification for any
role -- it is simply invisible to this module. A rolled-back deploy that
reads the old key back is unaffected by anything written here.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field, replace
from enum import Enum
from typing import Protocol

from dev_health_ops.api.services.configuration.generic import SettingsService
from dev_health_ops.models.settings import SettingCategory

#: Version of the persisted profile envelope's own JSON shape -- distinct
#: from ``readiness_version``/``certification_key`` below, which invalidate
#: an individual role's *capability* certification. Bumping this constant is
#: for when the envelope's storage shape itself changes incompatibly.
ROLE_CERTIFICATION_PROFILE_VERSION = "ask-dev-role-certification.v1"

ROLE_CERTIFICATION_SETTING_KEY = "ask_dev_role_certification_profile"
PLATFORM_ROLE_CERTIFICATION_SETTING_KEY = "platform_ask_dev_role_certification_profile"


class AgentRole(str, Enum):
    """The Wave 3.1 Ask Dev provider roles (CHAOS-3285 plan §2.1).

    Only ``LEGACY_AGENT`` has a working probe in this changeset.
    ``INTENT_CLASSIFICATION`` and ``ANSWER_FRAME_NARRATIVE`` land as enum
    members and store slots only -- their probes are CHAOS-3285 PR4, gated
    on CHAOS-3294's ``dev_question_intent.v1`` / ``dev_narrative.v1``
    schemas landing on main.
    """

    LEGACY_AGENT = "legacy_agent"
    INTENT_CLASSIFICATION = "intent_classification"
    ANSWER_FRAME_NARRATIVE = "answer_frame_narrative"


class RoleCertificationState(str, Enum):
    """Verdict for one (provider, role) certification attempt.

    ``COMPATIBLE`` and ``INCOMPATIBLE``/``FAILED`` split what the old binary
    ``AgentReadinessOutcome.FAILED`` conflated: a deterministic, structural
    rejection (unsupported request shape, output/reasoning exhaustion,
    malformed structured output) will not resolve by retrying and must never
    be treated as "try again later"; a transient/environmental failure
    (timeout, rate limit, transport, missing credentials) might.
    """

    COMPATIBLE = "compatible"
    INCOMPATIBLE = "incompatible"
    FAILED = "failed"
    STALE = "stale"
    UNCHECKED = "unchecked"


@dataclass(frozen=True, slots=True)
class RoleCertificationRecord:
    role: AgentRole
    certification_key: str
    readiness_version: str
    checked_at: str
    state: RoleCertificationState
    safe_error_code: str | None = None

    def is_current(self, *, certification_key: str) -> bool:
        """True only for a COMPATIBLE record whose capability inputs still match.

        An INCOMPATIBLE, FAILED, or STALE record is never current -- the
        runtime must not select a provider on the strength of a record that
        isn't an affirmative, still-valid COMPATIBLE verdict (CHAOS-3285
        acceptance: "an old record must not read as certified for any role").
        """

        return (
            self.state is RoleCertificationState.COMPATIBLE
            and self.certification_key == certification_key
        )


@dataclass(frozen=True, slots=True)
class RoleCertificationProfile:
    """The full per-role certification envelope persisted for one provider slot."""

    version: str = ROLE_CERTIFICATION_PROFILE_VERSION
    records: Mapping[AgentRole, RoleCertificationRecord] = field(default_factory=dict)

    def for_role(self, role: AgentRole) -> RoleCertificationRecord | None:
        return self.records.get(role)

    def with_record(self, record: RoleCertificationRecord) -> RoleCertificationProfile:
        """Return a new profile with ``record`` set, leaving every other role's
        stored record untouched -- certifying one role must never clobber an
        already-certified sibling role."""

        updated = dict(self.records)
        updated[record.role] = record
        return replace(self, records=updated)


def _empty_profile() -> RoleCertificationProfile:
    return RoleCertificationProfile(
        version=ROLE_CERTIFICATION_PROFILE_VERSION, records={}
    )


class RoleCertificationStore(Protocol):
    async def load(self) -> RoleCertificationProfile: ...

    async def save(self, profile: RoleCertificationProfile) -> None: ...


class SettingsRoleCertificationStore:
    """Persists the per-role profile envelope under a key distinct from the
    legacy binary readiness record (see module docstring for the
    backward-compatibility rationale)."""

    def __init__(
        self, settings: SettingsService, *, key: str = ROLE_CERTIFICATION_SETTING_KEY
    ):
        self._settings = settings
        self._key = key

    async def load(self) -> RoleCertificationProfile:
        raw = await self._settings.get(self._key, category=SettingCategory.LLM.value)
        if not raw:
            return _empty_profile()
        try:
            payload = json.loads(raw)
            if (
                not isinstance(payload, Mapping)
                or payload.get("version") != ROLE_CERTIFICATION_PROFILE_VERSION
            ):
                # An envelope-shape mismatch (including a future version this
                # build predates) is never partially trusted -- every role
                # reads as UNCHECKED rather than risk misreading a foreign
                # shape as a certification.
                return _empty_profile()
            raw_records = payload.get("records")
            if not isinstance(raw_records, Mapping):
                return _empty_profile()
            records: dict[AgentRole, RoleCertificationRecord] = {}
            for role_value, record_payload in raw_records.items():
                role = AgentRole(role_value)
                records[role] = RoleCertificationRecord(
                    role=role,
                    certification_key=str(record_payload["certification_key"]),
                    readiness_version=str(record_payload["readiness_version"]),
                    checked_at=str(record_payload["checked_at"]),
                    state=RoleCertificationState(record_payload["state"]),
                    safe_error_code=(
                        str(record_payload["safe_error_code"])
                        if record_payload.get("safe_error_code")
                        else None
                    ),
                )
            return RoleCertificationProfile(
                version=ROLE_CERTIFICATION_PROFILE_VERSION, records=records
            )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            return _empty_profile()

    async def save(self, profile: RoleCertificationProfile) -> None:
        payload = {
            "version": ROLE_CERTIFICATION_PROFILE_VERSION,
            "records": {
                record.role.value: {
                    **asdict(record),
                    "role": record.role.value,
                    "state": record.state.value,
                }
                for record in profile.records.values()
            },
        }
        await self._settings.set(
            self._key,
            json.dumps(payload, separators=(",", ":"), sort_keys=True),
            category=SettingCategory.LLM.value,
            description="Safe per-role Ask Dev provider certification result",
        )


__all__ = [
    "PLATFORM_ROLE_CERTIFICATION_SETTING_KEY",
    "ROLE_CERTIFICATION_PROFILE_VERSION",
    "ROLE_CERTIFICATION_SETTING_KEY",
    "AgentRole",
    "RoleCertificationProfile",
    "RoleCertificationRecord",
    "RoleCertificationState",
    "RoleCertificationStore",
    "SettingsRoleCertificationStore",
]
