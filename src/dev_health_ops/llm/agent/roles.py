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
import uuid
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Protocol

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.api.services.configuration.generic import SettingsService
from dev_health_ops.models.settings import Setting, SettingCategory

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

    async def save_record(self, record: RoleCertificationRecord) -> None: ...

    async def save(self, profile: RoleCertificationProfile) -> None: ...


def _role_setting_key(base_key: str, role: AgentRole) -> str:
    return f"{base_key}:{role.value}"


async def _upsert_setting_row(
    session: AsyncSession,
    *,
    org_id: str,
    category: str,
    key: str,
    value: str,
    description: str,
) -> None:
    """Atomic, single-statement insert-or-update of exactly one settings row,
    targeting the table's own ``uq_settings_org_category_key`` unique
    constraint -- never ``SettingsService.set()``'s select-then-insert-or-
    update, which races under true concurrent writers: two sessions can both
    observe "no row yet", both attempt INSERT, and the loser's flush raises
    ``IntegrityError``. Bypassing the ORM here mirrors the codebase's
    existing dialect-aware upsert precedent (``sync.dispatch_outbox.
    upsert_outbox_wakeup``).

    Winner policy for two sessions certifying the SAME role concurrently:
    **last COMMIT wins** -- ``ON CONFLICT DO UPDATE`` always applies whichever
    writer's transaction commits last, overwriting the other's value. This is
    the same "the most recent preflight result governs" semantic an operator
    already expects from re-running preflight; this fix only makes it
    race-safe (no uncaught ``IntegrityError``) rather than changing what it
    means to certify a role twice.
    """

    now = datetime.now(timezone.utc)
    dialect_name = session.get_bind().dialect.name
    table = Setting.__table__
    insert_values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "org_id": org_id,
        "category": category,
        "key": key,
        "value": value,
        "is_encrypted": False,
        "description": description,
        "created_at": now,
        "updated_at": now,
    }
    if dialect_name in ("postgresql", "sqlite"):
        dialect_insert: Any
        if dialect_name == "postgresql":
            from sqlalchemy.dialects.postgresql import insert as dialect_insert
        else:
            from sqlalchemy.dialects.sqlite import insert as dialect_insert

        stmt = dialect_insert(table).values(**insert_values)
        stmt = stmt.on_conflict_do_update(
            index_elements=["org_id", "category", "key"],
            set_={
                "value": stmt.excluded.value,
                "description": stmt.excluded.description,
                "updated_at": stmt.excluded.updated_at,
            },
        )
        await session.execute(stmt)
        await session.flush()
        return
    # Any other dialect: still atomic (a SAVEPOINT rolls back only this
    # operation on conflict, never the caller's outer transaction, unlike an
    # uncaught IntegrityError from a bare select-then-insert), just not a
    # single-statement native upsert.
    async with session.begin_nested():
        existing = (
            await session.execute(
                select(Setting).where(
                    Setting.org_id == org_id,
                    Setting.category == category,
                    Setting.key == key,
                )
            )
        ).scalar_one_or_none()
        if existing is None:
            session.add(
                Setting(
                    key=key,
                    category=category,
                    value=value,
                    org_id=org_id,
                    description=description,
                )
            )
        else:
            existing.value = value
            existing.description = description
        await session.flush()


class SettingsRoleCertificationStore:
    """Persists one settings row **per role**, not one shared envelope blob.

    This is deliberate, not incidental: an earlier version of this store held
    the whole three-role profile as a single JSON blob under one settings
    key, read-modified-written on every certification. Two roles certifying
    concurrently (session A certifies ``legacy_agent``, session B certifies
    ``intent_classification``, both starting from the same snapshot) raced a
    classic lost update -- whichever session's ``save()`` committed second
    silently discarded the other's sibling record, even though the two
    updates touched logically independent roles. Splitting storage per role
    removes the shared mutable state that race depended on: two different
    roles are two different settings rows, so there is no read-before-write
    dependency between them and no commit order can lose either one.

    A same-role concurrent write (two sessions certifying the identical role
    at the identical moment) is handled by ``_upsert_setting_row``'s atomic
    ``INSERT ... ON CONFLICT DO UPDATE`` -- see that function's docstring for
    the stated winner policy.
    """

    def __init__(
        self, settings: SettingsService, *, key: str = ROLE_CERTIFICATION_SETTING_KEY
    ):
        self._settings = settings
        self._base_key = key

    async def load(self) -> RoleCertificationProfile:
        records: dict[AgentRole, RoleCertificationRecord] = {}
        for role in AgentRole:
            record = await self._load_role(role)
            if record is not None:
                records[role] = record
        return RoleCertificationProfile(
            version=ROLE_CERTIFICATION_PROFILE_VERSION, records=records
        )

    async def _load_role(self, role: AgentRole) -> RoleCertificationRecord | None:
        raw = await self._settings.get(
            _role_setting_key(self._base_key, role), category=SettingCategory.LLM.value
        )
        if not raw:
            return None
        try:
            payload = json.loads(raw)
            if (
                not isinstance(payload, Mapping)
                or payload.get("version") != ROLE_CERTIFICATION_PROFILE_VERSION
            ):
                # An envelope-shape mismatch (including a future version this
                # build predates) is never partially trusted -- the role
                # reads as UNCHECKED rather than risk misreading a foreign
                # shape as a certification.
                return None
            record_payload = payload.get("record")
            if not isinstance(record_payload, Mapping):
                return None
            stored_role = AgentRole(record_payload["role"])
            if stored_role is not role:
                # Defensive: a row found under this role's own key must
                # actually describe this role. A mismatch means storage was
                # corrupted or hand-edited -- never trust it either way.
                return None
            return RoleCertificationRecord(
                role=stored_role,
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
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            return None

    async def save_record(self, record: RoleCertificationRecord) -> None:
        """Atomically upsert exactly one role's row. This is the write path
        ``RoleReadinessService`` uses -- no read-modify-write of any other
        role's data, so certifying one role can never lose another's, and no
        select-then-insert race against a concurrent same-role write."""

        payload = {
            "version": ROLE_CERTIFICATION_PROFILE_VERSION,
            "record": {
                **asdict(record),
                "role": record.role.value,
                "state": record.state.value,
            },
        }
        await _upsert_setting_row(
            self._settings.session,
            org_id=self._settings.org_id,
            category=SettingCategory.LLM.value,
            key=_role_setting_key(self._base_key, record.role),
            value=json.dumps(payload, separators=(",", ":"), sort_keys=True),
            description=f"Safe Ask Dev {record.role.value} role certification result",
        )

    async def save(self, profile: RoleCertificationProfile) -> None:
        """Upsert every record in ``profile`` -- a convenience for tests and
        bulk seeding. Each record is written through ``save_record``, so this
        is exactly as safe (and touches exactly the same rows) as calling
        ``save_record`` once per record."""

        for record in profile.records.values():
            await self.save_record(record)


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
