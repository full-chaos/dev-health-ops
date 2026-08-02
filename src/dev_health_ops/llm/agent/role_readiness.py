"""Ties a per-role probe to the per-role certification store (CHAOS-3285).

Only ``AgentRole.LEGACY_AGENT`` has a working probe in this changeset;
``INTENT_CLASSIFICATION`` and ``ANSWER_FRAME_NARRATIVE`` are valid role
arguments the store can hold a slot for, but certifying them raises
``NotImplementedError`` until their probes land (CHAOS-3285 PR4).

The caller supplies ``certification_key`` -- computed by
``api.dev.production_runtime._readiness_fingerprint(candidate, role=role)``,
which folds every capability input that must invalidate a stale
certification (prompt/tool-contract/budget-policy versions, provider
identity, role). This module deliberately does not import anything from
``api.dev`` itself, to keep the dependency direction one-way (``api.dev`` ->
``llm.agent``, never the reverse) at the module-import level.
"""

from __future__ import annotations

from datetime import datetime, timezone

from .contracts import AgentLLMProvider, CancellationSignal
from .probes.legacy_agent import certify_legacy_agent
from .roles import (
    AgentRole,
    RoleCertificationRecord,
    RoleCertificationStore,
)


class RoleReadinessService:
    def __init__(self, store: RoleCertificationStore):
        self._store = store

    async def certify_role(
        self,
        role: AgentRole,
        provider: AgentLLMProvider,
        *,
        certification_key: str,
        timeout_seconds: float = 30,
        signal: CancellationSignal | None = None,
    ) -> RoleCertificationRecord:
        if role is not AgentRole.LEGACY_AGENT:
            raise NotImplementedError(
                f"{role.value} has no production-representative probe yet "
                "(CHAOS-3285 PR4)"
            )
        probe_result = await certify_legacy_agent(
            provider, timeout_seconds=timeout_seconds, signal=signal
        )
        record = RoleCertificationRecord(
            role=role,
            certification_key=certification_key,
            readiness_version=provider.capabilities.readiness_version,
            checked_at=datetime.now(timezone.utc).isoformat(),
            state=probe_result.state,
            safe_error_code=probe_result.safe_error_code,
        )
        # Writes exactly this role's row -- no read-modify-write of the
        # whole profile, so a concurrent certification of a sibling role can
        # never be lost regardless of commit order (see
        # SettingsRoleCertificationStore's docstring).
        await self._store.save_record(record)
        return record


__all__ = ["RoleReadinessService"]
