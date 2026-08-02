"""Durable recorder adapter from the orchestrator to Ask Dev persistence."""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from collections.abc import Mapping
from typing import Any, Literal

from .contracts import DevAnswer, DevError, DevToolRequest
from .contracts_v2.frame import DevAnswerFrame
from .contracts_v2.intent import DevQuestionIntent
from .contracts_v2.narrative import DevNarrative
from .contracts_v2.result import DevInvestigationResult, DevSourceObservation
from .contracts_v2.subject import DevResolutionEntry, DevSubjectSet
from .orchestrator import RunState
from .persistence.service import DevPersistenceService
from .prompts import PROMPT_VERSION
from .tool_registry import TOOL_CONTRACT_VERSION, ToolExecution


def _digest(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode()).hexdigest()


class PersistenceRunRecorder:
    """Persist bounded metadata without prompts, reasoning, or provider payloads."""

    def __init__(
        self,
        service: DevPersistenceService,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        conversation_id: uuid.UUID,
        run_id: uuid.UUID,
        provider_source: Literal["platform", "byo"],
        started: float | None = None,
    ) -> None:
        self._service = service
        self._org_id = org_id
        self._user_id = user_id
        self._conversation_id = conversation_id
        self._run_id = run_id
        self._provider_source = provider_source
        self._started = started if started is not None else time.monotonic()

    async def transition(self, state: RunState) -> None:
        if state in {
            RunState.COMPLETED,
            RunState.INSUFFICIENT_EVIDENCE,
            RunState.REFUSED,
            RunState.FAILED,
            RunState.CANCELLED,
        }:
            return
        await self._service.update_run(
            org_id=self._org_id,
            user_id=self._user_id,
            run_id=self._run_id,
            state=state.value,
        )

    async def record_preflight(
        self,
        *,
        preflight_outcome: str | None,
        legacy_guard_reason: str | None,
    ) -> None:
        if preflight_outcome is None and legacy_guard_reason is None:
            return
        await self._service.record_run_diagnostics(
            org_id=self._org_id,
            user_id=self._user_id,
            run_id=self._run_id,
            preflight_outcome=preflight_outcome,
            legacy_guard_reason=legacy_guard_reason,
        )

    async def record_tool(
        self,
        *,
        ordinal: int,
        request: DevToolRequest,
        canonical_input_hash: str,
        execution: ToolExecution,
    ) -> None:
        result_json = json.dumps(
            execution.result.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
        )
        await self._service.append_tool_call(
            org_id=self._org_id,
            user_id=self._user_id,
            run_id=self._run_id,
            ordinal=ordinal,
            tool_id=request.tool_id.value,
            tool_version=request.tool_id.value,
            canonical_input_hash=canonical_input_hash,
            safe_scope_summary={
                "repository_count": len(request.scope.repositories),
                "entity_count": len(request.scope.entity_refs),
                "team_count": len(request.scope.team_ids),
            },
            status="completed",
            result_digest=_digest(result_json),
            evidence_ref_ids=[
                item.evidence_ref_id for item in execution.result.evidence
            ],
            latency_ms=execution.latency_ms,
            item_count=(
                len(execution.result.evidence)
                + len(execution.result.metrics)
                + len(execution.result.status_facts)
                + len(execution.result.pull_requests)
                + len(execution.result.ci_checks)
                + len(execution.result.deployments)
                + len(execution.result.incidents)
                + len(execution.result.graph_edges)
                + len(execution.result.data_health)
            ),
            byte_count=execution.serialized_bytes,
        )

    # -- Wave 3.1 (CHAOS-3299) v2 recorder adapters --------------------------
    # Thin adapters from a validated contracts_v2 object to the
    # DevPersistenceService recorder methods. The v2 orchestrator that
    # produces these objects (TRD v2 Section 10's server-owned stage
    # machine) is a separate lane's deliverable; these methods are the
    # interface it calls into, all pre-terminal, mirroring `record_tool`'s
    # existing placement ahead of `terminal()`.

    async def record_intent(self, intent: DevQuestionIntent) -> None:
        await self._service.record_intent(
            org_id=self._org_id,
            user_id=self._user_id,
            run_id=self._run_id,
            intent_id=intent.intent_id.value,
            cardinality=intent.cardinality.value,
            requires_clarification=intent.requires_clarification,
            interpreter_version=intent.interpreter_version,
            payload=intent.model_dump(mode="json"),
        )

    async def append_resolution(self, entry: DevResolutionEntry) -> None:
        await self._service.append_resolution(
            org_id=self._org_id,
            user_id=self._user_id,
            run_id=self._run_id,
            entry_ordinal=entry.entry_ordinal,
            mention_id=uuid.UUID(entry.mention_id),
            outcome=entry.outcome.value,
            resolved_at=entry.resolved_at,
            payload=entry.model_dump(mode="json"),
        )

    async def record_subject_set(self, subject_set: DevSubjectSet) -> None:
        await self._service.record_subject_set(
            org_id=self._org_id,
            user_id=self._user_id,
            run_id=self._run_id,
            set_id=uuid.UUID(subject_set.set_id),
            entity_kind=subject_set.entity_kind.value,
            cohort_complete=subject_set.cohort_complete,
            fingerprint=subject_set.fingerprint,
            payload=subject_set.model_dump(mode="json"),
        )

    async def append_source_observation(
        self, ordinal: int, observation: DevSourceObservation
    ) -> None:
        await self._service.append_source_observation(
            org_id=self._org_id,
            user_id=self._user_id,
            run_id=self._run_id,
            ordinal=ordinal,
            observation_id=uuid.UUID(observation.observation_id),
            source_class=observation.source_class.value,
            requirement_level=observation.requirement_level,
            observed_state=observation.observed_state.value,
            data_semantics=observation.data_semantics,
            usable_fact_count=observation.usable_fact_count,
            sample_count=observation.sample_count,
            subject_coverage=observation.subject_coverage,
            observed_at=observation.observed_at,
            payload=observation.model_dump(mode="json"),
        )

    async def record_investigation_result(self, result: DevInvestigationResult) -> None:
        """Persist every observation, then the plan-step partition + closure bit.

        Folded (orchestrator decision, CHAOS-3299): see
        ``DevPersistenceService.record_investigation_result`` -- observations
        are still recorded 1:N as before; the wrapper's own bookkeeping
        (which plan steps ran, and relationship-closure verification) is now
        set directly on ``dev_runs`` instead of a dedicated ninth table.
        """

        for ordinal, observation in enumerate(result.observations):
            await self.append_source_observation(ordinal, observation)
        await self._service.record_investigation_result(
            org_id=self._org_id,
            user_id=self._user_id,
            run_id=self._run_id,
            completed_steps=list(result.completed_steps),
            skipped_steps=list(result.skipped_steps),
            failed_steps=list(result.failed_steps),
            relationship_closure_verified=result.relationship_closure_verified,
        )

    async def record_frame(self, frame: DevAnswerFrame) -> None:
        await self._service.record_frame(
            org_id=self._org_id,
            user_id=self._user_id,
            run_id=self._run_id,
            frame_id=uuid.UUID(frame.frame_id),
            public_outcome=frame.public_outcome.value,
            payload=frame.model_dump(mode="json"),
        )

    async def rollback(self) -> None:
        """Discard pending writes on this request's session (CHAOS-3297).

        Called by the orchestrator after a failed ``record_frame`` flush,
        before any further write on this same recorder -- a session
        SQLAlchemy has marked rollback-only raises ``PendingRollbackError``
        on the next flush otherwise, which is exactly the failure mode this
        exists to prevent.
        """
        await self._service.session.rollback()

    async def record_narrative(self, narrative: DevNarrative) -> None:
        provider_fingerprint = None
        if narrative.provider_metadata is not None:
            provider_fingerprint = _digest(
                narrative.provider_metadata.model_fingerprint
            )
        await self._service.record_narrative(
            org_id=self._org_id,
            user_id=self._user_id,
            run_id=self._run_id,
            narrative_id=uuid.UUID(narrative.narrative_id),
            frame_id=uuid.UUID(narrative.frame_id),
            mode=narrative.mode,
            provider_fingerprint=provider_fingerprint,
            narrative_text=narrative.body,
            # `body` is stored separately as `narrative_text`; excluded here
            # to avoid duplicating it inside the JSONB payload.
            payload=narrative.model_dump(mode="json", exclude={"body"}),
        )

    async def append_stage_diagnostic(
        self,
        *,
        ordinal: int,
        stage_id: str,
        status: Literal["started", "completed", "failed", "skipped"],
        latency_ms: int | None = None,
        counts: Mapping[str, Any] | None = None,
    ) -> None:
        await self._service.append_stage_diagnostic(
            org_id=self._org_id,
            user_id=self._user_id,
            run_id=self._run_id,
            ordinal=ordinal,
            stage_id=stage_id,
            status=status,
            latency_ms=latency_ms,
            counts=dict(counts or {}),
        )

    async def record_answer(self, answer: DevAnswer) -> None:
        resolved_scope = answer.resolved_scope.resolved_scope
        scope = resolved_scope or answer.resolved_scope.requested_scope

        def validate(payload):
            return DevAnswer.model_validate(payload).model_dump(mode="json")

        await self._service.append_assistant_answer(
            org_id=self._org_id,
            user_id=self._user_id,
            conversation_id=self._conversation_id,
            answer_payload=answer.model_dump(mode="json"),
            validator=validate,
            scope_snapshot=scope.model_dump(mode="json"),
            rendered_content=answer.direct_summary,
        )

    async def terminal(
        self,
        *,
        state: RunState,
        answer: DevAnswer | None,
        error: DevError | None,
        usage,
        tool_call_count: int,
        provider_fingerprint: str | None,
        model_fingerprint: str | None,
        prompt_checksum: str | None,
        prompt_version: str | None = None,
    ) -> None:
        # Whichever prompt actually composed, not a literal: this row is the
        # run's prompt provenance, and the composer now emits v1 or v2
        # depending on whether the server committed a subject.
        prompt_version = prompt_version or PROMPT_VERSION
        if prompt_checksum is not None:
            prompt_version = f"{prompt_version}:sha256:{prompt_checksum}"
        terminal_reason = (
            error.code
            if error is not None
            else answer.status.value
            if answer is not None
            else "internal_error"
        )
        await self._service.update_run(
            org_id=self._org_id,
            user_id=self._user_id,
            run_id=self._run_id,
            state=state.value,
            answer_id=uuid.UUID(answer.answer_id) if answer is not None else None,
            terminal_reason=terminal_reason,
            provider_source=self._provider_source,
            provider_fingerprint=(
                _digest(provider_fingerprint) if provider_fingerprint else None
            ),
            model_fingerprint=(
                _digest(model_fingerprint) if model_fingerprint else None
            ),
            prompt_version=prompt_version,
            tool_contract_version=TOOL_CONTRACT_VERSION,
            metric_version=(
                answer.versions.metric_definition_version if answer else None
            ),
            query_version=answer.versions.query_version if answer else None,
            latency_ms=max(0, round((time.monotonic() - self._started) * 1000)),
            input_tokens=usage.input_tokens,
            output_tokens=usage.output_tokens,
            estimated_cost_microusd=usage.estimated_cost_microusd,
            tool_call_count=tool_call_count,
            citation_count=len(answer.evidence) if answer else 0,
            metric_count=len(answer.metrics) if answer else 0,
            grounding_validation_status="passed" if answer else "not_applicable",
            safe_error_code=error.code if error else None,
            # CHAOS-3297 Codex review HIGH #1: persist the exact validated
            # v1 DevError this terminal call carried -- whichever origin
            # built it (the orchestrator's own error() closure,
            # _provider_error, or a preflight termination's
            # project_preflight_error) -- so router._replayed_result can
            # replay it verbatim instead of reconstructing an approximation
            # from the frame.
            terminal_error_payload=(
                error.model_dump(mode="json") if error is not None else None
            ),
        )


__all__ = ["PersistenceRunRecorder"]
