"""Durable recorder adapter from the orchestrator to Ask Dev persistence."""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from typing import Literal

from .contracts import DevAnswer, DevError, DevToolRequest
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
        )


__all__ = ["PersistenceRunRecorder"]
