"""Shared deterministic harness for the CHAOS-3292 preflight suites.

Lives in the ``tests`` package (which has an ``__init__.py``, unlike
``tests/api/dev``) so both mypy and pytest resolve it the same way, following
``tests/_helpers``. The leading underscore keeps pytest from collecting it, and
the named acceptance cases live here too so the mutation suite can reuse them
without importing a test module.

Determinism is the whole point of this file: identical seeded catalog plus an
identical scripted-provider *script* must produce a byte-identical outcome
tuple on every iteration. Two things make that true — ``ScriptedAgentProvider``
consumes a ``deque``, so every iteration builds a fresh provider from a shared
script factory, and every server-minted UUID comes from an injected
counter-based factory rather than ``uuid.uuid4``.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from dev_health_ops.api.dev.alias_matching import alias_forms
from dev_health_ops.api.dev.contract_fixtures import positive_fixtures
from dev_health_ops.api.dev.contracts import (
    DevAnswer,
    DevContractVersions,
    DevMessageRequest,
    DevScope,
    DevScopeResolution,
    DevToolRequest,
    DevToolResult,
    QuestionClass,
    ToolID,
)
from dev_health_ops.api.dev.contracts_v2 import DevInvestigationResult, DevSubjectSet
from dev_health_ops.api.dev.orchestrator import DevOrchestrator, OrchestratorResult
from dev_health_ops.api.dev.orchestrator_states import RunState
from dev_health_ops.api.dev.question_interpreter import QuestionInterpreter
from dev_health_ops.api.dev.scope_catalog import merge_search_candidates
from dev_health_ops.api.dev.scope_service import (
    AuthorizedEntity,
    EntityKind,
    ScopeRef,
    ScopeRequestCache,
    ScopeResolutionService,
)
from dev_health_ops.api.dev.subject_preflight import SubjectPreflight
from dev_health_ops.api.dev.tool_registry import AskDevToolRegistry
from dev_health_ops.llm.agent.contracts import (
    AgentDecisionResult,
    AgentFinalAnswer,
    AgentMessage,
    AgentMessageRole,
    AgentToolDefinition,
    AgentToolRequest,
    AgentUsage,
    CancellationSignal,
)
from dev_health_ops.llm.agent.scripted import ScriptedAgentProvider, ScriptedStep

ORG_ID = "org_fullchaos"
OTHER_ORG_ID = "org_nightfall_holdings"
USER_ID = "user_01"
PERMISSION_FINGERPRINT = "permissions_01"
RUN_ID = "run_01"
CONVERSATION_ID = "conversation_01"
ANSWER_ID = "answer_01"

#: Seeded subjects, by owning organization. Every catalog method filters on
#: ``org_id`` exactly as every ``scope_catalog`` query does in SQL, so
#: cross-tenant is modelled as a *populated catalog owned by someone else* —
#: not as an empty catalog, which cannot distinguish forbidden from not-found.
ASK_DEV_PROJECT = AuthorizedEntity(EntityKind.PROJECT, "project-ask-dev", "Ask Dev")
PLATFORM_TEAM = AuthorizedEntity(EntityKind.TEAM, "team-platform", "Platform")
NIGHTFALL_PROJECT = AuthorizedEntity(
    EntityKind.PROJECT, "project-nightfall", "Nightfall"
)
ATLAS_PROJECT_ONE = AuthorizedEntity(EntityKind.PROJECT, "project-atlas-1", "Atlas")
ATLAS_PROJECT_TWO = AuthorizedEntity(EntityKind.PROJECT, "project-atlas-2", "Atlas")
PAGE_PROJECT = AuthorizedEntity(EntityKind.PROJECT, "project-page-ctx", "Beacon")


def _label_sort_key(entity: AuthorizedEntity) -> tuple[str, str]:
    return (entity.label.casefold(), entity.canonical_id)


class SeededCatalog:
    """An ``AuthorizedEntityCatalog`` over a fixed, org-scoped entity list."""

    def __init__(
        self,
        entities: Sequence[tuple[str, AuthorizedEntity]],
        *,
        fail_search: bool = False,
        fail_watermark: bool = False,
    ) -> None:
        self.entities = list(entities)
        self.fail_search = fail_search
        self.fail_watermark = fail_watermark
        self.search_calls: list[tuple[str, str]] = []

    async def watermark(self, org_id: str, kinds: tuple[EntityKind, ...]) -> str:
        if self.fail_watermark:
            raise RuntimeError("catalog watermark unavailable")
        del kinds
        return f"{org_id}:watermark-1"

    async def exact(
        self, org_id: str, ref: ScopeRef, *, limit: int
    ) -> list[AuthorizedEntity]:
        if self.fail_search:
            raise RuntimeError("catalog unavailable")
        return [
            entity
            for owner, entity in self.entities
            if owner == org_id
            and entity.kind is ref.kind
            and ref.value.casefold()
            in {entity.canonical_id.casefold(), entity.label.casefold()}
        ][:limit]

    async def search(
        self,
        org_id: str,
        query: str,
        kinds: tuple[EntityKind, ...],
        *,
        limit: int,
        include_alias_matches: bool = False,
        preferred_kinds: frozenset[EntityKind] = frozenset(),
    ) -> list[AuthorizedEntity]:
        self.search_calls.append((org_id, query))
        if self.fail_search:
            raise RuntimeError("catalog unavailable")
        needle = query.casefold()
        matched = [
            entity
            for owner, entity in self.entities
            if owner == org_id
            and entity.kind in kinds
            and (
                needle == entity.canonical_id.casefold()
                or needle in entity.label.casefold()
            )
        ]
        alias_matches: list[AuthorizedEntity] = []
        if include_alias_matches:
            # CHAOS-3388: mirrors ClickHouseAuthorizedEntityCatalog's roster
            # scan over the *same* seeded org-scoped entities, so a test can
            # exercise acronym/parenthetical-alias matching through this fake
            # without a live ClickHouse.
            #
            # CHAOS-3422 codex review, medium: this pass ran *independently*
            # of the substring pass in production and did not here -- it used
            # to skip any entity the substring pass had already matched. An
            # entity that matches both ways ("ACR Platform" for the query
            # "acr") is therefore an alias hit in production and was a
            # substring hit in the fake, so the fake ranked it one tier lower
            # than the live path would. Deduplication belongs to
            # ``merge_search_candidates``, which resolves the overlap in
            # alias's favour; the two passes must stay independent here for
            # it to see the overlap at all.
            seen_alias: set[tuple[EntityKind, str]] = set()
            for owner, entity in self.entities:
                if (
                    owner == org_id
                    and entity.kind in kinds
                    and entity.kind in {EntityKind.PROJECT, EntityKind.TEAM}
                    and (entity.kind, entity.canonical_id) not in seen_alias
                ):
                    forms = alias_forms(entity.label)
                    if needle in forms.literal_aliases or needle in forms.acronyms:
                        alias_matches.append(entity)
                        seen_alias.add((entity.kind, entity.canonical_id))
        # ORDER BY lowerUTF8(label), canonical_id ... LIMIT n — the ordering and
        # the truncation both happen in SQL, so an exact label sorted past the
        # page boundary never reaches the caller at all. The rank-then-truncate
        # step is the production function itself, imported rather than
        # re-implemented: a hand-mirrored copy is exactly how a fake drifts
        # from its producer and starts passing tests the live path fails.
        return merge_search_candidates(
            alias_hits=alias_matches,
            substring_hits=matched,
            preferred_kinds=preferred_kinds,
            limit=limit,
        )

    async def organization_repository_ids(
        self, org_id: str, *, limit: int
    ) -> tuple[list[str], int]:
        del org_id, limit
        return [], 0

    async def organization_project_entities(
        self, org_id: str, *, limit: int
    ) -> tuple[list[AuthorizedEntity], int]:
        """CHAOS-3393: mirrors ``ClickHouseAuthorizedEntityCatalog``'s own
        page+true-total shape over the seeded, org-scoped entities."""

        if self.fail_search:
            raise RuntimeError("catalog unavailable")
        matched = [
            entity
            for owner, entity in self.entities
            if owner == org_id and entity.kind is EntityKind.PROJECT
        ]
        matched.sort(key=_label_sort_key)
        return matched[:limit], len(matched)


def sequential_ids() -> Callable[[], str]:
    """Deterministic, canonical dashed lowercase UUIDs (CHAOS-3294 grammar)."""

    counter = 0

    def mint() -> str:
        nonlocal counter
        counter += 1
        return str(uuid.UUID(int=counter))

    return mint


def fixed_now() -> datetime:
    return datetime(2026, 7, 31, 12, 0, 0, tzinfo=UTC)


def scope_dict(**overrides: Any) -> dict[str, Any]:
    scope = deepcopy(positive_fixtures()["dev_message_request.v1"]["scope"])
    scope.update(
        {
            "direct_scope": "organization",
            "repositories": [],
            "entity_refs": [],
            "team_ids": [],
            "surface_context": None,
        }
    )
    scope.update(overrides)
    return scope


def request_for(
    question: str,
    *,
    question_class: QuestionClass = QuestionClass.STATUS,
    scope_overrides: dict[str, Any] | None = None,
    requested_metric_ids: Sequence[str] = (),
    organization_id: str = ORG_ID,
) -> DevMessageRequest:
    payload = deepcopy(positive_fixtures()["dev_message_request.v1"])
    scope = scope_dict(**(scope_overrides or {}))
    scope["organization_id"] = organization_id
    payload.update(
        {
            "question": question,
            "question_class": question_class.value,
            "scope": scope,
            "requested_metric_ids": list(requested_metric_ids),
        }
    )
    return DevMessageRequest.model_validate(payload)


def versions() -> DevContractVersions:
    return DevContractVersions.model_validate(
        positive_fixtures()["dev_answer.v1"]["versions"]
    )


def organization_resolution(scope: DevScope) -> DevScopeResolution:
    return DevScopeResolution.model_validate(
        {
            "schema_version": "dev_scope_resolution.v1",
            "requested_scope": scope.model_dump(mode="json"),
            "resolved_scope": scope.model_dump(mode="json"),
            "outcome": "inherited",
            "authorized_repository_ids": [],
            "authorized_entity_ids": [],
            "candidates": [],
            "fallbacks": [],
            "warnings": [],
            "resolved_at": fixed_now(),
        }
    )


def answer_payload(*, script_id: str) -> dict[str, Any]:
    """The stock answer with claims dropped.

    The fixture claim hardcodes a repository ``validity_scope``; these runs
    commit a project or organization scope, so keeping it would fail answer
    validation for a reason unrelated to what is under test. Metrics and
    evidence still come from the tool results, so the grounding floor is met.
    """

    import hashlib

    payload = deepcopy(positive_fixtures()["dev_answer.v1"])
    payload["claims"] = []
    payload["model"] = {
        "provider_source": "platform",
        "provider_family": "scripted",
        "model_fingerprint": hashlib.sha256(script_id.encode()).hexdigest()[:24],
    }
    return payload


def grounded_answer_payload(
    *, script_id: str, summary: str, validity_scope: dict[str, Any]
) -> dict[str, Any]:
    """An evidence-backed answer that narrates ``summary``.

    Keeping the fixture's claim (and re-pointing its ``validity_scope`` at the
    run's own scope) is what makes the CHAOS-3289 narration clause reachable:
    the claim-count clause short-circuits before it on a claim-free answer, so
    a claim-free fixture cannot exercise fabricated narration at all.
    """

    import hashlib

    payload = deepcopy(positive_fixtures()["dev_answer.v1"])
    payload["direct_summary"] = summary
    payload["claims"][0]["text"] = summary
    payload["claims"][0]["validity_scope"] = deepcopy(validity_scope)
    payload["model"] = {
        "provider_source": "platform",
        "provider_family": "scripted",
        "model_fingerprint": hashlib.sha256(script_id.encode()).hexdigest()[:24],
    }
    return payload


class RecordingProvider:
    """Captures the exact tool definitions and messages each round was given."""

    def __init__(self, steps: list[ScriptedStep], *, script_id: str) -> None:
        self._inner = ScriptedAgentProvider(steps, script_id=script_id)
        self.tool_ids: list[tuple[str, ...]] = []
        self.user_texts: list[str] = []
        self.system_texts: list[str] = []

    @property
    def capabilities(self) -> Any:
        return self._inner.capabilities

    @property
    def provider_fingerprint(self) -> str:
        return self._inner.provider_fingerprint

    @property
    def model_fingerprint(self) -> str:
        return self._inner.model_fingerprint

    async def decide(
        self,
        messages: Sequence[AgentMessage],
        tools: Sequence[AgentToolDefinition],
        response_schema: Mapping[str, Any],
        timeout_seconds: float,
        max_output_tokens: int,
        signal: CancellationSignal | None = None,
    ) -> AgentDecisionResult:
        self.tool_ids.append(tuple(tool.tool_id for tool in tools))
        for message in messages:
            if message.role is AgentMessageRole.SYSTEM:
                self.system_texts.append(message.content)
            elif message.role is AgentMessageRole.USER:
                self.user_texts.append(message.content)
        return await self._inner.decide(
            messages=messages,
            tools=tools,
            response_schema=response_schema,
            timeout_seconds=timeout_seconds,
            max_output_tokens=max_output_tokens,
            signal=signal,
        )

    async def aclose(self) -> None:
        await self._inner.aclose()


class Recorder:
    def __init__(self) -> None:
        self.transitions: list[RunState] = []
        self.answers: list[DevAnswer] = []
        self.terminals: list[RunState] = []
        self.preflight_diagnostics: list[tuple[str | None, str | None]] = []
        self.frames: list[Any] = []
        self.resolutions: list[Any] = []
        #: CHAOS-3533: the ``authorizing_mention_id`` passed alongside each
        #: recorded frame -- ``None`` for every terminal that made no
        #: clarification offer, which is itself the assertable claim.
        self.record_frame_authorizing_mention_ids: list[str | None] = []
        #: CHAOS-3389: every QUA shadow record this recorder was asked to
        #: persist. Empty whenever the shadow seam is unwired/disabled --
        #: exactly the RED test's own assertion surface.
        self.qua_shadow_records: list[Any] = []
        #: CHAOS-3325: the recorder-method call sequence, by name -- proves
        #: append_resolution lands before record_frame, not just that both
        #: were called (two same-length lists alone prove nothing about
        #: order).
        self.call_order: list[str] = []

    async def transition(self, state: RunState) -> None:
        self.transitions.append(state)

    async def record_tool(self, **values: Any) -> None:
        del values

    async def record_answer(self, answer: DevAnswer) -> None:
        self.answers.append(answer)

    async def record_error_message(self, error: Any, **_values: Any) -> None:
        """No-op here; CHAOS-3423's persistence-prerequisite suite passes a
        REAL ``PersistenceRunRecorder`` (never this fake) to assert on the
        actual ``dev_messages`` row -- this only satisfies the RunRecorder
        protocol shape so every other fixture in this module keeps
        constructing ``DevOrchestrator`` with the default fake recorder.
        """
        del error

    async def record_preflight(
        self, *, preflight_outcome: str | None, legacy_guard_reason: str | None
    ) -> None:
        self.preflight_diagnostics.append((preflight_outcome, legacy_guard_reason))

    async def record_subject_set(self, subject_set: DevSubjectSet) -> None:
        """No-op here; CHAOS-3301's SubjectSetRecorder subclass captures this."""
        del subject_set

    async def append_resolution(self, entry: Any) -> None:
        self.resolutions.append(entry)
        self.call_order.append("append_resolution")

    async def record_frame(
        self, frame: Any, *, authorizing_mention_id: str | None = None
    ) -> None:
        self.frames.append(frame)
        #: CHAOS-3533: captured, not discarded -- this is the mention whose
        #: ambiguity terminated the run, and a test that wants to prove the
        #: orchestrator names the RIGHT one (rather than merely names one)
        #: needs it here. Note the orchestrator wraps this call in a broad
        #: `except Exception` intended for database faults, so a fake that
        #: rejected this keyword would have its TypeError swallowed and would
        #: report a missing frame instead of a signature mismatch.
        self.record_frame_authorizing_mention_ids.append(authorizing_mention_id)
        self.call_order.append("record_frame")

    async def rollback(self) -> None:
        pass

    async def record_investigation_result(self, result: DevInvestigationResult) -> None:
        """No-op here; CHAOS-3295's InvestigationRecorder subclass captures this."""
        del result

    async def record_qua_shadow(self, record: Any) -> None:
        self.qua_shadow_records.append(record)

    async def record_narrative(self, narrative: Any) -> None:
        """No-op here; CHAOS-3297 stack #4's narrative synthesis is not
        under test in this module -- only the RunRecorder protocol shape."""
        del narrative

    async def terminal(self, **values: Any) -> None:
        self.terminals.append(values["state"])


def stock_executor(
    calls: list[DevToolRequest],
) -> Callable[[Any, DevToolRequest], Any]:
    """The one successful-tool-result executor every suite here shares.

    Exposed separately from ``recording_registry`` so a suite that needs a
    registry where only *some* tools behave normally (CHAOS-3332) can compose
    against the same success shape instead of re-deriving it and drifting.
    """

    async def execute(_context: Any, request: DevToolRequest) -> DevToolResult:
        calls.append(request)
        payload = deepcopy(positive_fixtures()["dev_tool_result.v1"])
        payload.update(
            {
                "run_id": request.run_id,
                "tool_call_id": request.tool_call_id,
                "tool_id": request.tool_id.value,
            }
        )
        return DevToolResult.model_validate(payload)

    return execute


def recording_registry(calls: list[DevToolRequest]) -> AskDevToolRegistry:
    execute = stock_executor(calls)
    return AskDevToolRegistry({tool_id: execute for tool_id in ToolID})


def status_then_answer(script_id: str) -> list[ScriptedStep]:
    """The script a well-behaved model runs once a subject is committed."""

    return [
        ScriptedStep(
            decision=AgentToolRequest(
                tool_id="status_snapshot.v1",
                arguments={"limit": 25, "include_comparison": False},
                call_id="tool_call_01",
            ),
            usage=AgentUsage(input_tokens=100, output_tokens=10),
        ),
        ScriptedStep(decision=AgentFinalAnswer(answer_payload(script_id=script_id))),
    ]


@dataclass(slots=True)
class RunOutput:
    result: OrchestratorResult
    calls: list[DevToolRequest] = field(default_factory=list)
    provider: RecordingProvider | None = None
    recorder: Recorder | None = None

    def outcome_tuple(self) -> tuple[Any, ...]:
        """The byte-comparable outcome for the determinism loops.

        Deliberately excludes minted correlation handles and timestamps, and
        deliberately *includes* the recorded tool-call list: an outcome tuple
        that only carried the terminal state would report "deterministic" for a
        run that executed different tools each time.

        The demoted legacy guard's reason code is excluded because it is
        telemetry, not outcome — A13 compares these tuples with the guard
        disabled, and a firing that *did* change the tuple would mean the guard
        was still deciding something.
        """

        return (
            self.result.state.value,
            self.result.error.code if self.result.error is not None else None,
            self.result.error.retryable if self.result.error is not None else None,
            self.result.answer is not None,
            tuple(request.tool_id.value for request in self.calls),
            self.preflight_outcomes(),
        )

    def preflight_outcomes(self) -> tuple[str | None, ...]:
        """Preflight outcomes only.

        The demoted legacy guard re-records the same outcome alongside its own
        reason code; those rows belong to ``guard_reasons``, not here.
        """

        if self.recorder is None:
            return ()
        return tuple(
            outcome
            for outcome, guard in self.recorder.preflight_diagnostics
            if guard is None
        )

    def guard_reasons(self) -> tuple[str, ...]:
        if self.recorder is None:
            return ()
        return tuple(
            guard
            for _outcome, guard in self.recorder.preflight_diagnostics
            if guard is not None
        )


async def run_preflight_orchestrator(
    *,
    question: str,
    entities: Sequence[tuple[str, AuthorizedEntity]],
    script: Callable[[str], list[ScriptedStep]] | None = None,
    script_id: str = "preflight",
    question_class: QuestionClass = QuestionClass.STATUS,
    scope_overrides: dict[str, Any] | None = None,
    requested_metric_ids: Sequence[str] = (),
    org_id: str = ORG_ID,
    user_id: str = USER_ID,
    conversation_id: str = CONVERSATION_ID,
    run_id: str = RUN_ID,
    answer_id: str = ANSWER_ID,
    fail_search: bool = False,
    preflight_enabled: bool = True,
    recorder_factory: Callable[[], Recorder] = Recorder,
    plan_registry: Any = None,
    plan_executor: Any = None,
    registry_factory: Callable[[list[DevToolRequest]], AskDevToolRegistry] = (
        recording_registry
    ),
    qua_shadow: Any = None,
) -> RunOutput:
    """One full orchestrator run with the preflight wired the way production wires it.

    ``recorder_factory`` defaults to the plain ``Recorder`` above; CHAOS-3301's
    harness passes a subclass that also captures ``record_subject_set`` calls,
    without duplicating this whole function for one extra capture point.

    ``registry_factory`` defaults to ``recording_registry`` above; CHAOS-3332's
    suite passes registries whose executors raise, so the orchestrator's
    executor-fault handling is exercised through this same seam rather than a
    second, divergent runner. It still receives the shared ``calls`` list, so a
    faulting executor's *attempted* request is recorded even though no tool
    result is produced.

    ``user_id``/``conversation_id``/``run_id``/``answer_id`` default to the
    module's own opaque fixture constants (unchanged for every existing
    caller) -- CHAOS-3423/3424's persistence-prerequisite suite overrides all
    four with real UUID strings so a ``recorder_factory`` can build a REAL
    ``PersistenceRunRecorder`` against a seeded database and assert on the
    actual rows a live run leaves behind, not a fake recorder's captured
    call list.
    """

    catalog = SeededCatalog(entities, fail_search=fail_search)
    scope_service = ScopeResolutionService(catalog, cache=ScopeRequestCache())
    mint = sequential_ids()
    preflight = (
        SubjectPreflight(
            interpreter=QuestionInterpreter(mint_id=mint, now=fixed_now),
            scope_service=scope_service,
            versions=versions(),
            mint_id=mint,
            now=fixed_now,
        )
        if preflight_enabled
        else None
    )
    request = request_for(
        question,
        question_class=question_class,
        scope_overrides=scope_overrides,
        requested_metric_ids=requested_metric_ids,
        organization_id=org_id,
    )
    resolution = organization_resolution(request.scope)

    async def resolve(**_values: Any) -> DevScopeResolution:
        return resolution

    calls: list[DevToolRequest] = []
    provider = RecordingProvider(
        (script or status_then_answer)(script_id), script_id=script_id
    )
    recorder = recorder_factory()
    orchestrator = DevOrchestrator(
        provider=provider,
        provider_source="platform",
        provider_family="scripted",
        registry=registry_factory(calls),
        scope_resolver=resolve,
        versions=versions(),
        recorder=recorder,
        preflight=preflight,
        plan_registry=plan_registry,
        plan_executor=plan_executor,
        qua_shadow=qua_shadow,
    )
    result = await orchestrator.run(
        request=request,
        org_id=org_id,
        user_id=user_id,
        permission_fingerprint=PERMISSION_FINGERPRINT,
        run_id=run_id,
        conversation_id=conversation_id,
        answer_id=answer_id,
        cancellation=asyncio.Event(),
    )
    return RunOutput(result=result, calls=calls, provider=provider, recorder=recorder)


# ---------------------------------------------------------------------------
# The named acceptance cases
# ---------------------------------------------------------------------------
#
# The case factories live beside the harness rather than in the acceptance test
# module so the mutation suite can reuse them without importing a test module
# (which mypy cannot resolve and pytest would collect twice).


async def case_a1() -> RunOutput:
    """A known real project."""

    return await run_preflight_orchestrator(
        question="What's the status of the Ask Dev project?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id="a1",
    )


async def case_a2() -> RunOutput:
    """A target that does not exist in this organization's catalog."""

    return await run_preflight_orchestrator(
        question="What's the status of the Nightfall project?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id="a2",
    )


async def case_a3() -> RunOutput:
    """Two authorized entities share the named label."""

    return await run_preflight_orchestrator(
        question="What's the status of the Atlas project?",
        entities=[(ORG_ID, ATLAS_PROJECT_ONE), (ORG_ID, ATLAS_PROJECT_TWO)],
        script_id="a3",
    )


#: CHAOS-3388 live repro fixture: the real production catalog row (org
#: 70d529e0-3c06-4597-8480-794fd02328b6, read live from ClickHouse
#: `projects`) and the exact question text from the incident report.
CHAOS_3388_ACR_PROJECT = AuthorizedEntity(
    EntityKind.PROJECT,
    "60997592-f9f4-462b-87c3-ef82671df270",
    "Dev Health Agent Context Runtime (Context Fabric)",
)
CHAOS_3388_ACR_QUESTION = (
    "What's the status of the ACR Project and what drivers are blocking it? "
    "What's left to complete?"
)


async def case_chaos_3388_acr_close_match() -> RunOutput:
    """The live repro: an acronym-named project with no literal catalog match.

    Orchestrator-level repro of the wrong-terminal defect. Before the
    CHAOS-3388 fixes, "ACR Project" never typed a mention at all (the
    kind-noun regex's case-sensitivity bug), so this run proceeded to the
    model round exactly like ``case_a4`` -- and if the model then also never
    tried (or mis-tried) ``resolve_scope.v1``, it fell all the way to the
    grounding-floor ``insufficient_evidence``/"did not include enough
    grounded detail" failure, never a deterministic no-match. The assertion
    that matters here is the same shape as A2/A3: the preflight alone
    decides the outcome, and the scripted provider is never invoked at all
    (``output.provider.tool_ids == []``) -- proving the deterministic
    machinery caught this before a single model token was spent, not that a
    model happened to behave helpfully.
    """

    return await run_preflight_orchestrator(
        question=CHAOS_3388_ACR_QUESTION,
        entities=[(ORG_ID, ASK_DEV_PROJECT), (ORG_ID, CHAOS_3388_ACR_PROJECT)],
        script_id="chaos_3388_acr",
    )


async def case_a4() -> RunOutput:
    """An organization-wide question that names nothing."""

    return await run_preflight_orchestrator(
        question="How are we doing on delivery this month?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id="a4",
    )


async def case_a6_real_then_fake() -> RunOutput:
    return await run_preflight_orchestrator(
        question="Compare project Ask Dev and project Nightfall",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id="a6a",
    )


async def case_a6_fake_then_real() -> RunOutput:
    return await run_preflight_orchestrator(
        question="Compare project Nightfall and project Ask Dev",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script_id="a6b",
    )


async def case_a8() -> RunOutput:
    """Seeded and real — just owned by another tenant."""

    return await run_preflight_orchestrator(
        question="What's the status of the Nightfall project?",
        entities=[(OTHER_ORG_ID, NIGHTFALL_PROJECT), (ORG_ID, ASK_DEV_PROJECT)],
        script_id="a8",
    )


async def case_a9() -> RunOutput:
    """The catalog raises."""

    return await run_preflight_orchestrator(
        question="What's the status of the Ask Dev project?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        fail_search=True,
        script_id="a9",
    )


async def case_a10() -> RunOutput:
    """A context ref pointing at an entity that no longer exists."""

    return await run_preflight_orchestrator(
        question="What is the current state?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        scope_overrides={
            "entity_refs": [
                {
                    "entity_type": "project",
                    "entity_id": "project-deleted",
                    "display_label": "Deleted project",
                    "repository_id": None,
                }
            ],
            "direct_scope": "project",
        },
        script_id="a10",
    )


async def case_a11() -> RunOutput:
    """A named team, which the catalog can resolve but v1 cannot scope."""

    return await run_preflight_orchestrator(
        question="What is the status of the Platform team?",
        entities=[(ORG_ID, PLATFORM_TEAM)],
        script_id="a11",
    )
