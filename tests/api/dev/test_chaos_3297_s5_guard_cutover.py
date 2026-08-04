"""CHAOS-3297 stack #5 -- the CHAOS-3289/3290 legacy-guard cutover.

The property this stack exists to establish lives at the *run* boundary, so
every control here is a full orchestrator run through the same harness the
CHAOS-3292 acceptance suite uses, not an assertion about a predicate in
isolation.

The property: once the server owns the frame (stack #1) and the
deterministic fallback renders from it (stack #4), a legacy guard rejection
must stop **erasing** a run that has server-verified material. The guard
verdict becomes a recorded diagnostic; what ships is a server-grounded,
frame-rendered result that never carries one token of the rejected model
prose.

The halves that make these controls non-vacuous:

* C1/C1b/C1c are RED against the pre-cutover tree -- today the same run
  terminates ``insufficient_evidence`` with no answer at all, while the
  canonical tool results it holds are exactly the material the frame
  renders from. C3 pins the surviving pre-cutover behaviour on the
  flag-off path, so the contrast is asserted in both directions.
* C2/C3 are the negative controls. A guard firing on a run with **no**
  server-verified material still fails safe, and the flag-off path (no
  preflight => ``ask_dev_wave_3_1`` off for the org) keeps every reason
  terminal exactly as production does today. Without these, "the guard was
  demoted" would be indistinguishable from "the guard was deleted".
* M1/M2 defeat the two named seams (``_frame_cutover_active`` and the
  server-material predicate) and observe which control notices -- the
  in-tree equivalent of running C1 against old code.
"""

from __future__ import annotations

import inspect
import json
from copy import deepcopy
from typing import Any, cast

import pytest

from dev_health_ops.api.dev.contract_fixtures import positive_fixtures
from dev_health_ops.api.dev.contracts import (
    AnswerStatus,
    DevAnswer,
    DevCoverage,
    DevModelMetadata,
    DevScope,
    DevScopeResolution,
    DevToolRequest,
    DevToolResult,
    ToolID,
)
from dev_health_ops.api.dev.contracts_v2.base import PublicOutcome
from dev_health_ops.api.dev.orchestrator import (
    GUARD_DEMOTED_NAMED_ENTITY_STATUS,
    SERVER_GROUNDED_SUMMARY,
    DevOrchestrator,
)
from dev_health_ops.api.dev.orchestrator_states import RunState
from dev_health_ops.api.dev.subject_preflight import (
    PreflightDecision,
    SubjectPreflightResult,
)
from dev_health_ops.api.dev.tool_registry import AskDevToolRegistry
from dev_health_ops.llm.agent.contracts import AgentFinalAnswer
from dev_health_ops.llm.agent.scripted import ScriptedStep
from tests._chaos_3292_preflight import (
    ANSWER_ID,
    ASK_DEV_PROJECT,
    CONVERSATION_ID,
    ORG_ID,
    Recorder,
    RecordingProvider,
    RunOutput,
    fixed_now,
    grounded_answer_payload,
    organization_resolution,
    recording_registry,
    run_preflight_orchestrator,
    scope_dict,
    status_then_answer,
    versions,
)

#: The unresolved bare name the question uses and the answer narrates. The
#: catalog is seeded with Ask Dev only, so "Nightfall" is a name the
#: preflight could not resolve -- the exact CHAOS-3289 shape, and the one
#: that re-arms the backstop as terminal
#: (``subject_preflight`` sets ``legacy_guard_required=True``).
_UNRESOLVED_NAME = "Nightfall"
_NARRATION = "Nightfall completed twelve work items this period."

#: A v2-shaped evidence handle (``ev1_`` + 40 lowercase hex,
#: ``contracts_v2.base.EvidenceHandle``).
#:
#: The shared harness fixture still mints the pre-v2 ``ev_01`` shape, which
#: ``terminal_frames.wrap_legacy_answer_as_frame`` cannot project into a
#: ``DevEvidenceRefV2`` -- so every run through that fixture silently
#: degrades to an ``internal_error`` frame (CHAOS-3340, filed; two confirmed
#: instances). These controls are about what the FRAME carries, so they
#: cannot be asserted on a fixture that never produces a real one. This
#: registry mints the handle shape production's
#: ``EvidenceHandleService.issue`` actually returns. Repairing the shared
#: fixture is CHAOS-3340's blast-radius call, not this stack's.
_V2_EVIDENCE_HANDLE = "ev1_" + ("a1b2c3d4e5" * 4)


def _with_v2_handles(payload: dict[str, Any]) -> dict[str, Any]:
    """Rewrite the fixture's ``ev_01`` handles to the v2 grammar."""

    return dict(
        json.loads(
            json.dumps(payload, default=str).replace("ev_01", _V2_EVIDENCE_HANDLE)
        )
    )


class _ArtifactRecorder(Recorder):
    """Also captures narratives and the terminal kwargs.

    The base harness recorder drops both (stack #4 was not under test
    there). The cutover's whole claim is about what the user is left
    holding, so the shipped artifacts -- frame, narrative, and the run's own
    diagnostic columns -- have to be observable.
    """

    def __init__(self) -> None:
        super().__init__()
        self.narratives: list[Any] = []
        self.terminal_kwargs: list[dict[str, Any]] = []

    async def record_narrative(self, narrative: Any) -> None:
        self.narratives.append(narrative)

    async def terminal(self, **values: Any) -> None:
        self.terminal_kwargs.append(dict(values))
        await super().terminal(**values)


def _narrating_script(script_id: str) -> list[ScriptedStep]:
    """One status tool call, then an answer narrating the unresolved name."""

    steps = status_then_answer(script_id)
    steps[-1] = ScriptedStep(
        decision=AgentFinalAnswer(
            _with_v2_handles(
                grounded_answer_payload(
                    script_id=script_id,
                    summary=_NARRATION,
                    validity_scope=scope_dict(),
                )
            )
        )
    )
    return steps


def _ungrounded_narrating_script(script_id: str) -> list[ScriptedStep]:
    """The same narration, carrying no citation the server can canonicalize.

    Used with ``_barren_registry``: the model cites nothing, so no claim can
    reference an evidence/metric id the tool results never returned, and the
    run reaches the CHAOS-3289 guard rather than dying earlier on an
    unrelated "claim references unknown evidence IDs" invariant.
    """

    steps = status_then_answer(script_id)
    payload = grounded_answer_payload(
        script_id=script_id, summary=_NARRATION, validity_scope=scope_dict()
    )
    payload["status"] = "partial"
    payload["claims"][0]["kind"] = "inferred"
    payload["claims"][0]["confidence"] = 0.6
    payload["claims"][0]["evidence_ref_ids"] = []
    payload["claims"][0]["metric_ref_ids"] = []
    steps[-1] = ScriptedStep(decision=AgentFinalAnswer(payload))
    return steps


def _v2_evidence_registry(calls: list[DevToolRequest]) -> AskDevToolRegistry:
    """The stock success registry, with v2-projectable evidence handles."""

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
        payload = _with_v2_handles(payload)
        # F10: a v1-sourced metric never carries evidence_ref_ids -- see
        # terminal_frames._wrap_legacy_metric.
        for metric in payload.get("metrics", []):
            metric["evidence_ref_ids"] = []
        return DevToolResult.model_validate(payload)

    return AskDevToolRegistry({tool_id: execute for tool_id in ToolID})


def _barren_registry(calls: list[DevToolRequest]) -> AskDevToolRegistry:
    """A registry whose tools succeed but return nothing citable.

    Not an *erroring* registry: a tool fault would terminate the run for a
    different reason entirely and prove nothing about the guard. This is the
    honest "the sources answered, and they had no metric, evidence, status
    fact, edge or definition to give" case -- the one where there genuinely
    is no server-verified material to ship in place of the rejected prose.
    """

    async def execute(_context: Any, request: DevToolRequest) -> DevToolResult:
        calls.append(request)
        payload = deepcopy(positive_fixtures()["dev_tool_result.v1"])
        payload.update(
            {
                "run_id": request.run_id,
                "tool_call_id": request.tool_call_id,
                "tool_id": request.tool_id.value,
                "metrics": [],
                "evidence": [],
                "status_facts": [],
                "graph_edges": [],
                "metric_definitions": [],
                "data_health": [],
            }
        )
        return DevToolResult.model_validate(payload)

    return AskDevToolRegistry({tool_id: execute for tool_id in ToolID})


async def _narrating_run(
    *,
    script_id: str,
    preflight_enabled: bool = True,
    script: Any = _narrating_script,
    registry_factory: Any = _v2_evidence_registry,
) -> RunOutput:
    return await run_preflight_orchestrator(
        question=f"How is {_UNRESOLVED_NAME} doing?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script=script,
        script_id=script_id,
        preflight_enabled=preflight_enabled,
        recorder_factory=_ArtifactRecorder,
        registry_factory=registry_factory,
    )


def _preflight_result_stub() -> SubjectPreflightResult:
    """The minimum shape ``_frame_cutover_active`` reads (mirrors
    ``test_chaos_3292_review_findings.py``'s own stub for the sibling seam).
    """

    return SubjectPreflightResult(
        decision=PreflightDecision.PROCEED,
        interpretation=None,  # type: ignore[arg-type]
        ledger=None,
        committed_resolution=None,
        answer=None,
        outcome=None,
        allowed_tools=frozenset(),
        diagnostic="test",
    )


def _bare_orchestrator() -> DevOrchestrator:
    """A ``DevOrchestrator`` wired only enough to call one pure-ish helper.

    No run is driven through it, so the provider script and registry are
    inert -- but they are the harness's own, not new stubs, so this cannot
    drift from what the e2e cases construct.
    """

    async def resolve(**_values: Any) -> DevScopeResolution:
        return _resolution()

    return DevOrchestrator(
        provider=RecordingProvider(status_then_answer("bare"), script_id="bare"),
        provider_source="platform",
        provider_family="scripted",
        registry=recording_registry([]),
        scope_resolver=resolve,
        versions=versions(),
        recorder=Recorder(),
    )


def _resolution() -> DevScopeResolution:
    return organization_resolution(DevScope.model_validate(scope_dict()))


def _coverage() -> DevCoverage:
    return DevCoverage(
        required_source_count=1,
        available_source_count=1,
        unavailable_required_sources=[],
        stale_required_sources=[],
        as_of=fixed_now(),
    )


def _model() -> DevModelMetadata:
    return DevModelMetadata(
        provider_source="platform",
        provider_family="scripted",
        model_fingerprint="0" * 24,
    )


def _canonical_tool_result() -> DevToolResult:
    """One successful tool result carrying real canonical evidence."""

    payload = deepcopy(positive_fixtures()["dev_tool_result.v1"])
    payload = _with_v2_handles(payload)
    for metric in payload.get("metrics", []):
        metric["evidence_ref_ids"] = []
    return DevToolResult.model_validate(payload)


def _empty_shell_answer(**kwargs: Any) -> DevAnswer:
    """The answer a defeated material predicate would produce: server copy,
    nothing else. Used only by the M2 mutation.
    """

    return DevAnswer(
        schema_version="dev_answer.v1",
        answer_id=kwargs["answer_id"],
        conversation_id=kwargs["conversation_id"],
        generated_at=kwargs["now"],
        resolved_scope=kwargs["resolution"],
        as_of=kwargs["now"],
        status=AnswerStatus.PARTIAL,
        direct_summary=SERVER_GROUNDED_SUMMARY,
        claims=[],
        metrics=[],
        evidence=[],
        conflicts=[],
        coverage=kwargs["coverage"],
        warnings=[],
        suggested_follow_up_questions=[],
        versions=versions(),
        model=kwargs["model"],
    )


def _artifacts(output: RunOutput) -> _ArtifactRecorder:
    assert isinstance(output.recorder, _ArtifactRecorder)
    return output.recorder


# ---------------------------------------------------------------------------
# C1 -- a guard rejection no longer erases a server-grounded result
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_c1_the_guard_fires_and_the_run_still_ships_a_grounded_result() -> None:
    """RED before the cutover: this exact run terminates
    ``insufficient_evidence`` with ``answer is None`` today, while holding
    canonical metrics and evidence the frame renders from.
    """

    output = await _narrating_run(script_id="s5-c1")

    # Anti-vacuity: the guard really did fire on this run. Without this the
    # rest of the case would pass just as well against a run the guard never
    # looked at.
    assert output.guard_reasons() == ("narrated_unresolved_entity",)

    assert output.result.state is RunState.COMPLETED
    assert output.result.error is None
    answer = output.result.answer
    assert answer is not None

    # What ships is server-owned, not the model's rejected prose.
    assert answer.direct_summary == SERVER_GROUNDED_SUMMARY
    assert list(answer.claims) == []
    assert answer.status is not AnswerStatus.COMPLETE
    # ...and it is substantive: the canonical tool material survived.
    assert answer.metrics or answer.evidence


@pytest.mark.asyncio
async def test_c1b_the_rejected_prose_reaches_no_shipped_artifact() -> None:
    """The narration the guard rejected must not survive anywhere.

    Checked across every artifact the run produces -- the v1 answer, the
    persisted frame, and the deterministic narrative rendered from that
    frame -- rather than only on the field the fix happened to touch.
    """

    output = await _narrating_run(script_id="s5-c1b")
    recorder = _artifacts(output)

    answer = output.result.answer
    assert answer is not None
    assert _UNRESOLVED_NAME.casefold() not in answer.model_dump_json().casefold()

    assert len(recorder.frames) == 1
    frame = recorder.frames[0]
    assert frame.public_outcome is PublicOutcome.ANSWERED_WITH_GAPS
    assert _UNRESOLVED_NAME.casefold() not in frame.model_dump_json().casefold()

    # Stack #4's deterministic fallback renders from that frame, so the
    # narrative is the user-facing prose. It must be frame-derived, which is
    # what makes "the name cannot appear" structural rather than incidental.
    assert len(recorder.narratives) == 1
    narrative = recorder.narratives[0]
    assert narrative.mode == "deterministic_fallback"
    assert _UNRESOLVED_NAME.casefold() not in narrative.body.casefold()
    assert SERVER_GROUNDED_SUMMARY.split(".")[0] in narrative.body


@pytest.mark.asyncio
async def test_c1c_the_guard_verdict_is_recorded_as_a_run_diagnostic() -> None:
    """Demoted, not deleted: the verdict is still recorded, and the run is
    still distinguishable from one that simply passed grounding validation.
    """

    output = await _narrating_run(script_id="s5-c1c")
    recorder = _artifacts(output)

    assert output.guard_reasons() == ("narrated_unresolved_entity",)
    assert len(recorder.terminal_kwargs) == 1
    assert (
        recorder.terminal_kwargs[0]["grounding_validation_status"]
        == GUARD_DEMOTED_NAMED_ENTITY_STATUS
    )


# ---------------------------------------------------------------------------
# C2 -- the negative control: no server material, still fails safe
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_c2_a_guard_rejection_with_no_server_material_still_fails_safe() -> None:
    """The cutover must not become "always ship something".

    Same question, same narrating model, same guard firing -- but the tools
    returned nothing citable and no plan ran, so there is no server-verified
    result to put in place of the rejected prose. The run must terminate
    exactly as it does today rather than ship an empty shell dressed as an
    answer.
    """

    output = await _narrating_run(
        script_id="s5-c2",
        script=_ungrounded_narrating_script,
        registry_factory=_barren_registry,
    )

    assert output.result.state is RunState.INSUFFICIENT_EVIDENCE
    assert output.result.answer is None
    assert output.result.error is not None
    assert output.result.error.code == "insufficient_evidence"


# ---------------------------------------------------------------------------
# C3 -- the flag gate: the flag-off path is unchanged
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_c3_the_flag_off_path_keeps_every_guard_reason_terminal() -> None:
    """``ask_dev_wave_3_1`` off means no preflight, no plan executor, and no
    proven frame path -- so the backstop stays the only check there is,
    terminating exactly as production does today.

    The question is phrased so the *legacy* grammar can see the name
    ("the <Name> project"): with no preflight there are no
    ``unresolved_name_spans`` to lend the guard, and the bare-name phrasing
    C1 uses is exactly the shape ``_named_entity_phrases`` is blind to. This
    run holds the same canonical material C1 does -- same registry, same
    narration -- and still ships nothing, which is the whole point: the
    difference is the cohort gate, not the evidence.
    """

    output = await run_preflight_orchestrator(
        question=f"What's the status of the {_UNRESOLVED_NAME} project?",
        entities=[(ORG_ID, ASK_DEV_PROJECT)],
        script=_narrating_script,
        script_id="s5-c3",
        preflight_enabled=False,
        recorder_factory=_ArtifactRecorder,
        registry_factory=_v2_evidence_registry,
    )

    assert output.result.state is RunState.INSUFFICIENT_EVIDENCE
    assert output.result.answer is None
    assert output.result.error is not None
    assert output.result.error.code == "insufficient_evidence"


def test_c3b_the_flag_gate_seam_is_exactly_the_wave_3_1_cohort() -> None:
    """The gate's own two cells, asserted directly.

    C3 shows the flag-off consequence end to end and M1 shows the flag-on
    run collapsing back to it when the seam is defeated; this pins what the
    seam actually reads, so a future change that widened it to (say) "a plan
    ran" rather than "this org is in the cohort" fails here rather than
    silently shipping the cutover to organizations the rollout gate
    excludes.
    """

    assert DevOrchestrator._frame_cutover_active(None) is False
    assert DevOrchestrator._frame_cutover_active(_preflight_result_stub()) is True


# ---------------------------------------------------------------------------
# M -- mutations: defeat each seam, observe which control notices
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_m1_defeating_the_flag_gate_restores_the_pre_cutover_outcome(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_frame_cutover_active`` forced false is the pre-cutover tree.

    This is the in-tree stand-in for running C1 against old code: with this
    one decision defeated, the flag-on run terminates ``insufficient_
    evidence`` with no answer -- exactly what C1 asserts must no longer
    happen.
    """

    monkeypatch.setattr(
        DevOrchestrator, "_frame_cutover_active", staticmethod(lambda _preflight: False)
    )
    output = await _narrating_run(script_id="s5-m1")

    assert output.result.state is RunState.INSUFFICIENT_EVIDENCE
    assert output.result.answer is None


@pytest.mark.asyncio
async def test_m2_defeating_the_material_predicate_breaks_the_negative_control(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The fail-safe in C2 is load-bearing, not incidental.

    Forcing the canonical extraction to report material makes the demotion
    believe there is something to ship when there is not. C2's run then
    completes carrying no metric, no evidence and no claim -- the empty
    shell the CHAOS-3290 floor exists to prevent, and the reason the
    predicate reads real canonical tuples rather than an unconditional yes.
    """

    monkeypatch.setattr(
        DevOrchestrator,
        "_server_grounded_answer",
        lambda self, **kwargs: _empty_shell_answer(**kwargs),
    )
    output = await _narrating_run(
        script_id="s5-m2",
        script=_ungrounded_narrating_script,
        registry_factory=_barren_registry,
    )

    assert output.result.state is RunState.COMPLETED
    answer = output.result.answer
    assert answer is not None
    assert not answer.metrics and not answer.evidence and not answer.claims


# ---------------------------------------------------------------------------
# C4 -- plan findings alone are NOT shippable material
# ---------------------------------------------------------------------------


class _ExplodingInvestigationResult:
    """Any read of this object is a test failure.

    Stronger than passing a findings fixture and asserting the result: a
    fixture proves the answer for ONE investigation_result, this proves the
    parameter is not consulted for ANY of them.
    """

    def __getattr__(self, name: str) -> Any:
        raise AssertionError(
            f"_server_grounded_answer must not read investigation_result "
            f"(accessed {name!r}) -- plan findings are not shippable v1 material"
        )


def test_c4_plan_findings_are_never_the_material_that_justifies_demoting() -> None:
    """Codex adversarial review round 1 (HIGH), closed here permanently.

    An earlier revision counted the plan's health/deficiency findings as
    sufficient material. They are real server-computed content, but
    ``finish()`` embeds them into the FRAME, and no client surface reads a
    frame today -- ``streaming.py`` sends ``result.answer`` live and the
    router's replay prefers the stored v1 answer. So demoting on findings
    alone terminated COMPLETED while the client received an answer with no
    claim, no metric and no evidence: strictly worse than the honest
    ``insufficient_evidence`` it replaced.

    Both cells asserted, so this is a statement about the predicate rather
    than about one input: with no canonical material the seam refuses even
    though an investigation_result is present, and with canonical material
    it builds one without ever consulting it.
    """

    orchestrator = _bare_orchestrator()
    common: dict[str, Any] = {
        "answer_id": ANSWER_ID,
        "conversation_id": CONVERSATION_ID,
        "resolution": _resolution(),
        "coverage": _coverage(),
        "investigation_result": cast(Any, _ExplodingInvestigationResult()),
        "model": _model(),
        "now": fixed_now(),
        "cutover_active": True,
    }

    assert orchestrator._server_grounded_answer(tool_results=(), **common) is None

    with_material = orchestrator._server_grounded_answer(
        tool_results=(_canonical_tool_result(),), **common
    )
    assert with_material is not None
    assert with_material.evidence
    assert with_material.direct_summary == SERVER_GROUNDED_SUMMARY


def test_c4b_the_grounding_floor_can_never_reach_the_demotion_seam() -> None:
    """The implication the orchestrator's own comment states, pinned.

    ``validate_answer_candidate`` is handed an answer whose ``metrics`` and
    ``evidence`` the orchestrator has already overwritten with the canonical
    tuples, so ``answer_grounding_floor_not_met`` implies both are empty --
    and empty canonical tuples are exactly what ``_server_grounded_answer``
    refuses to build on. The CHAOS-3290 floor therefore never had server
    material to erase. If a future change to the floor's trigger makes that
    branch genuinely live, this fails rather than the branch quietly
    starting to ship answers nobody re-reviewed.
    """

    from dev_health_ops.api.dev import answer_validator

    source = inspect.getsource(answer_validator._answer_has_material_grounding)
    assert "answer.metrics" in source and "answer.evidence" in source, (
        "the floor no longer keys on answer.metrics/answer.evidence; the "
        "orchestrator's 'this branch cannot fire' reasoning must be re-derived"
    )
