"""CHAOS-3297 stack #1 -- unit-level controls for ``terminal_frames.py``.

Property manifest cross-references (see
``/Users/chris/.claude/jobs/7ceca217/tmp/chaos-3297-s1-manifest.md``):

* F-CODES -- ``ORCHESTRATOR_ERROR_CODES`` is exactly the set of codes
  ``orchestrator.run()``'s own ``error(...)`` closure emits (extracted from
  the live source, not a hand-duplicated list).
* F-BUCKET -- ``PUBLIC_OUTCOME_BY_ERROR_CODE`` is total over
  ``ORCHESTRATOR_ERROR_CODES`` and lands inside ``ck_dev_runs_public_outcome``.
* F-TOOLSRC -- ``SOURCE_CLASS_BY_TOOL_ID`` is total over ``ToolID``; the
  ``"tool_results"`` sentinel (``orchestrator._budget_answer``) falls to
  ``SourceClass.SOURCE_HEALTH``.
* F-COHERENCE -- for every orchestrator error code, replaying a run
  terminated with that code reconstructs the *exact* original v1 code,
  regardless of whether the frame-reconstruction path or the
  ``run.safe_error_code``-exact-fidelity fallback is taken internally
  (``router._replayed_result``'s CHAOS-3297 guard).
* F-PLANID -- ``LEGACY_ANSWER_PLAN_ID`` is a deliberately unregistered plan
  id (grammar-valid, not in ``PLAN_REGISTRY``).
"""

from __future__ import annotations

import ast
import inspect
import re
import textwrap
import uuid
from copy import deepcopy
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest
from pydantic import ValidationError

from dev_health_ops.api.dev import orchestrator as _orchestrator_module
from dev_health_ops.api.dev import terminal_frames as tf
from dev_health_ops.api.dev.contract_fixtures import positive_fixtures
from dev_health_ops.api.dev.contract_fixtures_v2 import (
    positive_fixtures as positive_fixtures_v2,
)
from dev_health_ops.api.dev.contracts import DevAnswer, DevTimeRange, ToolID
from dev_health_ops.api.dev.contracts_v2 import validators as validators_module
from dev_health_ops.api.dev.contracts_v2.base import PublicOutcome, SourceClass
from dev_health_ops.api.dev.contracts_v2.frame import DevAnswerFrame as _FrameV2
from dev_health_ops.api.dev.contracts_v2.plan import PLAN_REGISTRY
from dev_health_ops.api.dev.orchestrator_states import RunState
from dev_health_ops.api.dev.router import _replayed_result
from dev_health_ops.llm.agent.errors import AgentProviderError, AgentProviderErrorCode

_TIME_RANGE = DevTimeRange(
    start=datetime(2026, 7, 1, tzinfo=UTC),
    end=datetime(2026, 7, 2, tzinfo=UTC),
    timezone="UTC",
)


#: The v1 evidence-handle grammar (`evidence_service.EvidenceHandleService.issue`).
_REAL_EVIDENCE_HANDLE = "ev1_" + ("a1b2c3d4e5" * 4)


def _legacy_answer(*, claims: list[dict[str, Any]] | None = None) -> DevAnswer:
    """A fully-validated v1 answer, mirroring the fixture but with real
    evidence handles (the fixture's ``ev_01`` fails ``EvidenceHandle``'s
    strict grammar, which only v2's embedded mirrors enforce)."""

    payload = deepcopy(positive_fixtures()["dev_answer.v1"])
    text = __import__("json").dumps(payload, default=str)
    payload = __import__("json").loads(re.sub(r"ev_\d+", _REAL_EVIDENCE_HANDLE, text))
    if claims is not None:
        payload["claims"] = claims
    return DevAnswer.model_validate(payload)


# ---------------------------------------------------------------------------
# F-CODES -- ORCHESTRATOR_ERROR_CODES matches the live source, not a
# hand-duplicated list (feedback_generate_fixtures_from_the_producer).
#
# CHAOS-3297 Codex review round 2 MEDIUM #2: the original version of this
# test regex-scraped literal ``error("...")`` call sites plus
# ``_provider_error``'s ``code_map`` dict literal's source text. That missed
# ``_LEGACY_GUARD_TERMINALS`` entirely -- its call site (``error(code,
# message)``) passes a *variable*, invisible to any source-text/regex/AST
# scrape of the call site itself -- and the gap was silent only because the
# table's three values today happen to coincide with codes registered
# elsewhere. Replaced with AST parsing for the one genuinely
# literal-only producer (the local closure's call sites), and true runtime
# enumeration -- reading the live ``_LEGACY_GUARD_TERMINALS`` dict object
# directly, and actually driving ``_provider_error`` for every
# ``AgentProviderErrorCode`` member -- for the two producers that have a
# real object/function to enumerate instead of scraping.
# ---------------------------------------------------------------------------


def _local_error_closure_codes() -> set[str]:
    """Every literal code ``orchestrator.run()``'s local ``error(...)``
    closure constructs, by walking the AST of ``DevOrchestrator.run``'s own
    source -- robust to reformatting in a way regex is not, and it is the
    same source ``inspect`` and Python's own compiler agree on.

    A call site that passes a *variable* for the code (the
    ``_LEGACY_GUARD_TERMINALS`` lookup) has no literal to extract here by
    construction -- see ``_legacy_guard_terminal_codes`` for that table's
    own runtime enumeration.
    """

    source = textwrap.dedent(
        inspect.getsource(_orchestrator_module.DevOrchestrator.run)
    )
    tree = ast.parse(source)
    codes: set[str] = set()
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "error"
            and node.args
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        ):
            codes.add(node.args[0].value)
    return codes


def _legacy_guard_terminal_codes() -> set[str]:
    """Runtime enumeration (not source scraping) of
    ``_LEGACY_GUARD_TERMINALS``'s own values.

    Codex round 2 repro: this table's call site (``error(code, message)``)
    passes a variable, so no scrape of the call site itself can see which
    code it carries -- only reading the table's live values does. Before
    this, the totality test never read this table at all; it passed only
    by coincidence (its three values today match codes registered via
    other, literal call sites), so swapping one value (e.g.
    ``resolve_scope_not_found`` -> ``"conversation_not_found"``) went
    completely undetected. See
    ``test_legacy_guard_terminal_value_drift_is_caught_by_the_totality_assertion``
    for the reproduction, made permanent.
    """

    return {
        code for code, _message in _orchestrator_module._LEGACY_GUARD_TERMINALS.values()
    }


def _provider_error_codes() -> set[str]:
    """Runtime enumeration of every code ``DevOrchestrator._provider_error``
    can produce -- drives the real function for every
    ``AgentProviderErrorCode`` member, rather than scraping its
    ``code_map`` dict literal's source text. Any future member added to
    ``AgentProviderErrorCode`` (or a remapped value) is picked up
    automatically, with zero regex to keep in sync.
    """

    codes: set[str] = set()
    for member in AgentProviderErrorCode:
        result = _orchestrator_module.DevOrchestrator._provider_error(
            "request_totality_probe", AgentProviderError(member)
        )
        codes.add(result.code)
    return codes


def test_orchestrator_error_codes_matches_the_live_source() -> None:
    local_codes = _local_error_closure_codes()
    legacy_guard_codes = _legacy_guard_terminal_codes()
    provider_codes = _provider_error_codes()
    assert local_codes, "sanity: orchestrator.run() must call error(...) at least once"
    assert legacy_guard_codes, "sanity: _LEGACY_GUARD_TERMINALS must have entries"
    assert provider_codes, "sanity: _provider_error must map at least one code"
    combined = local_codes | legacy_guard_codes | provider_codes
    assert combined == tf.ORCHESTRATOR_ERROR_CODES, (
        "a code was added to (or removed from) orchestrator.run()'s error() "
        "closure, _LEGACY_GUARD_TERMINALS, or "
        "DevOrchestrator._provider_error's code_map without updating "
        "terminal_frames.ORCHESTRATOR_ERROR_CODES -- "
        f"missing={combined - tf.ORCHESTRATOR_ERROR_CODES} "
        f"stale={tf.ORCHESTRATOR_ERROR_CODES - combined}"
    )


def test_legacy_guard_terminal_value_drift_is_caught_by_the_totality_assertion() -> (
    None
):
    """Rule 2: reproduce codex's exact mutation (a guard-table value swap)
    and show the totality computation now catches it.

    Mutates a *copy* of ``_LEGACY_GUARD_TERMINALS`` (never the live module
    dict -- this must not leak into other tests) so
    ``resolve_scope_not_found`` maps to an unregistered code, exactly
    codex's repro, and shows the combined-codes computation now includes
    that unregistered value -- the observable gap the old regex-only test
    could never have caught, because it never read this table at all.
    """

    mutated = dict(_orchestrator_module._LEGACY_GUARD_TERMINALS)
    original_message = mutated["resolve_scope_not_found"][1]
    mutated["resolve_scope_not_found"] = ("conversation_not_found", original_message)
    mutated_legacy_guard_codes = {code for code, _message in mutated.values()}

    combined = (
        _local_error_closure_codes()
        | mutated_legacy_guard_codes
        | _provider_error_codes()
    )
    assert "conversation_not_found" not in tf.ORCHESTRATOR_ERROR_CODES, (
        "sanity: the mutation must introduce a code the registry genuinely "
        "does not recognize"
    )
    assert combined != tf.ORCHESTRATOR_ERROR_CODES
    assert "conversation_not_found" in (combined - tf.ORCHESTRATOR_ERROR_CODES), (
        "the mutated guard-table value must be observable as a gap -- this "
        "is what makes the totality assertion load-bearing for this table, "
        "not merely present"
    )


# ---------------------------------------------------------------------------
# UnregisteredTerminalCode -- finish()'s fallback is dedicated, logged, and
# counted (CHAOS-3297 Codex review round 2 MEDIUM #2).
# ---------------------------------------------------------------------------


def test_build_error_frame_raises_the_dedicated_unregistered_exception() -> None:
    """A bare ValueError is not what orchestrator.finish() catches for this
    -- it must be exactly UnregisteredTerminalCode, so an unrelated
    ValueError from frame construction elsewhere is never silently folded
    into the same fallback."""

    with pytest.raises(tf.UnregisteredTerminalCode):
        tf.build_error_frame(
            code="not_a_real_code", run_id="run_01", generated_at=datetime.now(UTC)
        )


# The log-record + counter-increment coverage for finish()'s registry-gap
# fallback lives in test_orchestrator.py (see
# test_finish_falls_back_to_a_registered_bucket_for_an_unregistered_code),
# which already drives the real DevOrchestrator.run() through this branch
# and has the async test harness for it -- not duplicated here.


# ---------------------------------------------------------------------------
# F-BUCKET -- totality + landing inside the DB's closed public_outcome set.
# ---------------------------------------------------------------------------


def test_public_outcome_bucket_table_is_total_over_orchestrator_codes() -> None:
    assert set(tf.PUBLIC_OUTCOME_BY_ERROR_CODE) == tf.ORCHESTRATOR_ERROR_CODES


def test_public_outcome_bucket_table_stays_inside_the_closed_wire_vocabulary() -> None:
    # Mirrors ck_dev_runs_public_outcome / ck_dev_answer_frames_public_outcome
    # verbatim rather than importing a Python constant for it, so a change to
    # either the enum *or* the DB CHECK constraint is caught independently.
    db_closed_vocabulary = {
        "answered",
        "answered_with_gaps",
        "needs_clarification",
        "not_found",
        "temporarily_unavailable",
        "unsupported",
        "denied",
        "failed",
    }
    for outcome in tf.PUBLIC_OUTCOME_BY_ERROR_CODE.values():
        assert outcome.value in db_closed_vocabulary


def test_removing_a_bucket_entry_is_caught_by_the_totality_assertion() -> None:
    """Rule 2: observe the totality guard actually fail, not just exist.

    Reproduces the import-time check inline against a *mutated* copy of the
    table -- re-importing the module with a monkeypatched dict would not
    exercise the module-level assertion (it already ran once at import).
    """

    mutated = dict(tf.PUBLIC_OUTCOME_BY_ERROR_CODE)
    removed_code = next(iter(mutated))
    del mutated[removed_code]
    missing = tf.ORCHESTRATOR_ERROR_CODES - set(mutated)
    assert missing == {removed_code}, "the mutation must be observable as a gap"


# ---------------------------------------------------------------------------
# F-TOOLSRC -- SOURCE_CLASS_BY_TOOL_ID totality + the "tool_results" sentinel.
# ---------------------------------------------------------------------------


def test_source_class_by_tool_id_is_total_over_tool_id() -> None:
    assert set(tf.SOURCE_CLASS_BY_TOOL_ID) == frozenset(ToolID)


def test_budget_answer_tool_results_sentinel_falls_to_source_health() -> None:
    """orchestrator._budget_answer reports the literal string "tool_results"
    (not a ToolID member) as an unavailable source on a degraded partial
    answer. Pinned here rather than only mapped, per review: a silent
    mis-map would corrupt the legacy-wrap frame's coverage block."""

    answer = _legacy_answer()
    payload = answer.model_dump(mode="json")
    payload["status"] = "degraded"
    payload["coverage"]["unavailable_required_sources"] = ["tool_results"]
    payload["coverage"]["available_source_count"] = 0
    answer = DevAnswer.model_validate(payload)
    frame = tf.wrap_legacy_answer_as_frame(answer, run_id="run_budget_partial")
    assert frame.coverage.unavailable_required_sources == (SourceClass.SOURCE_HEALTH,)


# ---------------------------------------------------------------------------
# F-PLANID -- a registered compatibility plan id, enforced by
# validate_plan_registry_membership on every content-bearing frame
# (CHAOS-3297 Codex review MEDIUM #3).
# ---------------------------------------------------------------------------


def test_legacy_plan_id_is_a_registered_compatibility_entry() -> None:
    assert tf.LEGACY_ANSWER_PLAN_ID in PLAN_REGISTRY


def test_content_bearing_frame_rejects_an_unregistered_plan_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Validator-level coverage for the legacy-answer shape
    (``wrap_legacy_answer_as_frame``'s ``answered``/``answered_with_gaps``
    content-bearing path).

    ``versions.plan_id``'s type only enforces the dotted-token *grammar*
    (``PlatformVersionToken``) -- a grammar-valid but unregistered token
    (unlike ``"private/Nightfall"``, which the grammar itself already
    rejects) would have passed every validator that existed before this
    guard. Rule 2: the mutation pair below proves that -- disabling only
    ``validate_plan_registry_membership`` (the old validator set) lets the
    identical bad payload through; the guard is what changed.
    """

    payload = deepcopy(positive_fixtures_v2()["dev_answer_frame.v1"])
    assert payload["public_outcome"] == "answered"
    payload["versions"]["plan_id"] = "unregistered.plan.v1"

    with pytest.raises(ValidationError, match="PLAN_REGISTRY"):
        _FrameV2.model_validate(payload)

    monkeypatch.setattr(
        validators_module,
        "validate_plan_registry_membership",
        lambda *_a, **_k: None,
    )
    _FrameV2.model_validate(payload)  # old validator set: passes the bad frame


def test_scope_ambiguous_frame_rejects_an_unregistered_plan_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Validator-level coverage for ``build_error_frame``'s ``scope_ambiguous``
    path.

    ``needs_clarification`` is not a no-answer outcome (see
    ``NO_ANSWER_OUTCOMES``), so this frame carries a ``versions`` block too
    -- and, before this guard, its ``plan_id`` was equally unchecked against
    ``PLAN_REGISTRY``. Same mutation pair as the legacy-answer case above,
    against the ``scope_ambiguous`` frame shape specifically.
    """

    payload = tf.build_error_frame(
        code="scope_ambiguous", run_id="run_01", generated_at=datetime.now(UTC)
    ).model_dump(mode="json")
    assert payload["versions"] is not None
    payload["versions"]["plan_id"] = "unregistered.plan.v1"

    with pytest.raises(ValidationError, match="PLAN_REGISTRY"):
        _FrameV2.model_validate(payload)

    monkeypatch.setattr(
        validators_module,
        "validate_plan_registry_membership",
        lambda *_a, **_k: None,
    )
    _FrameV2.model_validate(payload)  # old validator set: passes the bad frame


# ---------------------------------------------------------------------------
# build_error_frame -- every orchestrator error code builds a valid frame in
# the bucket the table says it should.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("code", sorted(tf.ORCHESTRATOR_ERROR_CODES))
def test_build_error_frame_is_valid_for_every_orchestrator_code(code: str) -> None:
    frame = tf.build_error_frame(
        code=code, run_id="run_01", generated_at=datetime.now(UTC)
    )
    assert frame.public_outcome == tf.PUBLIC_OUTCOME_BY_ERROR_CODE[code]
    assert not frame.sections and not frame.facts, "an error frame carries no content"


def test_build_error_frame_rejects_an_unregistered_code() -> None:
    with pytest.raises(ValueError, match="unregistered"):
        tf.build_error_frame(
            code="not_a_real_code", run_id="run_01", generated_at=datetime.now(UTC)
        )


# ---------------------------------------------------------------------------
# wrap_legacy_answer_as_frame
# ---------------------------------------------------------------------------


def test_wrap_legacy_answer_is_always_answered_with_gaps_never_answered() -> None:
    answer = _legacy_answer()
    frame = tf.wrap_legacy_answer_as_frame(answer, run_id="run_01")
    assert frame.public_outcome is PublicOutcome.ANSWERED_WITH_GAPS
    assert frame.completion is not None and frame.completion.calculable is False


def test_wrap_legacy_answer_with_empty_claims_still_has_content() -> None:
    """orchestrator._budget_answer's metrics-only partial answer has zero
    claims; the frame must still satisfy validate_outcome_consistency's
    has_content check via the always-emitted synthetic section."""

    answer = _legacy_answer(claims=[])
    frame = tf.wrap_legacy_answer_as_frame(answer, run_id="run_01")
    assert frame.facts == ()
    assert len(frame.sections) == 1
    assert frame.sections[0].fact_ids == ()


def test_wrap_legacy_answer_drops_no_evidence_or_relationship_content() -> None:
    answer = _legacy_answer()
    frame = tf.wrap_legacy_answer_as_frame(answer, run_id="run_01")
    known_evidence = {item.evidence_ref_id for item in frame.evidence}
    for fact in frame.facts:
        assert set(fact.evidence_ref_ids) <= known_evidence


def test_frame_construction_is_deterministic_for_the_same_inputs() -> None:
    """P3: byte-identical frame bytes for the same (run_id, answer) pair."""

    answer = _legacy_answer()
    first = tf.wrap_legacy_answer_as_frame(answer, run_id="run_determinism")
    second = tf.wrap_legacy_answer_as_frame(answer, run_id="run_determinism")
    assert first.model_dump(mode="json") == second.model_dump(mode="json")


# ---------------------------------------------------------------------------
# F-COHERENCE -- replaying a run terminated with any orchestrator error code
# reconstructs the exact original v1 code, whichever internal path is taken.
# ---------------------------------------------------------------------------


def _fake_run(
    *, run_id: uuid.UUID, code: str, public_outcome: PublicOutcome
) -> SimpleNamespace:
    """A run row that predates 0079's ``terminal_error_payload`` column.

    Explicit ``terminal_error_payload=None`` (rather than omitting the
    attribute) so ``_replayed_result`` takes its frame-reconstruction
    fallback branch -- exactly what these F-COHERENCE tests exist to cover.
    A run created after 0079 always has this column populated by
    ``PersistenceRunRecorder.terminal`` and never reaches that branch; see
    ``test_router.py``'s two-POST replay tests for that (current) path.
    """

    now = datetime.now(UTC)
    return SimpleNamespace(
        id=run_id,
        conversation_id=uuid.uuid4(),
        request_id=uuid.uuid4(),
        state=RunState.FAILED.value
        if public_outcome is not PublicOutcome.NEEDS_CLARIFICATION
        else RunState.INSUFFICIENT_EVIDENCE.value,
        started_at=now,
        ended_at=now,
        public_outcome=public_outcome.value,
        safe_error_code=code,
        terminal_error_payload=None,
        input_tokens=0,
        output_tokens=0,
        estimated_cost_microusd=0,
        tool_call_count=0,
        provider_fingerprint=None,
        model_fingerprint=None,
    )


@pytest.mark.parametrize("code", sorted(tf.ORCHESTRATOR_ERROR_CODES))
def test_replay_reconstructs_the_exact_orchestrator_error_code(code: str) -> None:
    outcome = tf.PUBLIC_OUTCOME_BY_ERROR_CODE[code]
    run_id = uuid.uuid4()
    # Real UUID, matching the frame's own run_id exactly (as production
    # always has): a mismatch here would raise inside DevAnswerV2's own
    # validator and get masked by _replayed_result's broad `except
    # ValidationError`, making this test pass for the wrong reason (Rule 1
    # -- assert the state the system reaches, not that the code merely ran).
    frame = tf.build_error_frame(
        code=code, run_id=str(run_id), generated_at=datetime.now(UTC)
    )
    run = _fake_run(run_id=run_id, code=code, public_outcome=outcome)
    result = _replayed_result(
        run=run,
        answer_payload=None,
        frame_payload=frame.model_dump(mode="json"),
        organization_id="org_fullchaos",
        time_range=_TIME_RANGE,
    )
    assert result.error is not None
    assert result.error.code == code, (
        "replay must reconstruct the exact live v1 code regardless of "
        "whether the frame-projection path or the safe_error_code fallback "
        "was taken internally"
    )


def test_replay_coherence_guard_is_load_bearing() -> None:
    """Rule 2: observe the CHAOS-3297 router guard actually fail without it.

    Picks a code (``scope_forbidden``) whose frame-projected reconstruction
    is known to diverge from the live code (DENIED's fixed table code is
    ``forbidden``, not ``scope_forbidden``) and proves the *unguarded*
    reconstruction path would silently rewrite it -- i.e. the guard in
    ``_replayed_result`` is the thing preventing that, not an accident of
    the fixture.
    """

    from dev_health_ops.api.dev.contracts_v2.answer import (
        _OUTCOME_DISPLAY_LABELS,
        DevAnswerV2,
    )
    from dev_health_ops.api.dev.contracts_v2.frame import DevAnswerFrame as _FrameV2
    from dev_health_ops.api.dev.preflight_outcomes import project_preflight_error

    code = "scope_forbidden"
    outcome = tf.PUBLIC_OUTCOME_BY_ERROR_CODE[code]
    assert outcome is PublicOutcome.DENIED
    run_id = uuid.uuid4()
    run = _fake_run(run_id=run_id, code=code, public_outcome=outcome)
    frame = tf.build_error_frame(
        code=code, run_id=str(run_id), generated_at=datetime.now(UTC)
    )

    frame_obj = _FrameV2.model_validate(frame.model_dump(mode="json"))
    answer_v2 = DevAnswerV2(
        schema_version="dev_answer.v2",
        answer_id=str(run.id),
        conversation_id=str(run.conversation_id),
        run_id=str(run.id),
        generated_at=run.ended_at,
        public_outcome=frame_obj.public_outcome,
        outcome_display_label=_OUTCOME_DISPLAY_LABELS[frame_obj.public_outcome],
        frame=frame_obj,
        narrative=None,
    )
    unguarded = project_preflight_error(answer_v2, request_id=str(run.request_id))
    assert unguarded.code == "forbidden", (
        "sanity: the frame-projection path really does produce a different "
        "code than the live orchestrator's own 'scope_forbidden' -- if this "
        "fails, the guard test above proves nothing"
    )

    guarded = _replayed_result(
        run=run,
        answer_payload=None,
        frame_payload=frame.model_dump(mode="json"),
        organization_id="org_fullchaos",
        time_range=_TIME_RANGE,
    )
    assert guarded.error is not None
    assert guarded.error.code == code, (
        "the CHAOS-3297 guard must override the divergent projection"
    )
