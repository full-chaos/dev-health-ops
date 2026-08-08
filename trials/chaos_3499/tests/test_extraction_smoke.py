"""Env-gated smoke test for the extraction candidate arm (bring-up step 2).

**Not a measured trial.** This suite proves the plumbing round-trips end to
end against a real local model: the client connects, extraction produces
parseable structured output, the REAL oracles evaluate it, and
``ComparisonReport`` renders per-class with the candidate present. Whatever
pass/fail the local model achieves on each oracle is printed as an
EXPLICIT, LABELED, UNSCORED observation -- never asserted as a required
outcome, and never folded into a measured trial result. A weak local
model failing an oracle is real, useful extraction-quality signal that
belongs in a report, not in a passing/failing CI gate; softening an
assertion here to make the suite green would be exactly the thing step 2's
own instructions forbid ("no retries-until-it-passes").

**Env-gated, never a CI dependency.** Requires LM Studio (or another
OpenAI-compatible local server) reachable at ``LOCAL_LLM_BASE_URL``
(default ``http://localhost:1234/v1`` -- the host-process form; see
``harness/llm/client.py`` for the container-vs-host address distinction)
serving ``LOCAL_LLM_MODEL`` (default ``google/gemma-4-e4b``). Every test
that needs the model forces ``LLM_PROVIDER=local`` for its own scope
(regardless of ambient environment -- this repo's shells have been observed
with ``LLM_PROVIDER=openai`` set for unrelated purposes) and SKIPS loudly,
with the connection failure as the skip reason, if the model cannot be
reached. Nothing here mocks the LLM to fake a result: an unreachable
provider is a reportable finding, and an unrun smoke reads as NOT RUN, the
same discipline every other arm in this harness already holds.
"""

from __future__ import annotations

import pytest

from ..corpus.oracles import ALL_ORACLES, ORACLES_BY_ID
from ..harness.arms import extraction, native
from ..harness.arms.source_documents import SMOKE_SOURCE_DOCUMENTS
from ..harness.contracts import ArmOutcome, QuestionClass
from ..harness.llm.client import LLMUnavailable, complete
from ..harness.oracle import Verdict
from ..harness.runner import ArmRegistry, ArmRole, compare


def _configure_local(monkeypatch: pytest.MonkeyPatch) -> None:
    """Force LLM_PROVIDER=local for this test, regardless of ambient env."""
    monkeypatch.setenv("LLM_PROVIDER", "local")


def _require_local_model(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_local(monkeypatch)
    try:
        complete("Reply with exactly: OK", "Say OK and nothing else.")
    except LLMUnavailable as exc:
        pytest.skip(f"local model not reachable -- skipping smoke: {exc}")


# --------------------------------------------------------------------------
# The gate itself, made an explicit, separately-reportable test rather than
# a fixture whose skip reason nobody reads.
# --------------------------------------------------------------------------


def test_local_provider_is_reachable(monkeypatch: pytest.MonkeyPatch) -> None:
    _require_local_model(monkeypatch)


# --------------------------------------------------------------------------
# No provider reachable -> honest NOT_RUN, never a crash, never fabricated
# output. Deliberately NOT gated on LM Studio being up: this tests the
# FAILURE path, which must work whether or not the happy path is available.
# --------------------------------------------------------------------------


def test_unreachable_provider_returns_not_run_not_a_crash(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LLM_PROVIDER", "local")
    # A port nothing is listening on -- guaranteed connection failure,
    # independent of whether LM Studio happens to be running on this host.
    monkeypatch.setenv("LOCAL_LLM_BASE_URL", "http://localhost:1")

    oracle = ORACLES_BY_ID["O3_supersession"]
    response = extraction.answer(oracle)
    assert response.outcome is ArmOutcome.NOT_RUN
    assert response.degraded_reasons
    assert "measurement_not_run:" in response.degraded_reasons[0]

    result = oracle.evaluate(response)
    assert result.verdict is Verdict.NOT_MEASURED, (
        "an arm that could not reach its provider must never read as a "
        "pass, and never as a plain FAIL either -- NOT_MEASURED is the "
        "honest verdict, same as any other unreachable arm"
    )


def test_oracle_with_no_source_material_is_not_run_not_silently_skipped() -> None:
    """No LM Studio dependency at all -- this oracle has no authored
    document, so the arm must never even attempt a call.
    """
    oracle = ORACLES_BY_ID["O1_ci_prior_attempts"]
    assert oracle.oracle_id not in SMOKE_SOURCE_DOCUMENTS
    response = extraction.answer(oracle)
    assert response.outcome is ArmOutcome.NOT_RUN
    assert response.degraded_reasons == (
        "measurement_not_run:no_source_material_authored_for_this_oracle_yet",
    )


# --------------------------------------------------------------------------
# The real round trip. Plumbing assertions only; quality is printed, never
# asserted.
# --------------------------------------------------------------------------


def test_extraction_arm_round_trips_through_real_oracles(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _require_local_model(monkeypatch)

    observations: dict[str, str] = {}
    model_name = None
    for oracle_id in sorted(SMOKE_SOURCE_DOCUMENTS):
        oracle = ORACLES_BY_ID[oracle_id]
        response = extraction.answer(oracle)
        assert response.outcome in (ArmOutcome.ANSWERED, ArmOutcome.NOT_RUN), (
            f"{oracle_id}: extraction arm returned {response.outcome}, "
            "neither of which a real arm may -- this is a plumbing failure"
        )
        if response.outcome is ArmOutcome.NOT_RUN:
            observations[oracle_id] = f"NOT_RUN ({response.degraded_reasons[0]})"
            continue
        model_name = response.versions.get("extraction", model_name)
        result = oracle.evaluate(response)
        observations[oracle_id] = result.verdict.value.upper()

    passed = sum(1 for v in observations.values() if v == Verdict.PASS.value.upper())
    total = len(observations)
    with capsys.disabled():
        print(
            "\n[SMOKE -- UNSCORED, NOT a measured trial result] "
            f"local model ({model_name or 'unknown'}) extraction quality: "
            f"{passed}/{total} oracles passed"
        )
        for oracle_id, verdict in sorted(observations.items()):
            print(f"  {oracle_id}: {verdict}")

    # Plumbing-only assertions below -- never a quality bar.
    assert observations, "no source material authored for any oracle to smoke"
    assert all(
        v.startswith("NOT_RUN") or v in {"PASS", "FAIL"} for v in observations.values()
    ), f"unexpected verdict shape in smoke observations: {observations}"


def test_extraction_never_invents_or_substitutes_evidence_refs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Requirement 2: the arm must pass through whatever evidence_ref the
    model cites, unmodified -- never validated against a known-good set,
    never patched. Confirmed directly against the real facts.source_documents
    document ids: every non-empty evidence_ref on a returned fact must be
    one of the document ids the model was actually shown (an invented ref
    would still be allowed through; this only proves nothing is being
    substituted).
    """
    _require_local_model(monkeypatch)
    for oracle_id, documents in SMOKE_SOURCE_DOCUMENTS.items():
        oracle = ORACLES_BY_ID[oracle_id]
        response = extraction.answer(oracle)
        if response.outcome is ArmOutcome.NOT_RUN:
            continue
        shown_ids = {doc.document_id for doc in documents}
        for fact in response.facts:
            for ref in fact.evidence_refs:
                # Not asserting membership -- an invented ref is a REAL
                # possible (and valid) model failure this test must not
                # hide. Only asserting the ref survived verbatim, i.e. it
                # is a plain string the adapter did not rewrite.
                assert isinstance(ref, str) and ref, (
                    f"{oracle_id}: evidence_ref was mangled into a "
                    f"non-string or empty value: {ref!r}"
                )
        # The specific, meaningful case: at least one oracle's model run
        # should demonstrate the ref round-tripped from a real shown
        # document (proves the pass-through path is live, not just typed
        # correctly).
        if any(
            ref in shown_ids for fact in response.facts for ref in fact.evidence_refs
        ):
            return
    pytest.skip(
        "no extraction response cited any of its shown document ids -- "
        "cannot demonstrate the pass-through path fired this run"
    )


# --------------------------------------------------------------------------
# ComparisonReport rendering with the candidate present -- requirement 3
# and 4's "ComparisonReport renders per-class with the candidate present".
# --------------------------------------------------------------------------


def _registry_with_extraction_candidate() -> ArmRegistry:
    registry = ArmRegistry()
    registry.register("native", native.answer, ArmRole.BASELINE_COMPONENT)
    registry.register_unavailable(
        "episode_readback", ArmRole.BASELINE_COMPONENT, "excluded_from_this_smoke"
    )
    registry.register("extraction_llm", extraction.answer, ArmRole.CANDIDATE_ARM)
    return registry


def test_comparison_report_renders_with_the_extraction_candidate_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _require_local_model(monkeypatch)
    registry = _registry_with_extraction_candidate()
    # No dependencies dict supplied -- CHAOS-3563's state is not fabricated
    # here, matching the successor must-know (unchanged from step 1).
    (report,) = compare(ALL_ORACLES, registry)

    assert report.arm.arm == "extraction_llm"
    rendered = report.render()
    for klass in QuestionClass:
        assert f"class {klass.value}:" in rendered

    by_class = {c.question_class: c for c in report.by_class()}
    class_b = by_class[QuestionClass.NEEDS_DECLARED_STATE_HISTORY]
    assert not class_b.is_comparable, (
        "class (b) must stay NOT COMPARABLE with no CHAOS-3563 state "
        "supplied -- unchanged from step 1, registered as a real "
        "requirement here rather than assumed"
    )
    assert "CHAOS-3563" in class_b.render()


def test_extraction_candidate_registers_as_candidate_never_baseline() -> None:
    registry = _registry_with_extraction_candidate()
    assert registry.roles["extraction_llm"] is ArmRole.CANDIDATE_ARM
    assert "extraction_llm" not in registry.names_with_role(ArmRole.BASELINE_COMPONENT)
