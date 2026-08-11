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
reached. Nothing here mocks the LLM to fake a PASSING smoke result: an
unreachable provider is a reportable finding, and an unrun smoke reads as
NOT RUN, the same discipline every other arm in this harness already holds.
One test below (``test_fabricated_evidence_ref_survives_verbatim_and_fails_
the_oracle``) does feed a fake, deliberately-invalid client response through
the real adapter -- but only to prove the adapter does NOT sanitize or
repair bad model output, which is the opposite of faking a pass, and needs
no live model at all.
"""

from __future__ import annotations

import json

import pytest

from ..corpus.oracles import ALL_ORACLES, ORACLES_BY_ID
from ..harness.arms import extraction, native
from ..harness.arms.source_documents import NOT_AUTHORABLE_REASONS, SOURCE_DOCUMENTS
from ..harness.contracts import ALL_QUESTION_CLASSES, ArmOutcome, QuestionClass
from ..harness.llm import client as llm_client
from ..harness.llm.client import (
    DEFAULT_LOCAL_BASE_URL,
    DEFAULT_LOCAL_MODEL,
    LLMConfig,
    LLMResponse,
    LLMUnavailable,
    complete,
)
from ..harness.oracle import Verdict
from ..harness.runner import ArmRegistry, ArmRole, compare


def _configure_local(monkeypatch: pytest.MonkeyPatch) -> LLMConfig:
    """Force the FULL local-provider config for this test, regardless of
    ambient env.

    Not just LLM_PROVIDER: this repo's shells have been observed with
    LOCAL_LLM_BASE_URL/LOCAL_LLM_MODEL set to something other than the
    documented defaults too (for unrelated local work), and a smoke test
    that only pinned LLM_PROVIDER would silently talk to whatever those
    ambient values happened to be -- both wrong (an untested address) and
    unreportable (the printed smoke observation would not match what the
    test actually exercised). Pinning all three makes the config the smoke
    result is attributed to exactly what this test declares, not whatever
    the environment happened to contain.
    """
    monkeypatch.setenv("LLM_PROVIDER", "local")
    monkeypatch.setenv("LOCAL_LLM_BASE_URL", DEFAULT_LOCAL_BASE_URL)
    monkeypatch.setenv("LOCAL_LLM_MODEL", DEFAULT_LOCAL_MODEL)
    monkeypatch.delenv("LOCAL_LLM_API_KEY", raising=False)
    return LLMConfig.from_env()


def _require_local_model(monkeypatch: pytest.MonkeyPatch) -> LLMConfig:
    cfg = _configure_local(monkeypatch)
    try:
        complete("Reply with exactly: OK", "Say OK and nothing else.", config=cfg)
    except LLMUnavailable as exc:
        pytest.skip(f"local model not reachable -- skipping smoke: {exc}")
    return cfg


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


def test_not_authorable_oracle_is_not_run_with_its_specific_reason() -> None:
    """No LM Studio dependency at all -- this oracle has no authored
    document, so the arm must never even attempt a call.

    Authoring round: `O1_ci_prior_attempts` is now explicitly registered
    in `NOT_AUTHORABLE_REASONS` (structured episode data has no natural
    prose form for this arm), so its NOT_RUN reason is that SPECIFIC
    string, not the generic "not authored yet" one -- see
    `test_generic_not_yet_authored_reason_is_distinct_from_not_authorable`
    for the other branch.
    """
    oracle = ORACLES_BY_ID["O1_ci_prior_attempts"]
    assert oracle.oracle_id not in SOURCE_DOCUMENTS
    assert oracle.oracle_id in NOT_AUTHORABLE_REASONS
    response = extraction.answer(oracle)
    assert response.outcome is ArmOutcome.NOT_RUN
    assert response.degraded_reasons == (
        "measurement_not_run:not_authorable_for_extraction_arm:"
        f"{NOT_AUTHORABLE_REASONS[oracle.oracle_id]}",
    )


def test_generic_not_yet_authored_reason_is_distinct_from_not_authorable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The other branch of `answer()`'s NOT_RUN dispatch: an oracle_id in
    NEITHER dict gets the generic "not authored yet" reason, never the
    NOT-AUTHORABLE one. Every real oracle is now in one dict or the other
    (the authoring round's whole point), so this is pinned by monkeypatching
    a real oracle's id out of both dicts for the duration of the test,
    rather than asserting it against a real oracle that happens to
    qualify -- none does anymore, which is itself the coverage this test
    protects against silently regressing back into existing.
    """
    monkeypatch.setattr(
        extraction,
        "SOURCE_DOCUMENTS",
        {k: v for k, v in SOURCE_DOCUMENTS.items() if k != "O3_supersession"},
    )
    monkeypatch.setattr(
        extraction,
        "NOT_AUTHORABLE_REASONS",
        {k: v for k, v in NOT_AUTHORABLE_REASONS.items() if k != "O3_supersession"},
    )
    oracle = ORACLES_BY_ID["O3_supersession"]
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
    cfg = _require_local_model(monkeypatch)

    observations: dict[str, str] = {}
    model_name = None
    for oracle_id in sorted(SOURCE_DOCUMENTS):
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
            f"provider=local base_url={cfg.base_url} model={model_name or cfg.model} "
            f"extraction quality: {passed}/{total} oracles passed"
        )
        for oracle_id, verdict in sorted(observations.items()):
            print(f"  {oracle_id}: {verdict}")

    # Plumbing-only assertions below -- never a quality bar.
    assert observations, "no source material authored for any oracle to smoke"
    assert all(
        v.startswith("NOT_RUN") or v in {"PASS", "FAIL"} for v in observations.values()
    ), f"unexpected verdict shape in smoke observations: {observations}"


def test_fabricated_evidence_ref_survives_verbatim_and_fails_the_oracle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Requirement 2, pinned POSITIVELY rather than vacuously, and for every
    smoke oracle with no early return (the previous version of this test
    only checked ref *shape* and returned after the first oracle that
    happened to cite a real document id -- neither is a genuine pin of
    pass-through).

    Needs no live model: a FAKE client response containing an INVENTED
    evidence_ref is fed through the REAL adapter directly. The invented ref
    must survive verbatim into the fact the oracle evaluates -- proving the
    adapter neither validates nor substitutes it -- and the resulting
    response must FAIL. The fake fact deliberately supplies only ONE
    required identity with none of its other qualifiers (no closure, no
    flags): for O3_supersession and both O2 oracles this fails on the
    fabricated ref directly (`require_evidence_refs` demands the real one
    -- #1603 finding 5 added O2_blocking_observed's pin, matching its
    O2_blocking_valid twin); for O5_conflicts_injected, which has no
    `require_evidence_refs` of its own, it fails on the missing required
    flags instead -- the ref-survival assertion is unconditional either
    way and does not depend on which qualifier catches the response.

    O2's two oracles are `AS_OF` queries, so `extraction._apply_as_of_filter`
    would otherwise drop this fake fact before it ever reaches the oracle
    (no `"temporal"` block -> no `valid_from` -> filtered out under the
    VALID_TIME axis), which would prove nothing about ref pass-through. The
    fake row's `"temporal"` block below is deliberately wide enough to
    survive EITHER axis at `AS_OF_JUL_15` (`valid_from` well before it,
    `valid_to` open, `recorded_at` well before it too) and is simply unused
    data for the non-`AS_OF` oracles.
    """
    fabricated_ref = "ev_fabricated_never_planted_by_any_corpus_fixture"

    for oracle_id in sorted(SOURCE_DOCUMENTS):
        oracle = ORACLES_BY_ID[oracle_id]
        required = oracle.must_include[0]
        fake_row = {
            "subject_kind": required.subject.kind,
            "subject_id": required.subject.id,
            "predicate": required.predicate,
            "object_kind": required.object.kind,
            "object_id": required.object.id,
            "claim_kind": "observed",
            "evidence_ref": fabricated_ref,
            "flags": {"conflicting": False, "untrusted_content": False},
            "temporal": {
                "valid_from": "2026-07-01T00:00:00",
                "valid_to": None,
                "recorded_at": "2026-07-01T00:00:00",
            },
        }
        fake_content = json.dumps([fake_row])

        def _fake_complete(
            system_prompt: str,
            user_prompt: str,
            *,
            config: LLMConfig | None = None,
            _content: str = fake_content,
        ) -> LLMResponse:
            return LLMResponse(
                content=_content,
                model="fake-smoke-model",
                served_model="fake-smoke-model",
            )

        monkeypatch.setattr(llm_client, "complete", _fake_complete)

        response = extraction.answer(oracle)
        assert response.outcome is ArmOutcome.ANSWERED, (
            f"{oracle_id}: fake response should have been accepted as "
            f"parseable, got {response.outcome}"
        )
        matching = [
            f
            for f in response.facts
            if f.subject_ref == required.subject
            and f.predicate == required.predicate
            and f.object_ref == required.object
        ]
        assert matching, (
            f"{oracle_id}: the fabricated fact did not survive to the "
            "response at all -- cannot demonstrate ref pass-through"
        )
        assert matching[0].evidence_refs == (fabricated_ref,), (
            f"{oracle_id}: evidence_ref was not passed through verbatim -- "
            f"got {matching[0].evidence_refs!r}, expected exactly "
            f"({fabricated_ref!r},). The adapter must never validate or "
            "substitute this value."
        )

        result = oracle.evaluate(response)
        assert result.verdict is Verdict.FAIL, (
            f"{oracle_id}: a response built from one incompletely-qualified "
            "fake fact must not pass the real oracle"
        )


def test_malformed_rows_are_dropped_not_repaired(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Finding 1, pinned directly and offline against the REAL adapter (a
    prior version of this fix had no failing test at all: re-planting the
    old `raw.get("claim_kind") or "observed"` default left the entire
    offline suite green).

    Also pins the bonus fix found while re-verifying finding 1: a JSON
    `null` in a required field must be dropped as malformed, never coerced
    by `str(None)` into the literal string ``"None"``.

    Three rows through one fake completion, no live model needed:
    (a) missing `claim_kind` entirely -- must be dropped;
    (b) a required field is JSON `null` -- must be dropped, never survives
        as the string "None";
    (c) a well-formed row -- must survive, as the control proving (a) and
        (b) are not just an accident of the whole response being rejected.
    """
    oracle = ORACLES_BY_ID["O3_supersession"]
    rows = [
        {  # (a) no claim_kind at all.
            "subject_kind": "decision",
            "subject_id": "ADR-900",
            "predicate": "supersedes",
            "object_kind": "decision",
            "object_id": "ADR-901",
            "evidence_ref": "ev_missing_claim_kind",
        },
        {  # (b) JSON null in a required field.
            "subject_kind": "decision",
            "subject_id": "ADR-902",
            "predicate": "supersedes",
            "object_kind": None,
            "object_id": None,
            "claim_kind": "observed",
            "evidence_ref": "ev_null_field",
        },
        {  # (c) control: well-formed.
            "subject_kind": "decision",
            "subject_id": "ADR-903",
            "predicate": "supersedes",
            "object_kind": "decision",
            "object_id": "ADR-904",
            "claim_kind": "observed",
            "evidence_ref": "ev_well_formed",
        },
    ]
    fake_content = json.dumps(rows)

    def _fake_complete(
        system_prompt: str,
        user_prompt: str,
        *,
        config: LLMConfig | None = None,
        _content: str = fake_content,
    ) -> LLMResponse:
        return LLMResponse(
            content=_content,
            model="fake-smoke-model",
            served_model="fake-smoke-model",
        )

    monkeypatch.setattr(llm_client, "complete", _fake_complete)

    response = extraction.answer(oracle)
    assert response.outcome is ArmOutcome.ANSWERED

    subject_ids = {f.subject_ref.id for f in response.facts}
    assert "ADR-900" not in subject_ids, (
        "(a) a row missing claim_kind must be DROPPED, not granted a "
        "default 'observed' claim"
    )
    assert "ADR-902" not in subject_ids, (
        "(b) a row with a JSON null in a required field must be DROPPED"
    )
    assert not any(
        f.object_ref.kind == "None" or f.object_ref.id == "None" for f in response.facts
    ), (
        "(b) a JSON null field must never survive as the literal string "
        "'None' -- str(None) does this silently unless explicitly guarded"
    )
    assert "ADR-903" in subject_ids, (
        "(c) control: a well-formed row must survive -- if this fails too, "
        "(a) and (b) prove nothing about selective dropping"
    )


def test_self_evidencing_closure_with_no_evidence_ref_stays_uncited(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#1603 finding 2, adapter-side half: a fact with a stated ``valid_to``
    but NO ``evidence_ref`` must NOT get a fabricated
    ``Invalidation(refs=())`` -- that would cite nothing while still
    satisfying a naive ``invalidated_by is not None`` check. Before this
    fix, ``_to_temporal_fact`` built exactly that fabricated object
    whenever ``valid_to`` was present, regardless of whether
    ``evidence_ref`` was too. See the oracle-side half,
    ``test_oracle_fault_modes.py``'s
    ``test_dangling_endpoint_with_empty_refs_invalidation_still_fails``,
    which proves the gate independently catches an empty-refs
    ``Invalidation`` even if some future adapter bug re-introduces one.
    """
    oracle = ORACLES_BY_ID["O2_blocking_valid"]
    atl_101, _atl_105 = oracle.must_include
    row = {
        "subject_kind": atl_101.subject.kind,
        "subject_id": atl_101.subject.id,
        "predicate": atl_101.predicate,
        "object_kind": atl_101.object.kind,
        "object_id": atl_101.object.id,
        "claim_kind": "observed",
        # Deliberately NO evidence_ref -- this is the exact shape that
        # used to produce a fabricated Invalidation(refs=()).
        "flags": {"conflicting": False, "untrusted_content": False},
        "temporal": {
            "valid_from": "2026-07-02T09:00:00",
            "valid_to": "2026-07-18T16:00:00",
            "recorded_at": "2026-07-02T09:00:00",
        },
    }
    _install_fake_rows(monkeypatch, [row])

    response = extraction.answer(oracle)
    assert response.outcome is ArmOutcome.ANSWERED
    matching = [f for f in response.facts if f.subject_ref == atl_101.subject]
    assert matching, "the fact must still survive -- only its closure is uncited"
    assert matching[0].valid_to is not None, "the mutation setup itself is broken"
    assert matching[0].invalidated_by is None, (
        "a valid_to with no evidence_ref must leave invalidated_by None -- "
        "never a fabricated Invalidation(refs=()) that cites nothing"
    )


# --------------------------------------------------------------------------
# Step 3: axis-aware AS_OF filtering (_apply_as_of_filter). Offline,
# fake-completion pins independent of live model quality -- these prove the
# MECHANISM (deterministic filtering over model-emitted dates) is correct;
# whether a real model actually extracts those dates is a separate,
# measured-sweep question, not this suite's to assert.
# --------------------------------------------------------------------------


def _fake_row(expectation, *, valid_from, valid_to, recorded_at, evidence_ref):
    return {
        "subject_kind": expectation.subject.kind,
        "subject_id": expectation.subject.id,
        "predicate": expectation.predicate,
        "object_kind": expectation.object.kind,
        "object_id": expectation.object.id,
        "claim_kind": "observed",
        "evidence_ref": evidence_ref,
        "flags": {"conflicting": False, "untrusted_content": False},
        "temporal": {
            "valid_from": valid_from,
            "valid_to": valid_to,
            "recorded_at": recorded_at,
        },
    }


def _install_fake_rows(monkeypatch: pytest.MonkeyPatch, rows: list[dict]) -> None:
    fake_content = json.dumps(rows)

    def _fake_complete(
        system_prompt: str,
        user_prompt: str,
        *,
        config: LLMConfig | None = None,
        _content: str = fake_content,
    ) -> LLMResponse:
        return LLMResponse(
            content=_content,
            model="fake-smoke-model",
            served_model="fake-smoke-model",
        )

    monkeypatch.setattr(llm_client, "complete", _fake_complete)


def test_as_of_filter_includes_backfilled_fact_on_valid_time_axis(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ATL-105's ``valid_from`` (07-05) is before ``AS_OF_JUL_15`` even
    though its ``recorded_at`` (07-20) is after it -- the VALID_TIME axis
    must include it anyway. This is the oracle's own control case (ground
    truth: "as of 07-15, true on valid_time, not-yet-known on
    observed_time"), pinned directly against the adapter's filter rather
    than trusted to a live model.
    """
    oracle = ORACLES_BY_ID["O2_blocking_valid"]
    atl_101, atl_105 = oracle.must_include
    _install_fake_rows(
        monkeypatch,
        [
            _fake_row(
                atl_101,
                valid_from="2026-07-02T09:00:00",
                valid_to="2026-07-18T16:00:00",
                recorded_at="2026-07-02T09:00:00",
                evidence_ref="ev1_dep_101_110",
            ),
            _fake_row(
                atl_105,
                valid_from="2026-07-05T08:00:00",
                valid_to=None,
                recorded_at="2026-07-20T11:00:00",
                evidence_ref="ev1_dep_105_110",
            ),
        ],
    )

    response = extraction.answer(oracle)
    assert response.outcome is ArmOutcome.ANSWERED
    subject_ids = {f.subject_ref.id for f in response.facts}
    assert atl_101.subject.id in subject_ids
    assert atl_105.subject.id in subject_ids, (
        "the backfilled fact is TRUE on valid_time as of 07-15 (valid_from "
        "07-05) and must be included even though it was not recorded until "
        "07-20 -- dropping it here would be answering the observed-time "
        "question while claiming to answer the valid-time one"
    )

    result = oracle.evaluate(response)
    assert result.verdict is Verdict.PASS, [
        (a.assertion_id, a.detail) for a in result.assertions if not a.ok
    ]


def test_as_of_filter_excludes_backfilled_fact_on_observed_time_axis(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The same two facts, same dates -- but asked on OBSERVED_TIME. ATL-105
    was not recorded until 07-20, so it must NOT appear as of 07-15, even
    though the model correctly extracted that it was already true by then.
    """
    oracle = ORACLES_BY_ID["O2_blocking_observed"]
    (atl_101,) = oracle.must_include
    (atl_105,) = oracle.must_exclude
    _install_fake_rows(
        monkeypatch,
        [
            _fake_row(
                atl_101,
                valid_from="2026-07-02T09:00:00",
                valid_to="2026-07-18T16:00:00",
                recorded_at="2026-07-02T09:00:00",
                evidence_ref="ev1_dep_101_110",
            ),
            _fake_row(
                atl_105,
                valid_from="2026-07-05T08:00:00",
                valid_to=None,
                recorded_at="2026-07-20T11:00:00",
                evidence_ref="ev1_dep_105_110",
            ),
        ],
    )

    response = extraction.answer(oracle)
    assert response.outcome is ArmOutcome.ANSWERED
    subject_ids = {f.subject_ref.id for f in response.facts}
    assert atl_101.subject.id in subject_ids
    assert atl_105.subject.id not in subject_ids, (
        "the backfilled fact was not KNOWN until 07-20 and must not appear "
        "on an observed-time-as-of-07-15 answer, even though it was "
        "already true by then -- that is the valid-time question, not "
        "this one"
    )

    result = oracle.evaluate(response)
    assert result.verdict is Verdict.PASS, [
        (a.assertion_id, a.detail) for a in result.assertions if not a.ok
    ]


def test_as_of_filter_drops_fact_with_no_temporal_data(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A fact with no `"temporal"` block at all must be dropped from an
    `AS_OF` query, not guessed into either axis -- silence about a date is
    not evidence the fact holds at the queried instant.
    """
    oracle = ORACLES_BY_ID["O2_blocking_valid"]
    atl_101, _atl_105 = oracle.must_include
    row = {
        "subject_kind": atl_101.subject.kind,
        "subject_id": atl_101.subject.id,
        "predicate": atl_101.predicate,
        "object_kind": atl_101.object.kind,
        "object_id": atl_101.object.id,
        "claim_kind": "observed",
        "evidence_ref": "ev1_dep_101_110",
        "flags": {"conflicting": False, "untrusted_content": False},
        # no "temporal" key at all.
    }
    _install_fake_rows(monkeypatch, [row])

    response = extraction.answer(oracle)
    assert response.outcome is ArmOutcome.ANSWERED
    assert response.facts == (), (
        "a fact with no model-stated valid_from must be dropped from an "
        "AS_OF query, not defaulted into passing the valid_time filter"
    )


def test_as_of_filter_excludes_null_recorded_at_even_for_a_same_day_fact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#1603 finding 1, pinned directly: a PROMPT-OBEDIENT-shaped response
    (``recorded_at`` null) for a fact whose OWN ``valid_from`` predates
    ``as_of`` must still be EXCLUDED from an observed-time query, per the
    documented fallback contract (``_to_temporal_fact``'s docstring:
    "no `recorded_at` -> `observed_at` falls back to this run's own
    clock, which reads as `not yet observed` for any historical `as_of`").

    This is exactly the gap the original prompt wording created: it told
    the model to OMIT `recorded_at` when it matched `valid_from` (a
    same-day fact, like ATL-101's real one), which -- under this
    documented fallback -- would have wrongly excluded ATL-101 from
    O2_blocking_observed's answer. The prompt now asks for `recorded_at`
    whenever the text states ANY recording date, same-day or not (see
    `_SYSTEM_PROMPT`); this test pins what happens on the OTHER side of
    that fix -- if a response still omits it (whether from a model that
    disobeys the prompt, or genuinely has no recording date to give) --
    so the contract is enforced by the filter, not merely hoped for from
    prompt wording.
    """
    oracle = ORACLES_BY_ID["O2_blocking_observed"]
    (atl_101,) = oracle.must_include
    row = {
        "subject_kind": atl_101.subject.kind,
        "subject_id": atl_101.subject.id,
        "predicate": atl_101.predicate,
        "object_kind": atl_101.object.kind,
        "object_id": atl_101.object.id,
        "claim_kind": "observed",
        "evidence_ref": "ev1_dep_101_110",
        "flags": {"conflicting": False, "untrusted_content": False},
        "temporal": {
            # valid_from genuinely predates as_of (2026-07-15) -- if the
            # filter used valid_from as a stand-in for "observed", this
            # fact would wrongly pass. recorded_at is null: the
            # prompt-obedient-but-incomplete shape this test exists to
            # pin.
            "valid_from": "2026-07-02T09:00:00",
            "valid_to": "2026-07-18T16:00:00",
            "recorded_at": None,
        },
    }
    _install_fake_rows(monkeypatch, [row])

    response = extraction.answer(oracle)
    assert response.outcome is ArmOutcome.ANSWERED
    assert response.facts == (), (
        "a fact with a stated valid_from but no recorded_at must still be "
        "EXCLUDED from an observed-time AS_OF query -- valid_from is not "
        "evidence of when the fact was KNOWN, and the documented fallback "
        "(observed_at = this run's own indexing clock) correctly reads as "
        "'not yet observed' for any as_of before that clock"
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
    for klass in ALL_QUESTION_CLASSES:
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


# --------------------------------------------------------------------------
# Finding 10: an arm's role is fixed by its own module (`answer.declared_
# role`), not chosen by the call site. ArmRegistry.register must reject a
# mismatch in BOTH directions.
# --------------------------------------------------------------------------


def test_registry_rejects_extraction_arm_registered_as_baseline() -> None:
    registry = ArmRegistry()
    with pytest.raises(ValueError, match="declares role 'candidate_arm'"):
        registry.register(
            "extraction_wrong_role", extraction.answer, ArmRole.BASELINE_COMPONENT
        )


def test_registry_rejects_baseline_arm_registered_as_candidate() -> None:
    registry = ArmRegistry()
    with pytest.raises(ValueError, match="declares role 'baseline_component'"):
        registry.register("native_wrong_role", native.answer, ArmRole.CANDIDATE_ARM)


def test_registry_accepts_arms_registered_under_their_declared_role() -> None:
    """Control for the two tests above: the enforcement must not reject the
    correct pairing.
    """
    registry = ArmRegistry()
    registry.register("native", native.answer, ArmRole.BASELINE_COMPONENT)
    registry.register("extraction_llm", extraction.answer, ArmRole.CANDIDATE_ARM)
    assert registry.roles["native"] is ArmRole.BASELINE_COMPONENT
    assert registry.roles["extraction_llm"] is ArmRole.CANDIDATE_ARM
