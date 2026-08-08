"""Offline guards for the run-3 multi-model tier matrix.

Run 3 measures the SAME corpus against four named model tiers rather than
one. That change introduces four ways to produce a dishonest artifact, and
every test here exists to close exactly one of them:

1. **A tier's model silently resolving from the environment.** ``ops/.env``
   carries the deployed Ask Dev model (``LLM_MODEL="gpt-5-nano"`` as of
   2026-08-08). An arm that resolves its model from env would produce an
   artifact whose "gpt-5-mini" section was actually measured on whatever
   the deployment was configured with -- a mislabelled result is worse than
   a missing one, because a reader stops checking.
2. **A timed-out or unreachable call being scored.** The local tier is an
   order of magnitude slower than the cloud tiers; chris flagged that
   earlier gemma observations may have been false positives from exactly
   this. An infra failure must land as NOT_RUN, never as PASS and never as
   FAIL.
3. **A tier that never ran rendering as though it had.** If LM Studio is
   down, the local tier's section must say NOT_RUN and say why -- not be
   quietly omitted (a reader sees two tiers and assumes two were planned)
   and not carry a fabricated score.
4. **Latency going unrecorded.** Without it, no reader can tell whether a
   slow tier was given the time it needed.

Everything here is offline: hand-built results, a fake OpenAI SDK, no
network and no spend.
"""

from __future__ import annotations

import httpx
import openai
import pytest

from ..corpus.oracles import ORACLES_BY_ID
from ..harness.arms import extraction
from ..harness.contracts import ArmOutcome
from ..harness.llm.client import LLMConfig
from ..harness.oracle import AssertionResult, OracleResult, Verdict
from ..harness.runner import ArmRole, TrialReport
from ..run_measured_sweep import (
    MODEL_TIERS,
    CallRecord,
    ModelTier,
    TierOutcome,
    _latency_cell,
    _render_artifact,
    _render_per_oracle_table,
    _render_tier_section,
    resolve_tier_config,
)
from .test_llm_client import (
    _FakeChoicesResponse,
    _FakeResponsesResult,
    _install_fake_openai,
    _timeout_error,
)

# --------------------------------------------------------------------------
# 1. The model is named by the tier, never inherited from the environment.
# --------------------------------------------------------------------------

_HOSTILE_ENV = {
    "LLM_MODEL": "gpt-5-nano",
    "OPENAI_MODEL": "some-other-model-entirely",
    "LLM_PROVIDER": "local",
    "LOCAL_LLM_MODEL": "google/gemma-4-e4b",
}


def _poison_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set every env var any resolution path in this trial reads, to a
    value that is WRONG for the tier under test -- so a test passing
    proves the tier's own model was used, not that env happened to agree.
    """
    for key, value in _HOSTILE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("OPENAI_API_KEY", "test-key-not-real")


def test_declared_tiers_are_exactly_the_five_named() -> None:
    """The matrix is a decision input, so its membership is pinned, not
    incidental: nano = deployed parity, mini = ceiling, and two local
    tiers of different size = the cost regime plus a LOCAL SCALING
    comparator (e4b vs 31b separates "small models can't" from "local
    models can't").
    """
    assert tuple(t.key for t in MODEL_TIERS) == (
        "gpt-5-nano",
        "gpt-5-mini",
        "gpt-5.6-luna",
        "gemma-4-e4b-local",
        "gemma-4-31b-local",
    )
    by_key = {t.key: t for t in MODEL_TIERS}
    assert by_key["gpt-5-nano"].model == "gpt-5-nano"
    assert by_key["gpt-5-mini"].model == "gpt-5-mini"
    assert by_key["gemma-4-e4b-local"].model == "google/gemma-4-e4b"
    assert by_key["gemma-4-31b-local"].model == "google/gemma-4-31b"
    assert by_key["gpt-5.6-luna"].model == "gpt-5.6-luna"
    assert by_key["gpt-5-nano"].primary, (
        "nano is the DEPLOYED configuration -- it is the tier the ADR's "
        "parity claims rest on, and the artifact must say so"
    )
    assert not by_key["gpt-5-mini"].primary
    assert not by_key["gemma-4-e4b-local"].primary
    assert not by_key["gemma-4-31b-local"].primary
    assert not by_key["gpt-5.6-luna"].primary
    for local_key in ("gemma-4-e4b-local", "gemma-4-31b-local"):
        assert by_key[local_key].optional, (
            "the local tiers are informative for the cost regime, not "
            "parity -- the run must not fail when LM Studio is down"
        )


def test_exactly_one_tier_is_the_primary_scored_tier() -> None:
    """Two primaries (or none) would leave a reader with no answer to
    "which section is the deployed configuration" -- the one question the
    ADR's parity statements depend on.
    """
    assert sum(1 for t in MODEL_TIERS if t.primary) == 1


def test_local_tiers_get_a_longer_window_than_the_cloud_tiers() -> None:
    by_key = {t.key: t for t in MODEL_TIERS}
    cloud_max = max(by_key[k].timeout for k in ("gpt-5-nano", "gpt-5-mini"))
    for local_key in ("gemma-4-e4b-local", "gemma-4-31b-local"):
        assert by_key[local_key].timeout > cloud_max


@pytest.mark.parametrize("tier_key", ["gpt-5-nano", "gpt-5-mini"])
def test_resolved_tier_config_ignores_a_hostile_environment(
    monkeypatch: pytest.MonkeyPatch, tier_key: str
) -> None:
    _poison_env(monkeypatch)
    tier = next(t for t in MODEL_TIERS if t.key == tier_key)
    cfg = resolve_tier_config(tier)
    assert cfg.model == tier.model, (
        "OPENAI_MODEL/LLM_MODEL are both set to something else -- a config "
        "that agreed with either of them resolved from env, which is the "
        "exact mislabelling this matrix cannot survive"
    )
    assert cfg.provider == "cloud"
    assert cfg.timeout == tier.timeout


def test_extraction_arm_calls_the_model_the_tier_named(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The end of the chain: it is not enough for the CONFIG to be right,
    the arm must actually pass it down to the provider call. Before this
    round the arm called ``llm_client.complete()`` with no config at all,
    so every arm invocation re-resolved the model from the environment.
    """
    _poison_env(monkeypatch)
    # BOTH SDK branches answer successfully, deliberately: if the arm fell
    # back to the environment it would resolve LLM_PROVIDER=local ->
    # LOCAL_LLM_MODEL=google/gemma-4-e4b and take the chat-completions
    # branch. Leaving that branch unfed would make the test fail with an
    # incidental AttributeError instead of on the thing being asserted, so
    # the model NAME below is the sole discriminator.
    fake_message = type("Msg", (), {"content": "[]"})()
    fake_choice = type("Choice", (), {"message": fake_message})()
    _install_fake_openai(
        monkeypatch,
        responses_response=_FakeResponsesResult(output_text="[]"),
        chat_response=_FakeChoicesResponse(choices=[fake_choice]),
    )
    cfg = LLMConfig.for_cloud(model="gpt-5-mini", api_key="x")
    arm = extraction.make_answer(cfg)
    response = arm(ORACLES_BY_ID["O3_supersession"])
    assert response.outcome is ArmOutcome.ANSWERED
    assert response.versions["extraction"] == "gpt-5-mini", (
        "the arm reported a model other than the one its config named -- "
        "it resolved from the environment, which is the exact mislabelling "
        "the tier matrix cannot survive"
    )


def test_make_answer_keeps_the_candidate_role_declaration() -> None:
    """A per-tier arm is still a candidate arm -- the registry's
    role-enforcement must not be lost by wrapping.
    """
    arm = extraction.make_answer(LLMConfig.for_cloud(model="gpt-5-nano", api_key="x"))
    assert getattr(arm, "declared_role", None) is ArmRole.CANDIDATE_ARM


# --------------------------------------------------------------------------
# 2. A timed-out call is NOT_RUN -- never PASS, never FAIL.
# --------------------------------------------------------------------------


def test_timed_out_call_is_not_run_and_never_a_scored_verdict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """chris's run-3 requirement, pinned end to end: a slow local model
    that exceeds its window must not be recorded as a model-quality
    result. The oracle's verdict must be NOT_MEASURED -- neither PASS
    (which would credit an answer that never arrived) nor FAIL (which
    would blame the model for the clock).
    """
    _install_fake_openai(monkeypatch, responses_error=_timeout_error())
    cfg = LLMConfig.for_cloud(model="gpt-5-mini", api_key="x", timeout=7.0)
    arm = extraction.make_answer(cfg)
    oracle = ORACLES_BY_ID["O3_supersession"]

    response = arm(oracle)
    assert response.outcome is ArmOutcome.NOT_RUN
    assert "timed out" in response.degraded_reasons[0]

    result = oracle.evaluate(response)
    assert result.verdict is Verdict.NOT_MEASURED
    assert result.verdict is not Verdict.PASS
    assert result.verdict is not Verdict.FAIL


def test_timeout_reason_reaches_the_sweeps_infra_retry_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The bounded-retry policy only re-attempts reasons carrying the infra
    marker. A timeout is infra; if its wording drifted out of that marker
    the local tier would get one shot at a window it may legitimately need
    a retry on, and would report a miss it never made.
    """
    from ..run_measured_sweep import _INFRA_FAILURE_MARKER

    _install_fake_openai(monkeypatch, responses_error=_timeout_error())
    arm = extraction.make_answer(
        LLMConfig.for_cloud(model="gpt-5-mini", api_key="x", timeout=7.0)
    )
    response = arm(ORACLES_BY_ID["O3_supersession"])
    assert _INFRA_FAILURE_MARKER in response.degraded_reasons[0]


# --------------------------------------------------------------------------
# 3 + 4. Artifact shape: a section per tier, NOT_RUN tiers stated with a
# reason, and a latency column.
# --------------------------------------------------------------------------


def _pass(oracle_id: str, arm: str) -> OracleResult:
    return OracleResult(
        oracle_id=oracle_id,
        arm=arm,
        question_class=ORACLES_BY_ID[oracle_id].question_class,
        assertions=(AssertionResult("arm_outcome", Verdict.PASS, "ok"),),
    )


def test_per_oracle_table_carries_a_latency_column() -> None:
    oracles = (ORACLES_BY_ID["O3_supersession"],)
    reports = {
        "extraction_llm": TrialReport(
            arm="extraction_llm", results=(_pass("O3_supersession", "extraction_llm"),)
        )
    }
    records = {
        ("O3_supersession", "extraction_llm"): CallRecord(
            called_at="2026-08-08T00:00:00+00:00", latency_seconds=12.5
        )
    }
    table = _render_per_oracle_table(oracles, reports, records)
    assert "Latency" in table
    assert "12.50s" in table


def test_per_oracle_table_renders_an_uncalled_arms_latency_as_na() -> None:
    """An arm that returned NOT_RUN before ever reaching the provider has
    no latency. It must render honestly as n/a, never as 0.00s -- a zero
    reads as "instant", which is a measurement claim nobody made.
    """
    oracles = (ORACLES_BY_ID["O3_supersession"],)
    reports = {
        "extraction_llm": TrialReport(
            arm="extraction_llm",
            results=(
                OracleResult(
                    oracle_id="O3_supersession",
                    arm="extraction_llm",
                    question_class=ORACLES_BY_ID["O3_supersession"].question_class,
                    assertions=(
                        AssertionResult(
                            "measurement_happened", Verdict.NOT_MEASURED, "no docs"
                        ),
                    ),
                ),
            ),
        )
    }
    table = _render_per_oracle_table(oracles, reports, {})
    assert "0.00s" not in table
    assert "n/a" in table


def _measured_outcome(tier: ModelTier) -> TierOutcome:
    from ..harness.runner import ComparisonReport

    results = (_pass("O3_supersession", "x"),)
    report = ComparisonReport(
        baseline=TrialReport(arm="baseline", results=results),
        arm=TrialReport(arm="extraction_llm", results=results),
        dependencies={},
    )
    return TierOutcome(
        tier=tier,
        status="measured",
        not_run_reason=None,
        config=(
            LLMConfig.for_local(model=tier.model)
            if tier.provider == "local"
            else LLMConfig.for_cloud(model=tier.model, api_key="x")
        ),
        report=report,
        reports={"extraction_llm": TrialReport(arm="extraction_llm", results=results)},
        call_records={},
        started_at="2026-08-08T00:00:00+00:00",
        finished_at="2026-08-08T00:05:00+00:00",
        oracles=(ORACLES_BY_ID["O3_supersession"],),
    )


def _not_run_outcome(tier: ModelTier, reason: str) -> TierOutcome:
    return TierOutcome(
        tier=tier,
        status="not_run",
        not_run_reason=reason,
        config=None,
        report=None,
        reports=None,
        call_records={},
        started_at="2026-08-08T00:00:00+00:00",
        finished_at="2026-08-08T00:00:01+00:00",
        oracles=(),
    )


def _mixed_outcomes() -> tuple[TierOutcome, ...]:
    """One outcome per DECLARED tier, with the last one NOT_RUN.

    Built from MODEL_TIERS itself rather than hand-listing tiers, so
    adding a tier to the matrix without giving it a section makes these
    tests fail instead of silently covering three of four.
    """
    return tuple(
        _not_run_outcome(tier, "LM Studio down")
        if index == len(MODEL_TIERS) - 1
        else _measured_outcome(tier)
        for index, tier in enumerate(MODEL_TIERS)
    )


def test_a_not_run_tier_section_states_the_reason_and_carries_no_score() -> None:
    """The LM-Studio-down case. The section must exist (so a reader sees
    three tiers were planned), must say NOT_RUN, must say WHY, and must
    contain no per-class numbers at all -- a tier that did not run has no
    results to report, and rendering an empty comparison as 0/0 would read
    as a measured zero.
    """
    tier = next(t for t in MODEL_TIERS if t.key == "gemma-4-e4b-local")
    reason = "LM Studio not reachable at http://localhost:1234/v1"
    section = _render_tier_section(_not_run_outcome(tier, reason))
    assert "NOT_RUN" in section
    assert reason in section
    assert "delta" not in section, (
        "a tier that never ran must not render comparison rows -- there is "
        "nothing to compare"
    )


def test_artifact_renders_one_section_per_declared_tier() -> None:
    """Including tiers that did not run: silently dropping one is how a
    3-tier matrix reads as a 2-tier matrix nobody questions.
    """
    outcomes = _mixed_outcomes()
    artifact = _render_artifact(
        outcomes=outcomes,
        run_started_at="2026-08-08T00:00:00+00:00",
        run_finished_at="2026-08-08T00:30:00+00:00",
    )
    for tier in MODEL_TIERS:
        assert f"## Tier: {tier.label}" in artifact, (
            f"tier {tier.key} is declared in MODEL_TIERS but has no section "
            "in the artifact"
        )
    assert "LM Studio down" in artifact
    assert artifact.count("## Tier: ") == len(MODEL_TIERS)


def test_artifact_marks_which_tier_is_deployed_parity() -> None:
    """The ADR's parity claims rest on the nano tier specifically. A reader
    scanning three near-identical tables must be able to tell, from the
    artifact alone, which one is the deployed configuration.
    """
    outcomes = _mixed_outcomes()
    artifact = _render_artifact(
        outcomes=outcomes,
        run_started_at="2026-08-08T00:00:00+00:00",
        run_finished_at="2026-08-08T00:30:00+00:00",
    )
    assert "deployed parity" in artifact
    assert "PRIMARY" in artifact


def test_artifact_summary_matrix_lists_every_tier_status() -> None:
    outcomes = _mixed_outcomes()
    artifact = _render_artifact(
        outcomes=outcomes,
        run_started_at="2026-08-08T00:00:00+00:00",
        run_finished_at="2026-08-08T00:30:00+00:00",
    )
    summary = artifact.split("## Tier: ")[0]
    for tier in MODEL_TIERS:
        assert tier.key in summary
    assert "measured" in summary
    assert "not_run" in summary


def test_local_probe_reports_a_reason_when_the_provider_is_down(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The gemma tier must degrade to a recorded NOT_RUN, not an exception
    that takes the whole run down with it -- chris's explicit instruction.
    """
    from ..run_measured_sweep import probe_provider

    class _DeadModels:
        def list(self):
            raise openai.APIConnectionError(
                request=httpx.Request("GET", "http://localhost:1234/v1/models")
            )

    class _DeadClient:
        def __init__(self, **kwargs):
            self.models = _DeadModels()

    monkeypatch.setattr(openai, "OpenAI", _DeadClient)
    reason = probe_provider(LLMConfig.for_local(model="google/gemma-4-e4b"))
    assert reason is not None
    assert "localhost:1234" in reason


# --------------------------------------------------------------------------
# Run 3b: a SUBSET run (one tier, to add the frontier discriminator without
# re-spending on tiers already measured). New dishonesty risk this creates:
# an artifact holding 1 of 5 declared tiers looks exactly like a complete
# matrix to anyone who does not already know how many tiers exist.
# --------------------------------------------------------------------------


def test_subset_artifact_names_every_tier_it_did_not_measure() -> None:
    """The guard for the run-3b shape. If only some declared tiers ran, the
    artifact must say so and name the missing ones -- otherwise a partial
    matrix reads as the whole matrix, which is the "silent cap" failure
    this trial's own no-silent-caps rule exists to prevent.
    """
    only = MODEL_TIERS[2]  # the frontier tier, run alone
    artifact = _render_artifact(
        outcomes=(_measured_outcome(only),),
        run_started_at="2026-08-08T14:00:00+00:00",
        run_finished_at="2026-08-08T14:10:00+00:00",
    )
    assert "PARTIAL" in artifact, (
        "a subset run must announce itself as partial in the artifact"
    )
    for tier in MODEL_TIERS:
        if tier.key == only.key:
            continue
        assert tier.key in artifact, (
            f"tier {tier.key} was declared but not measured in this run, and "
            "the artifact does not name it -- a reader cannot tell this is "
            "1 of 5 rather than 1 of 1"
        )


def test_a_complete_run_carries_no_partial_banner() -> None:
    """Control for the test above. If the banner rendered unconditionally,
    that test would pass while proving nothing.
    """
    artifact = _render_artifact(
        outcomes=_mixed_outcomes(),
        run_started_at="2026-08-08T00:00:00+00:00",
        run_finished_at="2026-08-08T00:30:00+00:00",
    )
    assert "PARTIAL" not in artifact


def test_tier_selection_rejects_an_unknown_key() -> None:
    """A typo in --tiers must fail loudly. Silently selecting nothing would
    produce an empty artifact that still looks like a successful run.
    """
    from ..run_measured_sweep import select_tiers

    with pytest.raises(SystemExit):
        select_tiers(["gpt-5.6-lunar"])


def test_tier_selection_returns_the_requested_tiers_in_declared_order() -> None:
    from ..run_measured_sweep import select_tiers

    picked = select_tiers(["gemma-4-31b-local", "gpt-5-nano"])
    assert [t.key for t in picked] == ["gpt-5-nano", "gemma-4-31b-local"]


def test_tier_selection_defaults_to_every_declared_tier() -> None:
    from ..run_measured_sweep import select_tiers

    assert select_tiers(None) == list(MODEL_TIERS)


# --------------------------------------------------------------------------
# Codex round (2026-08-08): four scoring-integrity holes in the run-3
# harness. Each test below pins the fix for one CONFIRMED finding.
# --------------------------------------------------------------------------


def test_a_parse_failure_is_never_retried_even_if_it_says_could_not_reach(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """[M1] The retry predicate used to substring-match "could not reach"
    against the NOT_RUN reason -- and a parse-failure reason embeds up to
    500 characters of MODEL-CONTROLLED output. A model emitting that phrase
    could buy itself a re-roll of a genuine quality failure, which is
    retry-shopping with extra steps. The predicate must be the typed
    `infra_failure` field and nothing else.
    """
    from ..run_measured_sweep import _with_bounded_infra_retry

    hostile = "I could not reach a conclusion, so here is prose not JSON."
    _install_fake_openai(
        monkeypatch, responses_response=_FakeResponsesResult(output_text=hostile)
    )
    arm = extraction.make_answer(LLMConfig.for_cloud(model="gpt-5-mini", api_key="x"))

    calls = {"n": 0}

    def counting(oracle):
        calls["n"] += 1
        return arm(oracle)

    setattr(counting, "declared_role", ArmRole.CANDIDATE_ARM)  # noqa: B010
    wrapped = _with_bounded_infra_retry(counting)
    response = wrapped(ORACLES_BY_ID["O3_supersession"])

    assert response.outcome is ArmOutcome.NOT_RUN
    assert "could not reach" in response.degraded_reasons[0], (
        "precondition: the reason must actually contain the old marker, "
        "or this test proves nothing"
    )
    assert not response.infra_failure
    assert calls["n"] == 1, (
        "a model-quality parse failure was retried because its text "
        "contained the infra marker phrase"
    )


def test_a_genuine_timeout_is_still_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    """[M1] Control: tightening the predicate must not disable the real
    bounded re-attempt, which run 3 depended on.
    """
    from ..run_measured_sweep import _with_bounded_infra_retry

    _install_fake_openai(monkeypatch, responses_error=_timeout_error())
    arm = extraction.make_answer(LLMConfig.for_cloud(model="gpt-5-mini", api_key="x"))
    calls = {"n": 0}

    def counting(oracle):
        calls["n"] += 1
        return arm(oracle)

    setattr(counting, "declared_role", ArmRole.CANDIDATE_ARM)  # noqa: B010
    response = _with_bounded_infra_retry(counting)(ORACLES_BY_ID["O3_supersession"])
    assert response.infra_failure
    assert calls["n"] == 2, "a real timeout must still get its one re-attempt"


def test_a_served_model_mismatch_is_not_run_not_a_score(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """[H3] The tier table is a claim about WHICH MODEL produced each row.
    Nothing verified it: the response was discarded and the row labelled
    with the requested id, so a server answering with a different model
    (routine for a local server with something else loaded) would have its
    answers silently attributed to the requested one.
    """

    class _WrongModel(_FakeResponsesResult):
        def __init__(self) -> None:
            super().__init__(output_text="[]")
            self.model = "some-entirely-different-model"

    _install_fake_openai(monkeypatch, responses_response=_WrongModel())
    arm = extraction.make_answer(LLMConfig.for_cloud(model="gpt-5-mini", api_key="x"))
    oracle = ORACLES_BY_ID["O3_supersession"]
    response = arm(oracle)

    assert response.outcome is ArmOutcome.NOT_RUN
    assert "served_model_identity_mismatch" in response.degraded_reasons[0]
    assert not response.infra_failure, "a wrong model is not retryable"
    assert oracle.evaluate(response).verdict is Verdict.NOT_MEASURED


def test_a_matching_served_model_is_recorded_from_the_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """[H3] Control, and the positive half: the recorded model must come
    from the provider's own metadata, including a dated snapshot id that
    legitimately extends the requested alias.
    """

    class _SnapshotModel(_FakeResponsesResult):
        def __init__(self) -> None:
            super().__init__(output_text="[]")
            self.model = "gpt-5-mini-2025-08-07"

    _install_fake_openai(monkeypatch, responses_response=_SnapshotModel())
    arm = extraction.make_answer(LLMConfig.for_cloud(model="gpt-5-mini", api_key="x"))
    response = arm(ORACLES_BY_ID["O3_supersession"])
    assert response.outcome is ArmOutcome.ANSWERED
    assert response.versions["extraction"] == "gpt-5-mini-2025-08-07"
    assert response.versions["extraction_requested"] == "gpt-5-mini"


def test_an_arm_that_never_called_a_provider_has_no_latency() -> None:
    """[L1] The shipped run-3 artifact rendered `0.00s` for oracles the arm
    returned on before any provider call, because the record was written in
    a `finally` regardless. `0.00s` reads as "answered instantly" -- a
    measurement claim nobody made. The previous guard passed a record-less
    dict, so it never exercised the real path and could not have caught it.
    """
    from ..run_measured_sweep import _with_call_record

    sink: dict = {}
    arm = extraction.make_answer(LLMConfig.for_cloud(model="gpt-5-mini", api_key="x"))
    recorded = _with_call_record("extraction_llm", arm, sink)
    # O1_ci_prior_attempts is NOT AUTHORABLE -> returns before any call.
    recorded(ORACLES_BY_ID["O1_ci_prior_attempts"])

    record = sink[("O1_ci_prior_attempts", "extraction_llm")]
    assert record.provider_attempts == 0
    assert record.latency_seconds is None
    assert _latency_cell(record) == "n/a"


def test_a_timeout_and_its_recovery_are_persisted_for_the_artifact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """[H2] Run 3's one timeout survived only as a stderr line. The
    committed artifact showed a plain FAIL at 1009.43s with no indication a
    window was exceeded or a re-attempt made -- so the whole "the timeout
    machinery worked" claim was unauditable from the artifact.
    """
    from ..run_measured_sweep import _with_bounded_infra_retry, _with_call_record

    state = {"first": True}

    def flaky(oracle):
        if state["first"]:
            state["first"] = False
            _install_fake_openai(monkeypatch, responses_error=_timeout_error())
        else:
            _install_fake_openai(
                monkeypatch, responses_response=_FakeResponsesResult(output_text="[]")
            )
        return extraction.make_answer(
            LLMConfig.for_cloud(model="gpt-5-mini", api_key="x")
        )(oracle)

    setattr(flaky, "declared_role", ArmRole.CANDIDATE_ARM)  # noqa: B010
    sink: dict = {}
    recorded = _with_call_record(
        "extraction_llm", _with_bounded_infra_retry(flaky), sink
    )
    recorded(ORACLES_BY_ID["O3_supersession"])

    record = sink[("O3_supersession", "extraction_llm")]
    assert record.timed_out, "the timeout must be persisted, not only logged"
    assert record.recovered_after_retry
    assert record.provider_attempts == 2


def test_absent_served_model_metadata_is_not_run_not_an_assumed_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """[codex confirmation H] Identity verification previously failed OPEN:
    a provider returning no model metadata was treated as agreement, and
    the row was labelled with the REQUESTED model -- the exact assumption
    the check exists to stop. Unverifiable is not verified.
    """

    class _NoMetadata:
        def __init__(self) -> None:
            self.output_text = "[]"
            self.output: list = []
            # deliberately NO .model attribute

    _install_fake_openai(monkeypatch, responses_response=_NoMetadata())
    arm = extraction.make_answer(LLMConfig.for_cloud(model="gpt-5-mini", api_key="x"))
    response = arm(ORACLES_BY_ID["O3_supersession"])
    assert response.outcome is ArmOutcome.NOT_RUN
    assert "NO model metadata" in response.degraded_reasons[0]
    assert not response.infra_failure


def test_probe_refuses_a_provider_that_enumerates_no_models(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """[codex confirmation H] The probe accepted an empty listing, so a
    server that answers but enumerates nothing counted as "the tier is
    present". Presence must be positively confirmed.
    """
    from ..run_measured_sweep import probe_provider

    class _EmptyModels:
        def list(self):
            return []

    class _EmptyClient:
        def __init__(self, **kwargs):
            self.models = _EmptyModels()

    monkeypatch.setattr(openai, "OpenAI", _EmptyClient)
    reason = probe_provider(LLMConfig.for_local(model="google/gemma-4-e4b"))
    assert reason is not None
    assert "enumerated NO models" in reason


def test_records_capture_failed_assertions_for_measured_failures() -> None:
    """[codex confirmation M4] A bare `fail` in the artifact supports no
    diagnosis, so a claim ABOUT a failure had to come from an ad-hoc probe
    the committed record could not back. Failed assertion ids and details
    are now persisted per row.
    """
    from ..run_measured_sweep import _failed_assertions

    result = OracleResult(
        oracle_id="O5_conflicts",
        arm="extraction_llm",
        question_class=ORACLES_BY_ID["O5_conflicts"].question_class,
        assertions=(
            AssertionResult("arm_outcome", Verdict.PASS, "ok"),
            AssertionResult(
                "must_include:conflict side A",
                Verdict.FAIL,
                "claim_kind=observed, expected inferred",
            ),
        ),
    )
    failed = _failed_assertions(result)
    assert [f["assertion_id"] for f in failed] == ["must_include:conflict side A"]
    assert "expected inferred" in failed[0]["detail"]


def test_records_for_a_not_run_tier_carry_no_fabricated_rows() -> None:
    """A tier that never ran must serialize with an empty row list, not
    zero-valued rows that a consumer would average over.
    """
    from ..run_measured_sweep import _records_for

    tier = next(t for t in MODEL_TIERS if t.key == "gemma-4-31b-local")
    payload = _records_for(_not_run_outcome(tier, "LM Studio down"))
    assert payload["status"] == "not_run"
    assert payload["rows"] == []
    assert payload["not_run_reason"] == "LM Studio down"


def test_committed_markdown_is_reproducible_from_committed_records() -> None:
    """[codex final H3] "records.json is the source of truth" was a claim,
    not a fact: the markdown was rendered from in-memory objects while the
    records were written alongside it, so the two could disagree and only
    one was committed evidence.

    This is the guard that makes it true. It re-renders the committed
    markdown from the committed records, with NO model calls, and requires
    byte equality. It fails if anyone edits the artifact by hand, or if a
    renderer change lands without regenerating -- which is exactly the
    drift that let a stale number survive into a decision document.
    """
    import json
    from pathlib import Path

    from ..run_measured_sweep import render_markdown_from_records

    docs = Path(__file__).resolve().parents[1] / "docs"
    records = json.loads((docs / "measured-trial-results.records.json").read_text())
    committed = (docs / "measured-trial-results.md").read_text()

    assert render_markdown_from_records(records) == committed, (
        "the committed markdown is NOT reproducible from the committed "
        "records -- regenerate it rather than hand-editing, or the artifact "
        "and its evidence have diverged"
    )


def test_records_carry_the_provenance_a_cross_run_claim_needs() -> None:
    """[codex final H3] A records file that says what the answers were but
    not what the QUESTIONS were cannot support a cross-run comparison --
    and the ADR makes one (§7's same-input drift claim rests on the corpus
    being identical between two runs).
    """
    import json
    from pathlib import Path

    docs = Path(__file__).resolve().parents[1] / "docs"
    records = json.loads((docs / "measured-trial-results.records.json").read_text())
    provenance = records.get("corpus_provenance", {})
    for required in (
        "corpus/oracles.py",
        "corpus/ground_truth.py",
        "harness/arms/source_documents.py",
        "harness/arms/extraction.py",
    ):
        assert required in provenance, f"no content hash recorded for {required}"
        assert len(provenance[required]) == 64
    for tier in records["tiers"]:
        assert tier["prompt_version"], f"{tier['tier_key']} records no prompt version"
        if tier["status"] == "measured":
            assert tier["control_status"], (
                f"{tier['tier_key']} records no control status"
            )
