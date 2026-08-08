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


def test_declared_tiers_are_exactly_the_four_named() -> None:
    """The matrix is a decision input, so its membership is pinned, not
    incidental: nano = deployed parity, mini = ceiling, and two local
    tiers of different size = the cost regime plus a LOCAL SCALING
    comparator (e4b vs 31b separates "small models can't" from "local
    models can't").
    """
    assert tuple(t.key for t in MODEL_TIERS) == (
        "gpt-5-nano",
        "gpt-5-mini",
        "gemma-4-e4b-local",
        "gemma-4-31b-local",
    )
    by_key = {t.key: t for t in MODEL_TIERS}
    assert by_key["gpt-5-nano"].model == "gpt-5-nano"
    assert by_key["gpt-5-mini"].model == "gpt-5-mini"
    assert by_key["gemma-4-e4b-local"].model == "google/gemma-4-e4b"
    assert by_key["gemma-4-31b-local"].model == "google/gemma-4-31b"
    assert by_key["gpt-5-nano"].primary, (
        "nano is the DEPLOYED configuration -- it is the tier the ADR's "
        "parity claims rest on, and the artifact must say so"
    )
    assert not by_key["gpt-5-mini"].primary
    assert not by_key["gemma-4-e4b-local"].primary
    assert not by_key["gemma-4-31b-local"].primary
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
