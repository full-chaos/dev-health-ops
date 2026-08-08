#!/usr/bin/env python3
"""CHAOS-3499 measured sweep -- composed baseline vs the extraction
candidate arm over the pinned corpus, all 20 oracles, across a MATRIX of
named model tiers.

Deliberately a standalone script, not a pytest test: this run incurs real
OpenAI spend (chris-approved) and is a one-shot measurement, not something
that should re-run on every `pytest` invocation or CI trigger. See
`run_oracles.sh` for the actual test suite (offline oracle self-tests, plus
the local-model smoke -- both unaffected by this script).

Usage::

    OPENAI_API_KEY=... uv run --extra dev python -m trials.chaos_3499.run_measured_sweep

Discipline this script exists to hold, restated because it is the whole
point of the exercise:

* **No retry-shopping.** A failed oracle is a result. The only retry this
  script performs is ONE bounded re-attempt for an INFRA-level NOT_RUN from
  the extraction arm (the provider was genuinely unreachable, or the call
  exceeded its configured window) -- never for a parse failure or a real
  model-quality miss. Every retry is logged, so the report never silently
  launders a flaky call into a clean pass.
* **NOT_RUN stays loud.** Every oracle without authored source material (or
  that hits an unrecoverable provider failure) reports NOT_RUN, which
  `ComparisonReport`/`Oracle.evaluate` already treat as a failure, not a
  skip. NOT-AUTHORABLE oracles (see `harness/arms/source_documents.py`'s
  `NOT_AUTHORABLE_REASONS`) get their own distinguishable NOT_RUN reason
  string, not the generic "no source material yet" one -- a reader must be
  able to tell "we haven't gotten to this" from "there is nothing to
  author" without cross-referencing a second document.
* **A timed-out call is NOT_RUN, never a score.** Run-3 amendment (chris,
  2026-08-08): the local tier answers far more slowly than the cloud
  tiers, and earlier gemma observations may have been false positives from
  a too-short window. Each tier carries its OWN timeout
  (`ModelTier.timeout` -> `LLMConfig.timeout`), and a request that exceeds
  it raises an infra-marked `LLMUnavailable` that lands as NOT_RUN. A slow
  model has not answered wrongly; it has not answered at all, and the
  artifact must not say otherwise.
* **Every tier's model is named EXPLICITLY, never inherited from the
  environment.** `ops/.env` carries the deployed Ask Dev model
  (`LLM_MODEL="gpt-5-nano"` as of 2026-08-08). An env-first resolution
  would let a section labelled `gpt-5-mini` have been measured on whatever
  the deployment happened to be configured with -- a mislabelled result is
  worse than a missing one, because a reader stops checking. Arms are built
  with `extraction.make_answer(config)`; the environment supplies the API
  KEY and nothing else, because a credential is not a measurement
  parameter.
* **Per-oracle results are logged, not just the aggregate**: every
  arm x oracle pair's verdict, the wall-clock instant that arm was called,
  and how long the call took, are recorded into the committed artifact --
  per tier.
* **Class (b) comparability comes from a real `DependencyState`,** supplied
  below with the CHAOS-3563 citation exactly as given, never fabricated or
  assumed.
* **`O7_null_valid_from` stays outside class (a).** It already is,
  structurally (see `corpus/oracles.py`); this script does not touch that,
  and this comment exists so a reader auditing the sweep does not have to
  re-derive it.
* **No headline number.** `ComparisonReport` structurally cannot render
  one (see `harness/runner.py`); this script only ever prints/writes its
  own `render()` output, per class, per tier.
* **This script never calls a candidate arm more than once per oracle per
  tier** (beyond the one bounded infra retry) -- computing the per-oracle
  log and the `ComparisonReport` from the SAME `sweep()` call, never a
  second sweep: the extraction arm makes real, billable, non-deterministic
  model calls, so calling it twice would double the spend AND could
  disagree with itself.
* **An optional tier that cannot run is RECORDED, not fatal and not
  omitted.** If LM Studio is down, the local tier's section still appears,
  says NOT_RUN, and says why. Dropping it would turn a 3-tier matrix into
  a 2-tier matrix nobody questions.
"""

from __future__ import annotations

import os
import sys
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from trials.chaos_3499.corpus.oracles import ALL_ORACLES
from trials.chaos_3499.harness.arms import episode_readback, extraction, native
from trials.chaos_3499.harness.arms.extraction import _PROJECTION_VERSION
from trials.chaos_3499.harness.arms.source_documents import (
    NOT_AUTHORABLE_REASONS,
    SOURCE_DOCUMENTS,
)
from trials.chaos_3499.harness.contracts import ArmOutcome, QuestionClass
from trials.chaos_3499.harness.llm.client import (
    DEFAULT_CLOUD_TIMEOUT_SECONDS,
    DEFAULT_LOCAL_TIMEOUT_SECONDS,
    LLMConfig,
    LLMUnavailable,
)
from trials.chaos_3499.harness.oracle import Verdict
from trials.chaos_3499.harness.runner import (
    CLASS_DEPENDENCIES,
    UNRECORDED_DEPENDENCY,
    ArmRegistry,
    ArmRole,
    ComparisonReport,
    DependencyState,
    compose_baseline,
    sweep,
)

#: The citable dependency state exactly as supplied by the orchestrator for
#: this sweep -- verbatim, not paraphrased. See DependencyState's own
#: docstring: class (b) results are uninterpretable without this, and a
#: reader who cannot tell which branch state produced a score will read a
#: pre-increment baseline as evidence about a post-increment world.
CHAOS_3563_STATE = (
    "CHAOS-3563 declared-state history MERGED as 33a7f85d0 (ops "
    "feature/chaos-3498-context-fabric, 2026-08-08): "
    "project_declared_state_history + durable floor table live via "
    "migrations 074/075, argMax read path with content-hash tie-break, "
    "three-outcome floor contract."
)

#: The LLMUnavailable message shape this trial's client (harness/llm/
#: client.py) uses ONLY for genuine connection failures AND request
#: timeouts (all four branches -- Responses API and Chat Completions,
#: connection and timeout -- share this exact prefix) -- never for config
#: errors (missing key, disallowed host), empty-output, or parse failures,
#: which are permanent regardless of retry. This is the sole signal the
#: bounded-retry policy below acts on.
_INFRA_FAILURE_MARKER = "could not reach"

#: One bounded re-attempt, never more -- see this module's docstring.
_MAX_INFRA_RETRIES = 1


# ==========================================================================
# The model-tier matrix
# ==========================================================================


@dataclass(frozen=True)
class ModelTier:
    """One named model the corpus is measured against.

    ``model`` is a literal, and ``resolve_tier_config`` builds this tier's
    ``LLMConfig`` from it directly -- never from ``OPENAI_MODEL`` /
    ``LLM_MODEL`` / ``LOCAL_LLM_MODEL``. See the module docstring.
    """

    key: str
    model: str
    provider: str  # "cloud" | "local"
    label: str
    role: str
    #: The request window for this tier. The local tier gets a materially
    #: longer one -- see DEFAULT_LOCAL_TIMEOUT_SECONDS.
    timeout: float
    #: The deployed configuration. Parity claims in the ADR rest on this
    #: tier specifically, so the artifact marks it.
    primary: bool = False
    #: An informative tier the run must not fail over. A missing optional
    #: tier is recorded as NOT_RUN with a reason, never a crash.
    optional: bool = False


#: The run-3 matrix, per chris 2026-08-08 05:12. Order is meaningful: the
#: deployed-parity tier is first because it is the one the ADR's parity
#: statements are about.
MODEL_TIERS: tuple[ModelTier, ...] = (
    ModelTier(
        key="gpt-5-nano",
        model="gpt-5-nano",
        provider="cloud",
        label="gpt-5-nano (deployed parity)",
        role=(
            "DEPLOYED PARITY -- Ask Dev's configured model (ops/.env "
            "LLM_MODEL) as of 2026-08-08. This is the tier any claim about "
            "what the product would actually do must be read from."
        ),
        timeout=DEFAULT_CLOUD_TIMEOUT_SECONDS,
        primary=True,
    ),
    ModelTier(
        key="gpt-5-mini",
        model="gpt-5-mini",
        provider="cloud",
        label="gpt-5-mini (ceiling / comparative)",
        role=(
            "CEILING -- a tier above deployed parity, measured to show what "
            "the technique can do when model quality is not the binding "
            "constraint. Runs 1 and 2 used this tier; keeping it in the "
            "matrix is what makes those historical numbers comparable to "
            "this round's."
        ),
        timeout=DEFAULT_CLOUD_TIMEOUT_SECONDS,
    ),
    ModelTier(
        key="gemma-4-e4b-local",
        model="google/gemma-4-e4b",
        provider="local",
        label="google/gemma-4-e4b (local cost-regime arm)",
        role=(
            "COST REGIME, NOT PARITY -- a locally-hosted small model, "
            "informative for the ADR's cost-architecture input only. It is "
            "not the deployed configuration and no parity claim rests on "
            "it. Optional: if LM Studio is not serving, this tier records "
            "NOT_RUN and the run continues."
        ),
        timeout=DEFAULT_LOCAL_TIMEOUT_SECONDS,
        optional=True,
    ),
    ModelTier(
        key="gemma-4-31b-local",
        model="google/gemma-4-31b",
        provider="local",
        label="google/gemma-4-31b (local scaling comparator)",
        role=(
            "COST REGIME / LOCAL SCALING -- the same locally-hosted family "
            "at a materially larger size. Paired with the e4b tier it "
            "separates 'small models cannot do this' from 'LOCAL models "
            "cannot do this', which are different inputs to a "
            "cost-architecture decision. Not the deployed configuration; no "
            "parity claim rests on it. Optional, and ordered last so it "
            "never contends with the e4b arm for the same local server."
        ),
        timeout=DEFAULT_LOCAL_TIMEOUT_SECONDS,
        optional=True,
    ),
)


def resolve_tier_config(tier: ModelTier) -> LLMConfig:
    """This tier's config, with the model taken from the TIER.

    The environment contributes exactly one thing: ``OPENAI_API_KEY`` for
    cloud tiers. Everything that affects what is being measured -- model,
    provider, base URL, timeout -- comes from the tier definition, so a
    result labelled with a model was produced by that model.
    """
    if tier.provider == "cloud":
        return LLMConfig.for_cloud(
            model=tier.model,
            api_key=os.environ.get("OPENAI_API_KEY", ""),
            timeout=tier.timeout,
        )
    if tier.provider == "local":
        return LLMConfig.for_local(model=tier.model, timeout=tier.timeout)
    raise LLMUnavailable(f"tier {tier.key!r} has unknown provider {tier.provider!r}")


def probe_provider(config: LLMConfig) -> str | None:
    """``None`` if the provider answers, else a reason string.

    Exists so an unreachable OPTIONAL tier becomes a recorded NOT_RUN with
    a legible reason instead of 20 identical connection failures (or, worse,
    a crash that takes the paid tiers' results down with it). Uses a SHORT
    window deliberately -- this asks "is anything listening", not "can this
    model answer", so the tier's own long generation window does not apply.
    """
    try:
        from openai import OpenAI
    except ImportError as exc:  # pragma: no cover - openai is a pinned dep
        return f"openai package not importable: {exc}"
    try:
        client = OpenAI(
            base_url=config.base_url,
            api_key=config.api_key,
            timeout=min(30.0, config.timeout),
            max_retries=0,
        )
        client.models.list()
    except Exception as exc:  # noqa: BLE001 - any failure IS the answer
        return (
            f"provider at {config.base_url} did not answer a models.list() "
            f"probe: {type(exc).__name__}: {exc}"
        )
    return None


# ==========================================================================
# Per-call recording
# ==========================================================================


@dataclass(frozen=True)
class CallRecord:
    """When an arm was invoked for one oracle, and how long it took.

    ``latency_seconds`` covers the WHOLE arm invocation for that oracle,
    including the one bounded infra re-attempt if it happened -- stated
    here because that makes a retried call's latency legitimately roughly
    double, and a reader comparing latencies needs to know that rather
    than infer a slowdown that was really a retry. The retry itself is
    logged separately and loudly.
    """

    called_at: str
    latency_seconds: float


def _log(message: str) -> None:
    print(f"[sweep] {message}", file=sys.stderr, flush=True)


def _with_bounded_infra_retry(arm_fn):
    """Wrap a candidate arm callable with ONE bounded, logged re-attempt --
    infra-level NOT_RUN only (unreachable provider or exceeded window), per
    this module's "no retry-shopping" discipline. A parse failure or a
    genuine model-quality miss returns NOT_RUN/a low-scoring ANSWERED
    exactly once, unretried.
    """

    def wrapped(oracle):
        response = arm_fn(oracle)
        if response.outcome is not ArmOutcome.NOT_RUN or not response.degraded_reasons:
            return response
        reason = response.degraded_reasons[0]
        if _INFRA_FAILURE_MARKER not in reason:
            return response  # not infra -- no retry, stays loud as-is.
        _log(
            f"{oracle.oracle_id}: infra-level NOT_RUN ({reason!r}) -- "
            f"one bounded re-attempt (max {_MAX_INFRA_RETRIES})"
        )
        retried = arm_fn(oracle)
        if retried.outcome is ArmOutcome.NOT_RUN:
            _log(
                f"{oracle.oracle_id}: still NOT_RUN after bounded "
                "re-attempt -- no further retries, staying loud"
            )
        else:
            _log(f"{oracle.oracle_id}: bounded re-attempt recovered the call")
        return retried

    wrapped.declared_role = arm_fn.declared_role  # type: ignore[attr-defined]
    return wrapped


def _with_call_record(arm_name: str, arm_fn, sink: dict[tuple[str, str], CallRecord]):
    """Record WHEN this arm was invoked for THIS oracle and HOW LONG it took.

    The latency half is the run-3 addition: with three tiers of wildly
    different speed in one artifact, a pass rate without a latency beside
    it cannot be read -- a reader cannot tell a tier that answered slowly
    from one that timed out, which is exactly the confusion chris flagged
    in the earlier gemma observations.
    """

    def wrapped(oracle):
        started = time.monotonic()
        called_at = datetime.now(timezone.utc).isoformat()
        try:
            return arm_fn(oracle)
        finally:
            sink[(oracle.oracle_id, arm_name)] = CallRecord(
                called_at=called_at,
                latency_seconds=time.monotonic() - started,
            )

    declared_role = getattr(arm_fn, "declared_role", None)
    if declared_role is not None:
        wrapped.declared_role = declared_role  # type: ignore[attr-defined]
    return wrapped


def build_comparison_report(
    oracles, registry: ArmRegistry, dependencies
) -> tuple[ComparisonReport, Mapping]:
    """Exactly what `harness.runner.compare` does internally, inlined so
    the caller keeps the raw per-arm `reports` dict `compare()` throws
    away -- needed for per-oracle logging across every registered arm, not
    just the one candidate `compare()` returns a `ComparisonReport` for.
    Calls `sweep()` exactly ONCE, same as `compare()` does.
    """
    resolved = dict(dependencies or {})
    for klass, issue in CLASS_DEPENDENCIES.items():
        resolved.setdefault(klass, UNRECORDED_DEPENDENCY)
        if resolved[klass].issue != issue:
            raise ValueError(
                f"class {klass.value} depends on {issue}, but a "
                f"{resolved[klass].issue} state was supplied"
            )

    reports = sweep(oracles, registry)
    components = [
        reports[name] for name in registry.names_with_role(ArmRole.BASELINE_COMPONENT)
    ]
    if not components:
        raise ValueError(
            "no baseline components registered; a candidate arm scored "
            "against nothing is not a comparison"
        )
    baseline = compose_baseline(components)
    (candidate_name,) = registry.names_with_role(ArmRole.CANDIDATE_ARM)
    report = ComparisonReport(
        baseline=baseline, arm=reports[candidate_name], dependencies=resolved
    )
    return report, reports


# ==========================================================================
# Running one tier
# ==========================================================================


@dataclass(frozen=True)
class TierOutcome:
    """What happened for one tier -- measured, or NOT_RUN with a reason.

    A NOT_RUN tier keeps its place in this sequence and in the artifact. It
    carries `report=None`/`reports=None` rather than an empty
    `ComparisonReport`, so there is structurally no way to render a
    fabricated 0/0 for a tier that never ran.
    """

    tier: ModelTier
    status: str  # "measured" | "not_run"
    not_run_reason: str | None
    config: LLMConfig | None
    report: ComparisonReport | None
    reports: Mapping | None
    call_records: Mapping[tuple[str, str], CallRecord] = field(default_factory=dict)
    started_at: str = ""
    finished_at: str = ""
    oracles: Sequence = ()


def _dependencies() -> Mapping[QuestionClass, DependencyState]:
    return {
        QuestionClass.NEEDS_DECLARED_STATE_HISTORY: DependencyState(
            issue="CHAOS-3563", state=CHAOS_3563_STATE, recorded=True
        )
    }


def run_tier(tier: ModelTier, oracles=ALL_ORACLES) -> TierOutcome:
    """Measure one tier, or return a recorded NOT_RUN outcome.

    Two things can prevent a tier running, and both are RECORDED rather
    than raised: its config cannot be built (e.g. no API key), or its
    provider does not answer a reachability probe. For an optional tier
    that is the whole point (chris: do not fail the run over LM Studio
    being down, and do not start it yourself). For a required tier it is
    still better recorded than crashed -- the other tiers' paid results
    must survive one tier's infrastructure.
    """
    started_at = datetime.now(timezone.utc).isoformat()

    def _not_run(reason: str) -> TierOutcome:
        _log(f"tier {tier.key}: NOT_RUN -- {reason}")
        return TierOutcome(
            tier=tier,
            status="not_run",
            not_run_reason=reason,
            config=None,
            report=None,
            reports=None,
            call_records={},
            started_at=started_at,
            finished_at=datetime.now(timezone.utc).isoformat(),
            oracles=(),
        )

    try:
        config = resolve_tier_config(tier)
    except LLMUnavailable as exc:
        return _not_run(f"config could not be built: {exc}")

    probe_failure = probe_provider(config)
    if probe_failure is not None:
        return _not_run(probe_failure)

    _log(
        f"tier {tier.key}: model={config.model} provider={config.provider} "
        f"base_url={config.base_url} timeout={config.timeout}s "
        f"prompt_version={_PROJECTION_VERSION} "
        "temperature=API_DEFAULT for gpt-5-family (rejects a caller-selected "
        "value -- see harness/llm/client.py's module docstring)"
    )

    call_records: dict[tuple[str, str], CallRecord] = {}
    registry = ArmRegistry()
    registry.register(
        native.ARM_NAME,
        _with_call_record(native.ARM_NAME, native.answer, call_records),
        ArmRole.BASELINE_COMPONENT,
    )
    registry.register(
        episode_readback.ARM_NAME,
        _with_call_record(
            episode_readback.ARM_NAME, episode_readback.answer, call_records
        ),
        ArmRole.BASELINE_COMPONENT,
    )
    registry.register(
        extraction.ARM_NAME,
        _with_call_record(
            extraction.ARM_NAME,
            # make_answer binds THIS tier's model -- see the module
            # docstring on why no measured run may resolve it from env.
            _with_bounded_infra_retry(extraction.make_answer(config)),
            call_records,
        ),
        ArmRole.CANDIDATE_ARM,
    )

    report, reports = build_comparison_report(oracles, registry, _dependencies())
    finished_at = datetime.now(timezone.utc).isoformat()

    measured = sum(
        1 for r in report.arm.results if r.verdict is not Verdict.NOT_MEASURED
    )
    _log(
        f"tier {tier.key}: {len(oracles)} oracles, {measured} measured by the "
        f"candidate arm, {len(oracles) - measured} NOT_RUN "
        f"(native_control_status={report.native_control_status().value})"
    )

    return TierOutcome(
        tier=tier,
        status="measured",
        not_run_reason=None,
        config=config,
        report=report,
        reports=reports,
        call_records=call_records,
        started_at=started_at,
        finished_at=finished_at,
        oracles=tuple(oracles),
    )


# ==========================================================================
# Rendering
# ==========================================================================


def _oracle_reason(result) -> str:
    """The exact NOT_RUN reason string Oracle.evaluate embedded in its
    sole "measurement_happened" assertion -- surfaced verbatim rather than
    re-derived, so a NOT-AUTHORABLE oracle's reason, an exceeded-window
    reason, and a genuine unreachable-provider's reason all read
    differently in the table, exactly as differently as the underlying
    ArmResponse.degraded_reasons already made them.
    """
    if result.verdict is not Verdict.NOT_MEASURED:
        return ""
    for assertion in result.assertions:
        if assertion.assertion_id == "measurement_happened":
            return assertion.detail
    return ""  # pragma: no cover - NOT_MEASURED always carries this assertion.


def _latency_cell(record: CallRecord | None) -> str:
    """``n/a`` when the arm was never invoked, never ``0.00s``.

    A zero would read as "answered instantly", which is a measurement
    claim nobody made -- the arm returned NOT_RUN before any provider call
    happened at all.
    """
    if record is None:
        return "n/a"
    return f"{record.latency_seconds:.2f}s"


def _render_per_oracle_table(oracles, reports: Mapping, call_records: Mapping) -> str:
    arm_names = list(reports.keys())
    header = "| Oracle | Class | " + " | ".join(
        f"{name} (verdict @ called / Latency)" for name in arm_names
    )
    header += " |"
    sep = "|---|---|" + "|".join("---" for _ in arm_names) + "|"
    lines = [header, sep]
    results_by_arm = {
        name: {r.oracle_id: r for r in tr.results} for name, tr in reports.items()
    }
    for oracle in oracles:
        cells = [oracle.oracle_id, oracle.question_class.value]
        for name in arm_names:
            result = results_by_arm[name][oracle.oracle_id]
            record = call_records.get((oracle.oracle_id, name))
            verdict = result.verdict.value
            when = record.called_at if record is not None else "n/a"
            cell = f"`{verdict}` @ `{when}` / `{_latency_cell(record)}`"
            if result.verdict is Verdict.NOT_MEASURED:
                cell += f" ({_oracle_reason(result)})"
            cells.append(cell)
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def _render_tier_section(outcome: TierOutcome) -> str:
    tier = outcome.tier
    lines = [
        f"## Tier: {tier.label}",
        "",
        f"- tier key: `{tier.key}`",
        f"- model: `{tier.model}`",
        f"- provider: `{tier.provider}`",
        f"- configured timeout: `{tier.timeout}s`",
        f"- role: {tier.role}",
    ]
    if tier.primary:
        lines.append(
            "- **PRIMARY SCORED TIER** -- this is the deployed "
            "configuration; read parity statements from this section."
        )
    if tier.optional:
        lines.append(
            "- OPTIONAL tier -- informative only; its absence is recorded, not fatal."
        )
    lines.append("")

    if outcome.status != "measured" or outcome.report is None:
        lines += [
            f"**STATUS: NOT_RUN** -- reason: {outcome.not_run_reason}",
            "",
            "No results are reported for this tier because none were "
            "produced. This section deliberately carries no comparison "
            "rows and no scores: a tier that did not run has nothing to "
            "report, and rendering an empty result set as zeroes would "
            "read as a measured zero.",
            "",
        ]
        return "\n".join(lines)

    report = outcome.report
    total = len(outcome.oracles)
    measured = sum(
        1 for r in report.arm.results if r.verdict is not Verdict.NOT_MEASURED
    )
    config = outcome.config
    lines += [
        "**STATUS: MEASURED**",
        "",
        f"- base URL: `{config.base_url if config else 'n/a'}`",
        f"- prompt/schema version: `{_PROJECTION_VERSION}`",
        f"- started: `{outcome.started_at}`",
        f"- finished: `{outcome.finished_at}`",
        f"- {total} oracles total; {measured} measured by the extraction "
        f"candidate arm; {total - measured} NOT_RUN",
        f"- class (a) control status: `{report.native_control_status().value}`",
        "",
        "### Per-class comparison",
        "",
        "```",
        report.render(),
        "```",
        "",
        "### Per-oracle results",
        "",
        "Every arm x oracle verdict, the wall-clock instant that arm was "
        "called, and the call's latency. Latency renders `n/a` -- never "
        "`0.00s` -- when the arm returned NOT_RUN before any provider call "
        "happened.",
        "",
        _render_per_oracle_table(
            outcome.oracles, outcome.reports or {}, outcome.call_records
        ),
        "",
    ]
    return "\n".join(lines)


def _render_tier_matrix_summary(outcomes: Sequence[TierOutcome]) -> str:
    lines = [
        "| Tier | Model | Status | Class (a) control | Candidate measured | Reason if NOT_RUN |",
        "|---|---|---|---|---|---|",
    ]
    for outcome in outcomes:
        tier = outcome.tier
        if outcome.status == "measured" and outcome.report is not None:
            control = f"`{outcome.report.native_control_status().value}`"
            measured = sum(
                1
                for r in outcome.report.arm.results
                if r.verdict is not Verdict.NOT_MEASURED
            )
            measured_cell = f"{measured}/{len(outcome.oracles)}"
            reason = ""
        else:
            control = "n/a"
            measured_cell = "n/a"
            reason = outcome.not_run_reason or "unrecorded"
        lines.append(
            f"| `{tier.key}` | `{tier.model}` | `{outcome.status}` | "
            f"{control} | {measured_cell} | {reason} |"
        )
    return "\n".join(lines)


def _render_coverage_statement() -> str:
    authored = sorted(SOURCE_DOCUMENTS)
    not_authorable = sorted(NOT_AUTHORABLE_REASONS)
    lines = [
        f"{len(authored)} of {len(ALL_ORACLES)} oracles have authored source "
        f"material for the extraction arm; the other {len(not_authorable)} "
        "are classified NOT AUTHORABLE, each with its own stated reason "
        "(`harness/arms/source_documents.py`'s `NOT_AUTHORABLE_REASONS`). "
        "Every oracle in the corpus is therefore accounted for -- there is "
        "no unclassified remainder.",
        "",
        "Authored (measurable by this arm): " + ", ".join(f"`{o}`" for o in authored),
        "",
        "Not authorable, with reason:",
        "",
        "| Oracle | Reason |",
        "|---|---|",
    ]
    for oracle_id in not_authorable:
        lines.append(f"| `{oracle_id}` | `{NOT_AUTHORABLE_REASONS[oracle_id]}` |")
    return "\n".join(lines)


def _render_artifact(
    *,
    outcomes: Sequence[TierOutcome],
    run_started_at: str,
    run_finished_at: str,
) -> str:
    lines = [
        "# CHAOS-3499 measured trial results",
        "",
        "Generated by `trials/chaos_3499/run_measured_sweep.py`. This is the",
        "trial artifact -- the rendered per-class comparison, per model tier,",
        "committed. It is NOT a headline number -- there isn't one;",
        "`ComparisonReport` cannot render one by construction.",
        "",
        f"- run started: `{run_started_at}`",
        f"- run finished: `{run_finished_at}`",
        f"- dependency state for class (b): `{CHAOS_3563_STATE}`",
        "- temperature: API default (gpt-5-family rejects a caller-selected",
        "  value -- see `harness/llm/client.py`'s module docstring and",
        "  `src/dev_health_ops/llm/providers/openai_capabilities.py`'s",
        "  `supports_temperature`)",
        "",
        "## Model tier matrix",
        "",
        "Every tier's model is named explicitly by `MODEL_TIERS` and passed",
        "into the arm (`extraction.make_answer(config)`); no tier resolves",
        "its model from the environment, so a section's label and the model",
        "that produced it cannot disagree.",
        "",
        _render_tier_matrix_summary(outcomes),
        "",
        "## Corpus coverage",
        "",
        _render_coverage_statement(),
        "",
    ]
    for outcome in outcomes:
        lines.append(_render_tier_section(outcome))
        lines.append("")
    lines += [
        "## Interpretation notes",
        "",
        "This section is generated boilerplate about how to READ the report",
        "shape above, not a narrative about this specific run's numbers --",
        "run-specific observations (variance across runs, cross-run",
        "comparisons, etc.) belong in `docs/adr-draft.md`, which this file",
        "is never hand-edited to match; regenerating this artifact must",
        "always reproduce it byte-for-byte from this script (#1603 finding",
        "7) -- if a claim needs updating, it is either the shape guidance",
        "below, or it belongs in the ADR draft instead.",
        "",
        '- **Class (a) "NOT MEASURED" is not the same as "LOST."**',
        "  `native_control_status()` returns one of three states (see",
        "  `harness/runner.py`'s `ControlStatus`): `held`, `lost`, or",
        "  `not_measured`. Only `lost` means the baseline was actually",
        "  outscored; `not_measured` means the candidate has not been run",
        "  against every class-(a) oracle yet. The rendered banner (if any)",
        "  already makes this distinction in its own wording.",
        "- **No headline number, by construction.** `ComparisonReport` has",
        "  no method that aggregates across classes into one score --",
        "  per §15.2, weighting (a)x1 (b)x1 (c)x5 into one number would",
        "  flatter any extraction-capable candidate regardless of merit.",
        "  Read each class row on its own terms.",
        "- **A NOT_RUN row is never a model-quality result.** An oracle",
        "  without authored source material, an unreachable provider, and a",
        "  call that exceeded its tier's configured window all land as",
        "  NOT_RUN with their own distinguishable reason string. None of",
        "  them is a pass and none of them is a fail.",
        "- **Latency is per arm x oracle call**, covering the whole arm",
        "  invocation including the one bounded infra re-attempt if it",
        "  happened -- so a retried call's latency is legitimately about",
        "  double, and the retry is logged separately.",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    run_started_at = datetime.now(timezone.utc).isoformat()
    _log(f"run-3 matrix: {len(MODEL_TIERS)} tiers, started {run_started_at}")

    outcomes = [run_tier(tier) for tier in MODEL_TIERS]

    run_finished_at = datetime.now(timezone.utc).isoformat()
    for outcome in outcomes:
        if outcome.status == "measured" and outcome.report is not None:
            print(f"\n=== {outcome.tier.label} ===")
            print(outcome.report.render())
        else:
            print(f"\n=== {outcome.tier.label} === NOT_RUN: {outcome.not_run_reason}")

    artifact_path = Path(__file__).parent / "docs" / "measured-trial-results.md"
    artifact_path.write_text(
        _render_artifact(
            outcomes=outcomes,
            run_started_at=run_started_at,
            run_finished_at=run_finished_at,
        )
    )
    _log(f"artifact written: {artifact_path}")

    required_not_run = [
        o for o in outcomes if o.status != "measured" and not o.tier.optional
    ]
    if required_not_run:
        _log(
            "NON-ZERO EXIT: a REQUIRED tier did not run -- "
            + "; ".join(f"{o.tier.key}: {o.not_run_reason}" for o in required_not_run)
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
