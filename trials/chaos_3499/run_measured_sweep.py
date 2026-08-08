#!/usr/bin/env python3
"""CHAOS-3499 measured sweep -- composed baseline vs the extraction
candidate arm over the pinned corpus, all 20 oracles, cloud model.

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
  the extraction arm (the provider was genuinely unreachable) -- never for
  a parse failure or a real model-quality miss. Every retry is logged, so
  the report never silently launders a flaky call into a clean pass.
* **NOT_RUN stays loud.** Every oracle without authored source material (or
  that hits an unrecoverable provider failure) reports NOT_RUN, which
  `ComparisonReport`/`Oracle.evaluate` already treat as a failure, not a
  skip. NOT-AUTHORABLE oracles (see `harness/arms/source_documents.py`'s
  `NOT_AUTHORABLE_REASONS`) get their own distinguishable NOT_RUN reason
  string, not the generic "no source material yet" one -- a reader must be
  able to tell "we haven't gotten to this" from "there is nothing to
  author" without cross-referencing a second document.
* **Per-oracle results are logged, not just the aggregate** (authoring-round
  fix for the class-(c) "which oracle passed when" gap the #1603 review
  round flagged): every arm x oracle pair's verdict, and the wall-clock
  instant that arm was actually called for that oracle, are recorded into
  the committed artifact.
* **Class (b) comparability comes from a real `DependencyState`,** supplied
  below with the CHAOS-3563 citation exactly as given, never fabricated or
  assumed.
* **`O7_null_valid_from` stays outside class (a).** It already is,
  structurally (see `corpus/oracles.py` -- it is authored as its own
  oracle, never folded into `O7_valid`'s assertions); this script does not
  touch that, and this comment exists so a reader auditing the sweep does
  not have to re-derive it.
* **No headline number.** `ComparisonReport` structurally cannot render
  one (see `harness/runner.py`); this script only ever prints/writes its
  own `render()` output, per class.
* **This script never calls the candidate arm more than once per oracle**
  (beyond the one bounded infra retry) -- computing the per-oracle log and
  the `ComparisonReport` from the SAME `sweep()` call, never a second
  sweep, matters beyond tidiness: the extraction arm makes real, billable,
  non-deterministic model calls, so calling it twice would double the
  spend AND could disagree with itself.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path

from trials.chaos_3499.corpus.oracles import ALL_ORACLES
from trials.chaos_3499.harness.arms import episode_readback, extraction, native
from trials.chaos_3499.harness.arms.extraction import _PROJECTION_VERSION
from trials.chaos_3499.harness.contracts import ArmOutcome, QuestionClass
from trials.chaos_3499.harness.llm.client import DEFAULT_CLOUD_MODEL, LLMConfig
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
#: client.py) uses ONLY for genuine connection failures (both the Responses
#: API and Chat Completions branches share this exact prefix) -- never for
#: config errors (missing key, disallowed host), empty-output, or parse
#: failures, which are permanent regardless of retry. This is the sole
#: signal the bounded-retry policy below acts on.
_INFRA_FAILURE_MARKER = "could not reach"

#: One bounded re-attempt, never more -- see this module's docstring.
_MAX_INFRA_RETRIES = 1


def _log(message: str) -> None:
    print(f"[sweep] {message}", file=sys.stderr, flush=True)


def _with_bounded_infra_retry(arm_fn):
    """Wrap a candidate arm callable with ONE bounded, logged re-attempt --
    infra-level NOT_RUN only, per this module's "no retry-shopping"
    discipline. A parse failure or a genuine model-quality miss returns
    NOT_RUN/a low-scoring ANSWERED exactly once, unretried.
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


def _with_call_timestamp(arm_name: str, arm_fn, sink: dict[tuple[str, str], str]):
    """Record the wall-clock instant THIS arm was actually invoked for THIS
    oracle -- the per-oracle timestamp instruction 1 asks for. Captured at
    the first attempt only (a bounded infra retry, if any, is a detail of
    ONE oracle's measurement, not a second measurement of it).
    """

    def wrapped(oracle):
        sink[(oracle.oracle_id, arm_name)] = datetime.now(timezone.utc).isoformat()
        return arm_fn(oracle)

    declared_role = getattr(arm_fn, "declared_role", None)
    if declared_role is not None:
        wrapped.declared_role = declared_role  # type: ignore[attr-defined]
    return wrapped


def _require_cloud_config() -> LLMConfig:
    os.environ["LLM_PROVIDER"] = "cloud"
    cfg = LLMConfig.from_env()
    if cfg.model != DEFAULT_CLOUD_MODEL:
        _log(
            f"WARNING: OPENAI_MODEL={cfg.model!r} overrides the resolved "
            f"production-class default {DEFAULT_CLOUD_MODEL!r} -- confirm "
            "this is intentional, the trial's own instruction is to "
            "measure the production-class model, not a hand-picked one."
        )
    return cfg


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


def main() -> int:
    cfg = _require_cloud_config()
    run_started_at = datetime.now(timezone.utc)
    _log(
        f"model={cfg.model} provider={cfg.provider} base_url={cfg.base_url} "
        f"prompt_version={_PROJECTION_VERSION} "
        "temperature=API_DEFAULT (gpt-5-family rejects a caller-selected "
        "value -- see harness/llm/client.py's module docstring) "
        f"run_started_at={run_started_at.isoformat()}"
    )

    call_timestamps: dict[tuple[str, str], str] = {}
    registry = ArmRegistry()
    registry.register(
        native.ARM_NAME,
        _with_call_timestamp(native.ARM_NAME, native.answer, call_timestamps),
        ArmRole.BASELINE_COMPONENT,
    )
    registry.register(
        episode_readback.ARM_NAME,
        _with_call_timestamp(
            episode_readback.ARM_NAME, episode_readback.answer, call_timestamps
        ),
        ArmRole.BASELINE_COMPONENT,
    )
    registry.register(
        extraction.ARM_NAME,
        _with_call_timestamp(
            extraction.ARM_NAME,
            _with_bounded_infra_retry(extraction.answer),
            call_timestamps,
        ),
        ArmRole.CANDIDATE_ARM,
    )

    dependencies = {
        QuestionClass.NEEDS_DECLARED_STATE_HISTORY: DependencyState(
            issue="CHAOS-3563", state=CHAOS_3563_STATE, recorded=True
        )
    }

    report, reports = build_comparison_report(ALL_ORACLES, registry, dependencies)
    rendered = report.render()

    print(rendered)

    total = len(ALL_ORACLES)
    measured = sum(
        1 for r in report.arm.results if r.verdict is not Verdict.NOT_MEASURED
    )
    not_run = total - measured
    _log(
        f"headline honesty: {total} oracles total, {measured} measured by "
        f"the candidate arm, {not_run} NOT_RUN "
        f"(native_control_status={report.native_control_status().value})"
    )

    run_finished_at = datetime.now(timezone.utc)
    artifact_path = Path(__file__).parent / "docs" / "measured-trial-results.md"
    artifact_path.write_text(
        _render_artifact(
            report=report,
            reports=reports,
            call_timestamps=call_timestamps,
            cfg=cfg,
            run_started_at=run_started_at,
            run_finished_at=run_finished_at,
            total=total,
            measured=measured,
            not_run=not_run,
        )
    )
    _log(f"artifact written: {artifact_path}")
    return 0


def _oracle_reason(result) -> str:
    """The exact NOT_RUN reason string Oracle.evaluate embedded in its
    sole "measurement_happened" assertion -- surfaced verbatim rather than
    re-derived, so a NOT-AUTHORABLE oracle's reason and a genuine
    unreachable-provider's reason read differently in the table, exactly
    as differently as the underlying ArmResponse.degraded_reasons already
    made them.
    """
    if result.verdict is not Verdict.NOT_MEASURED:
        return ""
    for assertion in result.assertions:
        if assertion.assertion_id == "measurement_happened":
            return assertion.detail
    return ""  # pragma: no cover - NOT_MEASURED always carries this assertion.


def _render_per_oracle_table(oracles, reports: dict, call_timestamps: dict) -> str:
    arm_names = list(reports.keys())
    header = "| Oracle | Class | " + " | ".join(arm_names) + " |"
    sep = "|---|---|" + "|".join("---" for _ in arm_names) + "|"
    lines = [header, sep]
    results_by_arm = {
        name: {r.oracle_id: r for r in tr.results} for name, tr in reports.items()
    }
    for oracle in oracles:
        cells = [oracle.oracle_id, oracle.question_class.value]
        for name in arm_names:
            result = results_by_arm[name][oracle.oracle_id]
            verdict = result.verdict.value
            when = call_timestamps.get((oracle.oracle_id, name), "n/a")
            if result.verdict is Verdict.NOT_MEASURED:
                reason = _oracle_reason(result)
                cells.append(f"`{verdict}` @ `{when}` ({reason})")
            else:
                cells.append(f"`{verdict}` @ `{when}`")
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def _render_artifact(
    *,
    report,
    reports,
    call_timestamps,
    cfg,
    run_started_at,
    run_finished_at,
    total,
    measured,
    not_run,
) -> str:
    lines = [
        "# CHAOS-3499 measured trial results",
        "",
        "Generated by `trials/chaos_3499/run_measured_sweep.py`. This is the",
        "trial artifact -- the rendered per-class comparison, committed. It",
        "is NOT a headline number -- there isn't one; `ComparisonReport`",
        "cannot render one by construction.",
        "",
        "## Run parameters",
        "",
        f"- model: `{cfg.model}`",
        f"- provider: `{cfg.provider}` (`{cfg.base_url}`)",
        f"- prompt/schema version: `{_PROJECTION_VERSION}`",
        "- temperature: API default (gpt-5-family rejects a caller-selected",
        "  value -- see `harness/llm/client.py`'s module docstring and",
        "  `src/dev_health_ops/llm/providers/openai_capabilities.py`'s",
        "  `supports_temperature`)",
        f"- run started: `{run_started_at.isoformat()}`",
        f"- run finished: `{run_finished_at.isoformat()}`",
        f"- dependency state for class (b): `{CHAOS_3563_STATE}`",
        "",
        "## Headline honesty",
        "",
        f"- {total} oracles total",
        f"- {measured} measured by the extraction candidate arm",
        f"- {not_run} NOT_RUN (no source material authored yet, not",
        "  authorable for this arm at all, or an unrecoverable provider",
        "  failure after the one bounded retry -- see the per-oracle table",
        "  below for which reason applies to which oracle)",
        f"- class (a) control status: `{report.native_control_status().value}`",
        "",
        "## Per-class comparison",
        "",
        "```",
        report.render(),
        "```",
        "",
        "## Per-oracle results",
        "",
        "Every arm x oracle verdict, and the wall-clock instant that arm",
        "was actually called for that oracle (first attempt only -- a",
        "bounded infra retry, if any, is a detail of ONE measurement, not a",
        "second one). This is the fix for the #1603 review round's flagged",
        "gap: which oracle passed in which run is no longer ambiguous.",
        "",
        _render_per_oracle_table(ALL_ORACLES, reports, call_timestamps),
        "",
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
        "",
    ]
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
