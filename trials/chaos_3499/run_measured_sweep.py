#!/usr/bin/env python3
"""CHAOS-3499 step 3: the measured sweep -- composed baseline vs the
extraction candidate arm over the pinned corpus, all 20 oracles, cloud
model.

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
  skip.
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
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from trials.chaos_3499.corpus.oracles import ALL_ORACLES
from trials.chaos_3499.harness.arms import episode_readback, extraction, native
from trials.chaos_3499.harness.arms.extraction import _PROJECTION_VERSION
from trials.chaos_3499.harness.contracts import ArmOutcome, QuestionClass
from trials.chaos_3499.harness.llm.client import DEFAULT_CLOUD_MODEL, LLMConfig
from trials.chaos_3499.harness.oracle import Verdict
from trials.chaos_3499.harness.runner import (
    ArmRegistry,
    ArmRole,
    DependencyState,
    compare,
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

    registry = ArmRegistry()
    registry.register(native.ARM_NAME, native.answer, ArmRole.BASELINE_COMPONENT)
    registry.register(
        episode_readback.ARM_NAME,
        episode_readback.answer,
        ArmRole.BASELINE_COMPONENT,
    )
    registry.register(
        extraction.ARM_NAME,
        _with_bounded_infra_retry(extraction.answer),
        ArmRole.CANDIDATE_ARM,
    )

    dependencies = {
        QuestionClass.NEEDS_DECLARED_STATE_HISTORY: DependencyState(
            issue="CHAOS-3563", state=CHAOS_3563_STATE, recorded=True
        )
    }

    (report,) = compare(ALL_ORACLES, registry, dependencies=dependencies)
    rendered = report.render()

    print(rendered)

    # report.arm IS the extraction candidate's TrialReport from the ONE
    # sweep compare() already ran -- reusing it here (rather than sweeping
    # again to recompute this count) matters beyond tidiness: the
    # extraction arm makes real, billable, non-deterministic model calls,
    # so a second sweep would double the spend AND could disagree with the
    # first run's own numbers.
    total = len(ALL_ORACLES)
    measured = sum(
        1 for r in report.arm.results if r.verdict is not Verdict.NOT_MEASURED
    )
    not_run = total - measured
    _log(
        f"headline honesty: {total} oracles total, {measured} measured by "
        f"the candidate arm, {not_run} NOT_RUN "
        f"(native_control_holds={report.native_control_holds()})"
    )

    run_finished_at = datetime.now(timezone.utc)
    artifact_path = Path(__file__).parent / "docs" / "measured-trial-results.md"
    artifact_path.write_text(
        _render_artifact(
            report=report,
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


def _render_artifact(
    *, report, cfg, run_started_at, run_finished_at, total, measured, not_run
) -> str:
    lines = [
        "# CHAOS-3499 step 3 -- measured trial results",
        "",
        "Generated by `trials/chaos_3499/run_measured_sweep.py`. This is the",
        "trial artifact instruction 4 asks for: the rendered per-class",
        "comparison, committed. It is NOT a headline number -- there isn't",
        "one; `ComparisonReport` cannot render one by construction.",
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
        f"- {not_run} NOT_RUN (no source material authored yet, or an",
        "  unrecoverable provider failure after the one bounded retry)",
        f"- class (a) control holds: `{report.native_control_holds()}`",
        "",
        "## Per-class comparison",
        "",
        "```",
        report.render(),
        "```",
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
        '- **Class (a) "control did NOT hold" here means NOT MEASURED, not',
        "  LOST.** `native_control_holds()` requires the candidate arm to",
        "  have measured every class-(a) oracle before it can confirm the",
        "  control; a class where the candidate has zero authored source",
        "  material reports NOT_RUN on every oracle in it, and",
        "  `is_comparable` is correctly `False` as a result. The per-class",
        "  line above already says `NOT MEASURED`, not `lost` -- the banner",
        "  beneath it is the same boolean stated more alarmingly. Read the",
        "  per-class line, not just the banner.",
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
