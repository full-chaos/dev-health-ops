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

import argparse
import dataclasses
import hashlib
import json
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
    NOT_AUTHORABLE_CATEGORIES,
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
        label="gpt-5-mini (mid-tier comparative)",
        role=(
            "MID-TIER COMPARATIVE -- one step above deployed parity. Runs 1 "
            "and 2 used this tier, so keeping it in the matrix is what makes "
            "those historical numbers comparable to this round's. NOT the "
            "ceiling: gpt-5-nano and gpt-5-mini were both selected for "
            "CATEGORIZATION and landscape-shape explanation in the app, not "
            "for multi-turn, fuzzy-lookup, or interpretive question "
            "answering -- so neither tier establishes what a model can do at "
            "the top of the capability axis this corpus actually exercises. "
            "That is what the frontier tier below is for."
        ),
        timeout=DEFAULT_CLOUD_TIMEOUT_SECONDS,
    ),
    ModelTier(
        key="gpt-5.6-luna",
        model="gpt-5.6-luna",
        provider="cloud",
        label="gpt-5.6-luna (frontier discriminator)",
        role=(
            "FRONTIER DISCRIMINATOR -- present to answer exactly ONE "
            "question: is the class-(c) deficit a MODEL ceiling or a "
            "FRAMEWORK limitation? A flat result here (~2/4, matching "
            "gpt-5-mini and gemma-4-31b) says the binding constraint is the "
            "extraction contract and harness, and that no model purchase "
            "buys past it. A jump toward 4/4 says there is a real capability "
            "curve with a known top, and the tier table becomes a genuine "
            "cost/quality frontier. It is a DISCRIMINATOR, not a deployment "
            "proposal."
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


def _corpus_provenance() -> dict[str, str]:
    """Content hashes of every file that defines what was measured.

    Without these, a records file says what the answers were but not what
    the questions were, so two runs whose corpus differs are
    indistinguishable in the record. These are what let a later reader
    prove that runs N and N+1 asked the SAME thing -- the exact check the
    cross-run drift claim in the ADR depends on.
    """
    here = Path(__file__).parent
    files = {
        "corpus/oracles.py": here / "corpus" / "oracles.py",
        "corpus/ground_truth.py": here / "corpus" / "ground_truth.py",
        "harness/arms/source_documents.py": here
        / "harness"
        / "arms"
        / "source_documents.py",
        "harness/arms/extraction.py": here / "harness" / "arms" / "extraction.py",
    }
    return {
        name: hashlib.sha256(path.read_bytes()).hexdigest()
        for name, path in sorted(files.items())
    }


def select_tiers(keys: Sequence[str] | None) -> list[ModelTier]:
    """The tiers to measure this run, in DECLARED order.

    ``None`` means every declared tier. A subset exists so a newly-added
    tier can be measured without re-spending on tiers already measured and
    without re-running the slow local ones -- but see
    :func:`_render_artifact`, which is required to announce a subset run as
    PARTIAL and name what it skipped.

    An unknown key is fatal, deliberately. Silently selecting nothing would
    produce an empty artifact that still looks like a successful run, which
    is the same "measurement that did not happen reads as fine" failure the
    NOT_RUN discipline exists to prevent.
    """
    if keys is None:
        return list(MODEL_TIERS)
    known = {t.key: t for t in MODEL_TIERS}
    unknown = [k for k in keys if k not in known]
    if unknown:
        raise SystemExit(
            f"unknown tier key(s) {unknown}; declared tiers are "
            f"{sorted(known)}. Refusing to run: a typo here would measure "
            "nothing and still exit successfully."
        )
    wanted = set(keys)
    return [t for t in MODEL_TIERS if t.key in wanted]


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


def probe_provider(config: LLMConfig, evidence: dict | None = None) -> str | None:
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
        listing = client.models.list()
    except Exception as exc:  # noqa: BLE001 - any failure IS the answer
        return (
            f"provider at {config.base_url} did not answer a models.list() "
            f"probe: {type(exc).__name__}: {exc}"
        )
    # Reachability is not enough. "The server answered" and "the server has
    # the model this tier claims to measure" are different facts, and a
    # local server will happily answer with whatever it has loaded. If the
    # endpoint enumerates models at all, the requested id must be in it.
    try:
        served_ids = {m.id for m in listing}
    except Exception:  # noqa: BLE001 - endpoint may not enumerate
        served_ids = set()
    if evidence is not None:
        evidence["listed_models"] = sorted(served_ids)
    if not served_ids:
        return (
            f"provider at {config.base_url} answered the probe but "
            "enumerated NO models, so the requested model "
            f"{config.model!r} cannot be confirmed present. Refusing to "
            "measure an unverifiable tier identity."
        )
    if not any(i == config.model or i.startswith(config.model) for i in served_ids):
        return (
            f"provider at {config.base_url} is reachable but does not list "
            f"the requested model {config.model!r}. Refusing to measure: a "
            "tier row labelled with a model the server does not have would "
            "be attributing results to the wrong model."
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
    #: None when NO provider call happened (no source material, not
    #: authorable, or a baseline arm). Renders "n/a" -- never 0.00s, which
    #: would read as "answered instantly", a measurement claim nobody made.
    latency_seconds: float | None
    #: Provider calls actually made for this oracle, including the bounded
    #: re-attempt. 0 means the arm returned before reaching a provider.
    provider_attempts: int = 0
    #: True when at least one attempt exceeded its configured window.
    #: Persisted so the timeout story is auditable from the COMMITTED
    #: artifact rather than only from a run's stderr.
    timed_out: bool = False
    #: True when a timed-out/unreachable call was recovered by the single
    #: bounded re-attempt -- the difference between "this tier failed" and
    #: "this tier needed a second try".
    recovered_after_retry: bool = False


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
        if response.outcome is not ArmOutcome.NOT_RUN:
            return response
        # TYPED, never a substring match. The previous predicate searched
        # for a marker phrase inside the reason string -- and a parse
        # failure's reason embeds up to 500 characters of MODEL-CONTROLLED
        # output, so a model that emitted the phrase could buy itself a
        # retry of a genuine quality failure. See ArmResponse.infra_failure.
        if not response.infra_failure:
            return response  # not infra -- no retry, stays loud as-is.
        reason = response.degraded_reasons[0] if response.degraded_reasons else ""
        _log(
            f"{oracle.oracle_id}: infra-level NOT_RUN ({reason!r}) -- "
            f"one bounded re-attempt (max {_MAX_INFRA_RETRIES})"
        )
        retried = arm_fn(oracle)
        timed_out = "timed out" in reason
        recovered = retried.outcome is not ArmOutcome.NOT_RUN
        if not recovered:
            _log(
                f"{oracle.oracle_id}: still NOT_RUN after bounded "
                "re-attempt -- no further retries, staying loud"
            )
        else:
            _log(f"{oracle.oracle_id}: bounded re-attempt recovered the call")
        # Stamp the infra story onto the response so _with_call_record can
        # PERSIST it into the committed artifact. Before this, the only
        # trace of a timeout-and-recovery was a stderr line that no reader
        # of the artifact ever sees.
        retried = dataclasses.replace(
            retried,
            provider_attempts=(response.provider_attempts or 1)
            + (retried.provider_attempts or 1),
        )
        object.__setattr__(retried, "timed_out_at_least_once", timed_out)
        object.__setattr__(retried, "recovered_after_retry", recovered)
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
        response = None
        try:
            response = arm_fn(oracle)
            return response
        finally:
            attempts = getattr(response, "provider_attempts", 0) if response else 0
            elapsed = time.monotonic() - started
            sink[(oracle.oracle_id, arm_name)] = CallRecord(
                called_at=called_at,
                # No provider call -> no latency. Recording the microseconds
                # the arm spent deciding it had nothing to call would render
                # as 0.00s and read as a real measurement.
                latency_seconds=elapsed if attempts else None,
                provider_attempts=attempts,
                timed_out=getattr(response, "timed_out_at_least_once", False)
                if response
                else False,
                recovered_after_retry=getattr(response, "recovered_after_retry", False)
                if response
                else False,
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
    #: What actually backs this tier's model identity: the ids the provider
    #: enumerated, and the served id read back from responses. Persisted so
    #: "this row was produced by this model" is an auditable claim rather
    #: than a label.
    identity_evidence: Mapping[str, object] = field(default_factory=dict)
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

    identity: dict[str, object] = {"requested_model": config.model}
    probe_failure = probe_provider(config, identity)
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
    served_models_seen: set[str] = set()

    def _capture_served(arm_fn):
        """Record the model the provider said it served, per answered call."""

        def wrapped(oracle):
            response = arm_fn(oracle)
            served = (response.versions or {}).get("extraction")
            if served:
                served_models_seen.add(served)
            return response

        wrapped.declared_role = arm_fn.declared_role  # type: ignore[attr-defined]
        return wrapped

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
            _capture_served(_with_bounded_infra_retry(extraction.make_answer(config))),
            call_records,
        ),
        ArmRole.CANDIDATE_ARM,
    )

    report, reports = build_comparison_report(oracles, registry, _dependencies())
    finished_at = datetime.now(timezone.utc).isoformat()
    # Which model id the provider actually reported across this tier's
    # answered calls. More than one distinct value would mean the tier was
    # served by different models mid-run, which the artifact must show.
    identity["served_models_observed"] = sorted(served_models_seen)

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
        identity_evidence=identity,
        started_at=started_at,
        finished_at=finished_at,
        oracles=tuple(oracles),
    )


# ==========================================================================
# Rendering
# ==========================================================================


def _failed_assertions(result) -> list[dict]:
    """Every assertion this oracle failed, id + detail.

    Persisted because a bare `fail` in the artifact supports no diagnosis.
    The ADR's cheapest class-(c) lead -- that models mark root-cause
    attribution `observed` where the corpus demands `inferred` -- was
    obtained from an ad-hoc probe, NOT from the committed record, which
    meant the artifact could not back the claim the document made from it.
    """
    return [
        {"assertion_id": a.assertion_id, "detail": a.detail}
        for a in result.assertions
        if not a.ok
    ]


def _records_for(outcome: TierOutcome) -> dict:
    """Raw, re-renderable evidence for one tier -- the durability fix.

    The markdown artifact was previously the ONLY output, so re-rendering
    it (after any presentation fix) required paying for a fresh sweep. That
    made presentation defects expensive to correct and tempted re-use of
    stale numbers. These records are the measurement; the markdown is a
    view of them.
    """
    tier = outcome.tier
    rows = []
    if outcome.status == "measured" and outcome.reports:
        by_arm = {
            name: {r.oracle_id: r for r in tr.results}
            for name, tr in outcome.reports.items()
        }
        for oracle in outcome.oracles:
            for arm_name, results in by_arm.items():
                result = results[oracle.oracle_id]
                record = outcome.call_records.get((oracle.oracle_id, arm_name))
                rows.append(
                    {
                        "oracle_id": oracle.oracle_id,
                        "question_class": oracle.question_class.value,
                        "arm": arm_name,
                        "verdict": result.verdict.value,
                        "failed_assertions": _failed_assertions(result),
                        "not_run_reason": _oracle_reason(result) or None,
                        "called_at": record.called_at if record else None,
                        "latency_seconds": record.latency_seconds if record else None,
                        "provider_attempts": record.provider_attempts if record else 0,
                        "timed_out": record.timed_out if record else False,
                        "recovered_after_retry": (
                            record.recovered_after_retry if record else False
                        ),
                    }
                )
    report = outcome.report
    return {
        "tier_key": tier.key,
        "label": tier.label,
        "role": tier.role,
        "primary": tier.primary,
        "optional": tier.optional,
        "requested_model": tier.model,
        "provider": tier.provider,
        "base_url": outcome.config.base_url if outcome.config else None,
        "timeout_seconds": tier.timeout,
        "prompt_version": _PROJECTION_VERSION,
        "status": outcome.status,
        "not_run_reason": outcome.not_run_reason,
        "identity_evidence": dict(outcome.identity_evidence),
        "control_status": (
            report.native_control_status().value if report is not None else None
        ),
        # The rendered per-class block is STORED, not recomputed at render
        # time. That is what makes the markdown a pure function of this
        # file: a renderer that recomputed would be a second implementation
        # able to disagree with the one that produced the measurement.
        "rendered_comparison": report.render() if report is not None else None,
        "oracles_total": len(outcome.oracles),
        "oracles_measured_by_candidate": (
            sum(1 for r in report.arm.results if r.verdict is not Verdict.NOT_MEASURED)
            if report is not None
            else 0
        ),
        "arms": list(outcome.reports.keys()) if outcome.reports else [],
        "started_at": outcome.started_at,
        "finished_at": outcome.finished_at,
        "rows": rows,
    }


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
    if record is None or record.latency_seconds is None:
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
            if record is not None and record.provider_attempts > 1:
                cell += f" [attempts={record.provider_attempts}]"
            if record is not None and record.timed_out:
                cell += (
                    " **[TIMED OUT — infra, recovered by bounded re-attempt]**"
                    if record.recovered_after_retry
                    else " **[TIMED OUT — infra, NOT recovered]**"
                )
            if result.verdict is Verdict.NOT_MEASURED:
                cell += f" ({_oracle_reason(result)})"
            elif result.verdict is Verdict.FAIL:
                # A bare `fail` supports no diagnosis. Naming the assertion
                # that failed is what lets a claim ABOUT a failure (e.g.
                # "models say observed where the corpus demands inferred")
                # be backed by the artifact instead of an ad-hoc probe.
                failed = _failed_assertions(result)
                if failed:
                    cell += " — failed: " + "; ".join(
                        f"`{f['assertion_id']}`" for f in failed
                    )
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
        "- **model identity evidence** (what backs the claim that THIS model "
        "produced these rows): provider enumerated "
        f"`{outcome.identity_evidence.get('listed_models', 'n/a')}`; "
        "model ids the provider reported as served across answered calls: "
        f"`{outcome.identity_evidence.get('served_models_observed', 'n/a')}`. "
        "A request whose response carried no model metadata, or whose "
        "served id did not match the request, was recorded NOT_RUN rather "
        "than attributed to this tier.",
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
        "Not authorable, with reason AND category. The category is what "
        "makes this list readable as evidence: only `structural` entries "
        "say something about the technique (extraction architecturally "
        "cannot answer these, or they live downstream of extraction "
        "entirely). `source_shape` means prose-ifying the data would "
        "measure the author, not the model. `deferred` is scope -- a "
        "future round could close it -- and must NOT be cited as a "
        "limitation of the technique.",
        "",
        "| Oracle | Category | Reason |",
        "|---|---|---|",
    ]
    for oracle_id in not_authorable:
        category = NOT_AUTHORABLE_CATEGORIES.get(oracle_id, "UNCATEGORIZED")
        lines.append(
            f"| `{oracle_id}` | `{category}` | `{NOT_AUTHORABLE_REASONS[oracle_id]}` |"
        )
    counts: dict[str, int] = {}
    for oracle_id in not_authorable:
        key = NOT_AUTHORABLE_CATEGORIES.get(oracle_id, "UNCATEGORIZED")
        counts[key] = counts.get(key, 0) + 1
    lines += [
        "",
        "Category totals: "
        + ", ".join(f"`{k}` {v}" for k, v in sorted(counts.items()))
        + ".",
    ]
    return "\n".join(lines)


_INTERPRETATION_NOTES: list[str] = [
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


def _build_records(
    outcomes: Sequence[TierOutcome], run_started_at: str, run_finished_at: str
) -> dict:
    """The measurement, as committed data. See render_markdown_from_records."""
    return {
        "run_started_at": run_started_at,
        "run_finished_at": run_finished_at,
        "corpus_provenance": _corpus_provenance(),
        # Serialized, not looked up at render time. A renderer that read the
        # LIVE corpus would re-render a historical run with today's coverage
        # text and the byte-equality test would bless the drift.
        "coverage_statement": _render_coverage_statement(),
        "dependency_state_class_b": CHAOS_3563_STATE,
        "declared_tiers": [t.key for t in MODEL_TIERS],
        "measured_tiers": [o.tier.key for o in outcomes],
        "tiers": [_records_for(o) for o in outcomes],
    }


def _render_artifact(
    *,
    outcomes: Sequence[TierOutcome],
    run_started_at: str,
    run_finished_at: str,
) -> str:
    """Thin shim: build the records, then render THOSE.

    Deliberately not a second renderer. A markdown path that read
    in-memory objects while the records were written alongside it
    could disagree with the committed evidence, and only one of the
    two was committed.
    """
    return render_markdown_from_records(
        _build_records(outcomes, run_started_at, run_finished_at)
    )


# ==========================================================================
# records -> markdown. The ONLY renderer.
# ==========================================================================


def _latency_cell_from_row(row: dict) -> str:
    if row.get("latency_seconds") is None:
        return "n/a"
    return f"{row['latency_seconds']:.2f}s"


def _per_oracle_table_from_records(tier: dict) -> str:
    arms = tier["arms"]
    header = "| Oracle | Class | " + " | ".join(
        f"{a} (verdict @ called / Latency)" for a in arms
    )
    header += " |"
    sep = "|---|---|" + "|".join("---" for _ in arms) + "|"
    lines = [header, sep]
    by_oracle: dict[str, dict[str, dict]] = {}
    order: list[str] = []
    for row in tier["rows"]:
        if row["oracle_id"] not in by_oracle:
            by_oracle[row["oracle_id"]] = {}
            order.append(row["oracle_id"])
        by_oracle[row["oracle_id"]][row["arm"]] = row
    for oracle_id in order:
        first = next(iter(by_oracle[oracle_id].values()))
        cells = [oracle_id, first["question_class"]]
        for arm in arms:
            row = by_oracle[oracle_id][arm]
            when = row["called_at"] or "n/a"
            cell = f"`{row['verdict']}` @ `{when}` / `{_latency_cell_from_row(row)}`"
            if row.get("provider_attempts", 0) > 1:
                cell += f" [attempts={row['provider_attempts']}]"
            if row.get("timed_out"):
                cell += (
                    " **[TIMED OUT — infra, recovered by bounded re-attempt]**"
                    if row.get("recovered_after_retry")
                    else " **[TIMED OUT — infra, NOT recovered]**"
                )
            if row["verdict"] == "not_measured" and row.get("not_run_reason"):
                cell += f" ({row['not_run_reason']})"
            elif row["verdict"] == "fail" and row.get("failed_assertions"):
                cell += " — failed: " + "; ".join(
                    f"`{f['assertion_id']}`" for f in row["failed_assertions"]
                )
            cells.append(cell)
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def _tier_section_from_records(tier: dict) -> str:
    lines = [
        f"## Tier: {tier['label']}",
        "",
        f"- tier key: `{tier['tier_key']}`",
        f"- model: `{tier['requested_model']}`",
        f"- provider: `{tier['provider']}`",
        f"- configured timeout: `{tier['timeout_seconds']}s`",
        f"- role: {tier['role']}",
    ]
    if tier["primary"]:
        lines.append(
            "- **PRIMARY SCORED TIER** -- this is the deployed "
            "configuration; read parity statements from this section."
        )
    if tier["optional"]:
        lines.append(
            "- OPTIONAL tier -- informative only; its absence is recorded, not fatal."
        )
    lines.append("")
    if tier["status"] != "measured":
        lines += [
            f"**STATUS: NOT_RUN** -- reason: {tier['not_run_reason']}",
            "",
            "No results are reported for this tier because none were "
            "produced. This section deliberately carries no comparison "
            "rows and no scores: a tier that did not run has nothing to "
            "report, and rendering an empty result set as zeroes would "
            "read as a measured zero.",
            "",
        ]
        return "\n".join(lines)
    total = tier["oracles_total"]
    measured = tier["oracles_measured_by_candidate"]
    ident = tier["identity_evidence"]
    lines += [
        "**STATUS: MEASURED**",
        "",
        f"- base URL: `{tier['base_url']}`",
        f"- prompt/schema version: `{tier['prompt_version']}`",
        f"- started: `{tier['started_at']}`",
        f"- finished: `{tier['finished_at']}`",
        f"- {total} oracles total; {measured} measured by the extraction "
        f"candidate arm; {total - measured} NOT_RUN",
        f"- class (a) control status: `{tier['control_status']}`",
        "- **model identity evidence** (what backs the claim that THIS model "
        "produced these rows): provider enumerated "
        f"`{ident.get('listed_models', 'n/a')}`; "
        "model ids the provider reported as served across answered calls: "
        f"`{ident.get('served_models_observed', 'n/a')}`. "
        "A request whose response carried no model metadata, or whose "
        "served id did not match the request, was recorded NOT_RUN rather "
        "than attributed to this tier.",
        "",
        "### Per-class comparison",
        "",
        "```",
        tier["rendered_comparison"],
        "```",
        "",
        "### Per-oracle results",
        "",
        "Every arm x oracle verdict, the wall-clock instant that arm was "
        "called, and the call's latency. Latency renders `n/a` -- never "
        "`0.00s` -- when the arm returned NOT_RUN before any provider call "
        "happened.",
        "",
        _per_oracle_table_from_records(tier),
        "",
    ]
    return "\n".join(lines)


def _matrix_summary_from_records(records: dict) -> str:
    lines = [
        "| Tier | Model | Status | Class (a) control | Candidate measured | Reason if NOT_RUN |",
        "|---|---|---|---|---|---|",
    ]
    for tier in records["tiers"]:
        if tier["status"] == "measured":
            control = f"`{tier['control_status']}`"
            measured_cell = (
                f"{tier['oracles_measured_by_candidate']}/{tier['oracles_total']}"
            )
            reason = ""
        else:
            control = "n/a"
            measured_cell = "n/a"
            reason = tier["not_run_reason"] or "unrecorded"
        lines.append(
            f"| `{tier['tier_key']}` | `{tier['requested_model']}` | "
            f"`{tier['status']}` | {control} | {measured_cell} | {reason} |"
        )
    return "\n".join(lines)


class ProvenanceMismatch(Exception):
    """The recorded corpus hashes do not match the files on disk.

    Raised rather than warned. Re-rendering a historical run against a
    changed corpus produces a document that LOOKS regenerated and is
    describing different questions than the ones that were asked -- and the
    byte-equality test would then certify the drift instead of catching it.
    """


def verify_provenance(records: dict) -> None:
    """Fail closed if the corpus has moved since these records were made."""
    recorded = records.get("corpus_provenance") or {}
    if not recorded:
        raise ProvenanceMismatch(
            "records carry no corpus provenance; refusing to render, because "
            "nothing would tie this document to the corpus it describes"
        )
    current = _corpus_provenance()
    drifted = {
        name: (digest, current.get(name))
        for name, digest in recorded.items()
        if current.get(name) != digest
    }
    if drifted:
        detail = "; ".join(
            f"{name}: recorded {rec[:12]}… now {(cur or 'MISSING')[:12]}…"
            for name, (rec, cur) in sorted(drifted.items())
        )
        raise ProvenanceMismatch(
            "corpus files have changed since these records were produced "
            f"({detail}). Re-render is refused: the measurement answered the "
            "OLD questions, so regenerating against the new ones would "
            "silently misattribute it. Re-run the sweep instead."
        )


COMPARATIVE_FACTS_BEGIN = "<!-- GENERATED:comparative-facts BEGIN -->"
COMPARATIVE_FACTS_END = "<!-- GENERATED:comparative-facts END -->"


def render_comparative_facts(records: dict) -> str:
    """Every comparative statement about tier performance, GENERATED.

    Round four of the same defect class. Hand-written comparatives kept
    contradicting the records in forms no phrase-guard anticipated -- "the
    best available tier reaches 1/4", "the frontier scores the same as the
    mid tier" (that one carries no digits at all, so every numeric guard was
    blind to it). The fix is not another guard on last round's sentence: it
    is to stop writing the sentences by hand. Prose may point at this block;
    it may not characterise what the block says.
    """
    authored = sorted(
        {
            row["oracle_id"]
            for t in records["tiers"]
            for row in t["rows"]
            if row["question_class"] == "c"
            and row["arm"] == "extraction_llm"
            and row["verdict"] != "not_measured"
        }
    )
    scores = {}
    for tier in records["tiers"]:
        scores[tier["tier_key"]] = sum(
            1
            for row in tier["rows"]
            if row["arm"] == "extraction_llm"
            and row["oracle_id"] in authored
            and row["verdict"] == "pass"
        )
    denominator = len(authored)
    best = max(scores.values())
    worst = min(scores.values())
    best_tiers = sorted(k for k, v in scores.items() if v == best)
    ranked = sorted(scores.items(), key=lambda kv: (-kv[1], kv[0]))
    lines = [
        COMPARATIVE_FACTS_BEGIN,
        "",
        "_Generated from `measured-trial-results.records.json`. Do not edit,",
        "and do not restate these figures in prose -- cite this block._",
        "",
        f"- Class-(c) authored population: **{denominator} oracles**.",
        f"- Best measured tier score: **{best}/{denominator}** "
        f"({', '.join('`' + t + '`' for t in best_tiers)}).",
        f"- Worst measured tier score: **{worst}/{denominator}**.",
        f"- Spread between best and worst: **{best - worst} oracle(s)**.",
        "",
        "| Rank | Tier | Class-(c) authored score |",
        "|---|---|---|",
    ]
    for index, (key, value) in enumerate(ranked, start=1):
        lines.append(f"| {index} | `{key}` | {value}/{denominator} |")
    lines += ["", COMPARATIVE_FACTS_END]
    return "\n".join(lines)


def render_markdown_from_records(records: dict) -> str:
    """The committed markdown, derived ENTIRELY from the committed records.

    This is what makes "records are the source of truth" true rather than
    aspirational. Previously the markdown was rendered from in-memory
    objects and the records were written alongside it, so the two could
    disagree and only one of them was committed evidence. Now the markdown
    is a view, a presentation fix costs no model spend, and
    ``test_committed_markdown_is_reproducible_from_committed_records``
    fails if the two ever drift.
    """
    verify_provenance(records)
    lines = [
        "# CHAOS-3499 measured trial results",
        "",
        "Generated by `trials/chaos_3499/run_measured_sweep.py`. This is the",
        "trial artifact -- the rendered per-class comparison, per model tier,",
        "committed. It is NOT a headline number -- there isn't one;",
        "`ComparisonReport` cannot render one by construction.",
        "",
        "**This document is rendered from "
        "`measured-trial-results.records.json`, which is the source of "
        "truth.** Every number below is reproducible from that file with no "
        "model calls; a test pins byte-equality.",
        "",
        f"- run started: `{records['run_started_at']}`",
        f"- run finished: `{records['run_finished_at']}`",
        f"- dependency state for class (b): `{records['dependency_state_class_b']}`",
        "- temperature: API default (gpt-5-family rejects a caller-selected",
        "  value -- see `harness/llm/client.py`'s module docstring and",
        "  `src/dev_health_ops/llm/providers/openai_capabilities.py`'s",
        "  `supports_temperature`)",
        "",
        "## Corpus provenance",
        "",
        "Content hashes of the files that define WHAT was measured. Two runs",
        "with identical hashes asked the same questions; a cross-run",
        "comparison that does not check these is comparing unknowns.",
        "",
        "| File | sha256 |",
        "|---|---|",
    ]
    for name, digest in sorted(records.get("corpus_provenance", {}).items()):
        lines.append(f"| `{name}` | `{digest}` |")
    lines += [
        "",
        "## Model tier matrix",
        "",
        "Every tier's model is named explicitly by `MODEL_TIERS` and passed",
        "into the arm (`extraction.make_answer(config)`); no tier resolves",
        "its model from the environment, so a section's label and the model",
        "that produced it cannot disagree.",
        "",
    ]
    measured_keys = {t["tier_key"] for t in records["tiers"]}
    omitted = [k for k in records["declared_tiers"] if k not in measured_keys]
    if omitted:
        lines += [
            "> **PARTIAL RUN — this artifact does NOT contain every declared",
            f"> tier.** {len(measured_keys)} of {len(records['declared_tiers'])}",
            "> declared tiers were measured here. The tiers NOT measured in",
            "> this run, and therefore absent below, are:",
            "",
        ]
        lines += [f"> - `{k}`" for k in omitted]
        lines += [
            "",
            "> Their results, if any, live in a different run's artifact --",
            "> do not read the table below as the whole matrix.",
            "",
        ]
    lines += [
        _matrix_summary_from_records(records),
        "",
        "## Corpus coverage",
        "",
        records["coverage_statement"],
        "",
    ]
    for tier in records["tiers"]:
        lines.append(_tier_section_from_records(tier))
        lines.append("")
    lines += _INTERPRETATION_NOTES
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--tiers",
        default=None,
        help=(
            "Comma-separated tier keys to measure. Default: every declared "
            "tier. A subset run writes a PARTIAL artifact that names the "
            "tiers it skipped -- use --artifact to keep it out of the "
            "full run's file."
        ),
    )
    parser.add_argument(
        "--artifact",
        default=None,
        help=(
            "Artifact path. Default docs/measured-trial-results.md. Point a "
            "subset run at its own file rather than overwriting a complete "
            "matrix with a partial one."
        ),
    )
    args = parser.parse_args(argv)

    tiers = select_tiers(
        [k.strip() for k in args.tiers.split(",") if k.strip()] if args.tiers else None
    )

    run_started_at = datetime.now(timezone.utc).isoformat()
    _log(
        f"matrix: measuring {len(tiers)} of {len(MODEL_TIERS)} declared "
        f"tiers ({', '.join(t.key for t in tiers)}), started {run_started_at}"
    )
    if len(tiers) < len(MODEL_TIERS):
        _log(
            "PARTIAL RUN -- the artifact will name the skipped tiers; do not "
            "read it as the whole matrix"
        )

    outcomes = [run_tier(tier) for tier in tiers]

    run_finished_at = datetime.now(timezone.utc).isoformat()
    for outcome in outcomes:
        if outcome.status == "measured" and outcome.report is not None:
            print(f"\n=== {outcome.tier.label} ===")
            print(outcome.report.render())
        else:
            print(f"\n=== {outcome.tier.label} === NOT_RUN: {outcome.not_run_reason}")

    artifact_path = (
        Path(args.artifact)
        if args.artifact
        else Path(__file__).parent / "docs" / "measured-trial-results.md"
    )
    # Records first, markdown FROM the records -- never the other way round,
    # and never two independent renderings of the same run.
    records = _build_records(outcomes, run_started_at, run_finished_at)
    records_path = artifact_path.with_suffix(".records.json")
    records_path.write_text(json.dumps(records, indent=2, sort_keys=True) + "\n")
    artifact_path.write_text(render_markdown_from_records(records))

    _log(f"artifact written: {artifact_path}")
    _log(f"raw records written: {records_path}")

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
