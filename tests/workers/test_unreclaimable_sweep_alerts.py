"""Alerts for the strand that kept sync run 115e6246 open for thirteen hours.

Three things went unwatched at once, and each has its own rule here:

* the unreclaimable sweep SELECTED all 17 stranded units every second and
  terminalized none, because no deploy shape ever set
  ``SYNC_UNRECLAIMABLE_SWEEP`` and the compiled default is ``shadow``. The
  metric's own HELP text already described that exact signature -- "non-zero in
  shadow mode means work it WOULD have terminalized" -- and nothing alerted on
  it;
* a failed sweep pass reads ZERO on the candidate gauge, identically to a
  healthy idle system, so the first alert goes quiet at the moment the safety
  net stops working. That trap needs its own rule or it silences the other one;
* the dispatcher republished into an already-dead delivery 624 times and every
  one was reported as a success.

The tests below are shaped after
``tests/workers/test_provider_artifact_skip_alert.py``: every metric name an
alert references is read out of the Go source that actually renders
``/metrics``, never retyped here. A rule written against a metric nobody emits
is an unimplemented claim that never fires -- which is precisely the failure
mode of the shadow-mode "observability" these alerts exist to replace.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[2]
RULES_PATH = ROOT / "alerts" / "rules.yml"
SYNCRECONCILER_LOOP = ROOT / "internal" / "syncreconciler" / "loop.go"
JOBOUTBOX_LOOP = ROOT / "internal" / "joboutbox" / "loop.go"

STRAND_ALERT = "SyncDispatchUnreclaimableCandidatesNotTerminalizing"
SWEEP_FAILING_ALERT = "SyncDispatchUnreclaimableSweepFailing"
REARM_ALERT = "ProviderUnitStrandRearmsClimbing"


def _rules() -> list[dict[str, Any]]:
    document = yaml.safe_load(RULES_PATH.read_text(encoding="utf-8"))
    return [
        rule
        for group in document["groups"]
        for rule in group["rules"]
        if "alert" in rule
    ]


def _alert(name: str) -> dict[str, Any]:
    matches = [rule for rule in _rules() if rule["alert"] == name]
    assert len(matches) == 1, f"expected exactly one {name} rule, got {len(matches)}"
    return matches[0]


def _write_prometheus_body(path: Path) -> str:
    """The body of this file's own ``WritePrometheus``, and nothing else.

    Scoped to that one function for the same reason the artifact-skip test
    scopes its search: a metric name that appears only in a comment, a dead
    helper, or a test double must not satisfy a pin about what the live scrape
    endpoint emits.
    """
    text = path.read_text(encoding="utf-8")
    start = re.search(r"^func \([^)]+\) WritePrometheus\(", text, re.MULTILINE)
    assert start, f"no WritePrometheus function in {path}"
    following = re.search(r"^func ", text[start.end() :], re.MULTILINE)
    end = start.end() + (following.start() if following else len(text) - start.end())
    return text[start.start() : end]


# PromQL duration literals (``[15m]``, ``[1h]``) are stripped before
# tokenizing. Without this, ``m`` and ``h`` are extracted as "metric names" and
# then trivially "found" in any Go source, because every ordinary word contains
# those letters -- the check passes whether or not the real metric exists.
# Found by codex r1 (P3) against the first version of this file.
_DURATION = re.compile(r"\[\s*\d+[smhdwy]\s*\]")

# A PromQL function or keyword looks exactly like a metric name to a regex, so
# the known ones are SUBTRACTED rather than the metric names being listed --
# listing them would make this helper agree with whatever the test expected
# instead of reporting what the rule actually reads.
_PROMQL_WORDS = frozenset(
    {
        "sum",
        "max",
        "min",
        "avg",
        "count",
        "increase",
        "rate",
        "min_over_time",
        "max_over_time",
        "avg_over_time",
        "and",
        "or",
        "unless",
        "by",
        "without",
        "on",
        "ignoring",
        "group_left",
        "group_right",
        "instance",
        "job",
    }
)


def _metric_names(expr: str) -> set[str]:
    """Every bare metric identifier in a PromQL expression.

    A metric name in this codebase always contains an underscore (the
    Prometheus naming convention every rule here follows), which is what
    finally rules out single letters, label names and bare keywords -- a
    length threshold alone would still admit ``instance``.
    """
    without_durations = _DURATION.sub(" ", expr)
    return {
        token
        for token in re.findall(r"[a-zA-Z_][a-zA-Z0-9_]*", without_durations)
        if token not in _PROMQL_WORDS and "_" in token
    }


def test_the_metric_name_parser_is_not_vacuous() -> None:
    """The parser itself, pinned -- it is the thing every check below trusts.

    Both directions matter. Extracting a duration unit as a metric name makes
    the emission check below pass on any input (codex r1, P3: ``m`` and ``h``
    appear in every Go file). Extracting too little makes it pass by finding
    nothing to check at all.
    """
    extracted = _metric_names(
        "min_over_time(some_metric_total[15m]) > 0 and "
        "sum by (instance) (increase(other_metric_total[1h])) == 0"
    )
    assert extracted == {"some_metric_total", "other_metric_total"}, extracted
    # The specific failure codex found, asserted directly as a negative rather
    # than as part of an equality that could drift.
    assert "m" not in extracted
    assert "h" not in extracted
    # A rule that reads no metric at all must be visible as an empty set, not
    # silently satisfied.
    assert _metric_names("up == 0") == set()


def test_every_sweep_alert_metric_is_actually_emitted() -> None:
    emitted = _write_prometheus_body(SYNCRECONCILER_LOOP) + _write_prometheus_body(
        JOBOUTBOX_LOOP
    )
    checked = 0
    for name in (STRAND_ALERT, SWEEP_FAILING_ALERT, REARM_ALERT):
        for metric in _metric_names(str(_alert(name)["expr"])):
            checked += 1
            # Token-safe, not a substring: a metric name that is a PREFIX of a
            # real one would otherwise pass on the longer name's strength.
            assert re.search(rf"\b{re.escape(metric)}\b", emitted), (
                f"{name} reads {metric!r}, which no WritePrometheus emits -- "
                "an alert on a metric nobody publishes never fires, which is the "
                "exact shape of the shadow-mode observability this rule replaces"
            )
    # Four distinct metric reads across the three rules. Without this the whole
    # test passes when the parser returns nothing -- the vacuous-guard class
    # this file was already caught by once.
    assert checked == 4, checked


def test_strand_alert_reads_the_pair_not_either_half() -> None:
    """A non-zero candidate gauge alone is NOT a fault.

    A healthy active sweep also shows candidates on the pass that finds work;
    what separates recovering from watching is that its terminalized counter
    MOVES. An alert on the gauge alone would page on every successful
    terminalization.
    """
    expr = str(_alert(STRAND_ALERT)["expr"])
    assert "sync_dispatch_unreclaimable_candidates" in expr
    assert "sync_dispatch_unreclaimable_terminalized_total" in expr
    assert re.search(
        r"increase\(\s*sync_dispatch_unreclaimable_terminalized_total", expr
    )
    assert "== 0" in expr, (
        "the terminalized half must assert NO movement; without it this alert "
        "fires on a sweep that is working"
    )


def test_sweep_failure_alert_is_separate_from_the_strand_alert() -> None:
    """They cannot be folded together.

    On a failed pass the candidate gauge reads zero -- identical to a healthy
    idle system -- so the strand alert is structurally unable to fire while the
    sweep is broken. That is the metric's own documented trap ("read the pair"),
    and one rule cannot express both halves.
    """
    failing = str(_alert(SWEEP_FAILING_ALERT)["expr"])
    assert "sync_dispatch_unreclaimable_sweep_failures_total" in failing
    strand = str(_alert(STRAND_ALERT)["expr"])
    assert "sync_dispatch_unreclaimable_sweep_failures_total" not in strand


def test_strand_alert_for_duration_outlasts_a_pass_in_flight() -> None:
    """`for` must exceed a plausible in-flight pass.

    The sweep cannot select a unit until it has existed for
    DefaultUnreclaimableAge (1h) with no attempt, so anything it sees is already
    an hour stuck. A further 15 minutes with nothing terminalized is not a pass
    still running -- but a shorter `for` would page on one slow pass.
    """
    alert = _alert(STRAND_ALERT)
    match = re.fullmatch(r"(\d+)([hm])", str(alert["for"]))
    assert match, f"unexpected `for` format: {alert['for']!r}"
    minutes = int(match.group(1)) * (60 if match.group(2) == "h" else 1)
    assert minutes >= 15


def test_rearm_alert_threshold_exceeds_one_units_whole_budget() -> None:
    """One broken unit must never page on its own.

    sync.provider_unit's outbox delivery budget is 5 (contracts registry
    max_attempts) and StrandRepair stops rearming a row once it is spent, so a
    single unit contributes at most 4 rearms before the sweep takes it. A
    threshold at or below that would page on one bad repository.
    """
    registry = yaml.safe_load(
        (ROOT / "contracts" / "jobs" / "v1" / "registry.json").read_text(
            encoding="utf-8"
        )
    )
    provider_unit = next(
        job for job in registry["jobs"] if job["kind"] == "sync.provider_unit"
    )
    budget = int(provider_unit["max_attempts"])

    expr = str(_alert(REARM_ALERT)["expr"])
    match = re.search(r">\s*(\d+)", expr)
    assert match, f"no threshold in {expr!r}"
    assert int(match.group(1)) > budget, (
        f"threshold {match.group(1)} does not exceed one unit's whole delivery "
        f"budget ({budget}); a single permanently broken unit would page"
    )


def test_every_new_alert_carries_routing_and_a_pointer_to_the_evidence() -> None:
    """The evidence for these three lives in LOGS, not in another metric.

    ``worker_job_outbox.attempt_count`` is a column and the reconciler exports
    per-pass deltas, so there is no delivery-attempt gauge to link to. Each
    description must therefore name the log line that carries the identifiers,
    or an operator has nowhere to go from the page.
    """
    for name, expected in (
        (STRAND_ALERT, "unreclaimable_sweep_mode_resolved"),
        (SWEEP_FAILING_ALERT, "queue-role"),
        (REARM_ALERT, "dispatch_sync_run.publish_hit_terminal_delivery"),
    ):
        alert = _alert(name)
        assert alert["labels"]["team"] == "platform"
        assert alert["labels"]["severity"] in {"warning", "critical"}
        assert expected in str(alert["annotations"]["description"]), name
