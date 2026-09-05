#!/usr/bin/env python3
"""Render the generated tables in docs/go-migration-matrix.md.

Same shape as ``scripts/gen_python_go_ledger_docs.py`` (CHAOS-4433): the
mechanical facts (which datasets/families currently exist) are read straight
from their producers below; curated columns (citation/call-site text,
route-transport prose, ticket) are hand-authored in the ``*_LEDGER`` dicts in
this file. ``_consistency_guard`` refuses to render -- raises ``SystemExit``
-- the moment a family/dataset appears in a live producer with no curated row,
or a curated row no longer matches a live producer. That is what makes
``tests/docs/test_go_migration_matrix_drift.py`` fail loudly on drift instead
of the page quietly going stale (the exact failure mode CHAOS-4433 exists to
prevent, now extended past the sync/kind/route/worker-file surface that ledger
covers to the metrics-family and workgraph/investment surface this page adds).

Section 1 (provider sync) needs NO curated ledger at all: it is rendered
directly from ``contracts/provider-matrix/v1/matrix.json``, the frozen,
CI-verified provider x dataset parity contract (CUT-08) both
``internal/providersync/capability_matrix.go`` and
``src/dev_health_ops/workers/provider_unit_route.py`` already drift-test
against (``tests/workers/test_provider_matrix_contract.py``). There is
nothing for this generator to curate there -- it would just be re-typing an
already-authoritative, already-guarded source. chris, 2026-09-04 (via
team-lead): "there's already a matrix.json" -- do not write a parallel
hand-maintained table for sync.

Sections 2/3 (daily/remaining metrics families) read ``families.json``'s
family-name sets mechanically for coverage, but the EXECUTOR VERDICT itself
comes from ``contracts/native-families/v1/native-families.json``
(``load_native_families_artifact``) -- a Go-emitted, AST-derived artifact
``cmd/dev-health-worker/native_families_artifact_test.go`` regenerates
directly from ``daily.go``'s actual registration wiring (its own
``dailyNativeFamilyRegistrations`` map assignments for §2, and the
``metrics.remaining.*`` switch's ``addRemainingWorker`` calls for §3).
Neither ``families.json``'s own ``port``/``route`` fields nor a curated
Python dict may be the executor source of truth here (team-lead's 2026-09-04
ruling, after the earlier version of this generator used exactly that and
was correctly rejected as the same "told done" drift class CHAOS-4433
exists to prevent). ``DAILY_CITATION_LEDGER``/``REMAINING_EXECUTOR_LEDGER``
below carry ONLY citation/route/ticket prose now, never the executor value.

A THIRD EXECUTOR STATE (CHAOS-5118-class fix, 2026-09-05, gate-rounds finding
via review-bench on #2230): the Go artifact only knows whether a family's
REPO-scope partition executor is native -- it says nothing about
``run_daily_metrics_finalize`` (job_daily.py), a separate, always-Python
finalize step some families still depend on for their team/finalize scope.
A family whose repo scope goes native could render bare ``NATIVE`` while
Python still computes part of it, satisfying CHAOS-3092's "zero COMPAT
rows" close condition by omission. ``load_daily_finalize_compat_families``
closes this the same mechanical way as the rest of this file: it AST-walks
``run_daily_metrics_finalize``'s real body for every call it makes, and
every call matching the per-family write/compute naming shape MUST resolve
to a live family (§2 or §3) or generation fails -- inverted from a curated
allowlist specifically so a NEW Python finalize write cannot hide by nobody
updating a dict. A family with both a native repo-scope AND a proven
finalize-scope Python call renders "NATIVE (repo) / COMPAT-Python
(finalize)"; ``is_compat_executor`` (not exact-equality) is what any
COMPAT-counting logic, including this page's own ``count_compat_*``
helpers, must test against so a split row still counts.

Section 4 (workgraph/investment) has no families.json equivalent at all
(``internal/jobs/families.json`` does not exist) -- WORKGRAPH_INVESTMENT_LEDGER
is the sole source, entirely hand-maintained, with no live producer to
drift-guard against mechanically. Adding a machine-readable registry for these
5 kinds is proposed as a follow-up ticket, not done in this change.
"""

from __future__ import annotations

import ast
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOC_PATH = ROOT / "docs" / "go-migration-matrix.md"
MATRIX_JSON = ROOT / "contracts" / "provider-matrix" / "v1" / "matrix.json"
DAILY_FAMILIES_JSON = ROOT / "internal" / "jobs" / "metrics" / "daily" / "families.json"
REMAINING_FAMILIES_JSON = (
    ROOT / "internal" / "jobs" / "metrics" / "remaining" / "families.json"
)
NATIVE_FAMILIES_ARTIFACT = (
    ROOT / "contracts" / "native-families" / "v1" / "native-families.json"
)
JOB_DAILY_PY = ROOT / "src" / "dev_health_ops" / "metrics" / "job_daily.py"

PROVIDER_BEGIN = "<!-- BEGIN GENERATED PROVIDER SYNC MATRIX -->"
PROVIDER_END = "<!-- END GENERATED PROVIDER SYNC MATRIX -->"
DAILY_BEGIN = "<!-- BEGIN GENERATED DAILY METRICS MATRIX -->"
DAILY_END = "<!-- END GENERATED DAILY METRICS MATRIX -->"
REMAINING_BEGIN = "<!-- BEGIN GENERATED REMAINING METRICS MATRIX -->"
REMAINING_END = "<!-- END GENERATED REMAINING METRICS MATRIX -->"
WORKGRAPH_BEGIN = "<!-- BEGIN GENERATED WORKGRAPH INVESTMENT MATRIX -->"
WORKGRAPH_END = "<!-- END GENERATED WORKGRAPH INVESTMENT MATRIX -->"

# matrix.json's go_executor values, translated to this page's Executor legend.
# A value not in this dict is a genuinely new state the legend hasn't seen
# yet -- render_provider_sync_block refuses rather than guessing.
GO_EXECUTOR_TRANSLATION = {
    "native_go": "NATIVE",
}


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def load_matrix_pairs() -> list[dict]:
    return _load_json(MATRIX_JSON)["pairs"]


def load_daily_families() -> list[dict]:
    return _load_json(DAILY_FAMILIES_JSON)["families"]


def load_remaining_families() -> list[dict]:
    return _load_json(REMAINING_FAMILIES_JSON)["families"]


def load_native_families_artifact() -> dict:
    """The Go-emitted, AST-derived source of truth for §2/§3 executor verdicts.

    Regenerated by cmd/dev-health-worker/native_families_artifact_test.go
    (`UPDATE_NATIVE_FAMILIES_ARTIFACT=1 go test ./cmd/dev-health-worker/... -run
    TestNativeFamiliesArtifactUpToDate`) directly from daily.go's registration
    wiring -- no curated Python dict may substitute for this per team-lead's
    2026-09-04 ruling.
    """
    return _load_json(NATIVE_FAMILIES_ARTIFACT)


def _consistency_guard(
    label: str, live: set[str], curated: set[str], hint: str
) -> None:
    missing = live - curated
    extra = curated - live
    if missing:
        raise SystemExit(
            f"gen_go_migration_matrix_docs: {label} {sorted(missing)} exist in the live "
            f"producer but have no curated row in scripts/gen_go_migration_matrix_docs.py. {hint}"
        )
    if extra:
        raise SystemExit(
            f"gen_go_migration_matrix_docs: {label} {sorted(extra)} have a curated row but no "
            f"longer exist in the live producer -- remove the stale row (or, if renamed, update "
            f"it) in scripts/gen_go_migration_matrix_docs.py. {hint}"
        )


# ---------------------------------------------------------------------------
# CURATED: §2 citation/ticket text only, internal/jobs/metrics/daily/
# families.json (24 families as of e3e2e77c48a9e4902e48d962b8292f1b408bf47b).
# The Executor verdict comes from contracts/native-families/v1/native-families.json
# (load_native_families_artifact), NOT from families.json's own `port` field --
# see module docstring.
# ---------------------------------------------------------------------------
DAILY_CITATION_LEDGER: dict[str, dict[str, str]] = {
    "repo_user_commit": {
        "citation": "Go: `internal/jobs/metrics/daily/repouser/` (`RepoUserCommitExecutor`)",
        "ticket": "CHAOS-4275 (Done)",
    },
    "team_wellbeing": {
        "citation": "Go: `internal/jobs/metrics/daily/wellbeing_native_executor.go`",
        "ticket": "CHAOS-4276 (Done)",
    },
    "file_hotspots": {
        "citation": "Go: `internal/jobs/metrics/daily/file_hotspots_native_executor.go`",
        "ticket": "CHAOS-4277 (Done)",
    },
    "file_risk_hotspots": {
        "citation": "Go: `internal/jobs/metrics/daily/` (`FileRiskHotspotsExecutor`, `daily.go`)",
        "ticket": "CHAOS-4277 (Done)",
    },
    "work_item": {
        "citation": "Go: `internal/jobs/metrics/daily/work_item_native_executor.go` -- post_bridge, reuses `internal/jobs/metrics/workitemmetrics`'s pure compute (shared with the providersync sync-time deriver); ports `compute_work_items.py:1075 compute_work_item_metrics_daily`",
        "ticket": "CHAOS-4283",
    },
    "work_item_estimate": {
        "citation": "Go: `internal/jobs/metrics/daily/work_item_estimate_native_executor.go` -- post_bridge, same shared compute; ports `compute_work_items.py:1425 compute_estimate_coverage_metrics_daily`",
        "ticket": "CHAOS-4283",
    },
    "work_item_attribution": {
        "citation": "Python: `compute_work_items.py:1189 compute_work_item_team_attributions` (full daily compute -- distinct from §3's native staleness-only backstop of the same table)",
        "ticket": "CHAOS-4283",
    },
    "work_item_state": {
        "citation": "Go: `internal/jobs/metrics/daily/work_item_state_native_executor.go` -- post_bridge, runs AFTER the Python bridge (reads `work_item_team_attributions`, itself still Python-written)",
        "ticket": "CHAOS-4278 (Done)",
    },
    "review_edges": {
        "citation": "Python: `reviews.py:22 compute_review_edges_daily`",
        "ticket": "CHAOS-4279",
    },
    "cicd": {
        "citation": "Go: `internal/jobs/metrics/daily/cicd/`",
        "ticket": "CHAOS-4292 (Done)",
    },
    "testops_pipeline": {
        "citation": "Python: `compute_testops.py:105 compute_pipeline_metrics_daily`",
        "ticket": "CHAOS-4284",
    },
    "testops_test": {
        "citation": "Python: `compute_testops.py:207 compute_test_metrics_daily`",
        "ticket": "CHAOS-4284",
    },
    "testops_coverage": {
        "citation": "Python: `compute_testops.py:355 compute_coverage_metrics_daily`",
        "ticket": "CHAOS-4284",
    },
    "deploy": {
        "citation": "Go: `internal/jobs/metrics/daily/deploy_native_executor.go`",
        "ticket": "CHAOS-4293 (Done)",
    },
    "incident": {
        "citation": "Go: `internal/jobs/metrics/daily/incident_native_executor.go` (Python bridge was permanently zero-yield for this family, CHAOS-4269)",
        "ticket": "CHAOS-4295 (Done)",
    },
    "ai_governance": {
        "citation": "Python: `audit/ai_governance/loaders.py:113 build_governance_rows_for_day`",
        "ticket": "CHAOS-4285",
    },
    "ai_impact": {
        "citation": "Python: `ai_impact.py:312 compute_ai_impact_metrics_daily`",
        "ticket": "CHAOS-4280",
    },
    "ai_workflow": {
        "citation": "Python: `work_graph/extractors/ai_workflow.py:212 _extract_ai_workflow_for_day`",
        "ticket": "CHAOS-4286",
    },
    "work_graph_edges": {
        "citation": "Python: `ai_workflow.py extract_review_deployment_incident_edges`",
        "ticket": "CHAOS-4286",
    },
    "compounding_risk": {
        # CHAOS-4287: the ticket and this citation both said :502, which is
        # inside _repo_to_team_map_for_compounding_risk, not the writer. The
        # writer is :568. The TEAM-scope half lives at :613
        # (_write_compounding_risk_team_rows_for_day, called from
        # run_daily_metrics_finalize) and is still Python -- see the family's
        # phase_note in internal/jobs/metrics/daily/families.json.
        "citation": "Python: `job_daily.py:568 _write_compounding_risk_for_day` (repo scope, now native); `job_daily.py:613 _write_compounding_risk_team_rows_for_day` (team scope, still Python)",
        "ticket": "CHAOS-4287",
    },
    "team_cognitive_load": {
        "citation": "Python: `team_cognitive_load.py build_team_cognitive_load_rows_for_day` (finalize scope)",
        "ticket": "NONE found (per `.remember/remaining-python-compute-inventory-2026-09-01.md`)",
    },
    "testops_risk": {
        "citation": "Go: `internal/jobs/metrics/daily/testops_risk_native_executor.go`, reuses `internal/jobs/metrics/testops/compute.go`'s pure compute",
        "ticket": "CHAOS-4294 (Done)",
    },
    "benchmarking": {
        "citation": "Python: `benchmarking/runner.py:259 run_benchmarking_for_day`",
        "ticket": "CHAOS-4288",
    },
    "ic_finalize": {
        "citation": "Python: `compute_ic.py` (`compute_ic_metrics_daily`, `compute_ic_landscape_rolling`; finalize scope)",
        "ticket": "CHAOS-4290",
    },
}

# ---------------------------------------------------------------------------
# CURATED: §3 citation/ticket text only, internal/jobs/metrics/remaining/
# families.json (7 families). The Executor verdict itself comes from
# contracts/native-families/v1/native-families.json (load_native_families_artifact),
# NOT from this dict and NOT from the JSON's route field -- team-lead's
# 2026-09-04 ruling: no curated dict may be the source of truth for that.
# ---------------------------------------------------------------------------
REMAINING_EXECUTOR_LEDGER: dict[str, dict[str, str]] = {
    "dora": {
        "citation": "Go: `internal/jobs/metrics/remaining/dora_native.go`, `dora_native_clickhouse.go`",
        "route": "river, native (`daily.go:586-598`)",
        "ticket": "CHAOS-3092 R1 (Done)",
    },
    "capacity": {
        "citation": "Go: `internal/jobs/metrics/remaining/capacity_native.go`, `capacity_native_clickhouse.go`",
        "route": "river, native (`daily.go:571-581`)",
        "ticket": "CUT-20 R2 (Done)",
    },
    "recommendations": {
        "citation": "Go: `internal/jobs/metrics/remaining/recommendations_native.go`",
        "route": "river, native (`daily.go:610-620`)",
        "ticket": "CHAOS-4281/CHAOS-3092 (Done)",
    },
    "membership_backfill": {
        "citation": "Go: `internal/jobs/metrics/remaining/membership_native.go`",
        "route": "river, native (`daily.go:599-609`)",
        "ticket": "CHAOS-4282 (Done)",
    },
    "work_item_attribution": {
        "citation": "Go: `internal/jobs/metrics/remaining/work_item_attribution_native.go` -- CHAOS-3092 PR-B staleness-window backstop, NOT the full daily attribution compute (that's §2's `work_item_attribution` row, still COMPAT-Python)",
        "route": "river, native (`daily.go:625-634`)",
        "ticket": "CHAOS-3092 PR-B (Done)",
    },
    "complexity": {
        "citation": "Python: `metrics_extra.py` -> `job_complexity_db.py run_complexity_db_job`",
        "route": "river, bridge (`daily.go:582-585`, uses `compatibility` directly)",
        "ticket": "CHAOS-4291",
    },
    "release_impact": {
        "citation": "Python: `job_release_impact.py` -> `release_impact.py`",
        "route": "river, bridge (`daily.go:621-624`, uses `compatibility` directly)",
        "ticket": "CHAOS-4296",
    },
}

# ---------------------------------------------------------------------------
# CURATED: §4, no families.json equivalent exists. Entirely hand-maintained;
# see docs/go-migration-matrix.md's "Known gaps" section.
# ---------------------------------------------------------------------------
WORKGRAPH_INVESTMENT_LEDGER: dict[str, dict[str, str]] = {
    "workgraph.build": {
        "executor": "COMPAT-Python (narrow native pre/post-step)",
        "citation": (
            "Go: `internal/jobs/workgraph/prestep.go` (issue-PR edge mapping, runs BEFORE the "
            "bridge) + one `poststep.go` edge type (runs AFTER); Python: `worker_workgraph.py:367 "
            'execute` (LLM categorization -- "Python owns 100% of the compute" per prestep.go\'s '
            "own doc comment)"
        ),
        "route": "bridge -- `addWorkgraphWorker`'s `KindWorkGraphBuild` case still takes the HTTP `executor`",
        "ticket": "CHAOS-4924 (six remaining sub-builders + cutover)",
    },
    "investment.materialize": {
        "executor": "NATIVE",
        "citation": (
            "Go: `internal/jobs/investment/nativeexecutor.go` (implements the same "
            "`workgraph.CompatibilityExecutor` seam the bridge did) -> `materialize.go` "
            "orchestrator -> `chquery` fetch + `materializecomponent.go` assembly + "
            "`categorize` LLM plane + `chwrite` write. Python "
            "`materialize.py:1169-1854 materialize_investments()` is retained but no longer "
            "reached from the worker path (removal is CHAOS-4767)"
        ),
        "route": "river, native -- `addWorkgraphWorker`'s `KindInvestmentMaterialize` case takes `nativeInvestment`",
        "ticket": "CHAOS-4441 (cutover landed)",
    },
    "investment.dispatch": {
        "executor": "PYTHON-ONLY (dead Go shell)",
        "citation": "Go: wired, never invoked (`internal/jobs/workgraph/handler.go`); Python: Celery-only target, itself unreachable",
        "route": "bridge (dead in both directions)",
        "ticket": "CHAOS-4438 (dead-code removal, Backlog)",
    },
    "investment.chunk": {
        "executor": "PYTHON-ONLY (dead Go shell)",
        "citation": "Go: wired, never invoked (`internal/jobs/workgraph/handler.go`); Python: Celery-only target, itself unreachable",
        "route": "bridge (dead in both directions)",
        "ticket": "CHAOS-4438 (dead-code removal, Backlog)",
    },
    "investment.finalize": {
        "executor": "PYTHON-ONLY (dead Go shell)",
        "citation": "Go: wired, never invoked (`internal/jobs/workgraph/handler.go`); Python: Celery-only target, itself unreachable",
        "route": "bridge (dead in both directions)",
        "ticket": "CHAOS-4438 (dead-code removal, Backlog)",
    },
    "recommendations": {
        "executor": "NATIVE",
        "citation": "see §3",
        "route": "river, native",
        "ticket": "CHAOS-4281/CHAOS-3092 (Done)",
    },
    "DORA": {
        "executor": "NATIVE",
        "citation": "see §3",
        "route": "river, native",
        "ticket": "CHAOS-3092 R1 (Done)",
    },
    "cognitive load (team_cognitive_load)": {
        "executor": "COMPAT-Python",
        "citation": "see §2",
        "route": "bridge",
        "ticket": "NONE found",
    },
}


def render_provider_sync_block() -> str:
    pairs = load_matrix_pairs()
    unknown_executors = {p["go_executor"] for p in pairs} - set(GO_EXECUTOR_TRANSLATION)
    if unknown_executors:
        raise SystemExit(
            f"gen_go_migration_matrix_docs: matrix.json has go_executor value(s) "
            f"{sorted(unknown_executors)} not in GO_EXECUTOR_TRANSLATION -- add a legend mapping "
            "for it in scripts/gen_go_migration_matrix_docs.py before rendering."
        )
    lines = [
        PROVIDER_BEGIN,
        "| Provider | Dataset | Executor | Route destinations (tables written) | Route ready | Plannable |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for pair in sorted(pairs, key=lambda p: (p["provider"], p["dataset"])):
        executor = GO_EXECUTOR_TRANSLATION[pair["go_executor"]]
        destinations = ", ".join(f"`{d}`" for d in pair["route_destinations"]) or "--"
        lines.append(
            f"| {pair['provider']} | `{pair['dataset']}` | {executor} | {destinations} | "
            f"{pair['route_ready']} | {pair['plannable']} |"
        )
    lines.append(PROVIDER_END)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# THIRD STATE (CHAOS-5118-class fix, gate-rounds finding via review-bench on
# #2230): a family's Executor verdict above comes from ONE Go-AST fact --
# whether daily.go registers a native repo-scope partition executor for it.
# But `run_daily_metrics_finalize` (job_daily.py) is a SEPARATE, always-Python
# finalize step that some families still depend on for their team/finalize
# SCOPE, independent of whether their repo scope has gone native. A family
# whose repo scope goes native renders bare "NATIVE" with no way to see that
# finalize still computes part of it in Python -- CHAOS-3092's "zero COMPAT
# rows" close condition is then satisfiable while Python still computes half
# a family.
#
# THE FIX, INVERTED (team-lead's ruling, 2026-09-05): rather than a curated
# dict asserting WHICH families still have a Python finalize remainder (an
# allowlist a NEW Python addition could silently miss), the AST-derived set
# of every call inside run_daily_metrics_finalize's body is the source of
# truth, and EVERY in-scope call (one matching the per-family write/compute
# naming shape below) MUST resolve to a live family -- daily (§2) or
# remaining (§3), since a finalize call can belong to either. An unresolved
# in-scope call is a hard SystemExit: a new Python finalize write appeared
# and nothing here was taught about it. This is the same completeness
# direction _consistency_guard already uses elsewhere in this file, applied
# to a function body instead of a JSON family list.
# ---------------------------------------------------------------------------
DAILY_FINALIZE_FN = "run_daily_metrics_finalize"

# Finalize calls that do NOT embed their family name in the
# `_write_<family>_..._for_day` shape _finalize_call_family checks for by
# convention. Kept as small as possible on purpose -- this dict only NAMES
# an irregular call; it never asserts one exists (the AST walk does that).
# Values are (namespace, family) -- "daily" or "remaining" -- not a bare
# family name: a name shared by both family sets (e.g. work_item_attribution)
# is otherwise ambiguous, and an irregular call's name carries no naming-
# convention clue to resolve that itself (round-1 finding F4/codex).
#
# "_write_team_complexity_for_day" maps to §3's "complexity" family, not a
# §2 daily family: it reads back `repo_complexity_daily` (complexity's own
# output table) and writes a team-scope rollup, the same
# CHAOS-4365-item-3 finalize-step shape compounding_risk's team rows follow.
# Before this fix it had ZERO representation anywhere in this doc -- not
# hidden behind NATIVE (complexity already renders COMPAT-Python correctly
# today), just never mentioned on either axis. Found by this exact
# completeness check, which is the point of building it this way.
FINALIZE_CALL_IRREGULAR_FAMILY: dict[str, tuple[str, str]] = {
    "compute_ic_metrics_daily": ("daily", "ic_finalize"),
    "compute_ic_landscape_rolling": ("daily", "ic_finalize"),
    "_write_team_complexity_for_day": ("remaining", "complexity"),
}


def load_finalize_write_calls() -> set[str]:
    """Mechanical fact, not hand-typed: every call name inside
    run_daily_metrics_finalize's body in job_daily.py, read fresh from the
    file's AST every run. This is what lets the completeness check below
    catch a NEW Python finalize write nobody taught this generator about,
    not just a stale citation for one it already knew existed.

    Raises SystemExit if the body contains a call whose target is NOT a
    plain name or attribute access (e.g. `getattr(obj, "name")(...)`, a
    call returned by another call, a subscript, ...) -- round-1 finding F1
    (codex, CHAOS-5118): the walker cannot read a call name off an
    indirect/dynamic dispatch shape, so it used to silently skip it,
    meaning a finalize write hidden behind one evaded the completeness
    check entirely -- the exact "cannot hide by omission" guarantee this
    whole mechanism exists for, failing on the one call shape it couldn't
    read. Failing closed on any such shape (there are none in the function
    today) means a future rewrite to dynamic dispatch requires a human to
    update this function, rather than silently losing coverage.
    """
    tree = ast.parse(JOB_DAILY_PY.read_text(encoding="utf-8"))
    target = next(
        (
            node
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == DAILY_FINALIZE_FN
        ),
        None,
    )
    if target is None:
        raise SystemExit(
            f"gen_go_migration_matrix_docs: {DAILY_FINALIZE_FN} not found in "
            f"{JOB_DAILY_PY} -- function renamed/moved? Update load_finalize_write_calls "
            "and DAILY_FINALIZE_FN."
        )
    calls: set[str] = set()
    opaque: list[str] = []
    for node in ast.walk(target):
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Name):
                calls.add(func.id)
            elif isinstance(func, ast.Attribute):
                calls.add(func.attr)
            else:
                opaque.append(ast.dump(func))
    if opaque:
        raise SystemExit(
            f"gen_go_migration_matrix_docs: {DAILY_FINALIZE_FN} in {JOB_DAILY_PY} "
            f"contains {len(opaque)} call(s) whose target this AST walker cannot read "
            f"as a plain name (e.g. dynamic dispatch via getattr/a returned callable): "
            f"{opaque}. The completeness check cannot see what these calls write -- "
            "resolve manually and either rewrite them as a plain name/attribute call "
            "or teach load_finalize_write_calls to look inside this specific shape."
        )
    return calls


def _irregular_mapping_plausible(call_name: str, family: str) -> bool:
    """A minimal, deliberately weak SEMANTIC sanity check for an irregular-
    ledger mapping: at least one word (underscore-split, length >= 2) of the
    target family name must appear literally in the call name.

    This CANNOT prove a mapping is correct -- that is exactly why an
    irregular name needs a human to name it at all, since by definition it
    does not follow the naming convention the mechanical matcher otherwise
    relies on. What it DOES catch: a copy/paste or rename that points a
    real call at a family sharing no textual relationship with it whatsoever
    (round-1 finding F3, codex, CHAOS-5118: `_write_team_complexity_for_day`
    silently accepted a mapping to `release_impact`, a live family with zero
    connection to the call's own name -- `complexity`, the correct target,
    IS a substring of the call name and would have passed this check).
    """
    tokens = [t for t in family.split("_") if len(t) >= 2]
    if not tokens:
        return True
    return any(token in call_name for token in tokens)


def _finalize_call_family(
    call_name: str, daily_names: set[str], remaining_names: set[str]
) -> tuple[str, str] | None:
    """Maps one finalize call name to the (namespace, family) -- namespace
    is "daily" (§2) or "remaining" (§3) -- it writes/computes for.

    Returns None for a call OUT OF SCOPE of this check: generic
    infrastructure (loaders, sinks, resolvers, stdlib/dataclass calls) that
    is not a per-family write/compute at all, identified by NOT matching the
    `_write_*_for_day` naming convention and not being in the irregular-name
    ledger. Most of run_daily_metrics_finalize's calls are this shape --
    the completeness check below does not require every call to resolve,
    only every IN-SCOPE one.

    Raises SystemExit for an in-scope call that: names no live family in
    either namespace (a new finalize write appeared, or an existing one was
    renamed, and nothing here was updated to match); matches a family name
    that exists in BOTH namespaces via the naming convention (round-1
    finding F4, codex: e.g. work_item_attribution names both a full daily
    compute and a narrower remaining staleness backstop -- genuinely
    ambiguous from the call name alone, must be resolved by a human adding
    an explicit FINALIZE_CALL_IRREGULAR_FAMILY entry stating which one);
    or is an irregular-ledger mapping that fails the plausibility check.
    """
    if call_name in FINALIZE_CALL_IRREGULAR_FAMILY:
        namespace, family = FINALIZE_CALL_IRREGULAR_FAMILY[call_name]
        live = daily_names if namespace == "daily" else remaining_names
        if namespace not in ("daily", "remaining") or family not in live:
            raise SystemExit(
                f"gen_go_migration_matrix_docs: FINALIZE_CALL_IRREGULAR_FAMILY maps "
                f"{call_name!r} to ({namespace!r}, {family!r}), which is not a live "
                f"{namespace} family name -- family renamed/removed, or namespace typo? "
                "Update the ledger."
            )
        if not _irregular_mapping_plausible(call_name, family):
            raise SystemExit(
                f"gen_go_migration_matrix_docs: FINALIZE_CALL_IRREGULAR_FAMILY maps "
                f"{call_name!r} to {family!r}, but no word of {family!r} appears in the "
                "call name -- likely a copy/paste or rename error. Re-verify this "
                "mapping against the actual function body before trusting it."
            )
        return namespace, family
    if call_name.startswith("_write_") and call_name.endswith("_for_day"):
        middle = call_name[len("_write_") : -len("_for_day")]

        def all_matches(names: set[str]) -> list[str]:
            return sorted(
                family
                for family in names
                if middle == family or middle.startswith(family + "_")
            )

        daily_matches = all_matches(daily_names)
        remaining_matches = all_matches(remaining_names)
        # Round-1 finding F2 (codex, CHAOS-5118): a prefix match is not
        # unique just because a longest-match rule can always PICK one --
        # "work_item" and "work_item_state" can both textually match
        # "_write_work_item_state_details_for_day". Detecting the tie and
        # failing closed (rather than deterministically resolving it) is
        # what actually closes the ambiguity, in EITHER namespace alone,
        # not just across the two namespaces (F4's narrower case, still
        # checked below).
        if len(daily_matches) > 1:
            raise SystemExit(
                f"gen_go_migration_matrix_docs: finalize call {call_name!r} matches "
                f"MULTIPLE daily families via the naming convention: {daily_matches} -- "
                "ambiguous, cannot resolve mechanically. Add an explicit "
                "FINALIZE_CALL_IRREGULAR_FAMILY entry naming the intended one."
            )
        if len(remaining_matches) > 1:
            raise SystemExit(
                f"gen_go_migration_matrix_docs: finalize call {call_name!r} matches "
                f"MULTIPLE remaining families via the naming convention: "
                f"{remaining_matches} -- ambiguous, cannot resolve mechanically. Add an "
                "explicit FINALIZE_CALL_IRREGULAR_FAMILY entry naming the intended one."
            )
        daily_match = daily_matches[0] if daily_matches else None
        remaining_match = remaining_matches[0] if remaining_matches else None
        if daily_match and remaining_match:
            raise SystemExit(
                f"gen_go_migration_matrix_docs: finalize call {call_name!r} matches "
                f"family {daily_match!r} via the naming convention in BOTH the daily "
                "and remaining family sets -- ambiguous namespace, cannot resolve "
                "mechanically. Add an explicit FINALIZE_CALL_IRREGULAR_FAMILY entry "
                "naming which section this call actually belongs to."
            )
        if daily_match:
            return "daily", daily_match
        if remaining_match:
            return "remaining", remaining_match
        raise SystemExit(
            f"gen_go_migration_matrix_docs: finalize call {call_name!r} in "
            f"{DAILY_FINALIZE_FN} matches the `_write_<family>_..._for_day` naming "
            "convention but names no live daily or remaining family -- a new Python "
            "finalize write was added with no matching family/ticket. Add the family "
            "if it's genuinely new, or map this call in FINALIZE_CALL_IRREGULAR_FAMILY "
            "if the name is just irregular."
        )
    return None


def load_daily_finalize_compat_families(
    daily_names: set[str], remaining_names: set[str]
) -> set[tuple[str, str]]:
    """Every live (namespace, family) -- §2 daily or §3 remaining -- with a
    still-Python finalize-scope write, proven by an ACTUAL call inside
    run_daily_metrics_finalize's body -- never asserted by prose alone, and
    never losing which section a shared family name belongs to (round-1
    finding F4). Completeness runs in both directions: every in-scope call
    must resolve to exactly one (namespace, family) (checked here, fails
    generation otherwise -- see _finalize_call_family), and every
    FINALIZE_CALL_IRREGULAR_FAMILY entry must name a call that is actually
    present (also checked here, via the AST-derived call set itself being
    the only thing iterated -- a ledger entry for an absent call is simply
    never visited, so a SEPARATE guard is needed for that direction; see
    _assert_no_stale_finalize_ledger_entries)."""
    compat: set[tuple[str, str]] = set()
    for call_name in sorted(load_finalize_write_calls()):
        resolved = _finalize_call_family(call_name, daily_names, remaining_names)
        if resolved is not None:
            compat.add(resolved)
    return compat


def _assert_no_stale_finalize_ledger_entries(
    daily_names: set[str], remaining_names: set[str]
) -> None:
    """The OTHER completeness direction: every FINALIZE_CALL_IRREGULAR_FAMILY
    entry must name a call that is actually present in
    run_daily_metrics_finalize's body right now, not one that used to be
    there. load_daily_finalize_compat_families only iterates calls that ARE
    present, so a renamed/removed irregular call would otherwise just
    silently stop contributing its family to the compat set -- the doc
    would go quiet instead of failing loudly."""
    present_calls = load_finalize_write_calls()
    stale = set(FINALIZE_CALL_IRREGULAR_FAMILY) - present_calls
    if stale:
        raise SystemExit(
            f"gen_go_migration_matrix_docs: FINALIZE_CALL_IRREGULAR_FAMILY names call(s) "
            f"{sorted(stale)} that no longer appear in {DAILY_FINALIZE_FN}'s body -- "
            "renamed or removed? Update or drop the ledger entry."
        )
    for call_name, (namespace, family) in FINALIZE_CALL_IRREGULAR_FAMILY.items():
        live = daily_names if namespace == "daily" else remaining_names
        if namespace not in ("daily", "remaining") or family not in live:
            raise SystemExit(
                f"gen_go_migration_matrix_docs: FINALIZE_CALL_IRREGULAR_FAMILY maps "
                f"{call_name!r} to ({namespace!r}, {family!r}), which is not a live "
                f"{namespace} family name."
            )


def is_compat_executor(executor: str) -> bool:
    """True for any rendered Executor status with a Python-compat
    component -- covers plain "COMPAT-Python" AND a split
    "NATIVE (repo) / COMPAT-Python (finalize)" row alike. CHAOS-3092's
    "zero COMPAT rows" close condition MUST use this predicate, not an
    exact-equality check against "COMPAT-Python" -- an equality check is
    exactly the kind of check a partial-native split row passes by looking
    done, which is the defect this whole section exists to close."""
    return "COMPAT-Python" in executor


def daily_family_executor(
    name: str, artifact_daily: dict[str, str], finalize_compat: set[tuple[str, str]]
) -> str:
    artifact_value = artifact_daily.get(name, "compat")
    executor = "COMPAT-Python" if artifact_value == "compat" else "NATIVE"
    if artifact_value == "post_bridge":
        executor += ", post_bridge"
    if ("daily", name) in finalize_compat and artifact_value != "compat":
        executor = f"{executor} (repo) / COMPAT-Python (finalize)"
    return executor


def remaining_family_executor(
    name: str, artifact_remaining: dict[str, str], finalize_compat: set[tuple[str, str]]
) -> str:
    executor = "NATIVE" if artifact_remaining[name] == "native" else "COMPAT-Python"
    if name == "work_item_attribution":
        executor += " (narrow: staleness backstop only)"
    if ("remaining", name) in finalize_compat and artifact_remaining[name] == "native":
        executor = f"{executor} (repo) / COMPAT-Python (finalize)"
    return executor


def count_compat_daily_families() -> int:
    """Number of §2 daily families whose rendered Executor status is
    Python-compat in any part (see is_compat_executor) -- the mechanical
    count CHAOS-3092's "zero COMPAT rows" close condition must read for §2,
    since a partial-native split row must still count."""
    families = load_daily_families()
    live_names = {f["name"] for f in families}
    remaining_names = {f["name"] for f in load_remaining_families()}
    artifact_daily = load_native_families_artifact()["daily"]
    finalize_compat_families = load_daily_finalize_compat_families(
        live_names, remaining_names
    )
    return sum(
        1
        for f in families
        if is_compat_executor(
            daily_family_executor(f["name"], artifact_daily, finalize_compat_families)
        )
    )


def count_compat_remaining_families() -> int:
    """Same as count_compat_daily_families, for §3 remaining families."""
    families = load_remaining_families()
    live_names = {f["name"] for f in load_daily_families()}
    remaining_names = {f["name"] for f in families}
    artifact_remaining = load_native_families_artifact()["remaining"]
    finalize_compat_families = load_daily_finalize_compat_families(
        live_names, remaining_names
    )
    return sum(
        1
        for f in families
        if is_compat_executor(
            remaining_family_executor(
                f["name"], artifact_remaining, finalize_compat_families
            )
        )
    )


def render_daily_metrics_block() -> str:
    families = load_daily_families()
    live_names = {f["name"] for f in families}
    _consistency_guard(
        "daily metrics family(ies)",
        live_names,
        set(DAILY_CITATION_LEDGER),
        "Add a DAILY_CITATION_LEDGER row for it.",
    )
    artifact_daily = load_native_families_artifact()["daily"]
    unknown_artifact_names = set(artifact_daily) - live_names
    if unknown_artifact_names:
        raise SystemExit(
            f"gen_go_migration_matrix_docs: contracts/native-families/v1/native-families.json's "
            f'"daily" section names family(ies) {sorted(unknown_artifact_names)} that no longer '
            "exist in internal/jobs/metrics/daily/families.json -- regenerate the artifact "
            "(UPDATE_NATIVE_FAMILIES_ARTIFACT=1 go test ./cmd/dev-health-worker/... -run "
            "TestNativeFamiliesArtifactUpToDate)."
        )
    remaining_names = {f["name"] for f in load_remaining_families()}
    _assert_no_stale_finalize_ledger_entries(live_names, remaining_names)
    finalize_compat_families = load_daily_finalize_compat_families(
        live_names, remaining_names
    )
    lines = [
        DAILY_BEGIN,
        "| Family | Executor | Citation | Ticket |",
        "| --- | --- | --- | --- |",
    ]
    for family in sorted(families, key=lambda f: f["name"]):
        name = family["name"]
        executor = daily_family_executor(name, artifact_daily, finalize_compat_families)
        row = DAILY_CITATION_LEDGER[name]
        lines.append(f"| {name} | {executor} | {row['citation']} | {row['ticket']} |")
    lines.append(DAILY_END)
    return "\n".join(lines)


def render_remaining_metrics_block() -> str:
    families = load_remaining_families()
    live_names = {f["name"] for f in families}
    _consistency_guard(
        "remaining metrics family(ies)",
        live_names,
        set(REMAINING_EXECUTOR_LEDGER),
        "Add a REMAINING_EXECUTOR_LEDGER row for it (citation/ticket text; the Executor verdict "
        "comes from contracts/native-families/v1/native-families.json, not this dict).",
    )
    artifact_remaining = load_native_families_artifact()["remaining"]
    _consistency_guard(
        "remaining metrics family(ies) in native-families.json",
        set(artifact_remaining),
        live_names,
        "Regenerate the artifact (UPDATE_NATIVE_FAMILIES_ARTIFACT=1 go test "
        "./cmd/dev-health-worker/... -run TestNativeFamiliesArtifactUpToDate).",
    )
    daily_names = {f["name"] for f in load_daily_families()}
    finalize_compat_families = load_daily_finalize_compat_families(
        daily_names, live_names
    )
    lines = [
        REMAINING_BEGIN,
        "| Family | Executor | Citation | Route transport | Ticket |",
        "| --- | --- | --- | --- | --- |",
    ]
    for name in sorted(REMAINING_EXECUTOR_LEDGER):
        executor = remaining_family_executor(
            name, artifact_remaining, finalize_compat_families
        )
        row = REMAINING_EXECUTOR_LEDGER[name]
        lines.append(
            f"| {name} | {executor} | {row['citation']} | {row['route']} | {row['ticket']} |"
        )
    lines.append(REMAINING_END)
    return "\n".join(lines)


# Kinds in the ledger that the native-families artifact also records, and the
# ledger executor prefix each artifact label must agree with. The artifact is
# AST-derived from addWorkgraphWorker's dispatch switch, so it -- not this
# curated dict -- is the authority on native-vs-compat; the ledger keeps the
# prose (citation/route/ticket) the artifact has no room for.
_ARTIFACT_LABEL_TO_LEDGER_PREFIX: dict[str, tuple[str, ...]] = {
    "native": ("NATIVE",),
    "compat": ("COMPAT-Python", "PYTHON-ONLY"),
}


def _assert_ledger_matches_artifact() -> None:
    """Fail loudly when the curated §4 verdict disagrees with the wiring.

    This guard exists because CHAOS-4441 sat marked Done for a day while every
    workgraph/investment kind still dispatched to the Python bridge, and NO
    artifact in the tree could contradict the claim. The artifact now records
    the dispatch switch, so §4's executor column can be checked rather than
    trusted.
    """
    artifact = load_native_families_artifact().get("workgraph")
    if not artifact:
        raise SystemExit(
            "gen_go_migration_matrix_docs: contracts/native-families/v1/native-families.json "
            "has no `workgraph` section -- regenerate it with "
            "UPDATE_NATIVE_FAMILIES_ARTIFACT=1 go test ./cmd/dev-health-worker/... "
            "-run TestNativeFamiliesArtifactUpToDate"
        )
    for kind, label in sorted(artifact.items()):
        row = WORKGRAPH_INVESTMENT_LEDGER.get(kind)
        if row is None:
            raise SystemExit(
                f"gen_go_migration_matrix_docs: native-families artifact records kind {kind!r} "
                f"but WORKGRAPH_INVESTMENT_LEDGER has no entry for it -- add one."
            )
        expected = _ARTIFACT_LABEL_TO_LEDGER_PREFIX[label]
        if not row["executor"].startswith(expected):
            raise SystemExit(
                f"gen_go_migration_matrix_docs: kind {kind!r} is wired as {label!r} in "
                f"addWorkgraphWorker (per the native-families artifact) but the curated ledger "
                f"says {row['executor']!r}. The WIRING is authoritative -- fix the ledger."
            )


def render_workgraph_investment_block() -> str:
    _assert_ledger_matches_artifact()
    lines = [
        WORKGRAPH_BEGIN,
        "| Kind/area | Executor | Citation | Route transport | Ticket |",
        "| --- | --- | --- | --- | --- |",
    ]
    for name in sorted(WORKGRAPH_INVESTMENT_LEDGER):
        row = WORKGRAPH_INVESTMENT_LEDGER[name]
        lines.append(
            f"| {name} | {row['executor']} | {row['citation']} | {row['route']} | {row['ticket']} |"
        )
    lines.append(WORKGRAPH_END)
    return "\n".join(lines)


def _replace_block(
    doc: str, begin: str, end: str, rendered: str, doc_path: Path
) -> str:
    start = doc.find(begin)
    stop = doc.find(end)
    if start == -1 or stop == -1 or stop < start:
        raise SystemExit(
            f"gen_go_migration_matrix_docs: markers {begin}/{end} not found in {doc_path}"
        )
    stop += len(end)
    return f"{doc[:start]}{rendered}{doc[stop:]}"


def update_doc() -> None:
    doc = DOC_PATH.read_text(encoding="utf-8")
    doc = _replace_block(
        doc, PROVIDER_BEGIN, PROVIDER_END, render_provider_sync_block(), DOC_PATH
    )
    doc = _replace_block(
        doc, DAILY_BEGIN, DAILY_END, render_daily_metrics_block(), DOC_PATH
    )
    doc = _replace_block(
        doc, REMAINING_BEGIN, REMAINING_END, render_remaining_metrics_block(), DOC_PATH
    )
    doc = _replace_block(
        doc,
        WORKGRAPH_BEGIN,
        WORKGRAPH_END,
        render_workgraph_investment_block(),
        DOC_PATH,
    )
    DOC_PATH.write_text(doc, encoding="utf-8")


if __name__ == "__main__":
    update_doc()
