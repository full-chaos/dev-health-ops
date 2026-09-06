"""CHAOS-3092 close condition 3: structural guard against a deleted family's
compute call reappearing in job_daily.py.

Chris's ruling (verbatim, twice, 2026-09-05): "work_item_attribution python
doesn't need a skip, it just needs to be deleted" / "once go is in main that
does the same thing, skip flags are pointless." The standing rule going
forward for CHAOS-3092: for every family NATIVE on main, DELETE its Python
compute call from job_daily.py -- never add or extend a skip_families gate.

DELETED_NATIVE_FAMILY_COMPUTE_FUNCTIONS below is the deletion ledger: one
entry per family whose job_daily.py compute call has already been removed
(CHAOS-5233 is the first entry, work_item_attribution). It is expected to
GROW, one deletion PR at a time, as CHAOS-3092's audit (CHAOS-5234) works
through the rest of the native family list -- this is NOT yet a blanket
"no native family may call its compute function from job_daily.py" check,
because most families have not been migrated from skip-gating to deletion
yet. Widen this set as each subsequent deletion PR lands; do not remove
entries once added.

AST-BASED, not line/substring-based (fix for codex r1 Finding 2 on #2283,
CHAOS-5240): the original guard stripped only WHOLE-LINE comments
(`line.strip().startswith("#")`) before a plain substring search, which has
two independent holes codex constructed directly: (1) an INLINE trailing
comment (`sentinel = 1  # build_governance_rows_for_day`) is not a
whole-line comment, so its text survives the strip and can false-positive
the guard; (2) a real DYNAMIC reference split across adjacent string
literals (`getattr(job_daily, "build_" "governance_rows_for_day")`) does
not contain the function name as a contiguous substring in the source text,
so it can slip past a substring search entirely -- a false negative on an
actual live reference. Parsing with `ast` instead of scanning text closes
both: comments never appear in the AST at all (whole-line or inline, no
difference), and adjacent string literals are folded into a single
`ast.Constant` by the parser itself before this test ever sees them.
"""

from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
JOB_DAILY_SOURCE = ROOT / "src" / "dev_health_ops" / "metrics" / "job_daily.py"

# family name -> the compute function job_daily.py must no longer reference
# at all (no import, no call) once that family's deletion PR has landed.
DELETED_NATIVE_FAMILY_COMPUTE_FUNCTIONS = {
    # CHAOS-5233: work_item_attribution's daily compute deleted from
    # job_daily.py -- the native Go executor (WorkItemAttributionExecutor,
    # #2246/CHAOS-5078) is the only writer of work_item_team_attributions
    # for a daily partition now. compute_work_item_team_attributions itself
    # is NOT deleted from the codebase (job_work_items.py's
    # run_work_items_sync_job still calls it for an unrelated full-backfill
    # sync job), only job_daily.py's own reference to it.
    "work_item_attribution": "compute_work_item_team_attributions",
    # CHAOS-5234: ai_governance's daily compute deleted from job_daily.py --
    # the native Go executor (AIGovernanceExecutor, CHAOS-4285) is the only
    # writer of ai_policy_events/ai_governance_coverage_daily for a daily
    # partition now. Unlike work_item_attribution, build_governance_rows_
    # for_day itself was ALSO deleted (from audit/ai_governance/loaders.py)
    # -- codegraph_explore + rg confirmed job_daily.py was its only real
    # caller.
    "ai_governance": "build_governance_rows_for_day",
    # CHAOS-5234: file_hotspots's daily compute deleted from job_daily.py --
    # the native Go executor (FileHotspotsExecutor, CHAOS-4277) is the only
    # writer of file_metrics_daily for a daily partition now. Unlike
    # work_item_attribution above, compute_file_hotspots itself IS ALSO
    # deleted from the codebase (src/dev_health_ops/metrics/hotspots.py,
    # removed whole-file): its only other callers were golden-fixture
    # generators and unit tests, never a real production caller -- a
    # correction to an earlier pass on this same family, which left the
    # function in place on that flawed premise (see this PR's own body).
    "file_hotspots": "compute_file_hotspots",
    # CHAOS-5234: file_risk_hotspots's daily compute deleted from
    # job_daily.py -- the native Go executor (FileRiskHotspotsExecutor,
    # CHAOS-4277) is the only writer of file_hotspot_daily for a daily
    # partition now. compute_file_risk_hotspots itself is ALSO deleted from
    # the codebase (same file, same reasoning as file_hotspots above), along
    # with the three private job_daily.py helpers it used
    # (_hotspot_repo_ids/_load_complexity_map_for_repo/
    # _load_blame_map_for_repo) and every golden-fixture generator/unit test
    # that existed only to exercise these two families.
    "file_risk_hotspots": "compute_file_risk_hotspots",
    # CHAOS-5234: ai_impact's daily compute deleted from job_daily.py -- the
    # native Go executor (AIImpactExecutor, CHAOS-4280) is the only writer
    # of ai_impact_metrics_daily for a daily partition now. Unlike
    # work_item_attribution, compute_ai_impact_metrics_daily itself is ALSO
    # deleted (from metrics/ai_impact.py) -- codegraph_explore + rg confirmed
    # its only real callers, once job_daily.py's own reference was removed,
    # were its Go bit-exact oracle rot guard
    # (TestAIImpactMatchesLivePythonProduction +
    # testdata/python_ai_impact_oracle.py, both also deleted in this PR) and
    # its own dedicated tests (also deleted). AttributionBucket/AI_BUCKETS
    # (the same module) are NOT touched -- they have real, separate callers
    # (the GraphQL API resolver and the opportunities detector).
    "ai_impact": "compute_ai_impact_metrics_daily",
    # CHAOS-5234/CHAOS-3092: team_wellbeing's daily compute deleted from
    # job_daily.py -- the native Go executor (TeamWellbeingExecutor,
    # CHAOS-4276) is the only writer of team_metrics_daily for a daily
    # partition now. compute_team_wellbeing_metrics_daily itself is ALSO
    # deleted (the whole metrics/compute_wellbeing.py module) -- its only
    # other callers were its Go rot guard (TestTeamWellbeingGoldenMatches
    # LivePython + generate_daily_wellbeing_python_golden.py, both also
    # deleted in this PR) and its own dedicated test file (also deleted).
    "team_wellbeing": "compute_team_wellbeing_metrics_daily",
    # CHAOS-5234/CHAOS-3092: cicd's daily compute deleted from job_daily.py
    # -- the native Go executor (CICDExecutor, CHAOS-4292) is the only
    # writer of cicd_metrics_daily for a daily partition now.
    # compute_cicd_metrics_daily itself is ALSO deleted (the whole
    # metrics/compute_cicd.py module) -- its only other callers were its Go
    # rot guard (TestCICDGoldenMatchesLivePython +
    # generate_daily_cicd_python_golden.py, both also deleted in this PR)
    # and its own dedicated/shared tests (deleted or surgically trimmed).
    # pipeline_rows (loader.load_cicd_data) itself is NOT deleted -- it also
    # feeds active_repos, an unrelated live reader.
    "cicd": "compute_cicd_metrics_daily",
    # CHAOS-5234/CHAOS-3092: incident's daily compute deleted from
    # job_daily.py -- the native Go executor (IncidentExecutor,
    # CHAOS-4269/CHAOS-4295, with the NULL-guard fix already included) is
    # the only writer of incident_metrics_daily for a daily partition now.
    # Unlike the other families in this ledger, incident never had a
    # skip_families gate of its own here to begin with -- its Python
    # compute went straight from "always runs" to "deleted outright."
    # compute_incident_metrics_daily itself is ALSO deleted (the whole
    # metrics/compute_incidents.py module) -- its only other callers were
    # tests/metrics/test_compute_delivery_ops.py's incident test function
    # (removed) and tests/test_pagerduty_clickhouse_live.py's mixed
    # incident+dora test (surgically trimmed to drop only the incident
    # half).
    "incident": "compute_incident_metrics_daily",
    # CHAOS-5309 (CHAOS-5234/CHAOS-3092): deploy's daily compute deleted from
    # job_daily.py -- the native Go executor (DeployExecutor, CHAOS-4293) is
    # the only writer of deploy_metrics_daily for a daily partition now.
    # compute_deploy_metrics_daily itself is ALSO deleted (from
    # compute_deployments.py) -- rg confirmed job_daily.py was its only real
    # caller. The sibling constant DEPLOYMENT_FAILURE_STATUSES in the same
    # module is NOT touched -- it has a real, separate caller
    # (compute_dora.py, still Python).
    "deploy": "compute_deploy_metrics_daily",
    # CHAOS-5234: work_graph_edges's daily compute deleted from job_daily.py
    # -- the native Go executor (WorkGraphEdgesExecutor, CHAOS-4286) is the
    # only writer of work_graph_pr_review_outcome_edges/work_graph_pr_
    # deployment_edges/work_graph_deployment_incident_edges for a daily
    # partition now (closes CHAOS-5216 by construction: single native
    # reader). Same shape as ai_impact: extract_review_deployment_incident_
    # edges itself is ALSO deleted (from work_graph/extractors/ai_workflow.py)
    # -- rg confirmed its only real callers, once job_daily.py's own
    # reference was removed, were its Go bit-exact oracle rot guard
    # (TestWorkGraphEdgesMatchLivePythonProduction +
    # testdata/python_work_graph_edges_oracle.py, both also deleted in this
    # PR) and its own dedicated test (trimmed, not deleted --
    # tests/work_graph/test_ai_workflow.py's traversal tests survive).
    #
    # Merge note (CHAOS-5242, #2307 landed first): that PR deleted this same
    # function's OTHER half (ai_workflow's runs/artifact_edges/issue_edges,
    # via extract_ai_workflow_from_pull_requests). With both halves gone in
    # the merge, _extract_ai_workflow_for_day itself and the whole
    # work_graph/extractors/ai_workflow.py module (including
    # AIWorkflowExtractionResult and extract_ai_workflow_from_pull_requests)
    # are deleted too -- rg confirmed zero remaining callers of any of them.
    "work_graph_edges": "extract_review_deployment_incident_edges",
}


def _referenced_names(source: str, source_path: str) -> set[str]:
    """Every identifier/string-literal that could name a real reference to a
    deleted compute function, found via the AST rather than a text scan.

    Comments never appear in the AST at all -- Python's own parser discards
    them before producing it -- so this cannot be fooled by an explanatory
    comment (inline or whole-line) naming the deleted function to describe
    the deletion. Three node shapes cover every real-reference form: a bare
    name (`compute_x`), an attribute access (`module.compute_x`), and a
    string constant (covers `getattr(module, "compute_x")` -- and adjacent
    string literals like `"compute_" "x"` are folded into ONE `ast.Constant`
    by the parser itself before this function ever sees them, so a
    dynamic reference split across literals is detected exactly like a
    plain one, no manual concatenation-handling needed), and an import
    alias's ORIGINAL name (`ast.alias.name` -- codex r2 Finding 1 on #2283:
    `from package import compute_x as y` binds the local name `y`, which is
    all a bare `ast.Name`/`ast.Attribute` walk would ever see; the deleted
    function's REAL name is `ast.alias.name`, not the local alias it's bound
    to, so it must be collected explicitly).
    """
    tree = ast.parse(source, filename=source_path)
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            names.add(node.id)
        elif isinstance(node, ast.Attribute):
            names.add(node.attr)
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            names.add(node.value)
        elif isinstance(node, ast.alias):
            names.add(node.name)
    return names


def test_deleted_native_families_have_no_compute_reference_in_job_daily() -> None:
    assert JOB_DAILY_SOURCE.is_file(), f"missing source: {JOB_DAILY_SOURCE}"
    assert DELETED_NATIVE_FAMILY_COMPUTE_FUNCTIONS, (
        "the deletion ledger is empty -- CHAOS-5233 should have added the "
        "first entry; if this legitimately regressed to zero, this test's "
        "own docstring needs updating too"
    )

    source = JOB_DAILY_SOURCE.read_text(encoding="utf-8")
    referenced = _referenced_names(source, str(JOB_DAILY_SOURCE))
    still_referenced = sorted(
        f"{family} ({function})"
        for family, function in DELETED_NATIVE_FAMILY_COMPUTE_FUNCTIONS.items()
        if function in referenced
    )
    assert not still_referenced, (
        f"job_daily.py still references a compute function CHAOS-3092's "
        f"deletion ledger says was already removed: {still_referenced}. "
        "Either the deletion regressed (a re-import or a new call site "
        "crept back in) or this test's ledger is stale and the function "
        "genuinely has a new, deliberate caller in job_daily.py -- in that "
        "second case, remove the ledger entry and explain why in this "
        "file's docstring, do not just widen the check to tolerate it."
    )


def test_guard_ignores_comment_only_mentions() -> None:
    """Regression for codex r1 Finding 2 on #2283 (CHAOS-5240): an INLINE
    trailing comment naming a deleted function is not a real reference and
    must not be flagged -- this is exactly the construction codex used to
    show the old line-based guard's false positive
    (`sentinel = 1  # build_governance_rows_for_day` survived the old
    whole-line-only comment strip)."""
    source = "sentinel = 1  # build_governance_rows_for_day\n"
    assert "build_governance_rows_for_day" not in _referenced_names(source, "<test>")


def test_guard_detects_adjacent_string_literal_split_reference() -> None:
    """Regression for codex r1 Finding 2 on #2283 (CHAOS-5240): a dynamic
    getattr() call whose target name is written as two adjacent string
    literals must still be detected as a real reference -- this is exactly
    the construction codex used to show the old substring-search guard's
    false negative (the literal text never contains the function name as one
    contiguous substring, so a plain `in` check missed it)."""
    source = 'getattr(job_daily, "build_" "governance_rows_for_day")()\n'
    assert "build_governance_rows_for_day" in _referenced_names(source, "<test>")


def test_guard_detects_aliased_import_reference() -> None:
    """Regression for codex r2 Finding 1 on #2283 (CHAOS-5240): a deleted
    function imported under a local alias (`from package import
    build_governance_rows_for_day as daily_compute`) must still be detected
    by its REAL (pre-alias) name -- a bare Name/Attribute walk only ever
    sees the local alias (`daily_compute`), never the original name being
    imported, so the alias binding itself (`ast.alias.name`) has to be
    collected explicitly."""
    source = (
        "from package import build_governance_rows_for_day as daily_compute\n"
        "daily_compute()\n"
    )
    assert "build_governance_rows_for_day" in _referenced_names(source, "<test>")
