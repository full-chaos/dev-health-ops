#!/usr/bin/env python3
"""CHAOS-3300 machine-readable Wave 3.1 proof-gate manifest.

This module is the single source of truth for "what does CHAOS-3300's
blocking matrix require, and what actually proves it today". It is
deliberately conservative: an item is ``proven_e2e`` only if a real HTTP/SSE
acceptance run exercises it, ``proven_unit`` if only isolated
service/orchestrator-level tests exercise it (real code, real assertions,
but not through the live runtime), ``blocked`` if the production code path
does not exist yet (with a concrete reason), and ``deferred`` if proving it
requires infrastructure this repository's test suite cannot provide (live
third-party provider credentials, a deployed environment to scan, etc).

Every ``proven_*`` item's ``evidence`` must name real files that exist in
this repository -- ``validate_manifest`` checks that at import/report time,
so a claim that stops being true (a test file renamed or deleted) fails the
manifest rather than silently going stale. This is not a claim that the
referenced tests currently pass; running them is CI's job. It is a claim
that the evidence a status rests on still exists and is not fabricated.
"""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, TypedDict

__all__ = [
    "MANIFEST",
    "MANIFEST_SCHEMA_VERSION",
    "MIGRATION_COEXISTENCE_REASON",
    "STACK3_PERSISTENCE_GAP_REASON",
    "STACK3_WIRING_GAP_REASON",
    "TEAM_ATTRIBUTION_LIVE_DEFECT_REASON",
    "ManifestIntegrityError",
    "ManifestItem",
    "ManifestItemJSON",
    "ManifestReportJSON",
    "Status",
    "build_report",
    "execute_manifest",
    "run_evidence_tests",
    "validate_blocked_execution_artifact",
    "validate_execution_artifact",
    "validate_manifest",
]

MANIFEST_SCHEMA_VERSION = "wave_3_1_proof_gate_manifest.v1"

Status = Literal["proven_e2e", "proven_unit", "blocked", "deferred"]

_VALID_STATUSES: frozenset[str] = frozenset(
    {"proven_e2e", "proven_unit", "blocked", "deferred"}
)


@dataclass(frozen=True, slots=True)
class ManifestItem:
    """One CHAOS-3300 blocking-matrix / gate / attack / mutation entry.

    ``id`` is a stable slug -- referenced by exact-id tests below so a status
    cannot silently flip without a corresponding test change. ``evidence`` is
    a tuple of repository-relative file paths this item's status rests on;
    every path must exist (checked by ``validate_manifest``). ``blocked``
    items must carry a non-empty ``blocked_reason`` naming the concrete gap,
    not a vague "not done yet".
    """

    id: str
    category: str
    description: str
    status: Status
    evidence: tuple[str, ...] = ()
    blocked_reason: str | None = None
    #: Substrings that must actually appear in the (first) evidence file, for
    #: the handful of items where "the file exists" is too weak a claim.
    content_markers: tuple[str, ...] = ()
    #: Exact pytest node ids (``path::test_name``) that back a ``proven_*``
    #: claim. ``execute_manifest`` actually runs these -- a status is not
    #: "proven" because a file with a plausible name exists; it is proven
    #: because the specific test that exercises it currently passes. Empty
    #: only when ``requires_live_infra`` is True (a Compose/Docker scenario
    #: this repo's unit-test run cannot execute) or the item does not rest
    #: on a runnable test at all (e.g. a static-content claim).
    test_nodeids: tuple[str, ...] = ()
    #: True for the handful of ``proven_e2e`` items whose proof is a real
    #: HTTP/SSE Compose acceptance run, not something ``execute_manifest``
    #: can invoke inside a plain pytest process.
    requires_live_infra: bool = False
    #: Repository-relative path to the execution artifact
    #: (``acceptance_artifact.ScenarioRecorder.write``'s output) the smoke
    #: script itself produced when this scenario last ran. Codex finding
    #: (HIGH, 2026-08-02): before this field existed, a ``proven_e2e`` item
    #: needed no machine-verifiable execution at all -- any existing evidence
    #: file plus ``requires_live_infra=True`` validated clean, including a
    #: row that was never actually run. Every ``proven_e2e`` item must now
    #: set this; ``validate_manifest`` checks the artifact exists, parses,
    #: reports every assertion passing, and was generated from a commit that
    #: is an ancestor of (or equal to) current HEAD -- see
    #: ``acceptance_artifact.py`` for why ancestry, not equality.
    execution_artifact: str | None = None
    #: Exact assertion names (``acceptance_artifact.AssertionResult.name``)
    #: that must appear in ``execution_artifact`` with ``passed: true``.
    #: Codex finding (HIGH, 2026-08-02, round 2): "every assertion passed"
    #: alone does not bind an artifact to what THIS row claims -- a
    #: proven_e2e row citing an artifact full of unrelated boilerplate
    #: assertions (or one swapped from a different scenario, if only the
    #: filename were checked) would still validate clean. Naming the
    #: specific assertions load-bearing for this row's claim closes that:
    #: an artifact swap between two scenarios fails here even when both
    #: artifacts are individually all-passing, because each row demands the
    #: names its own claim actually rests on.
    required_assertion_names: tuple[str, ...] = ()
    #: Repository-relative path to an execution artifact backing a
    #: ``blocked`` item's claim that a live attempt genuinely happened and
    #: genuinely failed -- e.g. "we tried this live and hit CHAOS-3337",
    #: not a fabricated or merely-asserted repro. Codex finding (MED,
    #: 2026-08-03, round 3): before this field existed, a blocked row
    #: citing a failed live attempt had NOTHING checking that evidence --
    #: a nonexistent artifact path, or one recording ``status: "passed"``,
    #: validated exactly the same as a real one. ``validate_blocked_
    #: execution_artifact`` checks this the same way ``validate_execution_
    #: artifact`` checks a proven_e2e artifact (schema, non-dict rejection,
    #: tree_clean, ancestry, script_sha256) but additionally REQUIRES
    #: ``status == "failed"`` and the names in ``blocked_expected_failing_
    #: assertions`` to actually show ``passed: false`` -- a "passed"
    #: artifact backing a blocked claim would be self-contradictory. This
    #: field must only ever appear on a ``blocked`` item; validate_manifest
    #: rejects it on any other status. Critically, nothing in this module
    #: ever reads this field to promote a row's status -- it exists purely
    #: to bind the SUPPORTING evidence for staying blocked to something
    #: real, never as an alternate path to "proven".
    blocked_execution_artifact: str | None = None
    #: Assertion names that must appear in ``blocked_execution_artifact``
    #: with ``passed: false`` -- proves the artifact failed in the
    #: SPECIFIC expected way, not merely that ``status == "failed"``
    #: (which a completely unrelated failure would also satisfy).
    blocked_expected_failing_assertions: tuple[str, ...] = ()


class ManifestIntegrityError(RuntimeError):
    """The manifest itself makes a claim its own evidence does not support."""


class ManifestItemJSON(TypedDict):
    id: str
    category: str
    description: str
    status: str
    evidence: list[str]
    blocked_reason: str | None
    test_nodeids: list[str]
    requires_live_infra: bool
    execution_artifact: str | None
    required_assertion_names: list[str]


class ManifestReportJSON(TypedDict):
    schema_version: str
    item_count: int
    status_counts: dict[str, int]
    category_counts: dict[str, int]
    items: list[ManifestItemJSON]


#: Golden, exact-match reason text for the manifest's most load-bearing
#: ``blocked`` claims. Codex finding (MED, 2026-08-02): the tests that lock
#: these reasons previously used substring checks (``"X" in reason``), which
#: a reason silently reworded to add or remove meaning still satisfies as
#: long as the checked substrings survive -- codex demonstrated this by
#: appending "SILENTLY REWORDED" to each string and watching every lock stay
#: green. Promoting the reason text to a named module-level constant lets the
#: test compare with ``==`` instead: ANY wording change, not just a removed
#: substring, now requires a deliberate edit to the same constant the
#: manifest itself uses, so the golden text and the shipped text can never
#: drift apart.
STACK3_WIRING_GAP_REASON = (
    "PROJECT_HEALTH/TEAM_HEALTH/PORTFOLIO_STATUS/TEAM_WORKLOAD_BALANCE/"
    "OPERATIONAL_DEFICIENCY_INVENTORY have no DevInvestigationPlan in "
    "investigation_plans/plan_documents.py:CORE_PLANS_BY_INTENT and no "
    "StepRegistry registration; production_runtime.py:2169 wires only "
    "CORE_PLANS_BY_INTENT as the live plan_registry, so these intents "
    "cannot reach ProjectHealthService/TeamHealthService/"
    "PortfolioStatusService/TeamWorkloadService/"
    "OperationalDeficiencyService in production today even though all "
    "five services are implemented and unit-tested. This is SEQUENCED, "
    "not dropped: CHAOS-3303 comment d0985e79-051d-4b6f-8833-6137e8511aec "
    "(2026-08-02) ratifies deferring plan/step registry wiring to the "
    "CHAOS-3297 stack-3 lane, which owns landing "
    "DevInvestigationPlan+StepRegistry entries for these five intents "
    "once its own s2/flags prerequisites merge; re-run this manifest "
    "after stack-3 lands to flip these rows to real evidence"
)

MIGRATION_COEXISTENCE_REASON = (
    "CHAOS-3306 resolved as Backlog/Low with an explicit "
    "decision to keep the Ask Dev runtime permanently on Python "
    "in dev-health-ops (people-facing) alongside Go acr-api/"
    "acr-mcp (agents/MCP) -- 'not part of Wave 3.1', 'does not "
    "block any active Ask Dev, Web, Ops, or ACR implementation "
    "issue', and 'no implementation or decommission work is "
    "authorized by this backlog item today'. This gate assumes "
    "an active cutover with a sunset date, which 3306 did not "
    "approve; treated as not-applicable-by-decision rather than "
    "unmet, but the report must say so explicitly rather than "
    "silently skip it"
)

#: Historical record of the original CHAOS-3332 repro. The item this reason
#: was attached to (attack.team-attribution.e2e-blocked-by-live-defect) has
#: since flipped to proven_e2e after ops #1382 fixed the crash -- this
#: constant is kept, and still exported and tested, so the fact that a real
#: 100%-reproducible live defect was found and fixed here does not silently
#: disappear from the manifest's history.
TEAM_ATTRIBUTION_LIVE_DEFECT_REASON = (
    "ANY status question naming a real, resolvable team subject "
    "(tried all 3 fixture teams: core/growth/platform) returns "
    "a terminal ERROR/internal_error over the live HTTP/SSE API "
    "with ask_dev_wave_3_1 enabled -- 100% reproducible, zero "
    "flake. This is NOT the CORE_PLANS_BY_INTENT gap: TEAM is a "
    "supported_subject_kind on status.entity.v2 (a WIRED plan), "
    "so this is a distinct, previously-unknown defect in an "
    "intent CHAOS-3300 counted as working. Live diagnosis "
    "(dev_runs row for one repro, org "
    "0a155cab-8833-42ac-a4ef-0d121725a7b0, run_id "
    "36ef85a2-960a-4223-a699-333270b74c70): "
    "preflight_outcome=proceeded_committed_subject (team-name "
    "resolution and commit work correctly); "
    'plan_step_partition={"failed": [], "skipped": '
    '["evidence_expansion", "work_graph_expansion"], '
    '"completed": ["required_source_health", '
    '"status_snapshot"]} (the plan-governed investigation '
    "itself completes cleanly, 0 failed steps); "
    "tool_call_count=0 (crashes before or during the legacy "
    "model-round loop / frame emission that follows a "
    "successful plan-governed investigation); safe_error_code= "
    "internal_error with zero corresponding stderr/stdout log "
    "line in the API container across three separate repro "
    "runs (a silent server-side failure -- the exception is "
    "caught and mapped to internal_error without being logged "
    "anywhere this lane could find). team_repo_ownership was "
    "empty for all 3 fixture teams in this run, so an "
    "unattributed-team edge case is a plausible trigger, but "
    "not confirmed without reading the frame-emission code "
    "path directly, which this lane did not do (team-lead "
    "condition: report and stop rather than debug an hour). "
    "Filed as CHAOS-3332 (parent CHAOS-3293, Ask Dev project) "
    "with the full repro plus the silent-catch as a second fix "
    "requirement -- this item flips once CHAOS-3332 ships and a "
    "live re-run of smoke_ask_dev_exact_commit.py's pattern "
    "against a named team subject confirms it."
)

#: The CHAOS-3297 stack-3 plan wiring landed (ops #1383/#1387, merged to
#: main 2026-08-03) -- PROJECT_HEALTH/TEAM_HEALTH/TEAM_WORKLOAD_BALANCE/
#: OPERATIONAL_DEFICIENCY_INVENTORY are now real entries in the live
#: plan_registry (production_runtime.py's CORE_PLANS_BY_INTENT +
#: WAVE_3_1_PLANS_BY_INTENT). STACK3_WIRING_GAP_REASON's root cause is
#: therefore RESOLVED -- but attempting a live run against the fixed
#: wiring found a second, distinct, 100%-reproducible defect one layer
#: deeper, so these rows stay blocked with a NEW reason rather than
#: silently flipping. This is the CHAOS-3332 pattern repeating: a
#: wiring/registration gap gets fixed, and the newly-reachable code path
#: immediately surfaces a real bug that was unreachable, and therefore
#: invisible, until the gap closed.
STACK3_PERSISTENCE_GAP_REASON = (
    "PROJECT_HEALTH/TEAM_HEALTH/TEAM_WORKLOAD_BALANCE/"
    "OPERATIONAL_DEFICIENCY_INVENTORY are now wired in the live "
    "plan_registry (ops #1383/#1387), but every one of their mandatory "
    "steps crashes to a terminal ERROR/internal_error before scope "
    "resolution completes -- 100% reproducible, live-confirmed for "
    "TEAM_HEALTH/TEAM_WORKLOAD_BALANCE/OPERATIONAL_DEFICIENCY_INVENTORY "
    'against a named TEAM subject ("Is the Core team healthy?"/'
    '"overburdened?"/"What operational deficiencies does the Core '
    'team have?"); PROJECT_HEALTH shares the identical root cause but '
    "was not independently live-run since this fixture profile seeds "
    "zero PROJECT-kind rows (the same constraint documented on "
    "positive-control.real-project-status). Unlike CHAOS-3332, this "
    "crash IS logged: "
    "dev_health_ops.api.dev.persistence.service.DevPersistenceValidation"
    "Error: invalid source_class, raised from "
    "persistence/service.py:2255 append_source_observation, via "
    "orchestrator_persistence.py:175/203, via orchestrator.py:1109 "
    "run(). Root cause read directly from source: wave_3_1_plans.py's "
    "four plans declare source_class=SourceClass.HEALTH_PROFILE (three "
    "plans) or SourceClass.DEFICIENCY_INVENTORY (one plan) -- both "
    "legitimate members of the SourceClass contract enum -- but "
    "persistence/service.py's separate _SOURCE_CLASSES frozenset "
    "allowlist, which append_source_observation validates against "
    "before every mandatory-step write, was never updated with either "
    "value. Every investigation using any of the four plans fails on "
    "its first mandatory step. Filed as CHAOS-3337 (parent CHAOS-3297, "
    "Ask Dev project) with the full traceback and the one-line fix "
    "(add both string values to _SOURCE_CLASSES) -- these rows flip "
    "once CHAOS-3337 ships and a live re-run of "
    "smoke_ask_dev_stack3_intents.py confirms it."
)

#: Codex finding (HIGH, 2026-08-03). This row was briefly flipped to
#: proven_unit on a new synthesis-layer test, and that flip was WRONG in a
#: way worth recording, because it is the same overclaim the row was
#: originally blocked for, moved one layer down. The new test supplies
#: change_failure_rate_not_applicable=True for a PROJECT subject directly
#: to synthesize_health_profile. Production cannot reach that input:
#: ProjectHealthService.evaluate_project accepts only PROJECT scopes,
#: DirectScope.PROJECT is a member of CHANGE_FAILURE_RATE_SUPPORTED_SCOPES,
#: so the flag is always computed False and the metric is always queried.
#: The test therefore proves a property of the synthesis engine over an
#: input no project-service call can produce -- true, but not a proof about
#: project health. The test is kept (it is a legitimate synthesis-unit
#: check and it does kill a real mutation); it is simply not evidence for
#: THIS row.
PROJECT_HEALTH_UNREACHABLE_INPUT_REASON = (
    "No PROJECT-scoped proof exists that reaches both an UNKNOWN and a "
    "NOT_APPLICABLE dimension through production code. The only source "
    "that yields NOT_APPLICABLE at the synthesis layer is the "
    "change-failure-rate metric, and ProjectHealthService always computes "
    "change_failure_rate_not_applicable=False for a PROJECT subject "
    "(DirectScope.PROJECT is in CHANGE_FAILURE_RATE_SUPPORTED_SCOPES), so "
    "a NOT_APPLICABLE observation is unreachable for this subject kind "
    "through the real service. A synthesis-layer test can construct the "
    "flag by hand -- test_chaos_3303_health_profile_synthesis.py::"
    "test_project_profile_reports_unknown_and_not_applicable_dimensions_"
    "distinctly does, and it is a genuine test of the engine -- but citing "
    "it here would claim a production-reachable behaviour that does not "
    "exist. Closing this needs either a production input that genuinely "
    "yields both states for a PROJECT subject, or the row rewritten to "
    "claim the abstract synthesis property instead of project health. "
    "Separately, a literal PROJECT subject still cannot be live-verified: "
    "this fixture profile seeds zero PROJECT-kind rows (see "
    "positive-control.real-project-status)."
)

#: Codex finding (HIGH, 2026-08-03), the same overclaim in the other
#: direction: a rule-layer test can hand evaluate_rule an observation with
#: attribution_present=True and watch it reach the triggered state, but the
#: production adapter that feeds this rule
#: (investment_allocation_shift_observation) sets attribution_present=False
#: UNCONDITIONALLY pending CHAOS-3331, and the rule sets
#: attribution_required=True. Real evaluation therefore short-circuits to
#: UNKNOWN/missing_attribution before coverage or threshold is ever
#: considered. Disclosing "shadow-only" was not enough: even the shadow
#: finding shape cannot be produced by the real service today.
FEATURE_WORK_ATTRIBUTION_BLOCKED_REASON = (
    "The health-rule layer cannot produce this row's finding through "
    "production code today. health_rule.investment_allocation_shift.v1 "
    "sets attribution_required=True, and its only production adapter, "
    "dimension_observation_adapters.investment_allocation_shift_"
    "observation, reports attribution_present=False unconditionally while "
    "CHAOS-3331 is open (the writer path cannot distinguish a canonically "
    "attributed row from a fail-open one on the read side). evaluate_rule "
    "checks attribution BEFORE cohort/sample/coverage/threshold, so every "
    "real evaluation returns UNKNOWN with missing_attribution -- never the "
    "'light on feature work' finding this row claims. A rule-layer test "
    "that constructs attribution_present=True bypasses exactly the guard "
    "that makes the claim false, so citing one would overclaim. Closing "
    "this needs CHAOS-3331 to land and a test through TeamWorkloadService "
    "that produces the finding from real adapter output, or the row "
    "rewritten to claim the abstract rule-evaluator shape rather than an "
    "answer to the user's question."
)


def _core_defect_reproductions() -> tuple[ManifestItem, ...]:
    return (
        ManifestItem(
            id="defect.ask-dev-not-found",
            category="original_defect_reproduction",
            description=(
                "Seed no authorized project named Ask Dev: return not_found, "
                "state that organization data was not substituted, and "
                "execute zero status/health/evidence steps for the "
                "nonexistent subject."
            ),
            status="proven_unit",
            evidence=(
                "tests/_chaos_3292_preflight.py",
                "tests/api/dev/test_chaos_3292_preflight_acceptance.py",
            ),
            content_markers=("ASK_DEV_PROJECT",),
            test_nodeids=(
                "tests/api/dev/test_chaos_3292_preflight_acceptance.py::"
                "test_a2_unknown_target_is_not_found_and_runs_no_subject_tool",
            ),
        ),
        ManifestItem(
            id="defect.ask-dev-exact-commit",
            category="original_defect_reproduction",
            description=(
                "Seed an exact Ask Dev project: commit it before "
                "investigation and return its substantive status frame."
            ),
            status="proven_unit",
            evidence=("tests/api/dev/test_chaos_3292_preflight_acceptance.py",),
            test_nodeids=(
                "tests/api/dev/test_chaos_3292_preflight_acceptance.py::"
                "test_a1_known_project_commits_scope_before_the_status_tool",
            ),
        ),
        ManifestItem(
            id="defect.no-org-fallback-or-blank",
            category="original_defect_reproduction",
            description=(
                "Never render organization deployments, refused, "
                "forbidden/not_found, raw resolver reasons, or a blank "
                "answer for a preflight termination."
            ),
            status="proven_unit",
            evidence=(
                "src/dev_health_ops/api/dev/preflight_outcomes.py",
                "tests/api/dev/test_chaos_3292_mutations.py",
            ),
            content_markers=("FORBIDDEN_OR_NOT_FOUND",),
            test_nodeids=(
                "tests/api/dev/test_chaos_3292_mutations.py::"
                "test_m2_no_authorized_match_must_not_fall_through_to_organization_scope",
                "tests/api/dev/test_chaos_3292_mutations.py::"
                "test_m3a_leaky_canonical_copy_cannot_be_constructed_at_all",
            ),
        ),
        ManifestItem(
            id="defect.ask-dev-not-found.e2e-live-validated",
            category="original_defect_reproduction",
            description=(
                "The not-found defect reproduction proven through the real "
                "live Compose stack (real Postgres/ClickHouse/Valkey, real "
                "API, real HTTP/SSE, scripted OpenAI-compatible provider) "
                "rather than only the orchestrator-level unit harness -- "
                "no web/Playwright dependency, since the outcome never "
                "reaches the web UI."
            ),
            status="proven_e2e",
            evidence=(
                "scripts/acceptance/smoke_ask_dev_not_found.py",
                "scripts/acceptance/run_ask_dev_compose.sh",
                "tests/acceptance/test_ask_dev_not_found_smoke.py",
            ),
            requires_live_infra=True,
            execution_artifact="tests/acceptance/artifacts/not_found.json",
            required_assertion_names=(
                "terminal_error_event_present",
                "error_code_is_scope_not_found",
                "no_answer_produced",
                "safe_message_does_not_name_subject",
            ),
            # Codex finding (HIGH, 2026-08-02): this used to be a prose
            # narrative claiming a specific past run -- unverifiable by
            # validate_manifest, and a fabricated row citing only existing
            # files validated exactly the same as a real one. The narrative
            # is now redundant with, and superseded by, the machine-checked
            # execution_artifact above: see validate_execution_artifact for
            # what it actually verifies (artifact exists and parses, every
            # recorded assertion passed, the recorded commit is an ancestor
            # of current HEAD, and the recorded script bytes still match).
        ),
        ManifestItem(
            id="defect.ask-dev-exact-commit.e2e-live-validated",
            category="original_defect_reproduction",
            description=(
                "The exact-commit defect reproduction proven through the "
                "real live Compose stack -- the positive control the "
                "not-found negative control needs to hold against, over "
                "the same real HTTP/SSE surface."
            ),
            status="proven_e2e",
            evidence=(
                "scripts/acceptance/smoke_ask_dev_exact_commit.py",
                "scripts/acceptance/run_ask_dev_compose.sh",
                "tests/acceptance/test_ask_dev_exact_commit_smoke.py",
            ),
            requires_live_infra=True,
            execution_artifact="tests/acceptance/artifacts/exact_commit.json",
            required_assertion_names=(
                "scope_resolved_event_present",
                "named_repository_committed",
                "answer_completed_event_present",
                "answer_status_not_error",
                "stream_terminated_as_answer",
            ),
        ),
    )


def _real_project_positive_control() -> tuple[ManifestItem, ...]:
    return (
        ManifestItem(
            id="positive-control.real-project-status",
            category="real_project_positive_control",
            description=(
                "A seeded exact NAMED SUBJECT with known status, required "
                "work, and subject-linked evidence must reliably resolve "
                "and answer -- the CHAOS-3289 1-useful-answer-in-10 result "
                "fails this issue even if the other nine runs are "
                "described as safe. Proven against a repository subject, "
                "not literally a PROJECT-kind entity: this Compose "
                "profile's `dev-hops fixtures generate` never populates "
                "default.projects (confirmed live 2026-08-02 -- 0 rows; "
                "PROJECT-kind entities are sourced from an external "
                "issue-tracker's project_key, e.g. Jira/Linear, which "
                "these synthetic fixtures do not simulate), so no "
                "PROJECT-kind subject exists to name. Codex finding "
                "(HIGH, 2026-08-02) explicitly allows this: 'if the "
                "fixture profile genuinely can't support it, downgrade/"
                "rename ... to what the ... run proves -- honest downgrade "
                "over overclaim'. A named repository is the strongest "
                "real, resolvable, named subject this fixture profile "
                "offers, and the underlying claim this row exists to "
                "prove (a real named subject reliably resolves and "
                "substantively answers, unlike CHAOS-3289's 1-in-10) is "
                "satisfied by it."
            ),
            status="proven_e2e",
            evidence=(
                "scripts/acceptance/smoke_ask_dev_exact_commit.py",
                "tests/acceptance/test_ask_dev_exact_commit_smoke.py",
            ),
            requires_live_infra=True,
            execution_artifact="tests/acceptance/artifacts/exact_commit.json",
            required_assertion_names=(
                "named_repository_committed",
                "answer_completed_event_present",
                "answer_status_not_error",
                "answer_summary_not_empty",
            ),
            # Re-pointed from smoke_ask_dev_inherited_oracle.py (an org-wide
            # run with no SCOPE_RESOLVED assertion, previously overclaiming
            # "exact project") to smoke_ask_dev_exact_commit.py per codex
            # finding HIGH 2026-08-02 -- see the description above. Separate
            # finding preserved here since it no longer lives on this row's
            # evidence: `dev-hops fixtures generate --days 28` only backfills
            # 28 days, so a naive 28-day-current + 28-day-comparison window
            # (used by smoke_ask_dev_not_found.py/smoke_ask_dev_exact_commit
            # .py/smoke_ask_dev_core_intents.py) returns comparison_value=
            # null; smoke_ask_dev_inherited_oracle.py and
            # smoke_ask_dev_metric_comparison.py use 14+14 instead. The
            # 28+28 scripts' own assertions don't depend on comparison_value
            # so this doesn't invalidate them, but it does mean they prove
            # "safe non-crashing behavior," not "a fully grounded numeric
            # comparison" -- not retrofitted, time-boxed per wrap directive.
        ),
    )


def _attacks() -> tuple[ManifestItem, ...]:
    return (
        ManifestItem(
            id="attack.unrelated-evidence.exclusion",
            category="attack",
            description=(
                "Recent organization deployments, PRs, incidents, and "
                "metrics unrelated to the named target must be absent from "
                "the named-target answer."
            ),
            status="proven_unit",
            evidence=("tests/api/dev/test_chaos_3296_relationship_closure.py",),
            content_markers=(
                "test_an_edge_unrelated_to_the_committed_subject_fails_closed",
            ),
            test_nodeids=(
                "tests/api/dev/test_chaos_3296_relationship_closure.py::"
                "test_an_edge_unrelated_to_the_committed_subject_fails_closed",
            ),
        ),
        ManifestItem(
            id="attack.unrelated-evidence.availability",
            category="attack",
            description=(
                "The same unrelated-org facts must be available in an "
                "explicit organization-wide answer (the other half of the "
                "attack -- exclusion when named, availability when not). "
                "Proven at the layer that decides it: EvidenceService."
                "search over an ORGANIZATION-resolved scope returns "
                "evidence from a repository the scope never named. The "
                "non-obvious mechanism is _authorized_entity_ids, which "
                "excludes ORGANIZATION and REPOSITORY kinds, so an "
                "org-wide resolution yields an EMPTY valid_entity_ids that "
                "downstream authorization treats as unrestricted rather "
                "than as 'nothing authorized'; a mutation restricting "
                "records to the resolved scope's own entity ids kills the "
                "test. The fixture limitation that originally blocked this "
                "row is also fixed -- the scripted provider derives its "
                "search_evidence.v1 query from the question instead of "
                "hardcoding one repository identity, with the named-subject "
                "path byte-for-byte unchanged. NOT claimed: the ClickHouse "
                "SQL layer beneath EvidenceService is covered by neither "
                "test, and smoke_ask_dev_unrelated_evidence.py has not been "
                "re-run live against a --repo-count 2 bring-up since the "
                "provider fix, so the end-to-end 'multi-repository evidence "
                "actually appears in a live answer' claim remains unproven. "
                "That script's own docstring still describes the pre-fix "
                "provider (it hardcoded one repository identity) and is "
                "deliberately left byte-for-byte unedited: its bytes are "
                "bound by script_sha256 into "
                "attack.unrelated-evidence.e2e-live-validated's execution "
                "artifact, so correcting even a docstring there invalidates "
                "that artifact until the scenario is re-run live. Read this "
                "row, not that docstring, for the current provider "
                "behaviour."
            ),
            status="proven_unit",
            evidence=(
                "tests/api/dev/test_evidence_service.py",
                "tests/api/dev/test_acceptance_openai_runtime.py",
            ),
            content_markers=(
                "test_organization_wide_search_admits_evidence_from_multiple_repositories",
            ),
            test_nodeids=(
                "tests/api/dev/test_evidence_service.py::"
                "test_organization_wide_search_admits_evidence_from_multiple_repositories",
                "tests/api/dev/test_acceptance_openai_runtime.py::"
                "test_organization_wide_question_searches_are_not_restricted_to_one_repository",
                "tests/api/dev/test_acceptance_openai_runtime.py::"
                "test_named_repository_question_still_searches_that_exact_repository",
            ),
        ),
        ManifestItem(
            id="attack.unrelated-evidence.e2e-live-validated",
            category="attack",
            description=(
                "Live end-to-end unrelated-evidence proof: a named "
                "repository question over the real Compose stack excludes a "
                "second, unrelated real repository's evidence."
            ),
            status="proven_e2e",
            evidence=(
                "scripts/acceptance/smoke_ask_dev_unrelated_evidence.py",
                "tests/acceptance/test_ask_dev_unrelated_evidence_smoke.py",
            ),
            requires_live_infra=True,
            execution_artifact="tests/acceptance/artifacts/unrelated_evidence.json",
            # Codex finding (MED, 2026-08-02, round 2): required_assertion_
            # names is deliberately scoped to ONLY the exclusion (negative
            # control) assertions -- "org_wide_not_scope_blocked" (the
            # weaker positive control this row's description never claims)
            # is intentionally NOT required here, so a future change cannot
            # silently widen what this row is bound to prove without a
            # deliberate edit to this tuple. See
            # attack.unrelated-evidence.availability for the disclosed
            # positive-control gap and its own reason. Requires a standalone
            # --repo-count 2 bring-up (meridian/web-app + meridian/core-api)
            # -- NOT wired into the shared run_ask_dev_compose.sh launcher,
            # which seeds exactly one repository for the other proven_e2e
            # scripts.
            required_assertion_names=(
                "named_repository_committed",
                "unrelated_repo_excluded_from_named_answer",
            ),
        ),
        ManifestItem(
            id="attack.team-attribution",
            category="attack",
            description=(
                "Seed repository/org risk and reliability facts with no "
                "valid team relationship. They may appear only as clearly "
                "broader-scope context and cannot set a team-health "
                "dimension or team finding."
            ),
            status="proven_unit",
            evidence=("tests/api/dev/test_chaos_3303_team_health_service.py",),
            content_markers=(
                "test_evaluate_team_zero_attribution_suppresses_even_with_real_facts",
            ),
            test_nodeids=(
                "tests/api/dev/test_chaos_3303_team_health_service.py::"
                "test_evaluate_team_zero_attribution_suppresses_even_with_real_facts",
            ),
        ),
        ManifestItem(
            id="attack.team-attribution.e2e-blocked-by-live-defect",
            category="attack",
            description=(
                "Live end-to-end team-attribution proof, attempted per "
                "team-lead priority 2026-08-02. Originally blocked by a "
                "newly discovered, 100%-reproducible live defect (see "
                "TEAM_ATTRIBUTION_LIVE_DEFECT_REASON for the full original "
                "repro) -- flipped to proven after ops #1382 (CHAOS-3332) "
                "merged to main and this lane re-ran the exact-commit "
                "pattern against a named team subject on the fixed code. "
                "The named team subject still commits correctly "
                "(scope.resolved with a non-empty authorized_entity_ids) "
                "and the run now completes as a real ANSWER instead of a "
                "terminal ERROR/internal_error. This row's claim is exactly "
                "the CHAOS-3333 characterization, asserted directly rather "
                "than implied by 'not error': status is precisely "
                "``degraded`` (not complete/partial), metrics come back "
                "genuinely empty (query_metric.v1 does not yet support a "
                "TEAM-scoped request), and the limitation is named in "
                "coverage.unavailable_required_sources rather than silently "
                "dropped. A future CHAOS-3333 fix that adds TEAM-scoped "
                "metric support is expected to break this row's assertions "
                "-- that is the correct outcome, and the claim must be "
                "updated alongside the fix, not loosened in advance."
            ),
            status="proven_e2e",
            evidence=(
                "scripts/acceptance/smoke_ask_dev_team_attribution.py",
                "scripts/acceptance/run_ask_dev_compose.sh",
                "tests/acceptance/test_ask_dev_team_attribution_smoke.py",
            ),
            requires_live_infra=True,
            execution_artifact="tests/acceptance/artifacts/team_attribution.json",
            required_assertion_names=(
                "named_team_committed",
                "no_internal_error_event",
                "answer_completed_event_present",
                "answer_status_is_degraded",
                "metrics_empty_for_team_scope",
                "limitation_names_unavailable_metric_source",
            ),
        ),
        ManifestItem(
            id="attack.missing-data",
            category="attack",
            description=(
                "Remove source rows while keeping the source configured. "
                "The result must become no-data/unknown, not numeric zero, "
                "healthy, or a completed denominator."
            ),
            status="proven_unit",
            evidence=(
                "tests/api/dev/test_chaos_3303_dimension_observation_adapters.py",
                "tests/api/dev/test_chaos_3304_workload_observation_adapters.py",
            ),
            content_markers=(
                "test_data_trust_no_sources_is_no_data_not_measured_zero",
            ),
            test_nodeids=(
                "tests/api/dev/test_chaos_3303_dimension_observation_adapters.py::"
                "test_data_trust_no_sources_is_no_data_not_measured_zero",
                "tests/api/dev/test_chaos_3304_workload_observation_adapters.py::"
                "test_investment_shift_missing_comparison_window_is_no_data_not_zero",
            ),
        ),
        ManifestItem(
            id="attack.runtime-divergence",
            category="attack",
            description=(
                "Run the same authorized subject/source fixture through "
                "every enabled consumer boundary (Web trusted-assertion, "
                "hosted API, MCP). Any difference in canonical subject, "
                "source state, status/readiness, health finding, metric, "
                "relationship, or evidence applicability fails."
            ),
            status="deferred",
            blocked_reason=(
                "requires a cross-repo (dev-health-web + dev-health-acr) "
                "consumer-boundary harness that does not exist yet; the "
                "Wave 3.1 v2 contracts (dev_answer.v2 / dev_answer_frame.v1) "
                "are shared, but nothing runs the same fixture through Web, "
                "hosted API, and MCP in one assertion today"
            ),
        ),
    )


def _blocking_matrix_wired() -> tuple[ManifestItem, ...]:
    """Blocking-matrix items backed by the 6 wired core intents.

    ENTITY_STATUS, REMAINING_WORK, OBSERVED_CHANGE, REGISTERED_STATISTICS,
    METRIC_COMPARISON, DATA_TRUST -- the intents ``CORE_PLANS_BY_INTENT``
    actually routes in production (``production_runtime.py``).
    """

    return (
        ManifestItem(
            id="matrix.exact-project-complete",
            category="blocking_matrix",
            description=(
                "Exact named subject status with complete current data -- "
                "proven against a repository subject, not literally "
                "PROJECT-kind; see positive-control.real-project-status for "
                "why (this fixture profile seeds zero rows in "
                "default.projects, confirmed live)."
            ),
            status="proven_e2e",
            evidence=(
                "scripts/acceptance/smoke_ask_dev_exact_commit.py",
                "tests/acceptance/test_ask_dev_exact_commit_smoke.py",
            ),
            requires_live_infra=True,
            execution_artifact="tests/acceptance/artifacts/exact_commit.json",
            required_assertion_names=(
                "named_repository_committed",
                "evidence_is_linked_to_the_committed_subject",
                "answer_status_not_error",
            ),
        ),
        ManifestItem(
            id="matrix.project-incomplete-required-children",
            category="blocking_matrix",
            description="Exact project with incomplete required children.",
            status="proven_unit",
            # tests/fixtures/ask_dev/status_change/manifest.json is a
            # documentary case list; nothing in the repo actually loads it
            # (grep confirms zero references to its case ids from any test).
            # The real, executable proof is test_status_change_service.py,
            # whose case names differ but whose asserted behavior matches.
            evidence=("tests/api/dev/test_status_change_service.py",),
            content_markers=(
                "test_completed_parent_with_incomplete_required_child_is_not_ready",
            ),
            test_nodeids=(
                "tests/api/dev/test_status_change_service.py::"
                "test_completed_parent_with_incomplete_required_child_is_not_ready",
            ),
        ),
        ManifestItem(
            id="matrix.project-stale-required-source",
            category="blocking_matrix",
            description="Exact project with stale required source.",
            status="proven_unit",
            evidence=("tests/api/dev/test_status_change_service.py",),
            content_markers=("test_stale_source_is_partial_and_never_ready",),
            test_nodeids=(
                "tests/api/dev/test_status_change_service.py::"
                "test_stale_source_is_partial_and_never_ready",
            ),
        ),
        ManifestItem(
            id="matrix.project-source-unavailable-qualified",
            category="blocking_matrix",
            description=(
                "Exact project with unavailable required source but enough "
                "facts for a qualified answer."
            ),
            status="proven_unit",
            evidence=("tests/api/dev/test_status_change_service.py",),
            content_markers=(
                "test_merged_delivery_without_release_evidence_is_indeterminate",
            ),
            test_nodeids=(
                "tests/api/dev/test_status_change_service.py::"
                "test_merged_delivery_without_release_evidence_is_indeterminate",
            ),
        ),
        ManifestItem(
            id="matrix.no-authorized-match",
            category="blocking_matrix",
            description="Named project with no authorized match.",
            status="proven_unit",
            evidence=("tests/api/dev/test_chaos_3292_preflight_acceptance.py",),
            test_nodeids=(
                "tests/api/dev/test_chaos_3292_preflight_acceptance.py::"
                "test_a2_unknown_target_is_not_found_and_runs_no_subject_tool",
            ),
        ),
        ManifestItem(
            id="matrix.ambiguous-candidate-selection",
            category="blocking_matrix",
            description="Ambiguous named project and candidate selection.",
            status="proven_unit",
            evidence=("tests/api/dev/test_chaos_3292_preflight_acceptance.py",),
            test_nodeids=(
                "tests/api/dev/test_chaos_3292_preflight_acceptance.py::"
                "test_a3_ambiguous_target_needs_clarification_with_candidates",
            ),
        ),
        ManifestItem(
            id="matrix.registered-metric-catalog",
            category="blocking_matrix",
            description="Registered metric catalog.",
            status="proven_e2e",
            evidence=(
                "src/dev_health_ops/llm/agent/scripted_openai_service.py",
                "scripts/acceptance/smoke_ask_dev_core_intents.py",
                "tests/acceptance/test_ask_dev_core_intents_smoke.py",
            ),
            content_markers=("LIST_METRICS_QUESTION",),
            requires_live_infra=True,
            execution_artifact=(
                "tests/acceptance/artifacts/registered_metrics_organization_wide.json"
            ),
            required_assertion_names=(
                "answer_completed_event_present",
                "answer_status_not_error",
                "answer_summary_not_empty",
            ),
            # Previously cited only the scripted-provider source + the
            # (Playwright-only) inherited-oracle test file, with no
            # execution artifact of its own -- this lane had never actually
            # run the LIST_METRICS_QUESTION flow independently. Added as a
            # third scenario to smoke_ask_dev_core_intents.py and executed
            # live 2026-08-02 to close that gap.
        ),
        ManifestItem(
            id="matrix.multi-metric-comparison-organization-wide",
            category="blocking_matrix",
            description=(
                "Multi-metric comparison (items_completed + "
                "cyclomatic_per_kloc), organization-wide."
            ),
            status="proven_e2e",
            evidence=(
                "scripts/acceptance/smoke_ask_dev_metric_comparison.py",
                "tests/acceptance/test_ask_dev_metric_comparison_smoke.py",
            ),
            requires_live_infra=True,
            execution_artifact="tests/acceptance/artifacts/metric_comparison.json",
            required_assertion_names=(
                "run_started_event_present",
                "answer_completed_event_present",
                "answer_status_not_error",
                "stream_terminated_as_answer",
            ),
            # First live proof the wired metric.comparison.v1 plan runs at
            # all for a real multi-metric request -- confirmed separately
            # via direct dev_runs inspection (plan_step_partition showed
            # registered_metric_query completed, 0 failed), not something
            # this script's own artifact can assert since it has no public
            # API to read that table.
        ),
        ManifestItem(
            id="matrix.multi-metric-comparison-stale-source",
            category="blocking_matrix",
            description="Multi-metric comparison with one stale source.",
            status="deferred",
            blocked_reason=(
                "metric.comparison.v1 is a wired core intent and the "
                "multi-metric happy path is now live-proven (see "
                "matrix.multi-metric-comparison-organization-wide), but the "
                "specific one-stale-source variant is not: attempted live "
                "2026-08-02 with items_completed + cyclomatic_per_kloc "
                "against this Compose profile's fixtures (dev-hops fixtures "
                "generate --days 28) and direct dev_run_source_observations "
                "inspection showed BOTH metrics genuinely available and "
                "fresh (observed_state=available_current for both) -- this "
                "fixture profile does not naturally produce a stale metric "
                "source within the 28-day backfill window, so the negative "
                "case needs either a metric class structurally absent from "
                "these fixtures (not yet identified) or direct ClickHouse "
                "row manipulation to force staleness, neither done here"
            ),
        ),
        ManifestItem(
            id="matrix.remaining-work-exact-project",
            category="blocking_matrix",
            description=(
                "Remaining work in scope (organization-wide today; an exact "
                "named project awaits a real fixture-resolvable project "
                "identity, not yet built)."
            ),
            status="proven_e2e",
            evidence=(
                "scripts/acceptance/smoke_ask_dev_core_intents.py",
                "tests/acceptance/test_ask_dev_core_intents_smoke.py",
            ),
            requires_live_infra=True,
            execution_artifact=(
                "tests/acceptance/artifacts/remaining_work_organization_wide.json"
            ),
            required_assertion_names=(
                "answer_completed_event_present",
                "answer_status_not_error",
                "answer_summary_not_empty",
            ),
        ),
        ManifestItem(
            id="matrix.data-trust-organization-wide",
            category="blocking_matrix",
            description=(
                "Organization-wide data-trust question (freshness/coverage/"
                "configuration), safely answered."
            ),
            status="proven_e2e",
            evidence=(
                "scripts/acceptance/smoke_ask_dev_core_intents.py",
                "tests/acceptance/test_ask_dev_core_intents_smoke.py",
            ),
            requires_live_infra=True,
            execution_artifact=(
                "tests/acceptance/artifacts/data_trust_organization_wide.json"
            ),
            required_assertion_names=(
                "answer_completed_event_present",
                "answer_status_not_error",
                "answer_summary_not_empty",
            ),
        ),
        ManifestItem(
            id="matrix.observed-change-comparison-windows",
            category="blocking_matrix",
            description="Observed change across exact comparison windows.",
            status="proven_unit",
            evidence=("tests/api/dev/test_status_change_service.py",),
            content_markers=("test_change_summary_is_reproducible_and_tenant_scoped",),
            test_nodeids=(
                "tests/api/dev/test_status_change_service.py::"
                "test_change_summary_is_reproducible_and_tenant_scoped",
            ),
        ),
        ManifestItem(
            id="matrix.cross-tenant-identifier-change",
            category="blocking_matrix",
            description=(
                "Cross-tenant/cross-scope identifier and authorization change."
            ),
            status="proven_unit",
            evidence=("tests/api/dev/test_status_change_service.py",),
            content_markers=("test_change_summary_is_reproducible_and_tenant_scoped",),
            test_nodeids=(
                "tests/api/dev/test_status_change_service.py::"
                "test_change_summary_is_reproducible_and_tenant_scoped",
            ),
        ),
        ManifestItem(
            id="matrix.prohibited-write-request",
            category="blocking_matrix",
            description=("Prohibited write/execution/arbitrary-query request."),
            status="proven_unit",
            evidence=("tests/api/dev/test_tool_registry.py",),
            content_markers=("test_manifest_is_the_exact_nine_tool_server_allowlist",),
            test_nodeids=(
                "tests/api/dev/test_tool_registry.py::"
                "test_manifest_is_the_exact_nine_tool_server_allowlist",
            ),
        ),
        ManifestItem(
            id="matrix.provider-narrative-failure-after-frame",
            category="blocking_matrix",
            description=(
                "Provider timeout/refusal/malformed/empty/budget failure "
                "after a valid frame."
            ),
            status="proven_unit",
            evidence=("tests/api/dev/test_orchestrator.py",),
            content_markers=(
                "test_provider_timeout_is_caller_enforced_and_terminal_once",
                "test_budget_exhaustion_after_grounded_tool_data_returns_bounded_partial",
            ),
            test_nodeids=(
                "tests/api/dev/test_orchestrator.py::"
                "test_provider_timeout_is_caller_enforced_and_terminal_once",
                "tests/api/dev/test_orchestrator.py::"
                "test_budget_exhaustion_after_grounded_tool_data_returns_bounded_partial",
            ),
        ),
    )


def _blocking_matrix_blocked() -> tuple[ManifestItem, ...]:
    """Blocking-matrix items for the CHAOS-3303/3304/3305 health/workload/
    deficiency/portfolio services.

    STATE AS OF 2026-08-03: the CHAOS-3297 stack-3 wiring landed (ops
    #1383/#1387, merged to main) -- PROJECT_HEALTH/TEAM_HEALTH/
    TEAM_WORKLOAD_BALANCE/OPERATIONAL_DEFICIENCY_INVENTORY are now real
    ``plan_registry`` entries (``WAVE_3_1_PLANS_BY_INTENT``), so
    STACK3_WIRING_GAP_REASON no longer applies to those four intents. Most
    rows below flip to ``proven_unit``, backed by existing CHAOS-3303/3304/
    3305/3302 service-layer tests -- unaffected by the separate, deeper
    CHAOS-3337 persistence-layer defect a live attempt against these same
    four intents found (see STACK3_PERSISTENCE_GAP_REASON and
    matrix.stack3-intents.e2e-blocked-by-live-defect): ``proven_unit`` never
    claimed live reachability, only that the service's own evaluation logic
    is correct, and that claim is untouched by a bug in a different layer
    (the orchestrator's persistence write path) these unit tests never
    exercise. PORTFOLIO_STATUS stays deliberately, permanently unwired this
    wave (wave_3_1_plans.py's own module docstring: a real StepContext
    single-DevScope limitation, tracked on the CHAOS-3297 Linear issue) --
    its row stays honestly blocked, not force-fit to a citation that does
    not actually prove it.
    """

    return (
        ManifestItem(
            id="matrix.legitimate-org-wide-status",
            category="blocking_matrix",
            description=(
                "Legitimate organization-wide status. Note: ENTITY_STATUS "
                "was always a core-wired intent (one of the original 6 in "
                "CORE_PLANS_BY_INTENT) -- this row was never actually "
                "blocked by the stack-3 wiring gap the other 12 rows in "
                "this function were; it is a manifest bookkeeping catch-up, "
                "not a new proof."
            ),
            status="proven_unit",
            evidence=("tests/api/dev/test_chaos_3292_preflight_acceptance.py",),
            content_markers=("test_a4_organization_wide_question_executes_normally",),
            test_nodeids=(
                "tests/api/dev/test_chaos_3292_preflight_acceptance.py::"
                "test_a4_organization_wide_question_executes_normally",
            ),
        ),
        ManifestItem(
            id="matrix.organization-portfolio-status",
            category="blocking_matrix",
            description="Organization portfolio status.",
            status="blocked",
            blocked_reason=(
                "PORTFOLIO_STATUS is deliberately, not accidentally, "
                "unwired this wave -- wave_3_1_plans.py's own module "
                "docstring: PlanExecutor's StepContext carries exactly one "
                "DevScope, but PortfolioStatusService.evaluate_portfolio "
                "needs several project scopes at once, a real gap that "
                "needs a StepContext-widening decision before this can be "
                "wired (tracked on the CHAOS-3297 Linear issue). The "
                "closest existing test, "
                "test_chaos_3303_portfolio_status_service.py::"
                "test_evaluate_portfolio_all_provisional_registry_reports_no_elevated_state, "
                "is service-layer only and does not exercise the plan/"
                "orchestrator path this row's claim needs -- not cited as "
                "proof it would be overclaiming. The GAP itself being "
                "handled honestly (loud, non-terminal fallback) is a "
                "separate, already-proven claim -- see "
                "gate.plan-registry-gap-is-loud and its e2e-live-validated "
                "sibling."
            ),
        ),
        ManifestItem(
            id="matrix.project-health-mixed-dimensions",
            category="blocking_matrix",
            description=(
                "Project health with mixed dimension states -- proven as "
                "HEALTHY and UNKNOWN dimensions coexisting independently "
                "in one profile, no leakage between them. Caveat: the "
                "cited test's own docstring also mentions 'not_applicable', "
                "but every dimension it actually asserts resolves to "
                "HEALTHY or UNKNOWN, never DimensionState.NOT_APPLICABLE -- "
                "this row's claim is scoped to what is actually asserted, "
                "not the test's looser prose."
            ),
            status="proven_unit",
            evidence=("tests/api/dev/test_chaos_3303_health_profile_synthesis.py",),
            content_markers=(
                "test_mixed_profile_reports_complete_unknown_and_not_applicable_dimensions",
            ),
            test_nodeids=(
                "tests/api/dev/test_chaos_3303_health_profile_synthesis.py::"
                "test_mixed_profile_reports_complete_unknown_and_not_applicable_dimensions",
            ),
        ),
        ManifestItem(
            id="matrix.project-health-unknown-not-applicable",
            category="blocking_matrix",
            description=("Project health with unknown and not-applicable dimensions."),
            status="blocked",
            blocked_reason=PROJECT_HEALTH_UNREACHABLE_INPUT_REASON,
        ),
        ManifestItem(
            id="matrix.team-health-complete-attribution",
            category="blocking_matrix",
            description="Named team health with complete team attribution.",
            status="proven_unit",
            evidence=("tests/api/dev/test_chaos_3303_team_health_service.py",),
            content_markers=(
                "test_evaluate_team_with_attribution_and_real_facts_reports_real_findings",
            ),
            test_nodeids=(
                "tests/api/dev/test_chaos_3303_team_health_service.py::"
                "test_evaluate_team_with_attribution_and_real_facts_reports_real_findings",
            ),
        ),
        ManifestItem(
            id="matrix.team-health-unattributable-signals",
            category="blocking_matrix",
            description=(
                "Team health where repository/org signals are available "
                "but cannot be attributed to the team."
            ),
            status="proven_unit",
            evidence=("tests/api/dev/test_chaos_3303_team_health_service.py",),
            content_markers=(
                "test_evaluate_team_zero_attribution_suppresses_even_with_real_facts",
            ),
            test_nodeids=(
                "tests/api/dev/test_chaos_3303_team_health_service.py::"
                "test_evaluate_team_zero_attribution_suppresses_even_with_real_facts",
            ),
            # Same test attack.team-attribution cites (proven_unit, its own
            # row) -- legitimate dual-citation: that row's claim is about
            # EXCLUSION from findings under zero attribution; this row's is
            # about the resulting TEAM_HEALTH dimension-state outcome. One
            # real scenario proves both angles.
        ),
        ManifestItem(
            id="matrix.struggling-teams-positive",
            category="blocking_matrix",
            description=(
                "'Are there any struggling teams?' with a valid sustained "
                "multi-signal positive case."
            ),
            status="proven_unit",
            evidence=("tests/api/dev/test_chaos_3302_health_rule_e2e_controls.py",),
            content_markers=(
                "test_positive_two_independent_at_risk_dimensions_qualify_team_needs_attention",
            ),
            test_nodeids=(
                "tests/api/dev/test_chaos_3302_health_rule_e2e_controls.py::"
                "test_positive_two_independent_at_risk_dimensions_qualify_team_needs_attention",
            ),
        ),
        ManifestItem(
            id="matrix.struggling-teams-insufficient-sample",
            category="blocking_matrix",
            description=(
                "The struggling-teams question with insufficient "
                "sample/coverage and no unsupported label."
            ),
            status="proven_unit",
            evidence=("tests/api/dev/test_chaos_3302_health_rule_e2e_controls.py",),
            content_markers=("test_negative_cohort_below_minimum_suppresses_finding",),
            test_nodeids=(
                "tests/api/dev/test_chaos_3302_health_rule_e2e_controls.py::"
                "test_negative_cohort_below_minimum_suppresses_finding",
            ),
        ),
        ManifestItem(
            id="matrix.overburdened-teams-with-denominators",
            category="blocking_matrix",
            description=(
                "'Which teams are overburdened?' with approved denominators/baselines."
            ),
            status="proven_unit",
            evidence=("tests/api/dev/test_chaos_3304_workload_health_rules.py",),
            content_markers=(
                "test_positive_two_independent_workload_dimensions_qualify",
            ),
            test_nodeids=(
                "tests/api/dev/test_chaos_3304_workload_health_rules.py::"
                "test_positive_two_independent_workload_dimensions_qualify",
            ),
        ),
        ManifestItem(
            id="matrix.pressure-without-denominator",
            category="blocking_matrix",
            description=(
                "High observed pressure without a valid denominator, "
                "producing pressure/not-calculable language."
            ),
            status="proven_unit",
            evidence=("tests/api/dev/test_chaos_3304_workload_health_rules.py",),
            content_markers=(
                "test_negative_review_request_load_without_denominator_is_not_calculable",
            ),
            test_nodeids=(
                "tests/api/dev/test_chaos_3304_workload_health_rules.py::"
                "test_negative_review_request_load_without_denominator_is_not_calculable",
            ),
        ),
        ManifestItem(
            id="matrix.light-on-feature-work",
            category="blocking_matrix",
            description=(
                "'Which teams are light on feature work?' with adequate "
                "investment-classification coverage."
            ),
            status="blocked",
            blocked_reason=FEATURE_WORK_ATTRIBUTION_BLOCKED_REASON,
        ),
        ManifestItem(
            id="matrix.light-on-feature-work-unclassified",
            category="blocking_matrix",
            description=(
                "The feature-work question with high unclassified work and "
                "no unsupported conclusion."
            ),
            status="proven_unit",
            evidence=("tests/api/dev/test_chaos_3304_workload_health_rules.py",),
            content_markers=(
                "test_negative_investment_shift_insufficient_coverage_suppresses",
            ),
            test_nodeids=(
                "tests/api/dev/test_chaos_3304_workload_health_rules.py::"
                "test_negative_investment_shift_insufficient_coverage_suppresses",
            ),
        ),
        ManifestItem(
            id="matrix.operational-deficiencies-mixed",
            category="blocking_matrix",
            description=(
                "'What operational deficiencies do we have?' with mixed "
                "applicable, not-applicable, stale, unavailable, and "
                "unconfigured rules."
            ),
            status="proven_unit",
            evidence=(
                "tests/api/dev/test_chaos_3305_operational_deficiency_service.py",
            ),
            content_markers=(
                "test_data_integration_distinguishes_stale_missing_and_unconfigured",
            ),
            test_nodeids=(
                "tests/api/dev/test_chaos_3305_operational_deficiency_service.py::"
                "test_data_integration_distinguishes_stale_missing_and_unconfigured",
                "tests/api/dev/test_chaos_3305_operational_deficiency_service.py::"
                "test_rule_driven_category_status_partial_when_launch_and_suppressed_coexist",
            ),
            # Jointly: the first proves the mixed stale/missing/unconfigured
            # source-availability states this row's description names; the
            # second proves the mixed applicable/not-applicable rule-status
            # states (launch vs suppressed) coexisting. Neither alone covers
            # the full "mixed ... rules" claim.
        ),
        ManifestItem(
            id="matrix.unwired-intent-safe-fallback",
            category="blocking_matrix",
            description=(
                "A safe, honest outcome for a question whose intent has no "
                "wired plan -- traced, not guessed. Team-lead guidance "
                "2026-08-02: 'don't leave presumably UNSUPPORTED in the "
                "manifest -- one traced run, assert the exact outcome/code'. "
                "HISTORICAL NOTE (2026-08-03): originally traced against "
                "PROJECT_HEALTH/TEAM_HEALTH as the live examples, before the "
                "CHAOS-3297 stack-3 wiring landed (ops #1383/#1387) -- both "
                "are now real plan_registry entries, so this exact shape no "
                "longer describes their live behavior (see "
                "STACK3_PERSISTENCE_GAP_REASON: they now reach the "
                "plan-governed path and crash, rather than safely falling "
                "through). The cited tests below still prove the general "
                "mechanism correctly (they construct their own registry "
                "with the intent absent, the same technique "
                "gate.plan-registry-gap-is-loud's tests use) -- "
                "PORTFOLIO_STATUS is the current live example of this row's "
                "claim; see gate.plan-registry-gap-is-loud and its "
                "e2e-live-validated sibling for that proof."
            ),
            status="proven_unit",
            evidence=("tests/api/dev/test_chaos_3300_unwired_intent_fallback.py",),
            content_markers=(
                "test_project_health_question_falls_through_to_the_legacy_loop_not_a_plan",
            ),
            test_nodeids=(
                "tests/api/dev/test_chaos_3300_unwired_intent_fallback.py::"
                "test_project_health_question_falls_through_to_the_legacy_loop_not_a_plan",
                "tests/api/dev/test_chaos_3300_unwired_intent_fallback.py::"
                "test_team_health_question_also_falls_through_when_subject_resolves",
            ),
            # Traced actual behavior (orchestrator.py:967-969): the intent IS
            # still correctly interpreted; the plan-governed investigation
            # path is skipped (plan_registry.get returns None ->
            # plan_eligible=False); the run falls through to the legacy
            # pre-CHAOS-3295 model-tool-choice loop, which answers using
            # only the generic 9-tool registry (observed: status_snapshot.v1
            # alone) -- never a fabricated health verdict, never a crash,
            # but also never a real project/team-health profile. This is
            # safe-by-construction (CHAOS-3289's grounding guard still
            # applies) but is a silent capability downgrade, not a clean
            # "not supported yet" -- worth stack-3 landing a dedicated
            # UNSUPPORTED short-circuit rather than relying on the legacy
            # loop's grounding guard as the only backstop.
        ),
        # matrix.stack3-intents.e2e-blocked-by-live-defect (2026-08-03,
        # historical): live end-to-end proof attempt for the four
        # CHAOS-3297 stack-3 newly-wired intents, blocked by CHAOS-3337 (a
        # persistence-layer _SOURCE_CLASSES allowlist gap one layer beneath
        # the resolved wiring gap). CHAOS-3337 shipped (ops #1402, both the
        # Python allowlist and the mirrored Postgres CHECK constraint,
        # migrations 0081/0082) the same day -- re-run confirmed all three
        # TEAM-subject intents now complete live. Replaced below by three
        # proven_e2e rows rather than flipped in place: this row's single
        # claim covered three distinct intents with three distinct
        # artifacts, which the execution_artifact field (one path per row)
        # cannot represent as one row. STACK3_PERSISTENCE_GAP_REASON is
        # kept, still exported and tested, as the historical record of the
        # defect this row existed to track.
        ManifestItem(
            id="matrix.team-health.e2e-live-validated",
            category="blocking_matrix",
            description=(
                "TEAM_HEALTH live end-to-end: health.team.v1 (CHAOS-3297 "
                "stack-3, wired via ops #1383/#1387) reachable and "
                "answering for a named team subject, over the real "
                "Compose stack. Proven only after CHAOS-3337 shipped (ops "
                "#1402) -- before that fix this crashed with "
                "DevPersistenceValidationError('invalid source_class') on "
                "every run; see STACK3_PERSISTENCE_GAP_REASON for the "
                "original repro. Codex finding (HIGH, 2026-08-03, round "
                "4): scope+commit+non-error alone does not prove the "
                "CLAIMED plan ran -- a legacy-loop fallback (portfolio_"
                "status_gap's own proof) would pass those checks too. "
                "Closed by reading the persisted dev_runs row directly "
                "(preflight_outcome + plan_step_partition containing "
                "health.team.v1's own 'health_evaluation' mandatory step) "
                "-- never the scripted provider's intent-blind narrative. "
                "Negative-control demonstration performed once (recorded "
                "in the commit message, not automated as a permanent test "
                "since it needs editing the production plan registry): "
                "with health.team.v1 temporarily removed from the "
                "registry, this scenario's OLD assertions all still "
                "passed and only the new plan-step assertion caught the "
                "silently-absorbed missing plan."
            ),
            status="proven_e2e",
            evidence=(
                "scripts/acceptance/smoke_ask_dev_stack3_intents.py",
                "tests/acceptance/test_ask_dev_stack3_intents_smoke.py",
            ),
            requires_live_infra=True,
            execution_artifact="tests/acceptance/artifacts/team_health.json",
            required_assertion_names=(
                "scope_resolved_event_present",
                "named_team_committed",
                "answer_completed_event_present",
                "answer_status_not_hard_error",
                "preflight_proceeded_committed_subject",
                "claimed_plan_step_completed",
                "stream_terminated_as_answer",
            ),
        ),
        ManifestItem(
            id="matrix.team-workload-balance.e2e-live-validated",
            category="blocking_matrix",
            description=(
                "TEAM_WORKLOAD_BALANCE live end-to-end: balance.team_"
                "workload.v1 reachable and answering for a named team "
                "subject, over the real Compose stack. Proven only after "
                "CHAOS-3337 shipped (ops #1402) -- same root cause and "
                "fix as matrix.team-health.e2e-live-validated. Proves "
                "reachability, not the specific 'overburdened'/'light on "
                "feature work' launch-finding shapes -- those stay at "
                "matrix.overburdened-teams-with-denominators/matrix."
                "pressure-without-denominator's existing proven_unit level "
                "and matrix.light-on-feature-work's honest blocked state. "
                "Codex finding (HIGH, 2026-08-03, round 4): asserts "
                "balance.team_workload.v1's own mandatory step ('workload_"
                "evaluation') actually completed via a direct dev_runs "
                "read, not merely a non-error answer a legacy-loop "
                "fallback would also produce -- same fix and negative-"
                "control pattern as matrix.team-health.e2e-live-validated."
            ),
            status="proven_e2e",
            evidence=(
                "scripts/acceptance/smoke_ask_dev_stack3_intents.py",
                "tests/acceptance/test_ask_dev_stack3_intents_smoke.py",
            ),
            requires_live_infra=True,
            execution_artifact="tests/acceptance/artifacts/team_workload_balance.json",
            required_assertion_names=(
                "scope_resolved_event_present",
                "named_team_committed",
                "answer_completed_event_present",
                "answer_status_not_hard_error",
                "preflight_proceeded_committed_subject",
                "claimed_plan_step_completed",
                "stream_terminated_as_answer",
            ),
        ),
        ManifestItem(
            id="matrix.operational-deficiency.e2e-live-validated",
            category="blocking_matrix",
            description=(
                "OPERATIONAL_DEFICIENCY_INVENTORY live end-to-end: "
                "deficiency.operational.v1 reachable and answering for a "
                "named team subject, over the real Compose stack. Proven "
                "only after CHAOS-3337 shipped (ops #1402) -- same root "
                "cause and fix as matrix.team-health.e2e-live-validated. "
                "Proves reachability, not the specific mixed-rule-status "
                "shape matrix.operational-deficiencies-mixed's proven_unit "
                "citation covers. Codex finding (HIGH, 2026-08-03, round "
                "4): asserts deficiency.operational.v1's own mandatory "
                "step ('deficiency_evaluation') actually completed via a "
                "direct dev_runs read -- same fix and negative-control "
                "pattern as matrix.team-health.e2e-live-validated."
            ),
            status="proven_e2e",
            evidence=(
                "scripts/acceptance/smoke_ask_dev_stack3_intents.py",
                "tests/acceptance/test_ask_dev_stack3_intents_smoke.py",
            ),
            requires_live_infra=True,
            execution_artifact=(
                "tests/acceptance/artifacts/operational_deficiency_team.json"
            ),
            required_assertion_names=(
                "scope_resolved_event_present",
                "named_team_committed",
                "answer_completed_event_present",
                "answer_status_not_hard_error",
                "preflight_proceeded_committed_subject",
                "claimed_plan_step_completed",
                "stream_terminated_as_answer",
            ),
        ),
    )


def _gates() -> tuple[ManifestItem, ...]:
    return (
        ManifestItem(
            id="gate.deterministic",
            category="gate",
            description=(
                "The scripted-provider and canonical-service matrix passes "
                "100% with no substantive skips; assertions are "
                "machine-readable."
            ),
            status="proven_unit",
            evidence=("tests/acceptance/test_wave31_manifest.py",),
            # Deliberately NOT test_build_report_shape_over_the_real_manifest
            # or anything else that calls build_report()/execute_manifest():
            # this item's own test_nodeids are executed BY execute_manifest,
            # so citing a test that itself invokes execute_manifest would
            # recurse. test_the_landed_manifest_has_no_integrity_errors only
            # calls the non-recursive validate_manifest().
            test_nodeids=(
                "tests/acceptance/test_wave31_manifest.py::"
                "test_the_landed_manifest_has_no_integrity_errors",
            ),
        ),
        ManifestItem(
            id="gate.repeated-certified-provider",
            category="gate",
            description=(
                "For each certified provider profile and enabled role, run "
                "at least 20 repetitions of critical paths at the stated "
                "thresholds."
            ),
            status="deferred",
            blocked_reason=(
                "scripts/acceptance/run_ask_dev_provider_profile.sh + "
                "smoke_ask_dev_provider_profile.py already wire the "
                "lmstudio-local/ollama-local/ollama-cloud profile mechanics, "
                "but running 20 reps against real certified providers "
                "requires live credentials/endpoints not available in this "
                "sandboxed environment; missing credentials must mark the "
                "provider-profile gate non-passing per CHAOS-3300, not "
                "silently green"
            ),
        ),
        ManifestItem(
            id="gate.migration-coexistence",
            category="gate",
            description=(
                "Prove Python/Go coexistence, cutover, rollback, and "
                "decommission per the CHAOS-3306 selected design."
            ),
            status="blocked",
            blocked_reason=MIGRATION_COEXISTENCE_REASON,
        ),
        ManifestItem(
            id="gate.plan-registry-gap-is-loud",
            category="gate",
            description=(
                "A recognized-but-currently-unwired intent (PORTFOLIO_STATUS "
                "today) must fall back to the legacy model-round loop "
                "LOUDLY -- a structured WARNING log record plus "
                "ASK_DEV_PLAN_REGISTRY_GAP_TOTAL -- and still complete as a "
                "real, non-error answer, never terminate. Team-lead "
                "ratification (2026-08-02): this is a provable row, not an "
                "acknowledged gap -- the fallback IS the designed, correct "
                "behavior until the stack-5 guard cutover, and "
                "BOUNDED_INVESTIGATION's separate, always-silent fallthrough "
                "must never trigger the same signal."
            ),
            status="proven_unit",
            evidence=("tests/api/dev/test_chaos_3300_plan_registry_gap.py",),
            content_markers=(
                "test_plan_registry_gap_is_loud_for_a_normally_plan_governed_intent",
            ),
            test_nodeids=(
                "tests/api/dev/test_chaos_3300_plan_registry_gap.py::"
                "test_plan_registry_gap_is_loud_for_a_normally_plan_governed_intent",
                "tests/api/dev/test_chaos_3300_plan_registry_gap.py::"
                "test_bounded_investigation_never_triggers_the_gap_signal",
            ),
        ),
        ManifestItem(
            id="gate.plan-registry-gap-is-loud.e2e-live-validated",
            category="gate",
            description=(
                "The same plan_registry_gap fallback, proven over the real "
                "live Compose stack against PORTFOLIO_STATUS -- the intent "
                "actually in this gap today, not a simulated one. A "
                "portfolio-shaped question (\"What's the status of all our "
                'projects across the portfolio?") ran through many legacy '
                "model-round progress events (confirming the fallback loop "
                "actually engaged, not a fast-path), committed a real "
                "scope.resolved event, and completed as status EXACTLY "
                "'partial' (the legacy-loop budget-exhaustion outcome, not "
                "any non-error status a lucky 'complete' would also "
                "satisfy) rather than hanging or terminating. Codex finding "
                "(HIGH, 2026-08-03, round 3): the prior version of this row "
                "passed without requiring any of that -- scope resolution "
                "was only checked for team-scoped scenarios (portfolio is "
                "not), no assertion pinned the exact status, and 'not "
                "error' alone is satisfied by complete/partial/degraded "
                "alike. Fixed by asserting the specific state directly. "
                "Live-confirmed 2026-08-03 there is NO client-observable "
                "signal for the WARNING log line or "
                "ASK_DEV_PLAN_REGISTRY_GAP_TOTAL counter this row's other "
                "half of the claim needs: answer.warnings for this "
                "scenario is generic scripted-provider boilerplate "
                '("Deterministic scripted acceptance response.", '
                '"Provider health was measured through data_health.v1."), '
                "identical regardless of question, not text tied to the "
                "gap signal -- recorded as an informational assertion, not "
                "claimed as proof of the signal. gate.plan-registry-gap-is-"
                "loud proves that signal at the unit level, through the "
                "real orchestrator seam, the only place it is observable."
            ),
            status="proven_e2e",
            evidence=(
                "scripts/acceptance/smoke_ask_dev_stack3_intents.py",
                "tests/acceptance/test_ask_dev_stack3_intents_smoke.py",
            ),
            requires_live_infra=True,
            execution_artifact="tests/acceptance/artifacts/portfolio_status_gap.json",
            required_assertion_names=(
                "scope_resolved_event_present",
                "answer_completed_event_present",
                "answer_status_is_exactly_partial",
                "stream_terminated_as_answer",
                "warnings_present_but_not_a_plan_registry_gap_signal",
            ),
        ),
        ManifestItem(
            id="gate.web-default-ci",
            category="gate",
            description=(
                "Default required Playwright CI covers answer-v2 outcomes, "
                "subjects, rendering, and window/dev semantic equivalence."
            ),
            status="deferred",
            blocked_reason=(
                "Re-checked 2026-08-03 per team-lead priority: web #833 "
                "(CHAOS-3287, merged to dev-health-web@main at "
                "a24de7c90dafa98c4f46f0cf5fc4a76cf6767023) landed real, "
                "default (not opt-in) Playwright coverage -- verified "
                "directly against the merged spec files, not the PR "
                "description alone: tests/ask-dev-continuity.spec.ts "
                "proves window/dev semantic equivalence (a conversation "
                "started in either surface resumes with the same "
                "transcript and rendered answer in the other, both "
                "directions); tests/ask-dev-outcomes.spec.ts and "
                "tests/ask-dev-shared.spec.ts cover every rendered "
                "dev_answer.v1 status, evidence-first hierarchy, and "
                "availability gating; tests/ask-dev-vocabulary.spec.ts is "
                "the internal-enum-leak denylist (CHAOS-3291 cross-check). "
                "This row's own claim names 'answer-v2 outcomes' "
                "specifically, and #833's own PR body is explicit that "
                "dev_answer.v2's outcome taxonomy is separate CHAOS-3294/"
                "3298 work not yet landed in web -- honoring 'claim what "
                "you assert', this row stays deferred rather than "
                "force-flipped, narrowed to exactly the gap #833 left: "
                "v2 outcome coverage. Subjects/rendering/window-dev "
                "equivalence are otherwise real, live, default-suite "
                "coverage today, not aspirational."
            ),
        ),
        ManifestItem(
            id="gate.content-security-audit",
            category="gate",
            description=(
                "Scan logs, traces, metric labels, persistence metadata, CI "
                "artifacts, and retained reports for the listed leakage "
                "classes."
            ),
            status="deferred",
            blocked_reason=(
                "requires a live deployed system's logs/traces/artifacts to "
                "scan; a static/unit suite cannot prove absence in "
                "production telemetry"
            ),
        ),
    )


def _mutation_proofs() -> tuple[ManifestItem, ...]:
    return (
        ManifestItem(
            id="mutation.bypass-subject-preflight",
            category="mutation_proof",
            description="Bypass subject preflight.",
            status="proven_unit",
            evidence=("tests/api/dev/test_chaos_3292_mutations.py",),
            content_markers=(
                "test_m1a_pre_loop_gate_defeated_is_still_caught_at_dispatch",
            ),
            # Kill site: the assertion inside the test body itself (the
            # dispatch-time gate), not a shared setup fixture -- m1b proves
            # the second, independent gate the same way.
            test_nodeids=(
                "tests/api/dev/test_chaos_3292_mutations.py::"
                "test_m1a_pre_loop_gate_defeated_is_still_caught_at_dispatch",
                "tests/api/dev/test_chaos_3292_mutations.py::"
                "test_m1b_dispatch_gate_defeated_is_still_caught_before_the_loop",
            ),
        ),
        ManifestItem(
            id="mutation.remove-mention-from-set",
            category="mutation_proof",
            description="Remove one mention from a subject set.",
            status="proven_unit",
            evidence=("tests/api/dev/test_chaos_3292_mutations.py",),
            content_markers=(
                "test_m2_multi_mention_fallthrough_is_caught_in_both_orders",
            ),
            test_nodeids=(
                "tests/api/dev/test_chaos_3292_mutations.py::"
                "test_m2_multi_mention_fallthrough_is_caught_in_both_orders",
            ),
        ),
        ManifestItem(
            id="mutation.restore-org-fallback",
            category="mutation_proof",
            description="Restore organization fallback.",
            status="proven_unit",
            evidence=("tests/api/dev/test_chaos_3292_mutations.py",),
            content_markers=(
                "test_m2_no_authorized_match_must_not_fall_through_to_organization_scope",
            ),
            test_nodeids=(
                "tests/api/dev/test_chaos_3292_mutations.py::"
                "test_m2_no_authorized_match_must_not_fall_through_to_organization_scope",
            ),
        ),
        ManifestItem(
            id="mutation.no-data-to-zero",
            category="mutation_proof",
            description="Convert no data to zero.",
            status="proven_unit",
            evidence=(
                "tests/api/dev/test_chaos_3303_dimension_observation_adapters.py",
            ),
            content_markers=(
                "test_data_trust_no_sources_is_no_data_not_measured_zero",
            ),
            test_nodeids=(
                "tests/api/dev/test_chaos_3303_dimension_observation_adapters.py::"
                "test_data_trust_no_sources_is_no_data_not_measured_zero",
            ),
        ),
        ManifestItem(
            id="mutation.remove-relationship-filtering",
            category="mutation_proof",
            description="Remove relationship filtering.",
            status="proven_unit",
            evidence=("tests/api/dev/test_chaos_3296_relationship_closure.py",),
            content_markers=(
                "test_an_edge_unrelated_to_the_committed_subject_fails_closed",
            ),
            test_nodeids=(
                "tests/api/dev/test_chaos_3296_relationship_closure.py::"
                "test_an_edge_unrelated_to_the_committed_subject_fails_closed",
            ),
        ),
        ManifestItem(
            id="mutation.completion-without-denominator",
            category="mutation_proof",
            description="Calculate completion without a complete denominator.",
            status="proven_unit",
            evidence=("tests/api/dev/test_status_change_service.py",),
            content_markers=(
                "test_completed_parent_with_incomplete_required_child_is_not_ready",
            ),
            test_nodeids=(
                "tests/api/dev/test_status_change_service.py::"
                "test_completed_parent_with_incomplete_required_child_is_not_ready",
            ),
        ),
        ManifestItem(
            id="mutation.burden-without-denominator",
            category="mutation_proof",
            description="Calculate burden without a denominator/baseline.",
            status="proven_unit",
            evidence=(
                "tests/api/dev/test_chaos_3304_workload_observation_adapters.py",
            ),
            content_markers=(
                "test_review_request_load_without_denominator_reports_raw_value_not_calculable",
            ),
            test_nodeids=(
                "tests/api/dev/test_chaos_3304_workload_observation_adapters.py::"
                "test_review_request_load_without_denominator_reports_raw_value_not_calculable",
                "tests/api/dev/test_chaos_3304_workload_observation_adapters.py::"
                "test_review_request_load_zero_active_contributors_is_not_calculable",
            ),
        ),
        ManifestItem(
            id="mutation.single-signal-struggling-team",
            category="mutation_proof",
            description="Enable a single-signal struggling-team finding.",
            status="proven_unit",
            evidence=("tests/api/dev/test_chaos_3304_workload_health_rules.py",),
            content_markers=(
                "test_negative_single_at_risk_dimension_does_not_qualify",
            ),
            test_nodeids=(
                "tests/api/dev/test_chaos_3304_workload_health_rules.py::"
                "test_negative_single_at_risk_dimension_does_not_qualify",
            ),
        ),
        ManifestItem(
            id="mutation.expose-forbidden-or-not-found",
            category="mutation_proof",
            description="Expose forbidden_or_not_found as display copy.",
            status="proven_unit",
            # preflight_outcomes.py's own docstring claims unreachability by
            # construction; the executable proof that a leak is caught
            # (rather than merely asserted in a comment) is m3a, which
            # mutates the canonical no-answer copy table to inject a leaky
            # token and observes the termination fail closed.
            evidence=("tests/api/dev/test_chaos_3292_mutations.py",),
            content_markers=(
                "test_m3a_leaky_canonical_copy_cannot_be_constructed_at_all",
            ),
            test_nodeids=(
                "tests/api/dev/test_chaos_3292_mutations.py::"
                "test_m3a_leaky_canonical_copy_cannot_be_constructed_at_all",
            ),
        ),
        ManifestItem(
            id="mutation.disable-deterministic-fallback",
            category="mutation_proof",
            description="Disable deterministic fallback.",
            status="proven_unit",
            evidence=("tests/api/dev/test_chaos_3297_frame_reachability.py",),
            content_markers=(
                "test_replay_with_corrupted_frame_payload_falls_back_safely",
            ),
            test_nodeids=(
                "tests/api/dev/test_chaos_3297_frame_reachability.py::"
                "test_replay_with_corrupted_frame_payload_falls_back_safely",
                "tests/api/dev/test_chaos_3297_frame_reachability.py::"
                "test_replay_with_mismatched_frame_outcome_falls_back_safely",
            ),
        ),
        ManifestItem(
            id="mutation.duplicate-run-on-reconnect",
            category="mutation_proof",
            description="Duplicate a run during expand/reconnect.",
            status="deferred",
            blocked_reason=(
                "reconnect/expand duplication is a persistence-layer "
                "concern (CHAOS-3299's replay work); no mutation test in "
                "this lane's evidence set exercises it yet"
            ),
        ),
        ManifestItem(
            id="mutation.mutate-fixture-differently-go-python",
            category="mutation_proof",
            description="Mutate the same fixture differently in Go and Python.",
            status="deferred",
            blocked_reason=(
                "requires the cross-repo differential harness attack."
                "runtime-divergence also needs (Web/API/MCP/Go/Python), "
                "which does not exist yet"
            ),
        ),
    )


def _build_manifest() -> tuple[ManifestItem, ...]:
    return (
        _core_defect_reproductions()
        + _real_project_positive_control()
        + _attacks()
        + _blocking_matrix_wired()
        + _blocking_matrix_blocked()
        + _gates()
        + _mutation_proofs()
    )


MANIFEST: tuple[ManifestItem, ...] = _build_manifest()


def _git_command(root: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args], cwd=root, capture_output=True, text=True, check=False
    )


def _history_is_truncated(root: Path) -> bool:
    """True when ``root``'s object store cannot answer reachability questions
    about older commits -- a shallow clone, or not a usable git repo at all."""

    result = _git_command(root, "rev-parse", "--is-shallow-repository")
    if result.returncode != 0:
        return True
    return result.stdout.strip() == "true"


#: A full, canonical git object id. Codex finding (HIGH, 2026-08-03): the
#: ancestry check passed ``commit_sha`` straight to ``git merge-base``,
#: which happily accepts any revision expression -- so an artifact
#: recording the literal string "HEAD" (or "HEAD^{commit}") validated
#: clean, because the expression resolves at VALIDATION time to whatever
#: HEAD is now rather than naming the commit that allegedly ran. An
#: artifact must name an immutable object, so only a 40-character hex id
#: is accepted; symbolic refs, tags, and abbreviations are rejected before
#: git is consulted at all.
_CANONICAL_COMMIT_SHA = re.compile(r"\A[0-9a-f]{40}\Z")


def _ancestry_binding_errors(
    root: Path, *, item_id: str, label: str, commit_sha: str
) -> list[str]:
    """Bind an artifact's recorded ``commit_sha`` to the current HEAD.

    ``git merge-base --is-ancestor`` exits 1 for "genuinely unreachable" but
    128 for "I have never heard of that object", and a shallow checkout
    produces 128 for a perfectly real commit whose history was simply not
    fetched -- the two are indistinguishable from the exit status alone.
    Both remain FAILURES (this check never degrades to a skip: an unmeasured
    ancestry binding is not a satisfied one), but a truncated checkout gets
    its own message so the fix is "fetch the history", not "hunt a
    fabrication that isn't there". CI must therefore check out with full
    history (``actions/checkout`` ``fetch-depth: 0``); see
    ``.github/workflows/test.yml``.
    """

    if not _CANONICAL_COMMIT_SHA.fullmatch(commit_sha):
        return [
            f"{item_id}: {label}'s commit_sha {commit_sha!r} is not a "
            "canonical 40-character hexadecimal commit id. A symbolic ref, "
            "tag, abbreviation, or revision expression resolves at "
            "validation time instead of naming the immutable commit that "
            "actually ran, so it can never bind an artifact to anything."
        ]

    check = _git_command(root, "merge-base", "--is-ancestor", commit_sha, "HEAD")
    if check.returncode == 0:
        return []
    if _history_is_truncated(root):
        return [
            f"{item_id}: {label}'s commit {commit_sha} could not be checked "
            "against HEAD because this checkout's git history is truncated "
            "(shallow clone) -- the ancestry binding was NOT measured, which "
            "is a failure, never a pass. Check out with full history "
            "(actions/checkout fetch-depth: 0) and re-run."
        ]
    return [
        f"{item_id}: {label}'s commit {commit_sha} is not an ancestor of (or "
        "equal to) current HEAD -- fabricated, from an unrelated branch, or "
        "history was rewritten"
    ]


#: Same JWT-shaped pattern acceptance_artifact.redact_secrets guards against
#: at write time. This is the independent backstop codex asked for (HIGH,
#: 2026-08-02): redaction happening at write time does not by itself prove
#: no artifact on disk has a leaked secret -- an artifact could predate the
#: fix, or a future bypass could write one directly. validate_manifest scans
#: every proven_e2e artifact's raw bytes on every run, offline, so a leaked
#: credential fails the manifest the same way a fabricated claim does.
_ARTIFACT_JWT_PATTERN = re.compile(
    r"eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}"
)


def validate_execution_artifact(root: Path, item: ManifestItem) -> list[str]:
    """Check one ``proven_e2e`` item's execution artifact is real, current,
    all-passing, and actually bound to this row's claim. See
    ``acceptance_artifact.py`` for the artifact schema and why the commit
    check is "is an ancestor of HEAD", not "equals HEAD".

    Codex finding (HIGH, 2026-08-02): before this existed, a ``proven_e2e``
    row needed no machine-verifiable execution -- an existing evidence file
    plus ``requires_live_infra=True`` validated clean, including a row that
    was fabricated and never run. Codex finding (HIGH, 2026-08-02, round 2):
    "an artifact exists and all-passes" still did not bind an artifact to
    the row citing it (an artifact swapped from a different scenario, or one
    whose ``assertions`` list contains non-dict junk that silently sorts out
    of the failing check, both validated clean), nor prove the tree that
    produced it was actually clean (an artifact recorded against a dirty
    working tree does not reliably describe what commit_sha contains). Every
    check here closes one specific way a claim could still be false: no
    artifact at all, unparseable, a raw JWT/bearer token leaked into it,
    non-dict assertion entries, a failing or missing required assertion,
    generated from a commit that never led to the current tree, generated
    against a dirty tree, a script edited since it ran, or an artifact that
    belongs to a different scenario than the one this row cites.

    KNOWN LIMITATION, disclosed rather than hidden (codex finding, HIGH,
    2026-08-03, and NOT closed by this changeset). ``script_sha256`` binds
    the artifact to the smoke script's own bytes, and nothing else. A live
    scenario also executes a great deal of code the artifact does not
    fingerprint -- the scripted OpenAI-compatible provider it drives, the
    ``acceptance_artifact`` recorder itself, and the whole API image -- so
    a commit that changes any of those leaves every existing artifact
    validating clean while the run it describes is no longer reproducible.
    That is a false green: the gate reports 0 errors when an executed
    dependency has genuinely drifted. Closing it means binding artifacts to
    a runtime-dependency digest (or the tested tree), which by construction
    invalidates all 14 current artifacts at once and therefore cannot land
    without a live re-mint of every scenario -- a session with Compose, not
    an edit. Until then, read a ``proven_e2e`` row as "this scenario passed
    at the recorded commit", not as "this scenario passes today".
    """

    errors: list[str] = []
    if item.execution_artifact is None:
        errors.append(
            f"{item.id}: proven_e2e with no execution_artifact -- an "
            "evidence file existing is not proof the scenario was ever run"
        )
        return errors

    artifact_path = root / item.execution_artifact
    if not artifact_path.exists():
        errors.append(
            f"{item.id}: execution artifact does not exist: {item.execution_artifact}"
        )
        return errors

    raw_text = artifact_path.read_text(encoding="utf-8")
    if _ARTIFACT_JWT_PATTERN.search(raw_text):
        errors.append(
            f"{item.id}: execution artifact {item.execution_artifact} "
            "contains a JWT-shaped token -- a committed artifact must never "
            "carry a live credential; redact at the recorder and re-mint"
        )

    try:
        artifact = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        errors.append(
            f"{item.id}: execution artifact {item.execution_artifact} is not "
            f"valid JSON: {exc}"
        )
        return errors

    if not isinstance(artifact, dict):
        errors.append(
            f"{item.id}: execution artifact {item.execution_artifact} is not "
            "a JSON object"
        )
        return errors

    if artifact.get("schema_version") != "ask_dev_acceptance_artifact.v1":
        errors.append(
            f"{item.id}: execution artifact schema_version is "
            f"{artifact.get('schema_version')!r}, expected "
            "'ask_dev_acceptance_artifact.v1'"
        )

    expected_scenario_id = Path(item.execution_artifact).stem
    actual_scenario_id = artifact.get("scenario_id")
    if actual_scenario_id != expected_scenario_id:
        errors.append(
            f"{item.id}: execution artifact scenario_id "
            f"{actual_scenario_id!r} does not match {expected_scenario_id!r} "
            "expected from its own filename -- possible artifact swap"
        )

    assertions = artifact.get("assertions")
    if not isinstance(assertions, list) or not assertions:
        errors.append(
            f"{item.id}: execution artifact records no assertions -- a "
            "run that measured nothing is not proof"
        )
    else:
        non_dict_entries = [
            entry for entry in assertions if not isinstance(entry, dict)
        ]
        if non_dict_entries:
            errors.append(
                f"{item.id}: execution artifact contains non-dict assertion "
                f"entries: {non_dict_entries!r} -- a malformed or fabricated "
                "assertions list is not proof"
            )
        dict_entries = [entry for entry in assertions if isinstance(entry, dict)]
        failing = [
            entry.get("name")
            for entry in dict_entries
            if entry.get("passed") is not True
        ]
        if failing:
            errors.append(
                f"{item.id}: execution artifact records failing assertion(s): {failing}"
            )
        if item.required_assertion_names:
            passed_names = {
                entry.get("name")
                for entry in dict_entries
                if entry.get("passed") is True
            }
            missing_required = [
                name
                for name in item.required_assertion_names
                if name not in passed_names
            ]
            if missing_required:
                errors.append(
                    f"{item.id}: execution artifact is missing required "
                    f"passed assertion(s) {missing_required} -- present "
                    "assertions do not bind this artifact to this row's "
                    "specific claim"
                )

    if artifact.get("status") != "passed":
        errors.append(
            f"{item.id}: execution artifact status is "
            f"{artifact.get('status')!r}, expected 'passed'"
        )

    if artifact.get("tree_clean") is not True:
        errors.append(
            f"{item.id}: execution artifact does not record a clean "
            f"working tree at run time (tree_clean={artifact.get('tree_clean')!r}) "
            "-- a dirty or unrecorded tree means commit_sha may not "
            "actually describe what ran"
        )

    commit_sha = artifact.get("commit_sha")
    if not isinstance(commit_sha, str) or not commit_sha:
        errors.append(f"{item.id}: execution artifact has no commit_sha recorded")
    else:
        errors.extend(
            _ancestry_binding_errors(
                root,
                item_id=item.id,
                label="execution artifact",
                commit_sha=commit_sha,
            )
        )

    script_relative = artifact.get("script")
    script_sha256 = artifact.get("script_sha256")
    if not isinstance(script_relative, str) or not script_relative:
        errors.append(f"{item.id}: execution artifact has no script recorded")
    elif not isinstance(script_sha256, str) or not script_sha256:
        errors.append(f"{item.id}: execution artifact has no script_sha256 recorded")
    else:
        if script_relative not in item.evidence:
            errors.append(
                f"{item.id}: execution artifact's script {script_relative!r} "
                f"is not among this item's own evidence paths {item.evidence!r}"
                " -- the artifact must belong to a script this row cites"
            )
        script_path = root / script_relative
        if not script_path.exists():
            errors.append(
                f"{item.id}: execution artifact's script no longer exists: "
                f"{script_relative}"
            )
        else:
            current_hash = hashlib.sha256(script_path.read_bytes()).hexdigest()
            if current_hash != script_sha256:
                errors.append(
                    f"{item.id}: execution artifact's script_sha256 does not "
                    f"match the current bytes of {script_relative} -- the "
                    "script changed since this artifact was generated; "
                    "re-run to refresh"
                )
    return errors


def validate_blocked_execution_artifact(root: Path, item: ManifestItem) -> list[str]:
    """Check one ``blocked`` item's supporting failed-attempt evidence is
    real, current, and genuinely records the specific expected failure --
    never used to promote the item's status, only to bind its staying-
    blocked claim to something checkable. See ``ManifestItem.blocked_
    execution_artifact``'s docstring for the codex finding this closes.
    """

    errors: list[str] = []
    if item.status != "blocked":
        errors.append(
            f"{item.id}: blocked_execution_artifact set on a non-blocked "
            f"item (status={item.status!r}) -- this field exists only to "
            "back a blocked claim, never to promote status"
        )
        return errors
    assert item.blocked_execution_artifact is not None

    artifact_path = root / item.blocked_execution_artifact
    if not artifact_path.exists():
        errors.append(
            f"{item.id}: blocked_execution_artifact does not exist: "
            f"{item.blocked_execution_artifact}"
        )
        return errors

    raw_text = artifact_path.read_text(encoding="utf-8")
    if _ARTIFACT_JWT_PATTERN.search(raw_text):
        errors.append(
            f"{item.id}: blocked_execution_artifact {item.blocked_execution_artifact} "
            "contains a JWT-shaped token -- a committed artifact must never "
            "carry a live credential; redact at the recorder and re-mint"
        )

    try:
        artifact = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        errors.append(
            f"{item.id}: blocked_execution_artifact "
            f"{item.blocked_execution_artifact} is not valid JSON: {exc}"
        )
        return errors

    if not isinstance(artifact, dict):
        errors.append(
            f"{item.id}: blocked_execution_artifact "
            f"{item.blocked_execution_artifact} is not a JSON object"
        )
        return errors

    if artifact.get("schema_version") != "ask_dev_acceptance_artifact.v1":
        errors.append(
            f"{item.id}: blocked_execution_artifact schema_version is "
            f"{artifact.get('schema_version')!r}, expected "
            "'ask_dev_acceptance_artifact.v1'"
        )

    # The one deliberate divergence from validate_execution_artifact: a
    # blocked row's supporting evidence must be a FAILURE, not a pass --
    # "status: passed" here would be self-contradictory (why is the row
    # still blocked?) and is rejected exactly as loudly as a missing
    # artifact would be for a proven_e2e claim.
    if artifact.get("status") != "failed":
        errors.append(
            f"{item.id}: blocked_execution_artifact status is "
            f"{artifact.get('status')!r}, expected 'failed' -- a blocked "
            "row's supporting evidence must record a genuine failure"
        )

    assertions = artifact.get("assertions")
    if not isinstance(assertions, list) or not assertions:
        errors.append(
            f"{item.id}: blocked_execution_artifact records no assertions "
            "-- a run that measured nothing is not evidence of anything"
        )
    else:
        non_dict_entries = [
            entry for entry in assertions if not isinstance(entry, dict)
        ]
        if non_dict_entries:
            errors.append(
                f"{item.id}: blocked_execution_artifact contains non-dict "
                f"assertion entries: {non_dict_entries!r}"
            )
        dict_entries = [entry for entry in assertions if isinstance(entry, dict)]
        if item.blocked_expected_failing_assertions:
            failing_names = {
                entry.get("name")
                for entry in dict_entries
                if entry.get("passed") is False
            }
            missing_expected_failures = [
                name
                for name in item.blocked_expected_failing_assertions
                if name not in failing_names
            ]
            if missing_expected_failures:
                errors.append(
                    f"{item.id}: blocked_execution_artifact does not record "
                    f"the expected failing assertion(s) {missing_expected_failures} "
                    "-- present failures do not bind this artifact to this "
                    "row's specific expected failure"
                )

    if artifact.get("tree_clean") is not True:
        errors.append(
            f"{item.id}: blocked_execution_artifact does not record a "
            f"clean working tree at run time (tree_clean="
            f"{artifact.get('tree_clean')!r})"
        )

    commit_sha = artifact.get("commit_sha")
    if not isinstance(commit_sha, str) or not commit_sha:
        errors.append(
            f"{item.id}: blocked_execution_artifact has no commit_sha recorded"
        )
    else:
        errors.extend(
            _ancestry_binding_errors(
                root,
                item_id=item.id,
                label="blocked_execution_artifact",
                commit_sha=commit_sha,
            )
        )

    script_relative = artifact.get("script")
    script_sha256 = artifact.get("script_sha256")
    if not isinstance(script_relative, str) or not script_relative:
        errors.append(f"{item.id}: blocked_execution_artifact has no script recorded")
    elif not isinstance(script_sha256, str) or not script_sha256:
        errors.append(
            f"{item.id}: blocked_execution_artifact has no script_sha256 recorded"
        )
    else:
        if item.evidence and script_relative not in item.evidence:
            errors.append(
                f"{item.id}: blocked_execution_artifact's script "
                f"{script_relative!r} is not among this item's own evidence "
                f"paths {item.evidence!r}"
            )
        script_path = root / script_relative
        if not script_path.exists():
            errors.append(
                f"{item.id}: blocked_execution_artifact's script no longer "
                f"exists: {script_relative}"
            )
        else:
            current_hash = hashlib.sha256(script_path.read_bytes()).hexdigest()
            if current_hash != script_sha256:
                errors.append(
                    f"{item.id}: blocked_execution_artifact's script_sha256 "
                    f"does not match the current bytes of {script_relative} "
                    "-- the script changed since this artifact was "
                    "generated; re-run to refresh"
                )
    return errors


def validate_manifest(
    root: Path, items: tuple[ManifestItem, ...] = MANIFEST
) -> list[str]:
    """Return integrity errors. An empty list means every claim is honest.

    Checked, in order: duplicate ids; unknown status values; a ``proven_*``
    item with no evidence; a ``blocked`` item with no ``blocked_reason``; an
    evidence path that does not exist under ``root``; a ``content_markers``
    string absent from its (first) evidence file; a ``proven_unit`` item that
    is not ``requires_live_infra`` but names no ``test_nodeids`` (a status
    resting on "a file with this name exists" rather than "this specific
    test currently passes" is not proof); a ``test_nodeids`` entry whose file
    half is not one of the item's own ``evidence`` paths (evidence and the
    thing actually executed must agree); a ``proven_e2e`` item's execution
    artifact (see :func:`validate_execution_artifact`) missing, unparseable,
    reporting anything other than every assertion passing, generated from a
    commit that is not an ancestor of current HEAD, or citing a script whose
    bytes have changed since.

    This function is deliberately fast and offline -- it does NOT run any
    live scenario itself, only checks artifacts scenarios already wrote and
    runs cheap local ``git`` queries. That keeps every commit's normal test
    run able to catch a stale or fabricated ``proven_e2e`` claim without
    needing Docker/Compose.
    """

    errors: list[str] = []
    seen_ids: set[str] = set()
    for item in items:
        if item.id in seen_ids:
            errors.append(f"{item.id}: duplicate id")
        seen_ids.add(item.id)

        if item.status not in _VALID_STATUSES:
            errors.append(f"{item.id}: unknown status {item.status!r}")
            continue

        if item.status in ("proven_e2e", "proven_unit") and not item.evidence:
            errors.append(f"{item.id}: {item.status} claims no evidence")

        if item.status == "blocked" and not item.blocked_reason:
            errors.append(f"{item.id}: blocked without a blocked_reason")

        if (
            item.status == "proven_unit"
            and not item.requires_live_infra
            and not item.test_nodeids
        ):
            errors.append(
                f"{item.id}: proven_unit with no test_nodeids -- a file "
                "existing is not proof that a test exercises the claim"
            )

        if item.status == "proven_e2e" and not item.requires_live_infra:
            errors.append(
                f"{item.id}: proven_e2e without requires_live_infra -- if it "
                "can run without Compose/Docker it should carry test_nodeids "
                "and be executed like any other claim"
            )

        if item.status == "proven_e2e":
            errors.extend(validate_execution_artifact(root, item))

        if item.blocked_execution_artifact is not None:
            errors.extend(validate_blocked_execution_artifact(root, item))

        for node_id in item.test_nodeids:
            file_part = node_id.split("::", 1)[0]
            if file_part not in item.evidence:
                errors.append(
                    f"{item.id}: test_nodeids entry {node_id!r} is not under "
                    f"any of this item's own evidence paths {item.evidence!r}"
                )

        for relative_path in item.evidence:
            if not (root / relative_path).exists():
                errors.append(
                    f"{item.id}: evidence file does not exist: {relative_path}"
                )

        if item.content_markers and item.evidence:
            first_evidence = root / item.evidence[0]
            if first_evidence.exists():
                text = first_evidence.read_text(encoding="utf-8", errors="replace")
                for marker in item.content_markers:
                    if marker not in text:
                        errors.append(
                            f"{item.id}: content marker {marker!r} not found "
                            f"in {item.evidence[0]}"
                        )
    return errors


def run_evidence_tests(root: Path, node_ids: tuple[str, ...]) -> dict[str, str]:
    """Actually execute every distinct pytest node id and report its outcome.

    Returns ``{node_id: outcome}`` where outcome is ``"passed"``,
    ``"failed"``, ``"error"``, or ``"not_collected"``. A node id absent from
    pytest's own short summary after the run is reported ``"not_collected"``
    -- it is never assumed to have passed just because nothing said
    otherwise. This is the "a measurement that did not happen must FAIL"
    rule applied to this manifest's own proof: citing a test file is not
    proof the cited test runs, let alone passes.
    """

    if not node_ids:
        return {}
    unique_ids = sorted(set(node_ids))
    # sys.executable, not root/.venv/bin/python: this must also work when
    # `root` is a throwaway tmp_path (the guard-behavior tests below), which
    # has no venv of its own but does need pytest on its path -- the
    # interpreter already running this process (started via .venv/bin/pytest
    # in the real case) is always correct.
    #
    # -v (verbose per-test result lines), not -rA (the short summary at the
    # end): codex finding (MED, 2026-08-02) -- pytest's short-summary SKIPPED
    # line has the form "SKIPPED [1] file:line: reason", which carries no
    # parseable node id at all, so a skipped test's real outcome silently
    # fell through to the "not_collected" default instead of being reported
    # as "skipped". -v's per-test line is "<nodeid> <OUTCOME> ... [pct%]" for
    # every outcome (PASSED/FAILED/ERROR/SKIPPED/XFAIL/XPASS alike), always
    # carrying the full node id.
    process = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            *unique_ids,
            "-v",
            "--no-header",
            "--no-cov",
        ],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    outcomes: dict[str, str] = {}
    # Only "passed" is passing. XFAIL/XPASS/SKIPPED are real pytest outcomes
    # a cited test can report without the underlying claim actually being
    # proven -- an XFAIL means the test is *expected* to fail (i.e. the
    # behavior the manifest claims is proven is documented as broken);
    # XPASS means an expected failure unexpectedly passed (a claim that
    # something is still broken, not evidence it works); SKIPPED means the
    # measurement never happened at all. Mapping any of these to "passed"
    # is exactly the false-green failure mode this function exists to
    # prevent, so every non-PASSED marker maps to its own distinct,
    # non-passing outcome string rather than being coerced into "passed" or
    # silently absorbed into the "not_collected" default.
    _OUTCOMES = {
        "PASSED": "passed",
        "FAILED": "failed",
        "ERROR": "error",
        "SKIPPED": "skipped",
        "XFAIL": "xfail",
        "XPASS": "xpass",
    }
    node_id_set = set(unique_ids)
    for line in process.stdout.splitlines():
        if "::" not in line:
            continue
        first_token = line.split(" ", 1)[0]
        if first_token not in node_id_set:
            continue
        remainder = line[len(first_token) :].lstrip()
        for marker, outcome in _OUTCOMES.items():
            if remainder.startswith(marker):
                outcomes[first_token] = outcome
                break
    for node_id in unique_ids:
        outcomes.setdefault(node_id, "not_collected")
    return outcomes


def execute_manifest(
    root: Path, items: tuple[ManifestItem, ...] = MANIFEST
) -> list[str]:
    """Run every ``test_nodeids`` entry across ``items`` and report failures.

    Complements :func:`validate_manifest` (which never runs code): this is
    the part of the "measurement that did not happen must FAIL" discipline
    that actually executes the cited tests rather than trusting that a file
    with the right name existing means the claim holds.
    """

    all_node_ids = tuple(node_id for item in items for node_id in item.test_nodeids)
    outcomes = run_evidence_tests(root, all_node_ids)
    errors: list[str] = []
    for item in items:
        for node_id in item.test_nodeids:
            outcome = outcomes.get(node_id, "not_collected")
            if outcome != "passed":
                errors.append(f"{item.id}: {node_id} -> {outcome} (expected passed)")
    return errors


def build_report(
    root: Path, items: tuple[ManifestItem, ...] = MANIFEST
) -> ManifestReportJSON:
    """Build the CHAOS-3300 deliverable #1 JSON report.

    Raises :class:`ManifestIntegrityError` rather than emitting a report that
    rests on evidence which does not exist -- a report generator that
    "succeeds" while its own claims are false is exactly the false-green
    failure mode this manifest exists to prevent.
    """

    errors = validate_manifest(root, items)
    if errors:
        raise ManifestIntegrityError(
            "manifest integrity check failed:\n" + "\n".join(f"- {e}" for e in errors)
        )
    by_status = Counter(item.status for item in items)
    by_category = Counter(item.category for item in items)
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "item_count": len(items),
        "status_counts": dict(sorted(by_status.items())),
        "category_counts": dict(sorted(by_category.items())),
        "items": [
            {
                "id": item.id,
                "category": item.category,
                "description": item.description,
                "status": item.status,
                "evidence": list(item.evidence),
                "blocked_reason": item.blocked_reason,
                "test_nodeids": list(item.test_nodeids),
                "requires_live_infra": item.requires_live_infra,
                "execution_artifact": item.execution_artifact,
                "required_assertion_names": list(item.required_assertion_names),
            }
            for item in sorted(items, key=lambda i: (i.category, i.id))
        ],
    }


def main(argv: list[str] | None = None) -> int:
    root = Path(__file__).resolve().parents[2]
    try:
        report = build_report(root)
    except ManifestIntegrityError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    args = argv if argv is not None else sys.argv[1:]
    if "--skip-execution" not in args:
        print(
            "executing every cited test_nodeids entry (use --skip-execution to skip)..."
        )
        execution_errors = execute_manifest(root)
        if execution_errors:
            print(
                "manifest execution check failed -- a cited test did not pass:",
                file=sys.stderr,
            )
            for error in execution_errors:
                print(f"- {error}", file=sys.stderr)
            return 1

    output_path = root / "tests" / "acceptance" / "wave31-manifest-report.v1.json"
    output_path.write_text(
        json.dumps(report, indent=2, sort_keys=False) + "\n", encoding="utf-8"
    )
    print(f"wrote {output_path} ({report['item_count']} items)")
    for status, count in report["status_counts"].items():
        print(f"  {status}: {count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
