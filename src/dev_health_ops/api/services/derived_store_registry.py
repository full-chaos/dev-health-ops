"""CHAOS-3566: explicit registry of derived stores org deletion must visit.

`org_deletion._clickhouse_tables_from_migrations()` regex-scans the ClickHouse
migrations directory for every `org_id`-bearing table and stays the actual
deletion-time source of truth for ClickHouse -- this module does not replace
that scan (no behavior change to what is deleted for existing stores). It
backstops it: `CLICKHOUSE_DERIVED_STORES` is the explicit, human-reviewed
snapshot of what the scan is expected to discover, and
`tests/api/admin/test_derived_store_registry.py::
test_registry_covers_every_table_the_migration_scan_discovers` fails loudly
the moment a new migration adds an org_id-bearing table without a matching
registry entry -- an unmeasured deletion path must fail, not pass silently.

External (non-ClickHouse) derived stores have no migrations-directory
footprint at all, so the regex scan can never see them regardless of how
faithful it is. `EXTERNAL_DERIVED_STORES` is the extension point for those --
e.g. the CHAOS-3499/3500 discovery-lane temporal-graph / extraction-cache
shadow store. It is empty today: no such store exists yet in this codebase.
`OrganizationDeletionService._purge_external_stores` (org_deletion.py) already
iterates it on every `delete()` call, so wiring a real store in later is a
pure registry edit -- add a `DerivedStore(kind=EXTERNAL, visit=...)` entry;
nothing in the deletion service itself needs to change.

Residual note (recorded per CHAOS-3566 scope): `RetentionService.execute_policy`
(api/services/retention.py) is a SEPARATE, schedule-driven cleanup path, not
an org-deletion completeness mechanism, and it only ever implements
`RetentionResourceType.AUDIT_LOGS` -- every other declared resource type
returns a "Cleanup not implemented" error and deletes nothing (pinned by
`tests/api/services/test_retention_service_residual.py`). Org deletion (this
module + `org_deletion.py`) is the only place derived-store deletion
completeness is enforced; `RetentionService` does not cover it.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from enum import Enum

__all__ = [
    "CLICKHOUSE_DERIVED_STORES",
    "EXTERNAL_DERIVED_STORES",
    "DerivedStore",
    "DerivedStoreKind",
    "registered_clickhouse_tables",
    "unregistered_clickhouse_tables",
]


class DerivedStoreKind(str, Enum):
    CLICKHOUSE_TABLE = "clickhouse_table"
    EXTERNAL = "external"


@dataclass(frozen=True, slots=True)
class DerivedStore:
    name: str
    kind: DerivedStoreKind
    note: str = ""
    #: Only meaningful for EXTERNAL stores -- `(org_id, dry_run) -> row count
    #: visited/deleted`. `None` for a store that is registered (so it shows up
    #: in reviews and the registry stays the complete list) but not yet wired
    #: for deletion; `_purge_external_stores` warns rather than silently
    #: skipping that case.
    visit: Callable[[str, bool], Awaitable[int]] | None = None


#: Snapshot of every org_id-bearing ClickHouse table
#: `org_deletion._clickhouse_tables_from_migrations()` discovers as of this
#: commit. Sorted, deduplicated. A future migration that introduces a NEW
#: org_id-bearing table must add its name here in the same change --
#: `test_registry_covers_every_table_the_migration_scan_discovers` fails
#: loudly otherwise.
CLICKHOUSE_DERIVED_STORES: tuple[str, ...] = (
    "ai_attribution",
    "ai_attribution_new",
    "ai_governance_coverage_daily",
    "ai_impact_metrics_daily",
    "ai_policy_events",
    "ai_tool_allowlist",
    "ai_workflow_artifact_edges",
    "ai_workflow_issue_edges",
    "ai_workflow_runs",
    "atlassian_ops_alerts",
    "atlassian_ops_incidents",
    "atlassian_ops_schedules",
    "backfill_log",
    "capacity_forecasts",
    "ci_acceptance_checks",
    "ci_daily_rollup",
    "ci_job_runs",
    "ci_pipeline_runs",
    "cicd_metrics_daily",
    "commit_daily_rollup",
    "commit_metrics",
    "compounding_risk_daily",
    "coverage_snapshots",
    "deploy_metrics_daily",
    "deployment_daily_rollup",
    "deployments",
    "dora_metrics_daily",
    "estimate_coverage_metrics_daily",
    "feature_flag",
    "feature_flag_event",
    "feature_flag_link",
    "file_complexity_snapshots",
    "file_hotspot_daily",
    "file_metrics_daily",
    "git_blame",
    "git_commit_stats",
    "git_commits",
    "git_files",
    "git_pull_request_reviews",
    "git_pull_requests",
    "ic_landscape_rolling_30d",
    "identities",
    "incident_metrics_daily",
    "investment_classifications_daily",
    "investment_explanations",
    "investment_metrics_daily",
    "issue_type_metrics_daily",
    "jira_project_ops_team_links",
    "llm_token_usage",
    "manual_attribution_fallbacks",
    "members",
    "operational_alerts",
    "operational_escalation_policies",
    "operational_incident_notes",
    "operational_incident_responders",
    "operational_incident_timeline_events",
    "operational_incidents",
    "operational_on_call_assignments",
    "operational_on_call_schedules",
    "operational_service_repository_mappings",
    "operational_services",
    "operational_teams",
    "operational_users",
    "project_declared_state_floor",
    "project_declared_state_history",
    "projects",
    "recommendations_daily",
    "release_impact_daily",
    "repo_complexity_daily",
    "repo_metrics_daily",
    "report_plans",
    "report_provenance",
    "repos",
    "review_edges_daily",
    "security_alerts",
    "sprints",
    "team_drift_changes",
    "team_memberships",
    "team_metrics_daily",
    "team_project_ownership",
    "team_provider_observations",
    "team_repo_ownership",
    "team_sync_policies",
    "teams",
    "telemetry_signal_bucket",
    "test_case_results",
    "test_suite_results",
    "testops_benchmark_insights",
    "testops_coverage_metrics_daily",
    "testops_maturity_bands",
    "testops_metric_anomalies",
    "testops_metric_baselines",
    "testops_metric_correlations",
    "testops_period_comparisons",
    "testops_pipeline_metrics_daily",
    "testops_pipeline_stability",
    "testops_quality_drag",
    "testops_release_confidence",
    "testops_test_metrics_daily",
    "user_metrics_daily",
    "work_graph_deployment_incident_edges",
    "work_graph_edges",
    "work_graph_issue_pr",
    "work_graph_pr_commit",
    "work_graph_pr_deployment_edges",
    "work_graph_pr_review_outcome_edges",
    "work_graph_projection_runs",
    "work_item_cycle_times",
    "work_item_dependencies",
    "work_item_interactions",
    "work_item_metrics_daily",
    "work_item_reopen_events",
    "work_item_state_durations_daily",
    "work_item_team_attributions",
    "work_item_transitions",
    "work_item_user_metrics_daily",
    "work_items",
    "work_unit_investment_quotes",
    "work_unit_investments",
    "work_unit_membership",
    "work_unit_membership_runs",
    "work_unit_membership_scoped_runs",
    "work_unit_repo_effort",
    "worklogs",
)

#: Non-ClickHouse derived stores org deletion must also visit. Empty today --
#: no external derived store exists in this codebase yet. CHAOS-3499/3500's
#: discovery-lane shadow store registers itself here (with a real `visit`
#: callable) once it lands; nothing else needs to change.
EXTERNAL_DERIVED_STORES: tuple[DerivedStore, ...] = ()


def registered_clickhouse_tables() -> frozenset[str]:
    return frozenset(CLICKHOUSE_DERIVED_STORES)


def unregistered_clickhouse_tables(
    discovered: Iterable[str], *, registered: Iterable[str] | None = None
) -> frozenset[str]:
    """Tables a migration-directory scan found that the registry does not cover.

    Pure and filesystem-free -- `registered` defaults to the real,
    production `CLICKHOUSE_DERIVED_STORES`, but accepting it as a parameter
    keeps this directly unit-testable against a synthetic registry without
    monkeypatching or touching the real migrations directory.
    """
    covers = frozenset(
        registered if registered is not None else CLICKHOUSE_DERIVED_STORES
    )
    return frozenset(discovered) - covers
