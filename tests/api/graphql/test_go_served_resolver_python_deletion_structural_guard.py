"""Never-shrink ledger for Python absent from GO-SERVED GraphQL paths.

This is the QUERY-TIME counterpart to
``tests/api/internal/test_remaining_family_python_deletion_structural_guard.py``
(``metrics.remaining.*`` worker families) and
``tests/metrics/test_job_daily_skip_families_structural_guard.py``
(``job_daily.py``-resident families). Same contract as both: entries are added
one deletion at a time, and are NEVER removed.

# Why a query-time resolver is not deleted the way a worker family is

1. THE SDL IS A PYTHON ARTIFACT. ``contracts/graphql/v1/schema.graphql`` is
   EXPORTED from the Strawberry schema and pinned byte-for-byte by
   ``tests/api/graphql/test_schema_sdl_pinned.py``. Removing the three
   ``@strawberry.field`` registrations from ``api/graphql/schema.py`` drops 116
   lines from the export and moves the file's sha256 from
   ``29d509cd414cd957a7bcd73a1c0e78a07f17dd8a8794893233954aaa87241b88`` to
   a different value.

2. THAT FILE IS HALF THE ROUTING KEY. ``routeswitch.PostgresSwitch`` looks up
   ``go_api_routing_state`` by the 3-tuple (schema_digest, document_digest,
   selected_operation) (``cmd/query-api/internal/routeswitch/postgres_switch.go``),
   and the Python dispatcher computes the identical digest over the identical
   file (``go_api_dispatcher.py``). Changing it invalidates the routing row of
   EVERY registered operation at once. It is also gqlgen's input SDL, so the Go
   models for those types would vanish with it.

3. THE STRAWBERRY FIELD BODIES RAISE. These operations have no Python
   implementation: with no routing row, or when query-api fails, the field
   returns a GraphQL error and never a Python answer.

So the Strawberry FIELD DECLARATIONS and their input/output model classes are
load-bearing infrastructure, and this ledger does NOT cover them. It covers
Python that is absent.

# What this ledger holds

The EXECUTION path -- ``resolvers/capacity.py``, ``resolvers/forecast.py``,
``metrics/capacity_queries.py``, ``resolvers/operating_review.py`` and
``metrics/operating_review.py`` -- plus ``discover_team_scopes``, which has no
caller in ``src/``.

What is NOT here: ``metrics/compute_capacity.py`` and ``metrics/forecast.py``.
They are the Go kernels' parity ORACLES, kept on the
``metrics/compounding_risk.py`` precedent, and the last test below asserts they
exist AND that nothing under ``src/`` imports them -- an oracle that acquires a
production importer has become a live path again.

Widen this ledger as each subsequent deletion lands. Do not remove entries.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]

CAPACITY_QUERIES_SOURCE = (
    ROOT / "src" / "dev_health_ops" / "metrics" / "capacity_queries.py"
)

# module path -> the symbols that must not be defined in it.
SECURITY_RESOLVER_SOURCE = (
    ROOT / "src" / "dev_health_ops" / "api" / "graphql" / "resolvers" / "security.py"
)

IMPROVE_RESOLVER_SOURCE = (
    ROOT / "src" / "dev_health_ops" / "api" / "graphql" / "resolvers" / "improve.py"
)

REPORTS_RESOLVER_SOURCE = (
    ROOT / "src" / "dev_health_ops" / "api" / "graphql" / "resolvers" / "reports.py"
)

PRODUCT_TELEMETRY_RESOLVER_SOURCE = (
    ROOT
    / "src"
    / "dev_health_ops"
    / "api"
    / "graphql"
    / "resolvers"
    / "product_telemetry.py"
)
PRODUCT_TELEMETRY_LOADER_SOURCE = (
    ROOT / "src" / "dev_health_ops" / "api" / "product_telemetry" / "dashboard.py"
)

AI_RESOLVER_SOURCE = (
    ROOT / "src" / "dev_health_ops" / "api" / "graphql" / "resolvers" / "ai.py"
)

DATA_HEALTH_MODELS_SOURCE = (
    ROOT / "src" / "dev_health_ops" / "api" / "graphql" / "models" / "data_health.py"
)

WORKER_METRICS_SOURCE = (
    ROOT / "src" / "dev_health_ops" / "api" / "internal" / "worker_metrics.py"
)
WORKER_AUTH_SOURCE = (
    ROOT / "src" / "dev_health_ops" / "api" / "internal" / "worker_auth.py"
)

TEAM_ATTRIBUTION_RESOLVER_SOURCE = (
    ROOT
    / "src"
    / "dev_health_ops"
    / "api"
    / "graphql"
    / "resolvers"
    / "team_attribution.py"
)

DELETED_GO_SERVED_RESOLVER_SYMBOLS: dict[Path, frozenset[str]] = {
    # dataHealth (connectors, identity, mapping coverage, metric lineage): the
    # resolver module is deleted (modules ledger below); the lineage
    # computation lived beside the response types, which stay for the SDL.
    DATA_HEALTH_MODELS_SOURCE: frozenset(
        {
            "compute_metric_lineage",
            "METRIC_LINEAGE_REGISTRY",
            "_lineage_freshness",
            "_query_dicts",
            "_int",
            "_safe_table_name",
        }
    ),
    # A scheduler helper with no caller in src/.
    CAPACITY_QUERIES_SOURCE: frozenset({"discover_team_scopes"}),
    # securityOverview: only the function is deleted; the module stays for
    # securityAlerts and the shared filter builder.
    # securityAlerts and improveOpportunities: both resolver modules are deleted
    # (the modules ledger below keeps them from returning), and the symbols
    # stay named here.
    SECURITY_RESOLVER_SOURCE: frozenset(
        {
            "resolve_security_overview",
            "resolve_security_alerts",
            "_build_filter_clauses",
        }
    ),
    # workUnitTeamAttributions: the work-item attribution resolver stays.
    TEAM_ATTRIBUTION_RESOLVER_SOURCE: frozenset(
        {
            "resolve_work_unit_team_attributions",
            "_row_to_unit_attribution",
            "_SOURCE_RANK_SQL",
        }
    ),
    # The operator repair and bulk-redrive routes: the Go workerctl verbs run
    # those repairs natively, so the routes, their request models, their
    # helpers and their repair-token authorizers are gone.
    WORKER_METRICS_SOURCE: frozenset(
        {
            "repair_metric_execution",
            "redrive_daily_metrics",
            "_repair_execution",
            "_repair_id",
            "_bulk_redrive_ambiguous_executions",
            "MetricExecutionRepairRequest",
            "DailyMetricsRedriveRequest",
        }
    ),
    WORKER_AUTH_SOURCE: frozenset(
        {"authorize_metric_repair", "authorize_workgraph_repair"}
    ),
    IMPROVE_RESOLVER_SOURCE: frozenset(
        {
            "resolve_experiments",
            "_stable_experiment_id",
            "_metric_from_card",
            "resolve_improve_opportunities",
        }
    ),
    # testopsRisk: the whole resolver module goes (see the module list).
    # savedReports, savedReport and reportRuns: the read resolvers and the
    # run-row loader only they used. The module stays: the report mutations
    # (create, update, delete, clone, trigger) are Python and share its output
    # types and row mappers.
    REPORTS_RESOLVER_SOURCE: frozenset(
        {
            "resolve_saved_reports",
            "resolve_saved_report",
            "resolve_report_runs",
            "_load_report_run_rows",
            "_report_run_type_from_row",
        }
    ),
    # productTelemetryDashboard and productTelemetryPlatformDashboard: both
    # resolvers, both loaders and the organisation-name lookup are gone with
    # the modules.
    PRODUCT_TELEMETRY_RESOLVER_SOURCE: frozenset(
        {
            "resolve_product_telemetry_dashboard",
            "resolve_product_telemetry_platform_dashboard",
            "_load_org_hash_index",
            "_resolve_top_orgs",
            "_platform_dashboard_to_graphql",
        }
    ),
    PRODUCT_TELEMETRY_LOADER_SOURCE: frozenset(
        {
            "load_product_telemetry_dashboard",
            "load_product_telemetry_platform_dashboard",
        }
    ),
    # The nine query-api-served AI analytics operations, including
    # aiOpportunities. The module is deleted with them; the detector module
    # stays as the Go detectors' parity oracle.
    AI_RESOLVER_SOURCE: frozenset(
        {
            "resolve_ai_opportunities",
            "_normalize_opportunity_scope",
            "_resolve_repo_ref",
            "resolve_ai_impact_summary",
            "resolve_ai_comparison",
            "resolve_ai_review_load",
            "resolve_ai_risk_breakdown",
            "resolve_ai_governance_summary",
            "resolve_ai_workflow_drilldown",
            "resolve_ai_attributed_prs",
            "resolve_ai_attribution_overview",
            "_normalize_scope",
            "_normalize_attribution_scope",
            "_load_daily_records",
            "_load_scope_breakdowns",
            "_load_reviewer_concentration",
            "_load_engagement_slices",
            "_load_overlap_rows",
            "_resolve_repo_team_map",
            "_resolve_team_repo_ids",
            "_scope_rollups",
            "_aggregate_bucket_totals",
            "_aggregate_side",
            "_validate_date_range",
            "_to_aware",
        }
    ),
}

# label -> a module that must not exist on disk AT ALL.
#
# The EXECUTION path for the three query-api-served capacity/forecast
# operations. Their Strawberry field bodies raise; there is no Python code to
# call.
DELETED_GO_SERVED_RESOLVER_MODULES: dict[str, Path] = {
    # capacityForecast + capacityForecasts.
    "capacity resolver": ROOT
    / "src"
    / "dev_health_ops"
    / "api"
    / "graphql"
    / "resolvers"
    / "capacity.py",
    # throughputForecast, including its ClickHouse reads.
    "forecast resolver": ROOT
    / "src"
    / "dev_health_ops"
    / "api"
    / "graphql"
    / "resolvers"
    / "forecast.py",
    # The capacity resolver's only query layer; no other importer.
    "capacity query layer": ROOT
    / "src"
    / "dev_health_ops"
    / "metrics"
    / "capacity_queries.py",
    # operatingReview.
    "operating review resolver": ROOT
    / "src"
    / "dev_health_ops"
    / "api"
    / "graphql"
    / "resolvers"
    / "operating_review.py",
    # reviewEdges.
    "review edges resolver": ROOT
    / "src"
    / "dev_health_ops"
    / "api"
    / "graphql"
    / "resolvers"
    / "review_edges.py",
    # featureFlags and featureFlagEvents.
    "feature flags resolver": ROOT
    / "src"
    / "dev_health_ops"
    / "api"
    / "graphql"
    / "resolvers"
    / "feature_flags.py",
    # complexityTimeseries and hotspots.
    "complexity resolver": ROOT
    / "src"
    / "dev_health_ops"
    / "api"
    / "graphql"
    / "resolvers"
    / "complexity.py",
    # cognitiveLoad.
    "cognitive load resolver": ROOT
    / "src"
    / "dev_health_ops"
    / "api"
    / "graphql"
    / "resolvers"
    / "cognitive_load.py",
    # pr.
    "pull request detail resolver": ROOT
    / "src"
    / "dev_health_ops"
    / "api"
    / "graphql"
    / "resolvers"
    / "pr.py",
    # catalog (the catalogValues and acrRepositoryScopes documents).
    "catalog resolver": ROOT
    / "src"
    / "dev_health_ops"
    / "api"
    / "graphql"
    / "resolvers"
    / "catalog.py",
    # The catalog's dimension value loader and description helpers; their only
    # importers were the resolver and one loader test.
    "dimension value loader": ROOT
    / "src"
    / "dev_health_ops"
    / "api"
    / "graphql"
    / "loaders"
    / "dimension_loader.py",
    # busFactor (the churn-concentration computation stays in
    # metrics/knowledge.py for the daily job).
    "bus factor resolver": ROOT
    / "src"
    / "dev_health_ops"
    / "api"
    / "graphql"
    / "resolvers"
    / "bus_factor.py",
    # securityAlerts (the securityOverview function went earlier).
    "security alerts resolver": SECURITY_RESOLVER_SOURCE,
    # improveOpportunities (the experiments functions went earlier).
    "improve opportunities resolver": IMPROVE_RESOLVER_SOURCE,
    # compoundingRisk. The score computation stays in metrics/compounding_risk.py
    # for the daily job; the SDL types stay in types/compounding_risk.py.
    "compounding risk resolver": ROOT
    / "src"
    / "dev_health_ops"
    / "api"
    / "graphql"
    / "resolvers"
    / "compounding_risk.py",
    # The AI analytics operations, aiOpportunities last. The detector module
    # stays as the Go detectors' parity oracle.
    "ai resolver": ROOT
    / "src"
    / "dev_health_ops"
    / "api"
    / "graphql"
    / "resolvers"
    / "ai.py",
    # dataHealth: connectors, identity mapping, mapping coverage and metric
    # lineage. The Python resolver answered connectors with [] in production
    # (its session was never set on the context); Go is the only answer.
    "data health resolver": ROOT
    / "src"
    / "dev_health_ops"
    / "api"
    / "graphql"
    / "resolvers"
    / "data_health.py",
    # productTelemetryPlatformDashboard (and productTelemetryDashboard before
    # it): the resolvers, their loaders and the section dataclasses only they
    # used. The telemetry ingest router keeps its own modules.
    "product telemetry resolver": PRODUCT_TELEMETRY_RESOLVER_SOURCE,
    "product telemetry dashboard loader": PRODUCT_TELEMETRY_LOADER_SOURCE,
    # testopsRisk (the SDL types stay in types/testops_risk.py).
    "testops risk resolver": ROOT
    / "src"
    / "dev_health_ops"
    / "api"
    / "graphql"
    / "resolvers"
    / "testops_risk.py",
    # The workgraph operator repair route (its only route).
    "workgraph repair route": ROOT
    / "src"
    / "dev_health_ops"
    / "api"
    / "internal"
    / "worker_workgraph.py",
    # workGraphEdges, workGraphFlow and workGraphArtifacts.
    "work graph resolver": ROOT
    / "src"
    / "dev_health_ops"
    / "api"
    / "graphql"
    / "resolvers"
    / "work_graph.py",
    # The operating review computation; its only importer was the resolver.
    "operating review computation": ROOT
    / "src"
    / "dev_health_ops"
    / "metrics"
    / "operating_review.py",
    # analytics: the seven documents that select it (investment breakdown, full
    # investment, flow matrix, feature flag timeseries and the three test
    # operations documents).
    "analytics resolver": ROOT
    / "src"
    / "dev_health_ops"
    / "api"
    / "graphql"
    / "resolvers"
    / "analytics.py",
    # CHAOS-6240: NOT a query-api migration -- this internal HTTP bridge
    # (execute_remaining_metrics, read_metric_execution) had no caller
    # anywhere at all (ops Go, acr, ask-dev, web, deploy, scripts, ci), so
    # the whole module -- every subprocess-exec/capacity/ledger helper that
    # existed only to serve those two routes, WORKER_METRICS_SOURCE's
    # already-ledgered symbols included -- is deleted rather than replaced.
    # Tracked here anyway: this dict's own mechanism (test_deleted_symbols_
    # are_not_redefined) is what proves an absent file's ledgered symbols
    # stay gone, regardless of why the file was deleted.
    "worker metrics bridge": WORKER_METRICS_SOURCE,
    # CHAOS-6240: worker_auth.py's only importer was worker_metrics.py
    # (confirmed by grep across src/); with that route gone, this shared
    # "narrow internal worker bridge" auth helper (authorize_worker_bridge)
    # has zero callers left and is deleted with it, WORKER_AUTH_SOURCE's
    # already-ledgered symbols (authorize_metric_repair/
    # authorize_workgraph_repair, deleted in an earlier PR) included.
    "worker auth helper": WORKER_AUTH_SOURCE,
}

# The kernels RETAINED as Go-parity oracles, on the metrics/compounding_risk.py
# precedent: a pure kernel kept as the oracle for its golden generator.
#
# Deleting these would leave the Go ports' fixtures unfalsifiable: a golden
# captured from a producer that does not exist cannot disagree with anything,
# and the corpus guard that re-runs the generators against live Python would
# fail (all three importing generators die with ModuleNotFoundError).
RETAINED_ORACLE_MODULES: dict[str, Path] = {
    "capacity kernel": ROOT
    / "src"
    / "dev_health_ops"
    / "metrics"
    / "compute_capacity.py",
    "throughput kernel": ROOT / "src" / "dev_health_ops" / "metrics" / "forecast.py",
}

# The Strawberry declarations that MUST exist, because they produce the SDL
# that both planes digest for routing. Trimming "unused" Python here would
# silently change the schema digest and disable every registered operation.
SDL_LOAD_BEARING_SOURCES: tuple[Path, ...] = (
    ROOT / "src" / "dev_health_ops" / "api" / "graphql" / "resolvers" / "reports.py",
    ROOT / "src" / "dev_health_ops" / "api" / "graphql" / "types" / "testops_risk.py",
    ROOT / "src" / "dev_health_ops" / "api" / "graphql" / "models" / "pr.py",
    ROOT / "src" / "dev_health_ops" / "api" / "graphql" / "models" / "improve.py",
    ROOT / "src" / "dev_health_ops" / "api" / "graphql" / "models" / "ai.py",
    ROOT / "src" / "dev_health_ops" / "api" / "graphql" / "models" / "data_health.py",
    ROOT / "src" / "dev_health_ops" / "api" / "graphql" / "types" / "cognitive_load.py",
    ROOT / "src" / "dev_health_ops" / "api" / "graphql" / "types" / "complexity.py",
    ROOT / "src" / "dev_health_ops" / "api" / "graphql" / "types" / "review_edges.py",
    ROOT / "src" / "dev_health_ops" / "api" / "graphql" / "types" / "bus_factor.py",
    ROOT / "src" / "dev_health_ops" / "api" / "graphql" / "schema.py",
    ROOT / "src" / "dev_health_ops" / "api" / "graphql" / "models" / "inputs.py",
    ROOT / "src" / "dev_health_ops" / "api" / "graphql" / "models" / "outputs.py",
)

SDL_LOAD_BEARING_SYMBOLS: dict[str, frozenset[str]] = {
    "testops_risk.py": frozenset(
        {
            "TestOpsRiskInput",
            "TestOpsRiskTrendPoint",
            "TestOpsRiskBreakdownItem",
            "TestOpsRiskQuadrantPoint",
            "TestOpsRiskSparkPoint",
            "TestOpsRiskResult",
        }
    ),
    "reports.py": frozenset(
        {
            "SavedReportType",
            "ReportRunType",
            "SavedReportConnection",
            "ReportRunConnection",
        }
    ),
    "improve.py": frozenset(
        {
            "Experiment",
            "ExperimentsResult",
            "ExperimentStatus",
            "ImproveOpportunity",
            "ImproveOpportunityKind",
            "ImproveOpportunitiesResult",
        }
    ),
    "data_health.py": frozenset(
        {
            "ConnectorFailure",
            "ConnectorStatus",
            "UnmappedIdentity",
            "AliasSuggestion",
            "IdentityMappingHealth",
            "MissingMapping",
            "CoverageStat",
            "MappingCoverage",
            "WindowSpec",
            "MetricLineage",
            "DataHealth",
        }
    ),
    "ai.py": frozenset(
        {
            "AIOpportunitiesResult",
            "AIOpportunity",
            "AIOpportunityKind",
            "AIWorkGraphDrilldownRef",
        }
    ),
    "pr.py": frozenset(
        {
            "PullRequestDetail",
            "PullRequestReview",
            "PullRequestCommit",
            "PullRequestIssueLink",
        }
    ),
    "cognitive_load.py": frozenset(
        {
            "CognitiveLoadInput",
            "CognitiveLoadSignal",
            "CognitiveLoadResult",
        }
    ),
    "complexity.py": frozenset(
        {
            "ComplexityTimeseriesInput",
            "ComplexityTimeseriesResult",
            "ComplexityPoint",
            "HotspotsInput",
            "HotspotsResult",
            "HotspotRow",
        }
    ),
    "review_edges.py": frozenset(
        {"ReviewEdgesInput", "ReviewEdgeRow", "ReviewEdgesResult"}
    ),
    "bus_factor.py": frozenset(
        {
            "MaintainerShare",
            "RepoBusFactor",
            "BusFactorScope",
            "BusFactor",
            "BusFactorScopeInput",
        }
    ),
    "schema.py": frozenset(
        {
            "capacity_forecast",
            "capacity_forecasts",
            "throughput_forecast",
            "operating_review",
            "review_edges",
            "feature_flags",
            "feature_flag_events",
            "complexity_timeseries",
            "hotspots",
            "cognitive_load",
            "pr",
            "catalog",
            "bus_factor",
            "data_health",
            "security_overview",
            "work_unit_team_attributions",
            "experiments",
            "ai_opportunities",
            "product_telemetry_dashboard",
            "product_telemetry_platform_dashboard",
            "saved_reports",
            "saved_report",
            "report_runs",
            "testops_risk",
            "compounding_risk",
            "security_alerts",
            "improve_opportunities",
            "ai_impact_summary",
            "ai_comparison",
            "ai_review_load",
            "ai_risk_breakdown",
            "ai_governance_summary",
            "ai_workflow_drilldown",
            "ai_attributed_prs",
            "ai_attribution_overview",
            "work_graph_edges",
            "work_graph_flow",
            "work_graph_artifacts",
            "analytics",
        }
    ),
    "inputs.py": frozenset(
        {
            "CapacityForecastInput",
            "CapacityForecastFilterInput",
            "ThroughputForecastInput",
            "OperatingReviewInput",
            "WorkGraphEdgeFilterInput",
            "WorkGraphNodeTypeInput",
            "WorkGraphEdgeTypeInput",
        }
    ),
    "outputs.py": frozenset(
        {
            "CapacityForecast",
            "CapacityForecastEdge",
            "CapacityForecastConnection",
            "ThroughputForecast",
            "ThroughputRollingWindow",
            "ThroughputRiskOverlay",
            "ThroughputStaleWip",
            "ThroughputEstimateCoverage",
            "OperatingReview",
            "OperatingReviewSection",
            "OperatingReviewMetric",
            "OperatingReviewDelta",
            "CatalogDimension",
            "CatalogMeasure",
            "CatalogLimits",
            "CatalogValueItem",
            "CatalogResult",
            "WorkGraphEdgesResult",
            "WorkGraphEdgeResult",
            "WorkGraphFlowResult",
            "WorkGraphFlowRow",
            "WorkGraphArtifactsResult",
            "WorkGraphArtifactRow",
            "WorkGraphNodeType",
            "WorkGraphEdgeType",
            "WorkGraphProvenance",
            "FeatureFlagItem",
            "FeatureFlagRegistryResult",
            "FeatureFlagEventItem",
            "FeatureFlagEventsResult",
            "SecurityAlertNode",
            "SecurityAlertEdge",
            "SecurityAlertConnection",
            "WorkUnitTeamAttribution",
            "ProductTelemetryDashboardType",
            "ProductTelemetryPlatformDashboardType",
            "ProductTelemetrySessionSummaryType",
        }
    ),
}


def _defined_names(source: Path) -> set[str]:
    """Every top-level and class-level name defined in ``source``.

    Parsed with ``ast`` rather than imported: importing the GraphQL schema pulls
    in Strawberry, the resolvers and a database layer, and this guard must be
    able to fail cleanly on a tree where one of those is exactly what is broken.
    A substring search over the text would be the other obvious shortcut and is
    worse -- it cannot tell a definition from a mention in a docstring, which is
    precisely the difference this file is asserting.
    """
    tree = ast.parse(source.read_text(encoding="utf-8"))
    names: set[str] = set()

    def visit(node: ast.AST) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                names.add(child.name)
                visit(child)
            elif isinstance(child, ast.AnnAssign) and isinstance(
                child.target, ast.Name
            ):
                names.add(child.target.id)
            elif isinstance(child, ast.Assign):
                names.update(
                    target.id
                    for target in child.targets
                    if isinstance(target, ast.Name)
                )

    visit(tree)
    return names


def _imports_module(source: Path, dotted: str) -> bool:
    """True when ``source`` imports the module ``dotted`` in any import form.

    Resolves ``import a.b.c``, ``from a.b import c`` and relative imports
    (``from .c import x``, ``from . import c``) against the file's own package,
    so no import spelling can bypass the check.
    """
    tree = ast.parse(source.read_text(encoding="utf-8"))
    package = [
        "dev_health_ops",
        *source.relative_to(ROOT / "src" / "dev_health_ops").parent.parts,
    ]
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            if any(alias.name == dotted for alias in node.names):
                return True
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                base = package[: len(package) - (node.level - 1)]
                if node.module:
                    base = [*base, *node.module.split(".")]
            else:
                base = node.module.split(".") if node.module else []
            target = ".".join(base)
            if target == dotted:
                return True
            if any(f"{target}.{alias.name}" == dotted for alias in node.names):
                return True
    return False


def test_deleted_symbols_are_not_redefined() -> None:
    """A ledgered symbol stays undefined, in the module it was listed for.

    A symbol entry whose whole module is absent is satisfied by
    that module's entry in DELETED_GO_SERVED_RESOLVER_MODULES -- the symbol
    cannot come back while the file it lived in does not exist. That is the
    "move it, do not drop it" path, and it is checked rather than assumed: an
    absent module with no modules-ledger entry is a silently dropped assertion,
    which a never-shrink ledger exists to prevent.
    """
    ledgered_modules = set(DELETED_GO_SERVED_RESOLVER_MODULES.values())
    for source, deleted in DELETED_GO_SERVED_RESOLVER_SYMBOLS.items():
        if not source.exists():
            assert source in ledgered_modules, (
                f"{source} does not exist and is not in "
                "DELETED_GO_SERVED_RESOLVER_MODULES either, so nothing "
                "asserts that "
                f"{', '.join(sorted(deleted))} stays deleted. Add the module to "
                "the modules ledger rather than dropping the symbol entry -- "
                "this ledger never shrinks."
            )
            continue
        defined = _defined_names(source)
        restored = sorted(deleted & defined)
        assert not restored, (
            f"{', '.join(restored)} is defined again in {source.name}. These have "
            "no caller. If a real caller needs one, say so in the PR and remove "
            "the ledger entry deliberately rather than as a side effect."
        )


def test_deleted_modules_do_not_exist() -> None:
    """A module in the ledger must not be back on disk."""
    for label, module in DELETED_GO_SERVED_RESOLVER_MODULES.items():
        assert not module.exists(), (
            f"{label}: {module} exists. query-api serves its operation "
            "natively; restoring it re-creates a dual implementation that can "
            "drift from the Go plane."
        )


def test_sdl_load_bearing_declarations_still_exist() -> None:
    """The Strawberry declarations that produce the SDL must NOT be deleted.

    The inverse assertion to the two above, and the reason this file is worth
    reading before writing the next deletion PR. These names are not leftovers
    from a completed port -- they generate
    ``contracts/graphql/v1/schema.graphql``, which is gqlgen's input SDL, web's
    codegen schema, and half of both planes' routing key. Deleting them is not a
    cleanup; it disables every registered Go operation at once.
    """
    for source in SDL_LOAD_BEARING_SOURCES:
        assert source.exists(), (
            f"{source} has been deleted. It defines part of the GraphQL schema "
            "that contracts/graphql/v1/schema.graphql is exported from -- see "
            "this module's docstring for the measured consequence."
        )
        defined = _defined_names(source)
        expected = SDL_LOAD_BEARING_SYMBOLS[source.name]
        missing = sorted(expected - defined)
        assert not missing, (
            f"{source.name} does not define {', '.join(missing)}. Removing a "
            "Strawberry field or one of its input/output models changes the "
            "exported SDL and therefore the canonical schema digest, which is "
            "part of go_api_routing_state's primary key -- so it disables the "
            "routing rows of EVERY registered operation, not just this one's. "
            "Measured: dropping these three fields "
            "removed 116 SDL lines and moved the digest from 29d509cd... to "
            "5e2150ef.... If the schema genuinely must change, that is a "
            "schema-change PR with a regenerated pin and a routing-row "
            "migration, not a Python cleanup."
        )


def test_retained_oracles_exist_and_have_no_production_importer() -> None:
    """The kept kernels must survive, and must stay out of the production graph.

    Two assertions, and the second is the one that stops this arrangement from
    rotting. A module retained "as an oracle" that something under ``src/``
    starts importing is not an oracle -- it is a live path with a
    misleading docstring, and the Go port it is measured against has a Python
    twin executing beside it.

    ``tests/`` is excluded from the sweep on purpose: the generators and unit
    tests SHOULD import these, and that is the whole point of keeping them.
    """
    source_root = ROOT / "src"
    for label, module in RETAINED_ORACLE_MODULES.items():
        assert module.exists(), (
            f"{label}: {module} has been deleted. It is the parity oracle for a "
            "Go kernel -- see its own module docstring. Deleting it does not "
            "remove a Python code path (there is none); it removes the "
            "ability to prove the Go port agrees with anything, and it "
            "breaks TestEveryDiscoverableCorpusStillMatchesLivePython, which "
            "re-runs the golden generators against live Python."
        )

        dotted = "dev_health_ops.metrics." + module.stem
        importers: list[str] = []
        for candidate in source_root.rglob("*.py"):
            # An oracle importing another oracle is not a production path:
            # metrics/forecast.py takes ThroughputHistory from
            # metrics/compute_capacity.py, and both are kept for the same
            # reason. Excluding the retained set keeps this assertion about
            # what it is actually for -- something OUTSIDE the oracle island
            # reaching into it.
            if candidate == module or candidate in RETAINED_ORACLE_MODULES.values():
                continue
            if _imports_module(candidate, dotted):
                importers.append(str(candidate.relative_to(ROOT)))

        assert not importers, (
            f"{label}: {dotted} is imported from production code by "
            f"{', '.join(sorted(importers))}. It is retained ONLY as the parity "
            "oracle for its Go port and its docstring says so. A production "
            "importer means the Python executes beside the Go that "
            "serves the operation -- a dual implementation. Either "
            "the new caller belongs in Go, or this module is being promoted "
            "back to a live path, which is a decision to make explicitly rather "
            "than by adding an import."
        )


@pytest.mark.parametrize(
    "statement,package_dir,expected",
    [
        ("import dev_health_ops.metrics.forecast", "metrics", True),
        ("from dev_health_ops.metrics import forecast", "api", True),
        ("from dev_health_ops.metrics.forecast import x", "api", True),
        ("from .forecast import x", "metrics", True),
        ("from . import forecast", "metrics", True),
        ("from ..metrics import forecast", "api", True),
        ("from dev_health_ops.metrics import other", "api", False),
        ("from .other import forecast", "metrics", False),
    ],
)
def test_import_scan_recognises_every_import_form(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    statement: str,
    package_dir: str,
    expected: bool,
) -> None:
    root = tmp_path
    target = root / "src" / "dev_health_ops" / package_dir / "probe.py"
    target.parent.mkdir(parents=True)
    target.write_text(statement + "\n", encoding="utf-8")
    monkeypatch.setitem(globals(), "ROOT", root)
    assert _imports_module(target, "dev_health_ops.metrics.forecast") is expected
