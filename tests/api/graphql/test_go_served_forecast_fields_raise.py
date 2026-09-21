"""The query-api-served fields never answer from Python."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dev_health_ops.api.graphql.schema import (
    GoServedOperationUnavailableError,
    schema,
)

ORG_ID = "org-1"

_QUERIES = {
    "capacityForecast": (
        'query { capacityForecast(orgId: "org-1") { forecastId } }',
        None,
    ),
    "capacityForecasts": (
        'query { capacityForecasts(orgId: "org-1") { totalCount } }',
        None,
    ),
    "throughputForecast": (
        'query { throughputForecast(orgId: "org-1", '
        'input: {historyWeeks: 12, teamIds: ["team-1"]}) { forecastId } }',
        None,
    ),
    "catalog": (
        'query { catalog(orgId: "org-1") { limits { maxDays } } }',
        None,
    ),
    "busFactor": (
        'query { busFactor(orgId: "org-1") { value } }',
        None,
    ),
    "experiments": (
        'query { experiments(orgId: "org-1") { derivedFromOpportunities } }',
        None,
    ),
    "aiOpportunities": (
        'query { aiOpportunities(orgId: "org-1") { detectorReady } }',
        None,
    ),
    "productTelemetryDashboard": (
        'query { productTelemetryDashboard(orgId: "org-1", input: '
        '{startDate: "2026-01-01", endDate: "2026-01-08"}) '
        "{ sessionSummary { avgPagesViewed } } }",
        None,
    ),
    "productTelemetryPlatformDashboard": (
        "query { productTelemetryPlatformDashboard(input: "
        '{startDate: "2026-01-01", endDate: "2026-01-08"}) '
        "{ totals { events } } }",
        None,
    ),
    "savedReports": (
        'query { savedReports(orgId: "org-1") { total } }',
        None,
    ),
    "savedReport": (
        'query { savedReport(orgId: "org-1", '
        'reportId: "11111111-1111-1111-1111-111111111111") { id } }',
        None,
    ),
    "reportRuns": (
        'query { reportRuns(orgId: "org-1", '
        'reportId: "11111111-1111-1111-1111-111111111111") { total } }',
        None,
    ),
    "testopsRisk": (
        'query { testopsRisk(orgId: "org-1", input: '
        '{startDate: "2026-01-05", endDate: "2026-01-11"}) { orgId } }',
        None,
    ),
    "compoundingRisk": (
        'query { compoundingRisk(orgId: "org-1") { orgId } }',
        None,
    ),
    "aiImpactSummary": (
        'query { aiImpactSummary(orgId: "org-1", dateRange: '
        '{startDate: "2026-01-05", endDate: "2026-01-11"}) { totalPrs } }',
        None,
    ),
    "aiComparison": (
        'query { aiComparison(orgId: "org-1", dateRange: '
        '{startDate: "2026-01-05", endDate: "2026-01-11"}) { orgId } }',
        None,
    ),
    "aiReviewLoad": (
        'query { aiReviewLoad(orgId: "org-1", dateRange: '
        '{startDate: "2026-01-05", endDate: "2026-01-11"}) { orgId } }',
        None,
    ),
    "aiRiskBreakdown": (
        'query { aiRiskBreakdown(orgId: "org-1", dateRange: '
        '{startDate: "2026-01-05", endDate: "2026-01-11"}) { orgId } }',
        None,
    ),
    "aiGovernanceSummary": (
        'query { aiGovernanceSummary(orgId: "org-1", dateRange: '
        '{startDate: "2026-01-05", endDate: "2026-01-11"}) { orgId } }',
        None,
    ),
    "aiWorkflowDrilldown": (
        'query { aiWorkflowDrilldown(orgId: "org-1", rootType: ISSUE, '
        'rootId: "issue-1") { orgId } }',
        None,
    ),
    "aiAttributedPrs": (
        'query { aiAttributedPrs(orgId: "org-1", dateRange: '
        '{startDate: "2026-01-05", endDate: "2026-01-11"}) { total } }',
        None,
    ),
    "aiAttributionOverview": (
        'query { aiAttributionOverview(orgId: "org-1", dateRange: '
        '{startDate: "2026-01-05", endDate: "2026-01-11"}) { orgId } }',
        None,
    ),
    "securityAlerts": (
        'query { securityAlerts(orgId: "org-1") { totalCount } }',
        None,
    ),
    "improveOpportunities": (
        "query { improveOpportunities { detectorReady } }",
        None,
    ),
    "workUnitTeamAttributions": (
        'query { workUnitTeamAttributions(orgId: "org-1") { workUnitId } }',
        None,
    ),
    "securityOverview": (
        'query { securityOverview(orgId: "org-1") { kpis { openTotal } } }',
        None,
    ),
    "workGraphEdges": (
        'query { workGraphEdges(orgId: "org-1") { totalCount } }',
        None,
    ),
    "workGraphFlow": (
        'query { workGraphFlow(orgId: "org-1") { isPartial } }',
        None,
    ),
    "workGraphArtifacts": (
        'query { workGraphArtifacts(orgId: "org-1") { isPartial } }',
        None,
    ),
    "pr": (
        'query { pr(orgId: "org-1", id: "11111111-1111-1111-1111-111111111111#pr1") '
        "{ id } }",
        None,
    ),
    "cognitiveLoad": (
        'query { cognitiveLoad(input: {orgId: "org-1", '
        'sinceDate: "2026-01-05", untilDate: "2026-01-11"}) { totalDays } }',
        None,
    ),
    "complexityTimeseries": (
        'query { complexityTimeseries(input: {orgId: "org-1", '
        'sinceUtc: "2026-01-05T00:00:00Z", untilUtc: "2026-01-07T00:00:00Z", '
        "granularity: DAY, scope: REPO}) { totalScope } }",
        None,
    ),
    "hotspots": (
        'query { hotspots(input: {orgId: "org-1", '
        'sinceUtc: "2026-01-05T00:00:00Z", untilUtc: "2026-01-07T00:00:00Z"}) '
        "{ rows { filePath } } }",
        None,
    ),
    "featureFlags": (
        'query { featureFlags(orgId: "org-1") { totalCount } }',
        None,
    ),
    "featureFlagEvents": (
        'query { featureFlagEvents(orgId: "org-1") { totalCount } }',
        None,
    ),
    "reviewEdges": (
        'query { reviewEdges(input: {orgId: "org-1", sinceDate: "2026-01-01", '
        'untilDate: "2026-01-31"}) { totalCount } }',
        None,
    ),
    "analytics": (
        'query { analytics(orgId: "org-1", batch: {timeseries: [], breakdowns: []}) '
        "{ timeseries { dimension } } }",
        None,
    ),
    "operatingReview": (
        'query { operatingReview(orgId: "org-1", '
        'input: {weekStart: "2026-01-05"}) { orgId } }',
        None,
    ),
    "dataHealth": (
        'query { dataHealth(team: "ALL") { connectors { provider } } }',
        None,
    ),
}


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", sorted(_QUERIES))
async def test_field_returns_error_and_no_data(operation: str) -> None:
    query, _ = _QUERIES[operation]
    result = await schema.execute(query, context_value=None)

    assert result.errors is not None
    assert len(result.errors) == 1
    assert isinstance(
        result.errors[0].original_error, GoServedOperationUnavailableError
    )
    assert operation in result.errors[0].message
    assert not (result.data or {}).get(operation)


_LEDGER = (
    Path(__file__).resolve().parents[3]
    / "internal"
    / "goapiproof"
    / "goserved_ledger.json"
)


def test_the_go_served_ledger_names_exactly_the_raising_fields() -> None:
    ledger = json.loads(_LEDGER.read_text())
    # A named document over a root field that raises carries its own ledger row;
    # the field that raises is its response root.
    named_documents = {
        "catalogValues": "catalog",
        "acrRepositoryScopes": "catalog",
        "releaseImpact": "workGraphEdges",
        "connectorsDataHealth": "dataHealth",
        "dataHealthIdentity": "dataHealth",
        "mappingCoverageHealth": "dataHealth",
        "metricLineage": "dataHealth",
        "investmentBreakdown": "analytics",
        "investmentFull": "analytics",
        "featureFlagTimeseries": "analytics",
        "flowMatrix": "analytics",
        "testOpsPipeline": "analytics",
        "testOpsTest": "analytics",
        "testOpsCoverage": "analytics",
    }
    fields = {
        named_documents.get(entry["operation"], entry["operation"])
        for entry in ledger["entries"]
    }
    assert sorted(fields) == sorted(_QUERIES)


def test_every_document_over_the_data_health_root_has_its_own_ledger_row() -> None:
    """Four registered documents select `dataHealth`; the grouping above maps
    them to one raising field, so a dropped row for one of them would pass it."""
    ledger = json.loads(_LEDGER.read_text())
    operations = {entry["operation"] for entry in ledger["entries"]}
    assert {
        "connectorsDataHealth",
        "dataHealthIdentity",
        "mappingCoverageHealth",
        "metricLineage",
    } <= operations


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", sorted(_QUERIES))
async def test_the_wire_error_is_the_ledger_deletion_error(operation: str) -> None:
    """The prover recognises the deletion error by this exact text and path."""
    ledger = json.loads(_LEDGER.read_text())
    query, _ = _QUERIES[operation]
    result = await schema.execute(query, context_value=None)

    assert result.errors is not None and len(result.errors) == 1
    error = result.errors[0].formatted
    assert error["message"] == ledger["deletion_error_message"].replace(
        "{operation}", operation
    )
    assert error["path"] == [operation]
    assert set(error) <= {"message", "locations", "path"}


def test_every_document_over_the_analytics_root_has_its_own_ledger_row() -> None:
    """Seven registered documents select `analytics`; the grouping above maps
    them to one raising field, so a dropped row for one of them would pass it."""
    ledger = json.loads(_LEDGER.read_text())
    operations = {entry["operation"] for entry in ledger["entries"]}
    assert {
        "investmentBreakdown",
        "investmentFull",
        "flowMatrix",
        "featureFlagTimeseries",
        "testOpsPipeline",
        "testOpsTest",
        "testOpsCoverage",
    } <= operations
