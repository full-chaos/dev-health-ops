package graph

// fieldActions maps every served GraphQL root field ("Query.name",
// "Mutation.name") to the catalog action a decision request for it names.
var fieldActions = map[string]string{
	"Mutation.cloneSavedReport":               "reports.write",
	"Mutation.createSavedReport":              "reports.write",
	"Mutation.deleteSavedReport":              "reports.write",
	"Mutation.triggerReport":                  "reports.write",
	"Mutation.updateSavedReport":              "reports.write",
	"Query.aiAttributedPrs":                   "analytics.read",
	"Query.aiAttributionOverview":             "analytics.read",
	"Query.aiComparison":                      "analytics.read",
	"Query.aiGovernanceSummary":               "analytics.read",
	"Query.aiImpactSummary":                   "analytics.read",
	"Query.aiOpportunities":                   "analytics.read",
	"Query.aiReviewLoad":                      "analytics.read",
	"Query.aiRiskBreakdown":                   "analytics.read",
	"Query.aiWorkflowDrilldown":               "analytics.read",
	"Query.analytics":                         "analytics.read",
	"Query.busFactor":                         "metrics.read",
	"Query.capacityForecast":                  "analytics.read",
	"Query.capacityForecasts":                 "analytics.read",
	"Query.catalog":                           "analytics.read",
	"Query.cognitiveLoad":                     "metrics.read",
	"Query.complexityTimeseries":              "metrics.read",
	"Query.compoundingRisk":                   "metrics.read",
	"Query.coverageBaselines":                 "metrics.read",
	"Query.coverageScopeBaseline":             "metrics.read",
	"Query.dataHealth":                        "integrations.read",
	"Query.experiments":                       "analytics.read",
	"Query.featureFlagEvents":                 "analytics.read",
	"Query.featureFlags":                      "analytics.read",
	"Query.home":                              "analytics.read",
	"Query.hotspots":                          "metrics.read",
	"Query.improveOpportunities":              "analytics.read",
	"Query.operatingReview":                   "analytics.read",
	"Query.pr":                                "git.read",
	"Query.productTelemetryDashboard":         "analytics.read",
	"Query.productTelemetryPlatformDashboard": "admin.telemetry",
	"Query.recommendations":                   "analytics.read",
	"Query.reportRuns":                        "analytics.read",
	"Query.reviewEdges":                       "git.read",
	"Query.savedReport":                       "analytics.read",
	"Query.savedReports":                      "analytics.read",
	"Query.securityAlerts":                    "analytics.read",
	"Query.securityOverview":                  "analytics.read",
	"Query.sourceHealth":                      "analytics.read",
	"Query.testopsJobFailures":                "metrics.read",
	"Query.testopsRisk":                       "metrics.read",
	"Query.testopsTestWindow":                 "metrics.read",
	"Query.throughputForecast":                "analytics.read",
	"Query.workGraphArtifacts":                "git.read",
	"Query.workGraphEdges":                    "git.read",
	"Query.workGraphFlow":                     "git.read",
	"Query.workItemTeamAttributions":          "work_items.read",
	"Query.workUnitTeamAttributions":          "work_items.read",
}

// FieldActionTable returns a copy of the field action table.
func FieldActionTable() map[string]string {
	out := make(map[string]string, len(fieldActions))
	for key, action := range fieldActions {
		out[key] = action
	}
	return out
}
