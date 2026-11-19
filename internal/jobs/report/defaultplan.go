package report

// formMetricNames maps a metric label of the web New Report form to the report registry metric of the same quantity.
// Each entry follows the label the product already shows: web src/lib/metrics/catalog.ts -> ops
// internal/queryapi/home/metricspec.go (same table and rule as the report chart rule in dedup.go chartRules).
// "Deployment Frequency" is the former form label of Deploy Frequency, kept for stored reports. A label with no such
// chain is absent: it gets no chart.
var formMetricNames = map[string]string{
	"Change Failure Rate":  "change_failure_rate",
	"CI Success Rate":      "success_rate",
	"Deploy Frequency":     "deployments_count",
	"Deployment Frequency": "deployments_count",
}

// defaultChartSpecs makes one daily line chart per chartable requested metric, in request order, once each.
// A requested name is a form label with a mapping or a numeric registry metric; any other is skipped, because a
// chart of an unknown metric would fail the whole report.
func defaultChartSpecs(plan wirePlan) []wireChartSpec {
	var specs []wireChartSpec
	seen := map[string]bool{}
	groupBy := "day"
	for _, requested := range plan.RequestedMetrics {
		metric := requested
		if name, ok := formMetricNames[requested]; ok {
			metric = name
		}
		if seen[metric] || !chartableMetric(metric) {
			continue
		}
		seen[metric] = true
		specs = append(specs, wireChartSpec{
			ChartID: plan.PlanID + "-" + metric, PlanID: plan.PlanID, ChartType: "line", Metric: metric, GroupBy: &groupBy,
			FilterTeams: plan.ScopeTeams, FilterRepos: plan.ScopeRepos,
			TimeRangeStart: plan.TimeRangeStart, TimeRangeEnd: plan.TimeRangeEnd, OrganizationID: plan.OrganizationID,
		})
	}
	return specs
}

func chartableMetric(metric string) bool {
	definition, ok := supportedMetrics[metric]
	if !ok {
		return false
	}
	definition = withChartRule(definition)
	if _, declared := tableReads[definition.SourceTable]; !declared {
		return false
	}
	return definition.ValueKind == valueKindNumeric
}
