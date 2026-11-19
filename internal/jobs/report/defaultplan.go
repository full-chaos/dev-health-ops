package report

import "strings"

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

const (
	unchartedNoMetric    = "no metric matches this label"
	unchartedNotChart    = "this metric cannot be shown as a chart"
	unchartedSavedReport = "the saved report has no chart for this label"
)

// defaultChartSpecs makes one daily line chart per chartable requested metric, in request order, once each.
// A requested name is a form label with a mapping or a numeric registry metric; any other gets no chart, because a
// chart of an unknown metric would fail the whole report, and is returned once as uncharted so the report names it.
func defaultChartSpecs(plan wirePlan) ([]wireChartSpec, []UnchartedMetric) {
	var specs []wireChartSpec
	var uncharted []UnchartedMetric
	seen := map[string]bool{}
	named := map[string]bool{}
	groupBy := "day"
	for _, requested := range plan.RequestedMetrics {
		metric := requested
		name, mapped := formMetricNames[requested]
		if mapped {
			metric = name
		}
		if seen[metric] {
			continue
		}
		if !chartableMetric(metric) {
			if strings.TrimSpace(requested) == "" || named[requested] {
				continue
			}
			named[requested] = true
			reason := unchartedNoMetric
			if _, registered := supportedMetrics[metric]; registered || mapped {
				reason = unchartedNotChart
			}
			uncharted = append(uncharted, UnchartedMetric{Label: requested, Reason: reason})
			continue
		}
		seen[metric] = true
		specs = append(specs, wireChartSpec{
			ChartID: plan.PlanID + "-" + metric, PlanID: plan.PlanID, ChartType: "line", Metric: metric, GroupBy: &groupBy,
			FilterTeams: plan.ScopeTeams, FilterRepos: plan.ScopeRepos,
			TimeRangeStart: plan.TimeRangeStart, TimeRangeEnd: plan.TimeRangeEnd, OrganizationID: plan.OrganizationID,
		})
	}
	return specs, uncharted
}

// savedPlanUncharted names each requested label of a saved plan that carries no chart specs: such a plan gets no
// default charts, so none of its labels has a chart.
func savedPlanUncharted(plan wirePlan) []UnchartedMetric {
	var uncharted []UnchartedMetric
	named := map[string]bool{}
	for _, requested := range plan.RequestedMetrics {
		if strings.TrimSpace(requested) == "" || named[requested] {
			continue
		}
		named[requested] = true
		uncharted = append(uncharted, UnchartedMetric{Label: requested, Reason: unchartedSavedReport})
	}
	return uncharted
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
