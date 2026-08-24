package providerfoundation

import (
	"bytes"
	"strings"
	"testing"
)

// The execution-time canonical-incident refusal is ONE series for every gated
// provider; the provider label is what tells Jira and PagerDuty apart, and the
// seam label tells "refused before fetch" from "refused at the write boundary"
// (CHAOS-4219). Unbounded inputs collapse rather than mint a series.
func TestIncidentEntitlementRefusalCounterRendersPerProviderDatasetAndSeam(t *testing.T) {
	t.Parallel()
	metrics := NewMetrics()
	metrics.RecordIncidentEntitlementRefused("PagerDuty", "incidents", "collect")
	metrics.RecordIncidentEntitlementRefused("pagerduty", "incidents", "collect")
	metrics.RecordIncidentEntitlementRefused("pagerduty", "services", "write")
	metrics.RecordIncidentEntitlementRefused("jira", "incidents", "write")
	metrics.RecordIncidentEntitlementRefused("mystery", "org-4711/whatever", "somewhere-else")
	var nilMetrics *Metrics
	nilMetrics.RecordIncidentEntitlementRefused("jira", "incidents", "collect")

	var output bytes.Buffer
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	rendered := output.String()
	for _, want := range []string{
		"# TYPE dev_health_provider_incident_entitlement_refused_total counter",
		`dev_health_provider_incident_entitlement_refused_total{provider="pagerduty",dataset="incidents",seam="collect"} 2`,
		`dev_health_provider_incident_entitlement_refused_total{provider="pagerduty",dataset="services",seam="write"} 1`,
		`dev_health_provider_incident_entitlement_refused_total{provider="jira",dataset="incidents",seam="write"} 1`,
		`dev_health_provider_incident_entitlement_refused_total{provider="other",dataset="other",seam="other"} 1`,
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("missing %q in:\n%s", want, rendered)
		}
	}
	if got := strings.Count(rendered, "dev_health_provider_incident_entitlement_refused_total{"); got != 4 {
		t.Fatalf("series=%d want 4 in:\n%s", got, rendered)
	}
	if strings.Contains(rendered, "dev_health_provider_incident_entitlement_refused_total_") ||
		strings.Contains(rendered, "jira_incident_entitlement") {
		t.Fatalf("a second entitlement series was minted:\n%s", rendered)
	}
}
