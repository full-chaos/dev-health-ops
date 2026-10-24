package providersync

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// TestJiraWorkItemsRouteHoldsNoDerivedMetricEffect pins the route side of the
// former route oracle pair. The Jira Atlassian route used to build a
// work_item_metrics_daily effect, and this test compared it with the recorded
// Python answer (testdata/oracle_frozen, jira_work-items_metrics-daily_route).
// The route builds no derived effect now: the daily job computes that table
// from stored rows. The recorded answer stays on disk unchanged; this test no
// longer reads it. The fixture still completes an item on August 1, so a route
// that derived metrics again would produce a row here.
func TestJiraWorkItemsRouteHoldsNoDerivedMetricEffect(t *testing.T) {
	claim := jiraAtlassianClaim()
	since := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	before := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	claim.SinceAt, claim.BeforeAt = &since, &before
	normalizedAt := time.Date(2026, 8, 10, 12, 0, 0, 123456000, time.UTC)
	client := jiraWorkItemsTestClient(
		t,
		fakehttp.Client(&jiraAtlassianDoer{t: t}),
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	leftToDaily := providerfoundation.NewMetrics()
	client.Metrics = leftToDaily
	batch, err := (JiraAtlassianRouteHandler{
		StatusMapping: loadRealStatusMapping(t),
		Identity:      jiraRouteIdentity,
	}).Collect(context.Background(), claim, providerfoundation.Credential{}, client, normalizedAt)
	if err != nil {
		t.Fatal(err)
	}
	if len(jiraEffectObjects(t, batch.Effects, "work_items")) == 0 {
		t.Fatal("the route stored no work item: the absence of a derived effect would prove nothing")
	}
	assertJiraAtlassianRawOnlyEffects(t, batch)
	assertWorkItemDerivedTablesLeftToDailyJob(t, leftToDaily, "jira", 1)
}

func jiraEffectObjects(t *testing.T, effects []EffectBatch, destination string) []any {
	t.Helper()
	for _, effect := range effects {
		if effect.Destination != destination {
			continue
		}
		objects := make([]any, 0, len(effect.Rows))
		for _, raw := range effect.Rows {
			var object map[string]any
			if err := json.Unmarshal(raw, &object); err != nil {
				t.Fatal(err)
			}
			objects = append(objects, object)
		}
		return objects
	}
	t.Fatalf("Jira route omitted direct destination %q", destination)
	return nil
}
