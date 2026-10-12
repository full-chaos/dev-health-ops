package providersync

import (
	"context"
	"fmt"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// selectorConn is a connection the sink constructors keep and never call here.
type selectorConn struct{ driver.Conn }

// Each provider's work-items claim builds the handler and the sink the worker ran before the selection
// moved into SelectWorkItemsRoute: one pair per provider, the sink also the readback.
func TestSelectWorkItemsRouteBuildsEachProvidersHandlerAndSink(t *testing.T) {
	deps := WorkItemsRouteDeps{
		Conn: selectorConn{}, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
		Metrics: providerfoundation.NewMetrics(), StatusMapping: &StatusMapping{},
	}
	for provider, want := range map[string][2]string{
		"github": {"providersync.GitHubWorkItemsRouteHandler", "providersync.GitHubWorkItemClickHouseEffects"},
		"gitlab": {"providersync.GitLabWorkItemsRouteHandler", "providersync.GitLabWorkItemFamilyClickHouseEffects"},
		"jira":   {"providersync.JiraAtlassianRouteHandler", "providersync.JiraWorkItemCompositeClickHouseEffects"},
		"linear": {"providersync.LinearWorkItemFamilyRouteHandler", "providersync.LinearWorkItemFamilyClickHouseEffects"},
	} {
		route, ok, err := SelectWorkItemsRoute(provider, "work-items", deps)
		if err != nil || !ok {
			t.Fatalf("%s: ok=%v err=%v", provider, ok, err)
		}
		if got := fmt.Sprintf("%T", route.Handler); got != want[0] {
			t.Errorf("%s handler %s, want %s", provider, got, want[0])
		}
		if got := fmt.Sprintf("%T", route.Sink); got != want[1] {
			t.Errorf("%s sink %s, want %s", provider, got, want[1])
		}
		if fmt.Sprintf("%T", route.Readback) != fmt.Sprintf("%T", route.Sink) {
			t.Errorf("%s readback %T is not its sink", provider, route.Readback)
		}
	}
	for _, refused := range []struct{ provider, dataset string }{{"github", "prs"}, {"pagerduty", "work-items"}, {"github", "work-item-labels"}} {
		if _, ok, _ := SelectWorkItemsRoute(refused.provider, refused.dataset, deps); ok {
			t.Errorf("%s/%s selected a work-items route", refused.provider, refused.dataset)
		}
	}
	noMapping := deps
	noMapping.StatusMapping = nil
	if _, ok, _ := SelectWorkItemsRoute("jira", "work-items", noMapping); ok {
		t.Error("a work-items route was selected without a status mapping")
	}
}
