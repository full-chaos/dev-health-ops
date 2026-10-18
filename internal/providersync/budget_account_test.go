package providersync

import (
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/syncbudget"
)

// ambiguousBudgetFamilies is the closed list of (provider, dataset) pairs whose
// dispatch estimate names several route families on ONE budget dimension. There
// the family cannot be read off the wire or resolved from the unit's estimates,
// so every request of the route must carry an explicit budgetAccount label (and
// a route test proves it). The test below recomputes the list from the
// estimator, so an estimate that gains a second family on a dimension fails it
// until the route is labelled and the pair added here.
var ambiguousBudgetFamilies = []string{
	"gitlab/incidents",
	"gitlab/pr-comments",
	"gitlab/work-item-comments",
	"gitlab/work-item-history",
	"gitlab/work-items",
	"jira/incidents",
	"jira/work-item-comments",
	"jira/work-item-history",
	"jira/work-items",
	"launchdarkly/feature-flags",
	"linear/work-item-labels",
	"linear/work-items",
}

func allBudgetDatasets() []string {
	return []string{
		"repo-metadata", "commits", "commit-stats", "files", "blame", "prs", "pr-reviews", "pr-comments", "cicd",
		"tests", "deployments", "incidents", "security", "work-items", "work-item-labels", "work-item-projects",
		"work-item-history", "work-item-comments", "feature-flags",
	}
}

func estimatedFamilies(t *testing.T, provider, dataset string) map[string]map[string]bool {
	t.Helper()
	flags := map[string]bool{}
	for _, name := range []string{"sync_prs", "jira_fetch_worklogs", "fetch_worklogs", "atlassian_gql_enabled", "gql_enabled"} {
		flags[name] = true
	}
	estimates, err := syncbudget.EstimateProviderBudget(syncbudget.Context{Provider: provider, DatasetKey: dataset, ProcessorFlags: flags})
	if err != nil {
		t.Fatalf("estimate %s/%s: %v", provider, dataset, err)
	}
	byDimension := map[string]map[string]bool{}
	for _, estimate := range estimates {
		if byDimension[estimate.Bucket.Dimension] == nil {
			byDimension[estimate.Bucket.Dimension] = map[string]bool{}
		}
		byDimension[estimate.Bucket.Dimension][estimate.RouteFamily] = true
	}
	return byDimension
}

func TestTheAmbiguousBudgetFamilyListMatchesTheEstimator(t *testing.T) {
	var computed []string
	for _, provider := range []string{"github", "gitlab", "jira", "linear", "pagerduty", "launchdarkly"} {
		for _, dataset := range allBudgetDatasets() {
			for _, families := range estimatedFamilies(t, provider, dataset) {
				if len(families) > 1 {
					computed = append(computed, provider+"/"+dataset)
					break
				}
			}
		}
	}
	sort.Strings(computed)
	want := append([]string(nil), ambiguousBudgetFamilies...)
	sort.Strings(want)
	if strings.Join(computed, ",") != strings.Join(want, ",") {
		t.Fatalf("estimator has several families on one dimension for %v, the labelled list is %v: label the new route's requests with budgetAccount and update the list", computed, want)
	}
}

// assertRouteRequestsAreLabelled is the route test of an ambiguous pair: every
// request the route made carried a family the estimate names for the dimension
// it was counted under (none "unclassified"), and the families in mustHave were
// all seen.
func assertRouteRequestsAreLabelled(t *testing.T, provider, dataset string, ledger *providerfoundation.RequestLedger, mustHave ...string) {
	t.Helper()
	entries, dropped := ledger.Snapshot()
	if dropped != 0 || len(entries) == 0 {
		t.Fatalf("%s/%s: ledger has %d entries and %d dropped, want counted traffic", provider, dataset, len(entries), dropped)
	}
	estimated := estimatedFamilies(t, provider, dataset)
	seen := map[string]bool{}
	for _, entry := range entries {
		if entry.RouteFamily == providerfoundation.LedgerUnclassified {
			t.Errorf("%s/%s: %d request(s) under %s were not labelled with a route family", provider, dataset, entry.Requests, entry.Dimension)
			continue
		}
		if !estimated[entry.Dimension][entry.RouteFamily] {
			t.Errorf("%s/%s: %d request(s) labelled %s/%s, a pair the estimate does not name (estimate: %v)", provider, dataset, entry.Requests, entry.RouteFamily, entry.Dimension, estimated)
		}
		seen[entry.RouteFamily] = true
	}
	for _, family := range mustHave {
		if !seen[family] {
			t.Errorf("%s/%s: no request was counted under route family %s (saw %v)", provider, dataset, family, seen)
		}
	}
}
