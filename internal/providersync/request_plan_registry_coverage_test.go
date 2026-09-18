package providersync

import (
	"sort"
	"testing"
)

// unplannedRoutes pins every RouteReady+Plannable (provider,dataset) pair
// that ProviderRequestPlan does not cover yet, one reason each. This list
// may only shrink: a route leaving it (because a real plan was added) is
// caught below same as a route joining it (because a new active route
// shipped with no plan) -- neither can happen silently.
var unplannedRoutes = map[string]string{
	"github/blame":                   "no GitHub Blame budget term exists in githubRequestPlan",
	"github/commit-stats":            "no GitHub commit-stats budget term exists in githubRequestPlan",
	"github/commits":                 "no GitHub commits budget term exists in githubRequestPlan",
	"github/files":                   "no GitHub files budget term exists in githubRequestPlan",
	"github/repo-metadata":           "no GitHub repo-metadata budget term exists in githubRequestPlan",
	"github/security":                "no GitHub security budget term exists in githubRequestPlan",
	"gitlab/blame":                   "no GitLab blame budget term exists in gitLabRequestPlan",
	"gitlab/feature-flags":           "no GitLab feature-flags budget term exists in gitLabRequestPlan",
	"gitlab/files":                   "no GitLab files budget term exists in gitLabRequestPlan",
	"gitlab/prs":                     "no GitLab prs budget term exists in gitLabRequestPlan",
	"gitlab/repo-metadata":           "no GitLab repo-metadata budget term exists in gitLabRequestPlan",
	"gitlab/security":                "no GitLab security budget term exists in gitLabRequestPlan",
	"gitlab/work-items":              "no GitLab work-items budget term exists in gitLabRequestPlan",
	"pagerduty/business-services":    "ProviderRequestPlan has no pagerduty case at all",
	"pagerduty/escalation-policies":  "ProviderRequestPlan has no pagerduty case at all",
	"pagerduty/incident-alerts":      "ProviderRequestPlan has no pagerduty case at all",
	"pagerduty/incident-log-entries": "ProviderRequestPlan has no pagerduty case at all",
	"pagerduty/incident-notes":       "ProviderRequestPlan has no pagerduty case at all",
	"pagerduty/incidents":            "ProviderRequestPlan has no pagerduty case at all",
	"pagerduty/on-calls":             "ProviderRequestPlan has no pagerduty case at all",
	"pagerduty/schedules":            "ProviderRequestPlan has no pagerduty case at all",
	"pagerduty/services":             "ProviderRequestPlan has no pagerduty case at all",
	"pagerduty/teams":                "ProviderRequestPlan has no pagerduty case at all",
	"pagerduty/users":                "ProviderRequestPlan has no pagerduty case at all",
}

// TestRequestPlanCoversEveryPlannableRoute is the enumeration proof for the
// invariant this file exists to hold: for every RouteReady+Plannable route
// in the capability registry, the plan reserves at least the requests the
// route's own Collect sends. It walks datasetCapabilities -- the registry
// itself, not this package's own oracle case list -- so a route neither this
// file nor the oracle test names still gets checked.
//
// A route with a real plan is asserted non-empty here (the routes that also
// have an executed structural-and-quantitative harness proof, such as
// github/deployments, gitlab/deployments, and github/prs, carry that proof
// separately in request_budget_kind_coverage_test.go; this test only
// proves the plan is not silently empty). See
// TestPlannedRoutesAreHarnessProvenOrPinnedAsRemaining below for which
// planned routes that harness does and does not cover yet.
//
// A route with no plan must be named in unplannedRoutes with a reason. The
// two sides are asserted equal, not just "every empty one is pinned": a
// route leaving unplannedRoutes without a real plan shipping for it, or a
// new active route shipping with no plan, both fail this test.
func TestRequestPlanCoversEveryPlannableRoute(t *testing.T) {
	var actuallyUnplanned []string
	for provider, datasets := range datasetCapabilities {
		for dataset := range datasets {
			descriptor, ok := Descriptor(provider, dataset)
			if !ok || !descriptor.RouteReady || !descriptor.Plannable {
				continue
			}
			key := provider + "/" + dataset
			plan := ProviderRequestPlan(provider, dataset, 1, nil)
			if len(plan) == 0 {
				actuallyUnplanned = append(actuallyUnplanned, key)
				continue
			}
			if _, pinned := unplannedRoutes[key]; pinned {
				t.Errorf("%s is pinned in unplannedRoutes but ProviderRequestPlan now returns %d estimates -- drop it from unplannedRoutes", key, len(plan))
			}
		}
	}
	sort.Strings(actuallyUnplanned)

	var pinned []string
	for key := range unplannedRoutes {
		pinned = append(pinned, key)
	}
	sort.Strings(pinned)

	if len(actuallyUnplanned) != len(pinned) {
		t.Fatalf("unplannedRoutes is stale: registry has %d RouteReady+Plannable routes with an empty plan, unplannedRoutes pins %d\nregistry: %v\npinned:   %v", len(actuallyUnplanned), len(pinned), actuallyUnplanned, pinned)
	}
	for i := range actuallyUnplanned {
		if actuallyUnplanned[i] != pinned[i] {
			t.Fatalf("unplannedRoutes is stale at index %d: registry has %q, pinned has %q\nregistry: %v\npinned:   %v", i, actuallyUnplanned[i], pinned[i], actuallyUnplanned, pinned)
		}
	}
}

// harnessProvenRoutes are the planned routes request_budget_kind_coverage_test.go
// proves against both invariants (S: every request kind has a term; Q:
// planned >= counted on the grid, inside a stated domain) by running their
// real Collect against a fixture provider through a kind-classifying
// counting transport -- not merely asserted non-empty, the way every other
// planned route is proven above.
var harnessProvenRoutes = map[string]bool{
	"github/deployments": true,
	"gitlab/deployments": true,
	"github/prs":         true,
}

// harnessRemainingRoutes pins every OTHER planned route: a real,
// non-empty ProviderRequestPlan exists, but no executed structural/
// quantitative proof against its real Collect has been built yet. Each is
// the documented remainder of the same class of gap this ticket fixed for
// the three harnessProvenRoutes above -- named here, not fixed, so the
// follow-up scope is exact rather than "the rest, unspecified." This list
// may only shrink: a route gaining a harness proof leaves it (caught
// below), and a route cannot silently join or leave undetected.
var harnessRemainingRoutes = map[string]string{
	"github/cicd":                "no executed request-kind/bound proof yet; asserted non-empty only",
	"github/work-items":          "no executed request-kind/bound proof yet; asserted non-empty only",
	"gitlab/cicd":                "no executed request-kind/bound proof yet; asserted non-empty only",
	"gitlab/commit-stats":        "no executed request-kind/bound proof yet; asserted non-empty only",
	"gitlab/commits":             "no executed request-kind/bound proof yet; asserted non-empty only",
	"gitlab/incidents":           "no executed request-kind/bound proof yet; asserted non-empty only",
	"jira/incidents":             "no executed request-kind/bound proof yet; asserted non-empty only",
	"jira/work-items":            "no executed request-kind/bound proof yet; asserted non-empty only",
	"launchdarkly/feature-flags": "no executed request-kind/bound proof yet; asserted non-empty only",
	"linear/work-items":          "no executed request-kind/bound proof yet; asserted non-empty only",
}

// TestPlannedRoutesAreHarnessProvenOrPinnedAsRemaining is the structural
// half of this ticket's registry-enumerated proof, applied to the routes
// that DO have a plan rather than the ones that don't: every planned route
// must be either harness-proven (harnessProvenRoutes) or named in
// harnessRemainingRoutes with a reason. A planned route on neither list
// fails -- both a route this PR silently left unproven and unnamed, and a
// route whose harness proof was removed without updating
// harnessProvenRoutes, are caught the same way unplannedRoutes catches its
// own drift.
func TestPlannedRoutesAreHarnessProvenOrPinnedAsRemaining(t *testing.T) {
	var actuallyRemaining []string
	for provider, datasets := range datasetCapabilities {
		for dataset := range datasets {
			descriptor, ok := Descriptor(provider, dataset)
			if !ok || !descriptor.RouteReady || !descriptor.Plannable {
				continue
			}
			key := provider + "/" + dataset
			if len(ProviderRequestPlan(provider, dataset, 1, nil)) == 0 {
				continue
			}
			if harnessProvenRoutes[key] {
				if _, pinned := harnessRemainingRoutes[key]; pinned {
					t.Errorf("%s is both harness-proven and pinned in harnessRemainingRoutes -- drop it from harnessRemainingRoutes", key)
				}
				continue
			}
			actuallyRemaining = append(actuallyRemaining, key)
		}
	}
	sort.Strings(actuallyRemaining)

	var pinned []string
	for key := range harnessRemainingRoutes {
		pinned = append(pinned, key)
	}
	sort.Strings(pinned)

	if len(actuallyRemaining) != len(pinned) {
		t.Fatalf("harnessRemainingRoutes is stale: registry has %d planned routes with no harness proof and not marked proven, harnessRemainingRoutes pins %d\nregistry: %v\npinned:   %v", len(actuallyRemaining), len(pinned), actuallyRemaining, pinned)
	}
	for i := range actuallyRemaining {
		if actuallyRemaining[i] != pinned[i] {
			t.Fatalf("harnessRemainingRoutes is stale at index %d: registry has %q, pinned has %q\nregistry: %v\npinned:   %v", i, actuallyRemaining[i], pinned[i], actuallyRemaining, pinned)
		}
	}
}
