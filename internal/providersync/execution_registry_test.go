package providersync

import (
	"slices"
	"testing"
)

// TestCanonicalDescriptorRecognisesEveryCapability proves the unification: the
// production route descriptor and the dataset capability registry recognise
// exactly the same provider/dataset set, so no slice can be wired to a
// descriptor path the router does not consult.
func TestCanonicalDescriptorRecognisesEveryCapability(t *testing.T) {
	t.Parallel()
	switches := CompleteRouteSwitches{}
	for _, provider := range MatrixProviders() {
		capabilities := Capabilities(provider)
		if len(capabilities) == 0 {
			t.Fatalf("%s has no capabilities", provider)
		}
		for _, capability := range capabilities {
			descriptor, ok := switches.Descriptor(provider, capability.Dataset)
			if !ok || descriptor.Provider != provider ||
				descriptor.RequestedDataset != capability.Dataset ||
				descriptor.Executor != ProviderExecutor(provider, capability.Dataset) ||
				descriptor.NativeShadow != nativeShadowReady(provider, capability.Dataset) {
				t.Fatalf("%s/%s descriptor=%+v ok=%v", provider, capability.Dataset, descriptor, ok)
			}
		}
	}
	if _, ok := switches.Descriptor("github", "feature-flags"); ok {
		t.Fatal("unconfigured pair resolved a descriptor")
	}
	if _, ok := switches.Descriptor("bitbucket", "commits"); ok {
		t.Fatal("unknown provider resolved a descriptor")
	}
}

func TestCompleteRouteSwitchesCollapseWorkItemAliasesAndRemainIndependent(t *testing.T) {
	t.Parallel()
	switches := CompleteRouteSwitches{
		LinearWorkItems: true, JiraIncidents: true,
	}
	linear, ok := switches.Descriptor("linear", "work-items")
	if !ok || linear.RouteReady || linear.RouteEnabled ||
		linear.RouteDataset != "work-items" ||
		len(linear.Destinations) != len(workItemRouteDestinations()) {
		t.Fatalf("linear route=%+v ok=%v", linear, ok)
	}
	for _, alias := range []string{
		"work-item-labels", "work-item-projects",
		"work-item-history", "work-item-comments",
	} {
		descriptor, ok := switches.Descriptor("linear", alias)
		if !ok || descriptor.RouteReady || descriptor.RouteEnabled ||
			descriptor.RouteDataset != "work-items" {
			t.Fatalf("linear alias %s=%+v ok=%v", alias, descriptor, ok)
		}
	}
	jiraWorkItems, _ := switches.Descriptor("jira", "work-items")
	jiraIncidents, _ := switches.Descriptor("jira", "incidents")
	launchDarkly, _ := switches.Descriptor("launchdarkly", "feature-flags")
	if jiraWorkItems.RouteEnabled || !jiraIncidents.RouteReady ||
		!jiraIncidents.RouteEnabled || launchDarkly.RouteEnabled {
		t.Fatalf(
			"independent routes jira_work=%+v jira_incidents=%+v ld=%+v",
			jiraWorkItems, jiraIncidents, launchDarkly,
		)
	}
}

// TestGitHubWorkItemRouteDestinationManifest pins the semantic family contract
// independently of Linear expired-lease recovery. A missing or reordered entry
// would alter descriptor/snapshot identity and must fail before a route can be
// declared ready.
func TestGitHubWorkItemRouteDestinationManifest(t *testing.T) {
	t.Parallel()
	want := []string{
		"ai_attribution", "estimate_coverage_metrics_daily",
		"investment_classifications_daily", "investment_metrics_daily",
		"issue_type_metrics_daily", "sprints", "work_item_cycle_times",
		"work_item_dependencies", "work_item_interactions",
		"work_item_metrics_daily", "work_item_reopen_events",
		"work_item_state_durations_daily", "work_item_team_attributions",
		"work_item_transitions", "work_item_user_metrics_daily", "work_items",
	}
	got := workItemRouteDestinations()
	if !slices.Equal(got, want) {
		t.Fatalf("github work-item destinations=%v want=%v", got, want)
	}
	effects, err := BuildGitHubWorkItemEffects(GitHubWorkItemEffectRows{})
	if err != nil {
		t.Fatal(err)
	}
	got = got[:0]
	for _, effect := range effects {
		got = append(got, effect.Destination)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("effect destinations=%v want=%v", got, want)
	}
}

// TestShadowProjectionIsDerivedAndNarrow keeps the parity harness from
// becoming a second capability registry.
func TestShadowProjectionIsDerivedAndNarrow(t *testing.T) {
	t.Parallel()
	switches := CompleteRouteSwitches{}
	for _, provider := range []string{"github", "gitlab"} {
		descriptor, _ := switches.Descriptor(provider, "repo-metadata")
		shadow, ok := descriptor.Shadow(true)
		if !ok || shadow.Provider != provider || shadow.Dataset != "repo-metadata" ||
			!shadow.Write {
			t.Fatalf("%s shadow=%+v ok=%v", provider, shadow, ok)
		}
	}
	for _, test := range []struct{ provider, dataset string }{
		{"github", "commits"},
		{"github", "work-items"},
		{"gitlab", "work-item-labels"},
		{"linear", "work-items"},
		{"jira", "incidents"},
		{"launchdarkly", "feature-flags"},
		{"pagerduty", "incidents"},
	} {
		descriptor, ok := switches.Descriptor(test.provider, test.dataset)
		if !ok {
			t.Fatalf("%s/%s has no descriptor", test.provider, test.dataset)
		}
		if shadow, ok := descriptor.Shadow(true); ok {
			t.Fatalf("%s/%s shadow-eligible: %+v", test.provider, test.dataset, shadow)
		}
	}
}

// TestGitHubWorkItemFamilyIsAtomicAndCanonicalClaimOnly pins CHAOS-3606's
// all-five-alias activation boundary: GitHub now has one complete native
// family, while GitLab's still-unported fixture collectors remain closed.
func TestGitHubWorkItemFamilyIsAtomicAndCanonicalClaimOnly(t *testing.T) {
	t.Parallel()
	switches := CompleteRouteSwitches{
		LinearWorkItems: true, JiraWorkItems: true, JiraIncidents: true,
		LaunchDarklyFeatureFlags: true,
		GithubWorkItems:          true,
	}
	for _, dataset := range []string{
		"work-items", "work-item-labels", "work-item-projects",
		"work-item-history", "work-item-comments",
	} {
		descriptor, ok := switches.Descriptor("github", dataset)
		if !ok || descriptor.NativeShadow || !descriptor.RouteReady ||
			descriptor.Executor != ExecutorNativeGo ||
			!slices.Equal(descriptor.Destinations, workItemRouteDestinations()) {
			t.Fatalf("github/%s descriptor=%+v ok=%v", dataset, descriptor, ok)
		}
		if dataset == "work-items" {
			if !descriptor.RouteEnabled || !descriptor.PreparedManifestRecovery ||
				descriptor.RouteDataset != "work-items" {
				t.Fatalf("canonical github/%s descriptor=%+v", dataset, descriptor)
			}
			continue
		}
		// This is the direct-alias guard, not a missing capability. The Python
		// planner collapses aliases into the canonical claim and exposes them
		// only through family_dataset_* flags; letting a malformed direct alias
		// through here would run a partial writer. providerunit.Handler observes
		// this disabled descriptor before BuildExecutor can resolve credentials,
		// make HTTP calls, write effects, or advance a watermark.
		if descriptor.RouteEnabled || descriptor.PreparedManifestRecovery ||
			descriptor.RouteDataset != dataset {
			t.Fatalf("direct alias github/%s descriptor=%+v", dataset, descriptor)
		}
	}
	for _, dataset := range []string{
		"work-items", "work-item-labels", "work-item-projects",
		"work-item-history", "work-item-comments",
	} {
		descriptor, ok := switches.Descriptor("gitlab", dataset)
		if !ok || descriptor.NativeShadow || descriptor.RouteReady ||
			descriptor.RouteEnabled || descriptor.Executor != ExecutorNone {
			t.Fatalf("gitlab/%s descriptor=%+v ok=%v", dataset, descriptor, ok)
		}
	}
}

// TestPagerDutyIsCoveredByTheSameContract closes the largest CUT-08 gap: every
// PagerDuty dataset now resolves the same descriptor type as every other
// provider, with honest false readiness.
func TestPagerDutyIsCoveredByTheSameContract(t *testing.T) {
	t.Parallel()
	datasets := []string{
		"services", "business-services", "escalation-policies", "schedules",
		"on-calls", "users", "teams", "incidents", "incident-alerts",
		"incident-log-entries", "incident-notes",
	}
	switches := CompleteRouteSwitches{
		LinearWorkItems: true, JiraWorkItems: true, JiraIncidents: true,
		LaunchDarklyFeatureFlags: true,
	}
	for _, dataset := range datasets {
		descriptor, ok := switches.Descriptor("pagerduty", dataset)
		if !ok || descriptor.RouteReady || descriptor.RouteEnabled ||
			descriptor.Executor != ExecutorNone {
			t.Fatalf("pagerduty/%s descriptor=%+v ok=%v", dataset, descriptor, ok)
		}
		capability, ok := Capability("pagerduty", dataset)
		if !ok || len(capability.LegacyTargets) != 1 ||
			capability.LegacyTargets[0] != "operational" {
			t.Fatalf("pagerduty/%s capability=%+v ok=%v", dataset, capability, ok)
		}
	}
	if len(Capabilities("pagerduty")) != len(datasets) {
		t.Fatalf("pagerduty capabilities=%d", len(Capabilities("pagerduty")))
	}
}
