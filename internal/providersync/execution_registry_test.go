package providersync

import (
	"reflect"
	"slices"
	"testing"
)

// TestAggregateProviderDescriptorsAreNativeReadyAndPlannableMatchesTopology
// replaces the pre-CHAOS-4054 "default off" census: capability is always on
// in the binary, so what varies across this table is not activation but
// registry topology -- whether each identity is the canonical writer for its
// family (Plannable) or an alias kept RouteReady for audit/watermark
// visibility only.
func TestAggregateProviderDescriptorsAreNativeReadyAndPlannableMatchesTopology(t *testing.T) {
	t.Parallel()
	tests := []struct {
		provider     string
		dataset      string
		plannable    bool
		destinations []string
	}{
		{"gitlab", "deployments", true, []string{"deployments"}},
		{"gitlab", "feature-flags", true, []string{"feature_flag", "feature_flag_event", "work_graph_edges"}},
		{"gitlab", "files", true, []string{"git_files"}},
		{"gitlab", "blame", true, []string{"git_blame"}},
		{"gitlab", "prs", true, []string{"git_pull_requests", "git_pull_request_reviews"}},
		{"gitlab", "pr-reviews", false, []string{"git_pull_requests", "git_pull_request_reviews"}},
		{"gitlab", "pr-comments", false, []string{"git_pull_requests", "git_pull_request_reviews"}},
		{"gitlab", "security", true, []string{"security_alerts"}},
		{"gitlab", "work-items", true, workItemRouteDestinations()},
		{"gitlab", "work-item-labels", false, workItemRouteDestinations()},
		{"gitlab", "work-item-projects", false, workItemRouteDestinations()},
		{"gitlab", "work-item-history", false, workItemRouteDestinations()},
		{"gitlab", "work-item-comments", false, workItemRouteDestinations()},
		{"jira", "work-items", true, append(workItemRouteDestinations(), "worklogs", "project_membership_transitions", "projects")},
		{"jira", "work-item-labels", false, append(workItemRouteDestinations(), "worklogs", "project_membership_transitions", "projects")},
		{"jira", "work-item-projects", false, append(workItemRouteDestinations(), "worklogs", "project_membership_transitions", "projects")},
		{"jira", "work-item-history", false, append(workItemRouteDestinations(), "worklogs", "project_membership_transitions", "projects")},
		{"jira", "work-item-comments", false, append(workItemRouteDestinations(), "worklogs", "project_membership_transitions", "projects")},
		{"linear", "work-items", true, append(workItemRouteDestinations(), "project_membership_transitions", "projects")},
		{"linear", "work-item-labels", false, append(workItemRouteDestinations(), "project_membership_transitions", "projects")},
		{"linear", "work-item-projects", false, append(workItemRouteDestinations(), "project_membership_transitions", "projects")},
		{"linear", "work-item-history", false, append(workItemRouteDestinations(), "project_membership_transitions", "projects")},
		{"linear", "work-item-comments", false, append(workItemRouteDestinations(), "project_membership_transitions", "projects")},
		// PagerDuty's whole family is deliberately NOT collapsed: every dataset
		// is its own independent, canonical, plannable claim (CHAOS-4054).
		{"pagerduty", "services", true, []string{"operational_services", "operational_service_repository_mappings"}},
		{"pagerduty", "business-services", true, []string{"operational_services"}},
		{"pagerduty", "escalation-policies", true, []string{"operational_escalation_policies"}},
		{"pagerduty", "schedules", true, []string{"operational_on_call_schedules"}},
		{"pagerduty", "on-calls", true, []string{"operational_on_call_assignments"}},
		{"pagerduty", "users", true, []string{"operational_users"}},
		{"pagerduty", "teams", true, []string{"operational_teams"}},
		{"pagerduty", "incidents", true, []string{"operational_incidents"}},
		{"pagerduty", "incident-alerts", true, []string{"operational_alerts"}},
		{"pagerduty", "incident-log-entries", true, []string{"operational_incident_timeline_events"}},
		{"pagerduty", "incident-notes", true, []string{"operational_incident_notes"}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.provider+"/"+test.dataset, func(t *testing.T) {
			t.Parallel()
			descriptor, ok := Descriptor(test.provider, test.dataset)
			if !ok || descriptor.Executor != ExecutorNativeGo || !descriptor.RouteReady ||
				descriptor.Plannable != test.plannable ||
				!slices.Equal(descriptor.Destinations, test.destinations) {
				t.Fatalf("descriptor=%+v known=%t want plannable=%v destinations=%v",
					descriptor, ok, test.plannable, test.destinations)
			}
		})
	}
}

// TestCanonicalDescriptorRecognisesEveryCapability proves the unification: the
// production route descriptor and the dataset capability registry recognise
// exactly the same provider/dataset set, so no slice can be wired to a
// descriptor path the router does not consult.
func TestCanonicalDescriptorRecognisesEveryCapability(t *testing.T) {
	t.Parallel()
	for _, provider := range MatrixProviders() {
		capabilities := Capabilities(provider)
		if len(capabilities) == 0 {
			t.Fatalf("%s has no capabilities", provider)
		}
		for _, capability := range capabilities {
			descriptor, ok := Descriptor(provider, capability.Dataset)
			if !ok || descriptor.Provider != provider ||
				descriptor.RequestedDataset != capability.Dataset ||
				descriptor.Executor != ProviderExecutor(provider, capability.Dataset) ||
				descriptor.NativeShadow != nativeShadowReady(provider, capability.Dataset) {
				t.Fatalf("%s/%s descriptor=%+v ok=%v", provider, capability.Dataset, descriptor, ok)
			}
		}
	}
	if _, ok := Descriptor("github", "feature-flags"); ok {
		t.Fatal("unconfigured pair resolved a descriptor")
	}
	if _, ok := Descriptor("bitbucket", "commits"); ok {
		t.Fatal("unknown provider resolved a descriptor")
	}
}

// TestWorkItemAliasesStayCanonicalOnlyAcrossIndependentFamilies replaces the
// old switches-independence test. There is no configuration left to be
// independent of (CHAOS-4054: Descriptor takes none), so what this now pins
// is registry topology: every provider's work-item family collapses onto its
// one canonical `work-items` writer while its four sibling dataset identities
// stay RouteReady-but-not-Plannable aliases, and unrelated families
// (jira/incidents, launchdarkly/feature-flags) are unconditionally plannable
// on their own, independent of any other family's state.
func TestWorkItemAliasesStayCanonicalOnlyAcrossIndependentFamilies(t *testing.T) {
	t.Parallel()
	linear, ok := Descriptor("linear", "work-items")
	if !ok || !linear.RouteReady || !linear.Plannable ||
		linear.RouteDataset != "work-items" ||
		len(linear.Destinations) != len(workItemRouteDestinations())+2 {
		t.Fatalf("linear route=%+v ok=%v", linear, ok)
	}
	for _, alias := range []string{
		"work-item-labels", "work-item-projects",
		"work-item-history", "work-item-comments",
	} {
		descriptor, ok := Descriptor("linear", alias)
		if !ok || !descriptor.RouteReady || descriptor.Plannable ||
			descriptor.RouteDataset != "work-items" {
			t.Fatalf("linear alias %s=%+v ok=%v", alias, descriptor, ok)
		}
	}
	jiraWorkItems, _ := Descriptor("jira", "work-items")
	jiraIncidents, _ := Descriptor("jira", "incidents")
	launchDarkly, _ := Descriptor("launchdarkly", "feature-flags")
	if !jiraWorkItems.Plannable || !jiraIncidents.RouteReady ||
		!jiraIncidents.Plannable || !launchDarkly.Plannable {
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
	// CHAOS-4194: github writes two surfaces no other work-item provider does,
	// and they sit in ALPHABETICAL position because the manifest's declaration
	// order is the order effects are built and ledger-indexed in. The shared
	// family list that gitlab, jira and linear advertise deliberately stops
	// before these two, which is asserted directly below.
	want = slices.Concat(
		want[:5], []string{"project_membership_transitions", "projects"}, want[5:],
	)
	got := githubWorkItemRouteDestinations()
	if !slices.Equal(got, want) {
		t.Fatalf("github work-item destinations=%v want=%v", got, want)
	}
	// The divergence, asserted rather than implied. gitlab must never write a
	// project membership -- its "project" concept IS repo_id, which is why it
	// is refused for that kind by construction -- so publishing these two under
	// the shared family list would advertise a capability gitlab cannot have.
	for _, destination := range []string{"project_membership_transitions", "projects"} {
		if slices.Contains(workItemRouteDestinations(), destination) {
			t.Fatalf("%q entered the shared work-item family route", destination)
		}
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
	for _, provider := range []string{"github", "gitlab"} {
		descriptor, _ := Descriptor(provider, "repo-metadata")
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
		descriptor, ok := Descriptor(test.provider, test.dataset)
		if !ok {
			t.Fatalf("%s/%s has no descriptor", test.provider, test.dataset)
		}
		if shadow, ok := descriptor.Shadow(true); ok {
			t.Fatalf("%s/%s shadow-eligible: %+v", test.provider, test.dataset, shadow)
		}
	}
}

// TestGitHubWorkItemFamilyIsAtomicAndCanonicalClaimOnly pins CHAOS-3606's
// all-five-alias activation boundary: each completed provider has one native
// family while direct aliases remain closed claims.
func TestGitHubWorkItemFamilyIsAtomicAndCanonicalClaimOnly(t *testing.T) {
	t.Parallel()
	for _, dataset := range []string{
		"work-items", "work-item-labels", "work-item-projects",
		"work-item-history", "work-item-comments",
	} {
		descriptor, ok := Descriptor("github", dataset)
		if !ok || descriptor.NativeShadow || !descriptor.RouteReady ||
			descriptor.Executor != ExecutorNativeGo ||
			!slices.Equal(descriptor.Destinations, githubWorkItemRouteDestinations()) {
			t.Fatalf("github/%s descriptor=%+v ok=%v", dataset, descriptor, ok)
		}
		if dataset == "work-items" {
			if !descriptor.Plannable || !descriptor.PreparedManifestRecovery ||
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
		if descriptor.Plannable || descriptor.PreparedManifestRecovery ||
			descriptor.RouteDataset != dataset {
			t.Fatalf("direct alias github/%s descriptor=%+v", dataset, descriptor)
		}
	}
	for _, provider := range []string{"gitlab", "jira", "linear"} {
		// The SHARED family list: this loop covers gitlab, jira and linear.
		// gitlab alone never writes the two membership surfaces -- its
		// "project" concept IS repo_id, so it is refused for that kind by
		// construction. jira and linear write them directly via their own
		// internal sync routes as of CHAOS-4193, the same way github's does
		// (CHAOS-4194) -- not through the external-ingest path an earlier
		// design comment assumed.
		wantDestinations := workItemRouteDestinations()
		switch provider {
		case "jira":
			wantDestinations = append(wantDestinations, "worklogs", "project_membership_transitions", "projects")
		case "linear":
			wantDestinations = append(wantDestinations, "project_membership_transitions", "projects")
		}
		for _, dataset := range []string{
			"work-items", "work-item-labels", "work-item-projects",
			"work-item-history", "work-item-comments",
		} {
			descriptor, ok := Descriptor(provider, dataset)
			wantRouteDataset := dataset
			if provider == "jira" || provider == "linear" {
				wantRouteDataset = "work-items"
			}
			if !ok || descriptor.NativeShadow || !descriptor.RouteReady ||
				descriptor.Executor != ExecutorNativeGo ||
				descriptor.RouteDataset != wantRouteDataset ||
				!slices.Equal(descriptor.Destinations, wantDestinations) {
				t.Fatalf("%s/%s descriptor=%+v ok=%v", provider, dataset, descriptor, ok)
			}
			if descriptor.Plannable != (dataset == "work-items") {
				t.Fatalf("%s/%s enabled=%t", provider, dataset, descriptor.Plannable)
			}
		}
	}
}

// TestPagerDutyIsCoveredByTheSameContract closes the largest CUT-08 gap: every
// PagerDuty dataset now resolves the same descriptor type as every other
// provider, with native readiness. The family is deliberately NOT collapsed
// (CHAOS-4054 decision record): every dataset is its own independent,
// unconditionally plannable claim, not an alias of any sibling.
func TestPagerDutyIsCoveredByTheSameContract(t *testing.T) {
	t.Parallel()
	datasets := []string{
		"services", "business-services", "escalation-policies", "schedules",
		"on-calls", "users", "teams", "incidents", "incident-alerts",
		"incident-log-entries", "incident-notes",
	}
	for _, dataset := range datasets {
		descriptor, ok := Descriptor("pagerduty", dataset)
		if !ok || !descriptor.RouteReady || !descriptor.Plannable ||
			descriptor.Executor != ExecutorNativeGo || len(descriptor.Destinations) == 0 {
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

// TestPagerDutyDescriptorSwitchesArePerDataset had no successor: its premise
// was that only one dataset's switch was enabled at a time, isolating that
// dataset's Plannable=true from its siblings. CHAOS-4054 deleted the switch
// plane entirely, and the decision record deliberately keeps PagerDuty's
// family uncollapsed -- every one of its eleven datasets is unconditionally
// Plannable at once (see TestPagerDutyIsCoveredByTheSameContract). There is no
// "isolated switch" state left to assert; the per-dataset independence this
// test protected is now a permanent, unconditional fact, not a probe-by-probe
// one, so it was deleted rather than left checking something the API can no
// longer express.

// completeRouteWriterFamily names the writer family a RouteReady identity
// belongs to, matching the CHAOS-4054 decision record precisely: every
// provider's work-item dataset identities collapse onto that provider's
// "work-items"; GitHub/GitLab's pr-reviews and pr-comments collapse onto
// "prs"; GitHub/GitLab's tests collapses onto "cicd". Every other identity,
// including every PagerDuty dataset, is its own single-member family.
func completeRouteWriterFamily(provider, dataset string) string {
	switch {
	case isWorkItemFamilyDataset(dataset):
		return "work-items"
	case (provider == "github" || provider == "gitlab") &&
		(dataset == "pr-reviews" || dataset == "pr-comments"):
		return "prs"
	case (provider == "github" || provider == "gitlab") && dataset == "tests":
		return "cicd"
	default:
		return dataset
	}
}

// TestExactlyOnePlannableIdentityPerWriterFamily is CHAOS-4054's step-1
// acceptance for the plannable-identity accessor: Descriptor's Plannable
// field must resolve to exactly one plannable member per writer family,
// never a sibling alias independently listed alongside its canonical writer,
// and never a family with zero plannable members either.
func TestExactlyOnePlannableIdentityPerWriterFamily(t *testing.T) {
	t.Parallel()
	plannableByFamily := map[string]int{}
	familiesSeen := map[string]struct{}{}
	for _, pair := range BuildProviderMatrix().Pairs {
		if !pair.RouteReady {
			continue
		}
		family := pair.Provider + "/" + completeRouteWriterFamily(pair.Provider, pair.Dataset)
		familiesSeen[family] = struct{}{}
		if pair.Plannable {
			plannableByFamily[family]++
		}
	}
	if len(familiesSeen) == 0 {
		t.Fatal("no route-ready writer families found")
	}
	for family := range familiesSeen {
		if count := plannableByFamily[family]; count != 1 {
			t.Fatalf("writer family %s has %d plannable identities, want exactly 1", family, count)
		}
	}
}

// TestDescriptorTakesNoConfigurationFromTheEnvironment pins CHAOS-4054's core
// invariant directly: capability is always on in the binary, and Descriptor
// reads nothing but its two arguments. No environment variable -- including
// names shaped like the deleted per-route enable switches -- may move
// RouteReady or Plannable for any pair.
func TestDescriptorTakesNoConfigurationFromTheEnvironment(t *testing.T) {
	type probe struct {
		provider, dataset string
		descriptor        CompleteRouteDescriptor
	}
	var baseline []probe
	for _, provider := range MatrixProviders() {
		for _, capability := range Capabilities(provider) {
			descriptor, ok := Descriptor(provider, capability.Dataset)
			if !ok {
				t.Fatalf("%s/%s lost its descriptor", provider, capability.Dataset)
			}
			baseline = append(baseline, probe{provider, capability.Dataset, descriptor})
		}
	}
	// Deliberately shaped like the deleted route-enable switches, but never
	// matching the WORKER_*_ENABLED pattern itself: that exact family must
	// never be read anywhere in this package (see .remember/chaos-4054-context.md).
	for key, value := range map[string]string{
		"GITHUB_CICD_ENABLED":               "true",
		"GITHUB_TESTS_ENABLED":              "true",
		"GITHUB_WORK_ITEM_COMMENTS_ENABLED": "true",
		"GITLAB_BLAME_ENABLED":              "false",
		"LINEAR_WORK_ITEMS_ENABLED":         "true",
		"PAGERDUTY_INCIDENTS_ENABLED":       "false",
	} {
		t.Setenv(key, value)
	}
	for _, want := range baseline {
		got, ok := Descriptor(want.provider, want.dataset)
		if !ok || !reflect.DeepEqual(got, want.descriptor) {
			t.Fatalf(
				"%s/%s descriptor moved under environment configuration: got=%+v want=%+v",
				want.provider, want.dataset, got, want.descriptor,
			)
		}
	}
}
