package providersync

import "testing"

func TestExecutionRegistryMachineCoversEveryProviderCapability(t *testing.T) {
	t.Parallel()
	for _, provider := range []string{"github", "gitlab", "jira", "linear", "launchdarkly"} {
		capabilities := Capabilities(provider)
		descriptors := ExecutionDescriptors(provider)
		if len(descriptors) != len(capabilities) {
			t.Fatalf("%s descriptors=%d capabilities=%d", provider, len(descriptors), len(capabilities))
		}
		for index, capability := range capabilities {
			descriptor := descriptors[index]
			if descriptor.Provider != provider || descriptor.Dataset != capability.Dataset ||
				descriptor.Mode != ExecutionPythonCompatibility ||
				descriptor.CompatibilityAdapter != "dev_health_ops.processors.dataset_adapters.run_dataset_unit" ||
				descriptor.RouteEnabled {
				t.Fatalf("%s/%s descriptor=%+v", provider, capability.Dataset, descriptor)
			}
			if descriptor.NativeShadow != nativeShadowReady(provider, capability.Dataset) {
				t.Fatalf("%s/%s native shadow=%v", provider, capability.Dataset, descriptor.NativeShadow)
			}
		}
	}
}

func TestNewProviderRouteSwitchesAreIndependentAndFailClosedUntilComplete(t *testing.T) {
	t.Parallel()
	switches := RouteSwitches{Linear: true, Jira: true, LaunchDarkly: true}
	for _, test := range []struct {
		provider string
		dataset  string
	}{
		{provider: "linear", dataset: "work-items"},
		{provider: "jira", dataset: "incidents"},
		{provider: "launchdarkly", dataset: "feature-flags"},
	} {
		descriptor, ok := switches.Descriptor(test.provider, test.dataset)
		if !ok || descriptor.NativeShadow || descriptor.RouteEnabled {
			t.Fatalf("%s/%s descriptor=%+v ok=%v", test.provider, test.dataset, descriptor, ok)
		}
	}
	if github, ok := switches.Descriptor("github", "repo-metadata"); !ok || github.RouteEnabled {
		t.Fatalf("unrelated GitHub route=%+v ok=%v", github, ok)
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
	if jiraWorkItems.RouteEnabled || jiraIncidents.RouteEnabled ||
		launchDarkly.RouteEnabled {
		t.Fatalf(
			"independent routes jira_work=%+v jira_incidents=%+v ld=%+v",
			jiraWorkItems, jiraIncidents, launchDarkly,
		)
	}
}

func TestProviderRouteSwitchesAreIndependentAndOnlyEnableAuditableNativeShadows(t *testing.T) {
	t.Parallel()
	switches := RouteSwitches{GitHub: true}
	github, ok := switches.Descriptor("github", "repo-metadata")
	if !ok || !github.RouteEnabled {
		t.Fatalf("GitHub descriptor=%+v ok=%v", github, ok)
	}
	gitlab, ok := switches.Descriptor("gitlab", "repo-metadata")
	if !ok || gitlab.RouteEnabled {
		t.Fatalf("GitLab descriptor=%+v ok=%v", gitlab, ok)
	}
	code, ok := switches.Descriptor("github", "commits")
	if !ok || code.RouteEnabled || code.NativeShadow {
		t.Fatalf("code descriptor=%+v ok=%v", code, ok)
	}
}

func TestPartialWorkItemCollectorsCannotBeEnabledAsNativeRoutes(t *testing.T) {
	t.Parallel()
	for _, provider := range []string{"github", "gitlab"} {
		for _, dataset := range []string{
			"work-items",
			"work-item-labels",
			"work-item-projects",
			"work-item-history",
			"work-item-comments",
		} {
			descriptor, ok := (RouteSwitches{GitHub: true, GitLab: true}).Descriptor(provider, dataset)
			if !ok || descriptor.NativeShadow || descriptor.RouteEnabled {
				t.Fatalf("%s/%s descriptor=%+v ok=%v", provider, dataset, descriptor, ok)
			}
		}
	}
}
