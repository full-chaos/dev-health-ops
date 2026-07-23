package providersync

import "testing"

func TestExecutionRegistryMachineCoversEveryProviderCapability(t *testing.T) {
	t.Parallel()
	for _, provider := range []string{"github", "gitlab"} {
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
			if descriptor.NativeShadow != nativeShadowReady(capability.Dataset) {
				t.Fatalf("%s/%s native shadow=%v", provider, capability.Dataset, descriptor.NativeShadow)
			}
		}
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
