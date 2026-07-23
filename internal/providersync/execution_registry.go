package providersync

import (
	"slices"
	"sort"
)

type ExecutionMode string

const (
	ExecutionPythonCompatibility ExecutionMode = "python_compatibility"
)

type ExecutionDescriptor struct {
	Provider             string
	Dataset              string
	Mode                 ExecutionMode
	CompatibilityAdapter string
	NativeShadow         bool
	RouteEnabled         bool
}

func ExecutionDescriptors(provider string) []ExecutionDescriptor {
	capabilities := Capabilities(provider)
	descriptors := make([]ExecutionDescriptor, 0, len(capabilities))
	for _, capability := range capabilities {
		adapter := "dev_health_ops.processors.dataset_adapters.run_dataset_unit"
		descriptors = append(descriptors, ExecutionDescriptor{
			Provider: capability.Provider, Dataset: capability.Dataset,
			Mode: ExecutionPythonCompatibility, CompatibilityAdapter: adapter,
			NativeShadow: nativeShadowReady(capability.Provider, capability.Dataset),
			RouteEnabled: false,
		})
	}
	sort.Slice(descriptors, func(left, right int) bool {
		return descriptors[left].Dataset < descriptors[right].Dataset
	})
	return descriptors
}

// nativeShadowReady is deliberately narrower than the REST fixture collectors.
// Production maps every work-item* dataset unit to one full work-item job; the
// labels/projects/history/comments names are not independent sink semantics.
// Until a native handler emits that complete batch and has canary evidence,
// only repository metadata is eligible for auditable shadow execution.
func nativeShadowReady(provider, dataset string) bool {
	return (provider == "github" || provider == "gitlab") && dataset == "repo-metadata"
}

type RouteSwitches struct {
	GitHub       bool
	GitLab       bool
	Linear       bool
	Jira         bool
	LaunchDarkly bool
}

type CompleteRouteDescriptor struct {
	Provider         string
	RequestedDataset string
	RouteDataset     string
	Destinations     []string
	RouteReady       bool
	RouteEnabled     bool
}

type CompleteRouteSwitches struct {
	LinearWorkItems          bool
	JiraWorkItems            bool
	JiraIncidents            bool
	LaunchDarklyFeatureFlags bool
}

// Descriptor collapses Python's five work-item dataset aliases onto the one
// complete work-items unit. Alias identities remain visible for audit and
// watermark compatibility, but they can never be activated as partial routes.
func (switches CompleteRouteSwitches) Descriptor(
	provider string,
	dataset string,
) (CompleteRouteDescriptor, bool) {
	descriptor := CompleteRouteDescriptor{
		Provider: provider, RequestedDataset: dataset, RouteDataset: dataset,
	}
	workItemAlias := slices.Contains(linearBackfillWorkItemDatasets, dataset)
	switch {
	case provider == "linear" && workItemAlias:
		descriptor.RouteDataset = "work-items"
		descriptor.Destinations = workItemRouteDestinations()
		// The aliases are one complete Python collector, but the complete native
		// handler is not wired yet. Preserve the manifest while failing closed.
		descriptor.RouteReady = false
		descriptor.RouteEnabled = false
	case provider == "jira" && workItemAlias:
		descriptor.RouteDataset = "work-items"
		descriptor.Destinations = workItemRouteDestinations()
		descriptor.RouteReady = false
		descriptor.RouteEnabled = false
	case provider == "jira" && dataset == "incidents":
		descriptor.Destinations = []string{"operational_incidents"}
		descriptor.RouteReady = false
		descriptor.RouteEnabled = false
	case provider == "launchdarkly" && dataset == "feature-flags":
		descriptor.Destinations = []string{
			"feature_flag",
			"feature_flag_event",
			"feature_flag_link",
			"work_graph_edges",
		}
		descriptor.RouteReady = true
		descriptor.RouteEnabled = switches.LaunchDarklyFeatureFlags
	default:
		return CompleteRouteDescriptor{}, false
	}
	return descriptor, true
}

func workItemRouteDestinations() []string {
	return []string{
		"estimate_coverage_metrics_daily",
		"investment_classifications_daily",
		"investment_metrics_daily",
		"issue_type_metrics_daily",
		"sprints",
		"work_item_cycle_times",
		"work_item_dependencies",
		"work_item_interactions",
		"work_item_metrics_daily",
		"work_item_reopen_events",
		"work_item_state_durations_daily",
		"work_item_team_attributions",
		"work_item_transitions",
		"work_item_user_metrics_daily",
		"work_items",
	}
}

func (switches RouteSwitches) Descriptor(provider, dataset string) (ExecutionDescriptor, bool) {
	for _, descriptor := range ExecutionDescriptors(provider) {
		if descriptor.Dataset != dataset {
			continue
		}
		switch provider {
		case "github":
			descriptor.RouteEnabled = switches.GitHub && descriptor.NativeShadow
		case "gitlab":
			descriptor.RouteEnabled = switches.GitLab && descriptor.NativeShadow
		case "linear":
			descriptor.RouteEnabled = switches.Linear && descriptor.NativeShadow
		case "jira":
			descriptor.RouteEnabled = switches.Jira && descriptor.NativeShadow
		case "launchdarkly":
			descriptor.RouteEnabled = switches.LaunchDarkly && descriptor.NativeShadow
		default:
			return ExecutionDescriptor{}, false
		}
		return descriptor, true
	}
	return ExecutionDescriptor{}, false
}
