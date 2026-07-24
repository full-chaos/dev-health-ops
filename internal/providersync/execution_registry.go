package providersync

import (
	"slices"
	"strings"
)

// ExecutorKind names the fixed provider/dataset execution ownership required
// by CUT-09. It answers "which code would run this pair", never "may this pair
// route" — routing is decided only by RouteReady and RouteEnabled. Capability
// metadata is not execution evidence (TRD §10.1).
type ExecutorKind string

const (
	// ExecutorNativeGo means a Go CompleteRouteHandler for the pair is
	// compiled into this module and owns fetch, normalization, and effects.
	ExecutorNativeGo ExecutorKind = "native_go"
	// ExecutorPythonCompatibility means the pair is served by the bounded
	// API-side compatibility endpoint described in TRD §10.2: Go keeps lease,
	// status, completion, and watermark ownership and sends only bounded
	// identifiers plus its claim context — never credentials, URLs, module
	// names, or provider payloads. No pair is registered under this kind yet
	// because that endpoint does not exist; a later lane owns it.
	ExecutorPythonCompatibility ExecutorKind = "python_compatibility"
	// ExecutorNone means no Go-owned executor exists for the pair.
	ExecutorNone ExecutorKind = "none"
)

// providerExecutorRegistry is the fixed provider/dataset executor registry.
// Absent keys resolve to ExecutorNone. Membership here is a statement about
// compiled code only: a pair may be native_go while RouteReady is still false
// because live parity evidence has not been captured.
var providerExecutorRegistry = map[string]ExecutorKind{
	"launchdarkly/feature-flags": ExecutorNativeGo,
	"github/repo-metadata":       ExecutorNativeGo,
}

// ProviderExecutor reports the fixed executor kind for a provider/dataset pair.
func ProviderExecutor(provider, dataset string) ExecutorKind {
	kind, ok := providerExecutorRegistry[matrixKey(provider, dataset)]
	if !ok {
		return ExecutorNone
	}
	return kind
}

func matrixKey(provider, dataset string) string {
	return strings.ToLower(strings.TrimSpace(provider)) + "/" +
		strings.ToLower(strings.TrimSpace(dataset))
}

// nativeShadowReady is deliberately narrower than the REST fixture collectors.
// Production maps every work-item* dataset unit to one full work-item job; the
// labels/projects/history/comments names are not independent sink semantics.
// Until a native handler emits that complete batch and has canary evidence,
// only repository metadata is eligible for auditable shadow execution.
func nativeShadowReady(provider, dataset string) bool {
	return (provider == "github" || provider == "gitlab") && dataset == "repo-metadata"
}

// CompleteRouteDescriptor is the single canonical provider capability
// descriptor. The legacy parallel ExecutionDescriptor/RouteSwitches registry
// was removed in CUT-08: every consumer — production routing, the shadow
// parity harness, and the frozen provider matrix contract — now reads this
// type so no future slice can be wired to a dead descriptor path.
type CompleteRouteDescriptor struct {
	Provider         string
	RequestedDataset string
	RouteDataset     string
	Destinations     []string
	// Executor is the fixed executor kind for the requested pair.
	Executor ExecutorKind
	// NativeShadow marks pairs whose native fetch output may be compared
	// against the Python-owned sink for parity evidence. Shadow eligibility
	// never implies routing.
	NativeShadow bool
	RouteReady   bool
	RouteEnabled bool
}

// ShadowDescriptor is a fixture/parity-only projection of the canonical
// descriptor. It exists so the generation-block parity harness cannot become a
// second capability registry: it can only be derived from a descriptor the
// canonical system already recognises as shadow-eligible.
type ShadowDescriptor struct {
	Provider string
	Dataset  string
	// Write enables generation-block persistence inside the parity harness.
	// It is never derived from configuration and never set by a binary.
	Write bool
}

// Shadow projects a canonical descriptor onto the parity harness. It fails
// closed for any pair that is not shadow-eligible.
func (descriptor CompleteRouteDescriptor) Shadow(write bool) (ShadowDescriptor, bool) {
	if !descriptor.NativeShadow || descriptor.Provider == "" ||
		descriptor.RequestedDataset == "" {
		return ShadowDescriptor{}, false
	}
	return ShadowDescriptor{
		Provider: descriptor.Provider,
		Dataset:  descriptor.RequestedDataset,
		Write:    write,
	}, true
}

// CompleteRouteSwitches carries the config-gated route enablement flags. A
// provider/dataset pair without a field here can never be enabled, which is
// why adding github, gitlab, and pagerduty descriptors in CUT-08 cannot widen
// the live route surface.
type CompleteRouteSwitches struct {
	LinearWorkItems          bool
	JiraWorkItems            bool
	JiraIncidents            bool
	LaunchDarklyFeatureFlags bool
}

// Descriptor resolves the canonical capability descriptor for a claimed
// provider/dataset pair. Recognition is driven by the dataset capability
// registry, so the descriptor surface and the frozen matrix contract cover the
// same set by construction.
//
// Descriptor collapses Python's five work-item dataset aliases onto the one
// complete work-items unit. Alias identities remain visible for audit and
// watermark compatibility, but they can never be activated as partial routes.
func (switches CompleteRouteSwitches) Descriptor(
	provider string,
	dataset string,
) (CompleteRouteDescriptor, bool) {
	provider = strings.ToLower(strings.TrimSpace(provider))
	dataset = strings.ToLower(strings.TrimSpace(dataset))
	if _, known := Capability(provider, dataset); !known {
		return CompleteRouteDescriptor{}, false
	}
	descriptor := CompleteRouteDescriptor{
		Provider: provider, RequestedDataset: dataset, RouteDataset: dataset,
		Executor:     ProviderExecutor(provider, dataset),
		NativeShadow: nativeShadowReady(provider, dataset),
	}
	workItemAlias := slices.Contains(linearBackfillWorkItemDatasets, dataset)
	switch {
	case (provider == "linear" || provider == "jira") && workItemAlias:
		descriptor.RouteDataset = "work-items"
		descriptor.Destinations = workItemRouteDestinations()
		// The aliases are one complete Python collector, but the complete native
		// handler is not wired yet. Preserve the manifest while failing closed.
	case provider == "jira" && dataset == "incidents":
		descriptor.Destinations = []string{"operational_incidents"}
	case provider == "launchdarkly" && dataset == "feature-flags":
		descriptor.Destinations = launchDarklyRouteDestinations()
		descriptor.RouteReady = true
		descriptor.RouteEnabled = switches.LaunchDarklyFeatureFlags
	case (provider == "github" || provider == "gitlab") && dataset == "repo-metadata":
		// GitHub has a native complete-route handler and a repos effect sink,
		// but the route stays closed until live non-empty parity evidence
		// exists (plan §9 CUT-09 acceptance). GitLab has fixture fetch code
		// only and no complete-route handler at all.
		descriptor.Destinations = []string{"repos"}
	}
	return descriptor, true
}

func launchDarklyRouteDestinations() []string {
	return []string{
		"feature_flag",
		"feature_flag_event",
		"feature_flag_link",
		"work_graph_edges",
	}
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
