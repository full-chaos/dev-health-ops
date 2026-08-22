package providersync

import (
	"encoding/json"
	"sort"
)

// ProviderMatrixVersion is the frozen schema version of
// contracts/provider-matrix/v1/matrix.json.
const ProviderMatrixVersion = 1

// ProviderMatrixEntry is one frozen provider/dataset pair. Nothing here
// depends on deployment configuration: the contract freezes capability,
// readiness, and which identity of a writer family may be planned. There is no
// route enablement plane to record (CHAOS-4054).
type ProviderMatrixEntry struct {
	Provider          string            `json:"provider"`
	Dataset           string            `json:"dataset"`
	PythonSource      string            `json:"python_source"`
	CostClass         CostClass         `json:"cost_class"`
	Watermark         WatermarkBehavior `json:"watermark"`
	LegacyTargets     []string          `json:"legacy_targets"`
	ProcessorFlags    map[string]bool   `json:"processor_flags"`
	GoExecutor        ExecutorKind      `json:"go_executor"`
	NativeShadow      bool              `json:"native_shadow"`
	RouteDataset      string            `json:"route_dataset"`
	RouteDestinations []string          `json:"route_destinations"`
	RouteReady        bool              `json:"route_ready"`
	Plannable         bool              `json:"plannable"`
	CanonicalDataset  string            `json:"canonical_dataset"`
	CredentialModes   []string          `json:"credential_modes"`
}

// ProviderMatrix is the generated provider/dataset capability contract shared
// by Go and Python. Both languages regenerate their own side and fail when it
// diverges from the checked-in artifact.
type ProviderMatrix struct {
	SchemaVersion int                   `json:"schema_version"`
	Providers     []string              `json:"providers"`
	Pairs         []ProviderMatrixEntry `json:"pairs"`
}

// providerCredentialModes records the authentication modes each provider
// client actually implements today. TRD §10.1 additionally claims a PagerDuty
// "region" parameter; no such field exists in any provider auth or config
// class, so it is intentionally not recorded here.
var providerCredentialModes = map[string][]string{
	"github": {"personal_access_token", "github_app_installation"},
	"gitlab": {"personal_access_token"},
	"jira": {
		"basic_api_token", "oauth_bearer_token", "oauth_refresh_token",
	},
	"launchdarkly": {"api_key"},
	"linear":       {"api_key"},
	"pagerduty": {
		"api_token", "oauth_bearer_token", "oauth_authorization_code",
	},
}

// CredentialModes returns the frozen credential modes for a provider.
func CredentialModes(provider string) []string {
	return append([]string(nil), providerCredentialModes[provider]...)
}

// pythonMatrixSource anchors each pair back to the authoritative Python
// registry so a contract reader can verify membership without guessing.
func pythonMatrixSource(provider string) string {
	return `src/dev_health_ops/sync/datasets.py::_PROVIDER_SUPPORTED_DATASETS["` +
		provider + `"]`
}

// BuildProviderMatrix regenerates the Go side of the provider matrix from the
// dataset capability registry and the canonical route descriptor. It is the
// only Go producer of the contract artifact.
func BuildProviderMatrix() ProviderMatrix {
	providers := MatrixProviders()
	matrix := ProviderMatrix{
		SchemaVersion: ProviderMatrixVersion,
		Providers:     providers,
		Pairs:         make([]ProviderMatrixEntry, 0, 64),
	}
	for _, provider := range providers {
		for _, capability := range Capabilities(provider) {
			descriptor, ok := Descriptor(provider, capability.Dataset)
			if !ok {
				// Unreachable: Descriptor recognises exactly the capability
				// registry. Recording an honest "none" beats panicking in a
				// contract generator.
				descriptor = CompleteRouteDescriptor{
					Provider: provider, RequestedDataset: capability.Dataset,
					RouteDataset: capability.Dataset, Executor: ExecutorNone,
				}
			}
			matrix.Pairs = append(matrix.Pairs, ProviderMatrixEntry{
				Provider:          provider,
				Dataset:           capability.Dataset,
				PythonSource:      pythonMatrixSource(provider),
				CostClass:         capability.CostClass,
				Watermark:         capability.Watermark,
				LegacyTargets:     sortedStrings(capability.LegacyTargets),
				ProcessorFlags:    capability.ProcessorFlags,
				GoExecutor:        descriptor.Executor,
				NativeShadow:      descriptor.NativeShadow,
				RouteDataset:      descriptor.RouteDataset,
				RouteDestinations: sortedStrings(descriptor.Destinations),
				RouteReady:        descriptor.RouteReady,
				Plannable:         descriptor.Plannable,
				CanonicalDataset:  descriptor.CanonicalDataset,
				CredentialModes:   CredentialModes(provider),
			})
		}
	}
	sort.Slice(matrix.Pairs, func(left, right int) bool {
		if matrix.Pairs[left].Provider != matrix.Pairs[right].Provider {
			return matrix.Pairs[left].Provider < matrix.Pairs[right].Provider
		}
		return matrix.Pairs[left].Dataset < matrix.Pairs[right].Dataset
	})
	return matrix
}

// EncodeProviderMatrix renders the contract artifact deterministically. The
// exact bytes are what both languages verify against.
func EncodeProviderMatrix(matrix ProviderMatrix) ([]byte, error) {
	encoded, err := json.MarshalIndent(matrix, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(encoded, '\n'), nil
}

func sortedStrings(input []string) []string {
	values := append([]string(nil), input...)
	if values == nil {
		values = []string{}
	}
	sort.Strings(values)
	return values
}
