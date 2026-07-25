package providersync

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"
)

const providerMatrixContractPath = "../../contracts/provider-matrix/v1/matrix.json"

// expectedMatrixPairCounts is the independently asserted provider/dataset
// census from the CUT-08 audit. It exists so a silent registry deletion cannot
// be "fixed" by regenerating the contract.
var expectedMatrixPairCounts = map[string]int{
	"github": 17, "gitlab": 19, "jira": 6, "launchdarkly": 1,
	"linear": 5, "pagerduty": 11,
}

func TestProviderMatrixCoversEveryConfiguredPair(t *testing.T) {
	t.Parallel()
	matrix := BuildProviderMatrix()
	counts := map[string]int{}
	seen := map[string]struct{}{}
	for _, pair := range matrix.Pairs {
		key := pair.Provider + "/" + pair.Dataset
		if _, duplicate := seen[key]; duplicate {
			t.Fatalf("duplicate matrix pair %q", key)
		}
		seen[key] = struct{}{}
		counts[pair.Provider]++
		if pair.PythonSource == "" || pair.CostClass == "" ||
			pair.Watermark == "" || pair.RouteDataset == "" ||
			len(pair.CredentialModes) == 0 || pair.LegacyTargets == nil ||
			pair.ProcessorFlags == nil || pair.RouteDestinations == nil {
			t.Fatalf("incomplete matrix entry %q: %+v", key, pair)
		}
		switch pair.GoExecutor {
		case ExecutorNativeGo, ExecutorPythonCompatibility, ExecutorNone:
		default:
			t.Fatalf("%s executor=%q", key, pair.GoExecutor)
		}
	}
	total := 0
	for provider, want := range expectedMatrixPairCounts {
		if counts[provider] != want {
			t.Errorf("%s pairs=%d want %d", provider, counts[provider], want)
		}
		total += want
	}
	if len(matrix.Pairs) != total {
		t.Fatalf("matrix pairs=%d want %d", len(matrix.Pairs), total)
	}
}

// TestProviderMatrixKeepsEveryRouteClosedExceptLaunchDarkly is the freeze
// guard: adding descriptors for github, gitlab, and pagerduty in CUT-08 must
// not widen the routable surface.
func TestProviderMatrixKeepsEveryRouteClosedExceptLaunchDarkly(t *testing.T) {
	t.Parallel()
	ready := map[string]struct{}{}
	for _, pair := range BuildProviderMatrix().Pairs {
		if pair.RouteReady {
			ready[pair.Provider+"/"+pair.Dataset] = struct{}{}
		}
	}
	if len(ready) != 1 {
		t.Fatalf("route-ready pairs=%v", ready)
	}
	if _, ok := ready["launchdarkly/feature-flags"]; !ok {
		t.Fatalf("route-ready pairs=%v", ready)
	}
	// Enabling every switch may not make an unready pair routable.
	all := CompleteRouteSwitches{
		LinearWorkItems: true, JiraWorkItems: true, JiraIncidents: true,
		LaunchDarklyFeatureFlags: true,
	}
	for _, pair := range BuildProviderMatrix().Pairs {
		descriptor, ok := all.Descriptor(pair.Provider, pair.Dataset)
		if !ok {
			t.Fatalf("%s/%s has no descriptor", pair.Provider, pair.Dataset)
		}
		routable := descriptor.RouteReady && descriptor.RouteEnabled
		if routable != (pair.Provider == "launchdarkly") {
			t.Fatalf(
				"%s/%s routable=%v descriptor=%+v",
				pair.Provider, pair.Dataset, routable, descriptor,
			)
		}
	}
}

// TestProviderMatrixMatchesCheckedInContract regenerates the Go side of the
// contract and fails on any divergence. Set PROVIDER_MATRIX_UPDATE=1 to
// rewrite the artifact after an intentional registry change.
func TestProviderMatrixMatchesCheckedInContract(t *testing.T) {
	encoded, err := EncodeProviderMatrix(BuildProviderMatrix())
	if err != nil {
		t.Fatal(err)
	}
	// Regeneration writes and then falls through to the same comparison and
	// reparse. An accidentally exported PROVIDER_MATRIX_UPDATE must not turn
	// this test into a rubber stamp for the fields Python cannot independently
	// derive (route_destinations, native_shadow).
	if os.Getenv("PROVIDER_MATRIX_UPDATE") == "1" {
		if err := os.WriteFile(providerMatrixContractPath, encoded, 0o644); err != nil {
			t.Fatal(err)
		}
		t.Log("regenerated " + providerMatrixContractPath)
	}
	stored, err := os.ReadFile(providerMatrixContractPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stored, encoded) {
		t.Fatalf(
			"%s diverges from the Go capability registry; "+
				"rerun with PROVIDER_MATRIX_UPDATE=1 after an intentional change",
			providerMatrixContractPath,
		)
	}
	var reparsed ProviderMatrix
	if err := json.Unmarshal(stored, &reparsed); err != nil {
		t.Fatal(err)
	}
	if reparsed.SchemaVersion != ProviderMatrixVersion {
		t.Fatalf("contract schema_version=%d", reparsed.SchemaVersion)
	}
}

// TestProviderMatrixExecutorRegistryIsHonest binds the executor registry to
// compiled code: every native_go pair must have a real CompleteRouteHandler.
func TestProviderMatrixExecutorRegistryIsHonest(t *testing.T) {
	t.Parallel()
	handlers := map[string]CompleteRouteHandler{
		"launchdarkly/feature-flags": LaunchDarklyRouteHandler{},
		"github/repo-metadata":       GitHubRepositoryRouteHandler{},
	}
	native := map[string]struct{}{}
	for _, pair := range BuildProviderMatrix().Pairs {
		key := pair.Provider + "/" + pair.Dataset
		switch pair.GoExecutor {
		case ExecutorNativeGo:
			native[key] = struct{}{}
			if _, ok := handlers[key]; !ok {
				t.Errorf("%s claims native_go without a complete-route handler", key)
			}
		case ExecutorPythonCompatibility:
			t.Errorf(
				"%s claims python_compatibility, but the bounded compatibility "+
					"endpoint (TRD §10.2) does not exist yet", key,
			)
		}
	}
	for key := range handlers {
		if _, ok := native[key]; !ok {
			t.Errorf("%s has a handler but is not registered as native_go", key)
		}
	}
}
