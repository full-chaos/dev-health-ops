package providersync

import (
	"bytes"
	"encoding/json"
	"maps"
	"os"
	"reflect"
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

// routeReadyPairs is the complete, explicitly enumerated set of pairs that may
// carry live traffic. Every entry needs its own parity evidence; adding one
// here without it is the failure this guard exists to prevent.
//
//   - launchdarkly/feature-flags: CUT-08, native handler + live parity.
//   - github/repo-metadata: CHAOS-3123, fixture-level field parity against the
//     Python collector (TestGitHubRepositoryRouteEmitsOneBoundedReposEffect).
//     Canary staging and live-traffic parity are waived for this program.
//
// github/prs (CHAOS-3122) is deliberately NOT in this set despite having a
// real CompleteRouteHandler and passing fixture-level parity evidence: codex
// H1 found that three columns on its own destination table
// (first_review_at, reviews_count, changes_requested_count) are owned by
// Python's review-enrichment phase, which this handler does not perform, so
// it always writes them as zero. route_ready is a promise that the Go path
// produces the product data for a pair, not merely that it compiles and
// passes its own tests; writing fabricated zeros into columns it does not
// own would corrupt review-latency/rework/AI-impact analytics. It flips
// RouteReady together with github/pr-reviews when that pair lands with a
// real review fetch — see execution_registry.go's github/prs case and
// deploy/go-workers/provider-sync-porting-recipe.md.
var routeReadyPairs = map[string]struct{}{
	"launchdarkly/feature-flags": {},
	"github/repo-metadata":       {},
}

// TestProviderMatrixKeepsEveryRouteClosedExceptReadyPairs is the freeze guard:
// descriptors exist for github, gitlab, and pagerduty since CUT-08, and having
// a descriptor must never by itself widen the routable surface.
func TestProviderMatrixKeepsEveryRouteClosedExceptReadyPairs(t *testing.T) {
	t.Parallel()
	ready := map[string]struct{}{}
	for _, pair := range BuildProviderMatrix().Pairs {
		if pair.RouteReady {
			ready[pair.Provider+"/"+pair.Dataset] = struct{}{}
		}
	}
	if !maps.Equal(ready, routeReadyPairs) {
		t.Fatalf("route-ready pairs=%v want %v", ready, routeReadyPairs)
	}
	// Enabling every declared switch may not make an unready pair routable.
	// The literal below must name every field of CompleteRouteSwitches: a new
	// switch left out of it would leave its pair unexercised here.
	all := CompleteRouteSwitches{
		LinearWorkItems: true, JiraWorkItems: true, JiraIncidents: true,
		LaunchDarklyFeatureFlags: true, GithubRepoMetadata: true,
		GithubPRs: true,
	}
	if reflect.TypeOf(all).NumField() != 6 {
		t.Fatalf(
			"CompleteRouteSwitches gained a field; add it to `all` above so its " +
				"pair is exercised, then update this count",
		)
	}
	for _, pair := range BuildProviderMatrix().Pairs {
		key := pair.Provider + "/" + pair.Dataset
		descriptor, ok := all.Descriptor(pair.Provider, pair.Dataset)
		if !ok {
			t.Fatalf("%s has no descriptor", key)
		}
		_, wantRoutable := routeReadyPairs[key]
		if routable := descriptor.RouteReady && descriptor.RouteEnabled; routable != wantRoutable {
			t.Fatalf("%s routable=%v want %v descriptor=%+v", key, routable, wantRoutable, descriptor)
		}
	}
}

// TestGithubRepoMetadataSwitchDoesNotOpenGitLab pins the split that
// CHAOS-3123 introduced: gitlab/repo-metadata shares repo-metadata's
// destination manifest but has no CompleteRouteHandler, so folding it back
// into github's case would let one switch open a route with nothing behind it.
func TestGithubRepoMetadataSwitchDoesNotOpenGitLab(t *testing.T) {
	t.Parallel()
	switches := CompleteRouteSwitches{GithubRepoMetadata: true}
	descriptor, ok := switches.Descriptor("gitlab", "repo-metadata")
	if !ok {
		t.Fatal("gitlab/repo-metadata has no descriptor")
	}
	if descriptor.RouteReady || descriptor.RouteEnabled {
		t.Fatalf("gitlab/repo-metadata descriptor=%+v", descriptor)
	}
}

// TestGithubPRsSwitchAloneCannotOpenTheRoute pins codex's H1 finding: even
// with its switch on, github/prs must stay RouteReady=false until
// github/pr-reviews exists to own the three review-derived columns on the
// same destination table. This is the opposite direction from
// TestGithubRepoMetadataSwitchDoesNotOpenGitLab's "unrelated switch can't
// open an unready pair" — here the pair's OWN switch is on, and it still
// must not route, because RouteReady itself (not RouteEnabled) is the gate.
func TestGithubPRsSwitchAloneCannotOpenTheRoute(t *testing.T) {
	t.Parallel()
	switches := CompleteRouteSwitches{GithubPRs: true}
	descriptor, ok := switches.Descriptor("github", "prs")
	if !ok {
		t.Fatal("github/prs has no descriptor")
	}
	if descriptor.RouteReady {
		t.Fatalf("github/prs descriptor=%+v: RouteReady must stay false until "+
			"github/pr-reviews lands (codex H1)", descriptor)
	}
	// The destination manifest and the fixed native_go executor are still
	// recorded -- RouteReady is what fails closed, not the rest of the
	// descriptor going dark.
	if descriptor.Executor != ExecutorNativeGo ||
		len(descriptor.Destinations) != 1 ||
		descriptor.Destinations[0] != "git_pull_requests" {
		t.Fatalf("github/prs descriptor=%+v", descriptor)
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
		"github/prs":                 GitHubPullRequestRouteHandler{},
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
