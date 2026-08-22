package providersync

import (
	"bytes"
	"encoding/json"
	"maps"
	"os"
	"slices"
	"testing"
)

const providerMatrixContractPath = "../../contracts/provider-matrix/v1/matrix.json"

const (
	expectedProviderMatrixPairs   = 59
	expectedRouteReadyPairs       = 59
	expectedGitHubRouteReadyPairs = 17
)

// expectedMatrixPairCounts is the independently asserted provider/dataset
// census from the CUT-08 audit. It exists so a silent registry deletion cannot
// be "fixed" by regenerating the contract.
var expectedMatrixPairCounts = map[string]int{
	"github": 17, "gitlab": 19, "jira": 6, "launchdarkly": 1,
	"linear": 5, "pagerduty": 11,
}

// expectedRouteReadyPairCounts is deliberately a second literal rather than
// an alias of expectedMatrixPairCounts. The aggregate acceptance contract is
// that every one of the independently audited 59 pairs is native and ready;
// deleting a pair from both the capability registry and one expected map must
// not silently reduce that target.
var expectedRouteReadyPairCounts = map[string]int{
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
	if total != expectedProviderMatrixPairs {
		t.Fatalf("configured expected matrix census=%d want %d", total, expectedProviderMatrixPairs)
	}
	if len(matrix.Pairs) != expectedProviderMatrixPairs {
		t.Fatalf("matrix pairs=%d want %d", len(matrix.Pairs), total)
	}
}

// TestProviderMatrixRouteReadyCensus is intentionally independent from
// routeReadyPairs. A registry/matrix edit that deletes a ready identity from
// both the implementation and that explanatory set must still fail the
// aggregate 59/59 acceptance census.
func TestProviderMatrixRouteReadyCensus(t *testing.T) {
	t.Parallel()
	matrix := BuildProviderMatrix()
	ready, githubReady := 0, 0
	readyCounts := map[string]int{}
	for _, pair := range matrix.Pairs {
		if !pair.RouteReady {
			continue
		}
		ready++
		readyCounts[pair.Provider]++
		if pair.Provider == "github" {
			githubReady++
		}
	}
	if got, want := len(matrix.Pairs), expectedProviderMatrixPairs; got != want {
		t.Fatalf("matrix pairs=%d want %d", got, want)
	}
	if ready != expectedRouteReadyPairs || githubReady != expectedGitHubRouteReadyPairs {
		t.Fatalf("route-ready total=%d github=%d want total=%d github=%d",
			ready, githubReady, expectedRouteReadyPairs, expectedGitHubRouteReadyPairs)
	}
	if !maps.Equal(readyCounts, expectedRouteReadyPairCounts) {
		t.Fatalf("route-ready provider census=%v want %v", readyCounts, expectedRouteReadyPairCounts)
	}
}

// routeReadyPairs is the complete, explicitly enumerated set of capability
// identities that are RouteReady: a shipped, registered, compiled capability
// (CHAOS-4054). Capability is always on in the binary — there is no rollout
// switch left to admit it through. RouteReady does not itself mean an identity
// may be independently planned; see nonPlannableAliasPairs for that narrower
// set. Every entry needs its own parity evidence; adding one here without it
// is the failure this guard exists to prevent.
//
//   - launchdarkly/feature-flags: CUT-08, native handler + live parity.
//
//   - github/repo-metadata: CHAOS-3123, fixture-level field parity against the
//     Python collector (TestGitHubRepositoryRouteEmitsOneBoundedReposEffect).
//     Canary staging and live-traffic parity are waived for this program.
//
//   - github/cicd: delegates to the same complete-row TestOps unit as
//     github/tests; `tests` is a non-plannable alias of `cicd`, so only the
//     canonical identity can ever be independently claimed.
//
//   - gitlab/cicd: delegates to the same complete-row TestOps unit as
//     gitlab/tests; `tests` is a non-plannable alias of `cicd`, so only the
//     canonical identity can ever be independently claimed.
//
//   - github/commits: CHAOS-3177, live producer oracle parity plus
//     tenant-scoped FINAL readback.
//
//   - github/deployments: CHAOS-3176, differential row parity against the live
//     Python normalizer and builder plus tenant-scoped FINAL readback.
//
//   - github/security: CHAOS-3178, differential row parity against the live
//     production source mappings plus FINAL tenant-fenced ClickHouse effects.
//
//   - github/files: live traversal/row parity against backfill_file_records plus
//     tenant-qualified FINAL readback.
//
//   - github/commit-stats: CHAOS-3033, live producer oracle parity plus
//     tenant-scoped FINAL readback and production-worker construction.
//
//   - jira/incidents: CHAOS-3127, native JSM admission plus live whole-row
//     OperationalIncident parity and tenant-scoped FINAL readback.
//
//   - github/blame: CHAOS-3343, live selection and row parity plus
//     tenant-scoped persisted progress and crash-safe FINAL readback.
//
//   - gitlab/commits: CHAOS-3346, live producer oracle parity plus paginated
//     no-partial-success collection and tenant-scoped FINAL readback.
//
//   - gitlab/commit-stats: live producer oracle parity plus accepted Python
//     budget provenance and tenant-scoped FINAL retry convergence.
//
//   - github/tests: CHAOS-3336, live Python row/budget parity plus six
//     tenant-scoped effects and fail-closed bounded fetches.
//
//   - github/prs, github/pr-reviews, github/pr-comments: one complete PR-social
//     unit matching Python's _sync_github_prs_to_store_async boundary, with
//     REST comment counts, GraphQL review enrichment, two exact readback
//     effects, alias-preserving claim identity, and crash recovery.
//
//   - github/work-items plus work-item-labels, work-item-projects,
//     work-item-history, and work-item-comments: CHAOS-3606's one complete
//     sixteen-destination work-item family. Matrix readiness is atomic across
//     all five aliases, while the planner admits only canonical work-items
//     claims; direct sibling aliases deliberately route-reconcile before I/O.
var routeReadyPairs = map[string]struct{}{
	"launchdarkly/feature-flags":     {},
	"github/repo-metadata":           {},
	"github/cicd":                    {},
	"github/commits":                 {},
	"github/deployments":             {},
	"github/security":                {},
	"github/files":                   {},
	"github/commit-stats":            {},
	"jira/incidents":                 {},
	"gitlab/repo-metadata":           {},
	"gitlab/commits":                 {},
	"gitlab/commit-stats":            {},
	"gitlab/cicd":                    {},
	"gitlab/tests":                   {},
	"gitlab/incidents":               {},
	"gitlab/deployments":             {},
	"gitlab/feature-flags":           {},
	"gitlab/files":                   {},
	"gitlab/blame":                   {},
	"gitlab/prs":                     {},
	"gitlab/pr-reviews":              {},
	"gitlab/pr-comments":             {},
	"gitlab/security":                {},
	"gitlab/work-items":              {},
	"gitlab/work-item-labels":        {},
	"gitlab/work-item-projects":      {},
	"gitlab/work-item-history":       {},
	"gitlab/work-item-comments":      {},
	"jira/work-items":                {},
	"jira/work-item-labels":          {},
	"jira/work-item-projects":        {},
	"jira/work-item-history":         {},
	"jira/work-item-comments":        {},
	"linear/work-items":              {},
	"linear/work-item-labels":        {},
	"linear/work-item-projects":      {},
	"linear/work-item-history":       {},
	"linear/work-item-comments":      {},
	"pagerduty/services":             {},
	"pagerduty/business-services":    {},
	"pagerduty/escalation-policies":  {},
	"pagerduty/schedules":            {},
	"pagerduty/on-calls":             {},
	"pagerduty/users":                {},
	"pagerduty/teams":                {},
	"pagerduty/incidents":            {},
	"pagerduty/incident-alerts":      {},
	"pagerduty/incident-log-entries": {},
	"pagerduty/incident-notes":       {},
	"github/blame":                   {},
	"github/tests":                   {},
	"github/prs":                     {},
	"github/pr-reviews":              {},
	"github/pr-comments":             {},
	"github/work-items":              {},
	"github/work-item-labels":        {},
	"github/work-item-projects":      {},
	"github/work-item-history":       {},
	"github/work-item-comments":      {},
}

// nonPlannableAliasPairs is the complete, explicitly enumerated set of the 22
// RouteReady identities that are alias identities of a canonical writer
// (CHAOS-4054 decision record): {github,gitlab}/{pr-reviews,pr-comments} alias
// `prs`; {github,gitlab}/tests aliases `cicd`; and the four
// work-item-{labels,projects,history,comments} identities for each of
// github/gitlab/jira/linear alias that provider's `work-items`. RouteReady
// stays true for every one of them (audit and per-alias watermarks must keep
// telling the truth); Plannable is false because only the canonical writer
// may be planned and claimed.
var nonPlannableAliasPairs = map[string]struct{}{
	"github/pr-reviews": {}, "github/pr-comments": {}, "github/tests": {},
	"gitlab/pr-reviews": {}, "gitlab/pr-comments": {}, "gitlab/tests": {},
	"github/work-item-labels": {}, "github/work-item-projects": {},
	"github/work-item-history": {}, "github/work-item-comments": {},
	"gitlab/work-item-labels": {}, "gitlab/work-item-projects": {},
	"gitlab/work-item-history": {}, "gitlab/work-item-comments": {},
	"jira/work-item-labels": {}, "jira/work-item-projects": {},
	"jira/work-item-history": {}, "jira/work-item-comments": {},
	"linear/work-item-labels": {}, "linear/work-item-projects": {},
	"linear/work-item-history": {}, "linear/work-item-comments": {},
}

// TestProviderMatrixRouteReadyAndPlannableMatchAudit is the registry/topology
// guard replacing the old default-off rollout-switch guard (CHAOS-4054:
// Descriptor takes no configuration, so there is nothing left to admit a pair
// through). It proves two independent census facts hold for every one of the
// 59 matrix pairs at once: RouteReady matches the audited routeReadyPairs
// set, and Plannable is true for exactly the 37 pairs that are RouteReady and
// not a nonPlannableAliasPairs alias.
func TestProviderMatrixRouteReadyAndPlannableMatchAudit(t *testing.T) {
	t.Parallel()
	ready := map[string]struct{}{}
	plannableCount := 0
	for _, pair := range BuildProviderMatrix().Pairs {
		key := pair.Provider + "/" + pair.Dataset
		if pair.RouteReady {
			ready[key] = struct{}{}
		}
		_, isReady := routeReadyPairs[key]
		_, isAlias := nonPlannableAliasPairs[key]
		wantPlannable := isReady && !isAlias
		if pair.Plannable != wantPlannable {
			t.Fatalf("%s plannable=%v want %v descriptor=%+v", key, pair.Plannable, wantPlannable, pair)
		}
		if pair.Plannable {
			plannableCount++
		}
	}
	if !maps.Equal(ready, routeReadyPairs) {
		t.Fatalf("route-ready pairs=%v want %v", ready, routeReadyPairs)
	}
	if want := len(routeReadyPairs) - len(nonPlannableAliasPairs); plannableCount != want {
		t.Fatalf("plannable pairs=%d want %d", plannableCount, want)
	}
}

// TestGitHubPRSocialFamilyHasOneCanonicalPlannableWriter pins the D16/CHAOS-4054
// boundary: `prs`, `pr-reviews`, and `pr-comments` share one complete
// review-enriched route and the same destinations, but only `prs` — the
// canonical writer — may be independently planned. The other two stay
// RouteReady for audit/watermark visibility only.
func TestGitHubPRSocialFamilyHasOneCanonicalPlannableWriter(t *testing.T) {
	t.Parallel()
	for _, probe := range []struct {
		dataset   string
		plannable bool
	}{
		{"prs", true},
		{"pr-reviews", false},
		{"pr-comments", false},
	} {
		descriptor, ok := Descriptor("github", probe.dataset)
		if !ok || descriptor.Executor != ExecutorNativeGo || !descriptor.RouteReady ||
			descriptor.Plannable != probe.plannable || !slices.Equal(
			descriptor.Destinations, githubPRSocialRouteDestinations(),
		) {
			t.Fatalf("github/%s descriptor=%+v ok=%v want plannable=%v",
				probe.dataset, descriptor, ok, probe.plannable)
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
		"launchdarkly/feature-flags":     LaunchDarklyRouteHandler{},
		"github/repo-metadata":           GitHubRepositoryRouteHandler{},
		"github/prs":                     GitHubPullRequestSocialRouteHandler{},
		"github/pr-reviews":              GitHubPullRequestSocialRouteHandler{},
		"github/pr-comments":             GitHubPullRequestSocialRouteHandler{},
		"github/cicd":                    GitHubTestsRouteHandler{},
		"github/commits":                 GitHubCommitsRouteHandler{},
		"github/deployments":             GitHubDeploymentsRouteHandler{},
		"github/security":                GitHubSecurityRouteHandler{},
		"github/files":                   GitHubFilesRouteHandler{},
		"github/commit-stats":            GitHubCommitStatsRouteHandler{},
		"jira/incidents":                 JiraIncidentRouteHandler{},
		"gitlab/repo-metadata":           GitLabRepositoryRouteHandler{},
		"gitlab/commits":                 GitLabCommitsRouteHandler{},
		"gitlab/commit-stats":            GitLabCommitStatsRouteHandler{},
		"gitlab/cicd":                    GitLabTestsRouteHandler{},
		"gitlab/tests":                   GitLabTestsRouteHandler{},
		"gitlab/incidents":               GitLabIncidentsRouteHandler{},
		"gitlab/deployments":             GitLabDeploymentsRouteHandler{},
		"gitlab/feature-flags":           GitLabFeatureFlagsRouteHandler{},
		"gitlab/files":                   GitLabFilesRouteHandler{},
		"gitlab/blame":                   GitLabBlameRouteHandler{},
		"gitlab/prs":                     GitLabPullRequestRouteHandler{},
		"gitlab/pr-reviews":              GitLabPullRequestRouteHandler{},
		"gitlab/pr-comments":             GitLabPullRequestRouteHandler{},
		"gitlab/security":                GitLabSecurityRouteHandler{},
		"gitlab/work-items":              GitLabWorkItemsRouteHandler{},
		"gitlab/work-item-labels":        GitLabWorkItemsRouteHandler{},
		"gitlab/work-item-projects":      GitLabWorkItemsRouteHandler{},
		"gitlab/work-item-history":       GitLabWorkItemsRouteHandler{},
		"gitlab/work-item-comments":      GitLabWorkItemsRouteHandler{},
		"jira/work-items":                JiraAtlassianRouteHandler{},
		"jira/work-item-labels":          JiraAtlassianRouteHandler{},
		"jira/work-item-projects":        JiraAtlassianRouteHandler{},
		"jira/work-item-history":         JiraAtlassianRouteHandler{},
		"jira/work-item-comments":        JiraAtlassianRouteHandler{},
		"linear/work-items":              LinearWorkItemFamilyRouteHandler{},
		"linear/work-item-labels":        LinearWorkItemFamilyRouteHandler{},
		"linear/work-item-projects":      LinearWorkItemFamilyRouteHandler{},
		"linear/work-item-history":       LinearWorkItemFamilyRouteHandler{},
		"linear/work-item-comments":      LinearWorkItemFamilyRouteHandler{},
		"pagerduty/services":             PagerDutyServicesRouteHandler{},
		"pagerduty/business-services":    PagerDutyBusinessServicesRouteHandler{},
		"pagerduty/escalation-policies":  PagerDutyEscalationPoliciesRouteHandler{},
		"pagerduty/schedules":            PagerDutySchedulesRouteHandler{},
		"pagerduty/on-calls":             PagerDutyOnCallsRouteHandler{},
		"pagerduty/users":                PagerDutyUsersRouteHandler{},
		"pagerduty/teams":                PagerDutyTeamsRouteHandler{},
		"pagerduty/incidents":            PagerDutyIncidentFamilyRouteHandler{},
		"pagerduty/incident-alerts":      PagerDutyIncidentFamilyRouteHandler{},
		"pagerduty/incident-log-entries": PagerDutyIncidentFamilyRouteHandler{},
		"pagerduty/incident-notes":       PagerDutyIncidentFamilyRouteHandler{},
		"github/blame":                   GitHubBlameRouteHandler{},
		"github/tests":                   GitHubTestsRouteHandler{},
		"github/work-items":              GitHubWorkItemsRouteHandler{},
		"github/work-item-labels":        GitHubWorkItemsRouteHandler{},
		"github/work-item-projects":      GitHubWorkItemsRouteHandler{},
		"github/work-item-history":       GitHubWorkItemsRouteHandler{},
		"github/work-item-comments":      GitHubWorkItemsRouteHandler{},
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
