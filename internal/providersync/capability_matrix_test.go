package providersync

import (
	"bytes"
	"encoding/json"
	"maps"
	"os"
	"reflect"
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
// identities whose native route may be admitted through its default-off
// rollout switch. Route readiness does not itself activate live traffic. Every
// entry needs its own parity evidence; adding one here without it is the failure
// this guard exists to prevent.
//
//   - launchdarkly/feature-flags: CUT-08, native handler + live parity.
//
//   - github/repo-metadata: CHAOS-3123, fixture-level field parity against the
//     Python collector (TestGitHubRepositoryRouteEmitsOneBoundedReposEffect).
//     Canary staging and live-traffic parity are waived for this program.
//
//   - github/cicd: delegates to the same complete-row TestOps unit as
//     github/tests; config enforces mutual exclusion between their switches.
//
//   - gitlab/cicd: delegates to the same complete-row TestOps unit as
//     gitlab/tests; config enforces mutual exclusion between their switches.
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

// TestProviderMatrixKeepsEveryRouteClosedExceptReadyPairs is the default-off
// and canonical-family guard. Readiness describes a compiled capability; only
// the pair's own switch may enable it, and a noncanonical atomic work-item
// alias must remain non-routable even though its audit identity is ready.
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
	// The four noncanonical aliases in each atomic work-item family describe
	// matrix/watermark identities, not valid direct claims. planner.py collapses
	// each family into one canonical work-items claim, so direct aliases stay
	// disabled and enter route reconciliation before construction or I/O.
	// The literal below must name every field of CompleteRouteSwitches: a new
	// switch left out of it would leave its pair unexercised here.
	all := CompleteRouteSwitches{
		LocalAllRoutes:  true,
		LinearWorkItems: true, JiraWorkItems: true, JiraIncidents: true,
		LaunchDarklyFeatureFlags: true, GithubRepoMetadata: true,
		GitlabRepoMetadata:          true,
		GitlabCommits:               true,
		GitlabCommitStats:           true,
		GitlabCICD:                  true,
		GitlabTests:                 true,
		GitlabIncidents:             true,
		GitlabDeployments:           true,
		GitlabFeatureFlags:          true,
		GitlabFiles:                 true,
		GitlabBlame:                 true,
		GitlabPRs:                   true,
		GitlabPRReviews:             true,
		GitlabPRComments:            true,
		GitlabSecurity:              true,
		GitlabWorkItems:             true,
		PagerDutyServices:           true,
		PagerDutyBusinessServices:   true,
		PagerDutyEscalationPolicies: true,
		PagerDutySchedules:          true,
		PagerDutyOnCalls:            true,
		PagerDutyUsers:              true,
		PagerDutyTeams:              true,
		PagerDutyIncidents:          true,
		PagerDutyIncidentAlerts:     true,
		PagerDutyIncidentLogEntries: true,
		PagerDutyIncidentNotes:      true,
		GithubPRs:                   true, GithubPRReviews: true, GithubPRComments: true,
		GithubCICD: true, GithubCommits: true,
		GithubDeployments: true, GithubSecurity: true, GithubFiles: true,
		GithubCommitStats: true,
		GithubBlame:       true,
		GithubTests:       true,
		GithubWorkItems:   true,
	}
	if reflect.TypeOf(all).NumField() != 44 {
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
		if slices.Contains([]string{"github", "gitlab", "jira", "linear"}, pair.Provider) &&
			isWorkItemFamilyDataset(pair.Dataset) &&
			pair.Dataset != "work-items" {
			wantRoutable = false
		}
		if routable := descriptor.RouteReady && descriptor.RouteEnabled; routable != wantRoutable {
			t.Fatalf("%s routable=%v want %v descriptor=%+v", key, routable, wantRoutable, descriptor)
		}
	}
}

func TestGitHubBlameRequiresItsOwnSwitch(t *testing.T) {
	t.Parallel()
	descriptor, ok := (CompleteRouteSwitches{GithubBlame: true}).Descriptor("github", "blame")
	if !ok {
		t.Fatal("github/blame has no descriptor")
	}
	if descriptor.Executor != ExecutorNativeGo || !descriptor.RouteReady ||
		!descriptor.RouteEnabled || !slices.Equal(
		descriptor.Destinations, []string{"github_blame_path_progress", "git_blame"},
	) {
		t.Fatalf("github/blame descriptor=%+v", descriptor)
	}
	off, _ := (CompleteRouteSwitches{}).Descriptor("github", "blame")
	if off.RouteEnabled {
		t.Fatalf("default github/blame descriptor=%+v", off)
	}
}

// TestGithubRepoMetadataSwitchDoesNotOpenGitLab pins the independent switches:
// enabling GitHub repository sync must not route GitLab repository units.
func TestGithubRepoMetadataSwitchDoesNotOpenGitLab(t *testing.T) {
	t.Parallel()
	switches := CompleteRouteSwitches{GithubRepoMetadata: true}
	descriptor, ok := switches.Descriptor("gitlab", "repo-metadata")
	if !ok {
		t.Fatal("gitlab/repo-metadata has no descriptor")
	}
	if !descriptor.RouteReady || descriptor.RouteEnabled ||
		descriptor.Executor != ExecutorNativeGo {
		t.Fatalf("gitlab/repo-metadata descriptor=%+v", descriptor)
	}
}

func TestGitlabRepoMetadataSwitchRoutesOnlyGitLab(t *testing.T) {
	t.Parallel()
	switches := CompleteRouteSwitches{GitlabRepoMetadata: true}
	gitlab, ok := switches.Descriptor("gitlab", "repo-metadata")
	if !ok || !gitlab.RouteReady || !gitlab.RouteEnabled ||
		gitlab.Executor != ExecutorNativeGo {
		t.Fatalf("gitlab/repo-metadata descriptor=%+v ok=%v", gitlab, ok)
	}
	github, ok := switches.Descriptor("github", "repo-metadata")
	if !ok || github.RouteEnabled {
		t.Fatalf("github/repo-metadata descriptor=%+v ok=%v", github, ok)
	}
}

func TestGitLabCommitsRequiresItsOwnSwitch(t *testing.T) {
	t.Parallel()
	off, ok := (CompleteRouteSwitches{GithubCommits: true}).Descriptor("gitlab", "commits")
	if !ok || !off.RouteReady || off.RouteEnabled || off.Executor != ExecutorNativeGo {
		t.Fatalf("gitlab/commits with github switch descriptor=%+v ok=%v", off, ok)
	}
	on, ok := (CompleteRouteSwitches{GitlabCommits: true}).Descriptor("gitlab", "commits")
	if !ok || !on.RouteReady || !on.RouteEnabled || on.Executor != ExecutorNativeGo ||
		!slices.Equal(on.Destinations, []string{"git_commits"}) {
		t.Fatalf("gitlab/commits descriptor=%+v ok=%v", on, ok)
	}
	github, _ := (CompleteRouteSwitches{GitlabCommits: true}).Descriptor("github", "commits")
	if github.RouteEnabled {
		t.Fatalf("gitlab switch opened github/commits: %+v", github)
	}
}

func TestGitLabCommitStatsRequiresItsOwnSwitch(t *testing.T) {
	t.Parallel()
	off, ok := (CompleteRouteSwitches{GithubCommitStats: true}).Descriptor("gitlab", "commit-stats")
	if !ok || !off.RouteReady || off.RouteEnabled || off.Executor != ExecutorNativeGo {
		t.Fatalf("gitlab/commit-stats with github switch descriptor=%+v ok=%v", off, ok)
	}
	on, ok := (CompleteRouteSwitches{GitlabCommitStats: true}).Descriptor("gitlab", "commit-stats")
	if !ok || !on.RouteReady || !on.RouteEnabled || on.Executor != ExecutorNativeGo ||
		!slices.Equal(on.Destinations, []string{"git_commit_stats"}) {
		t.Fatalf("gitlab/commit-stats descriptor=%+v ok=%v", on, ok)
	}
	github, _ := (CompleteRouteSwitches{GitlabCommitStats: true}).Descriptor("github", "commit-stats")
	if github.RouteEnabled {
		t.Fatalf("gitlab switch opened github/commit-stats: %+v", github)
	}
}

func TestGitLabCICDRequiresItsOwnSwitch(t *testing.T) {
	t.Parallel()
	off, ok := (CompleteRouteSwitches{GithubCICD: true}).Descriptor("gitlab", "cicd")
	if !ok || !off.RouteReady || off.RouteEnabled || off.Executor != ExecutorNativeGo {
		t.Fatalf("gitlab/cicd with github switch descriptor=%+v ok=%v", off, ok)
	}
	on, ok := (CompleteRouteSwitches{GitlabCICD: true}).Descriptor("gitlab", "cicd")
	if !ok || !on.RouteReady || !on.RouteEnabled || on.Executor != ExecutorNativeGo ||
		!slices.Equal(on.Destinations, []string{"ci_pipeline_runs", "ci_job_runs", "ci_acceptance_checks", "test_suite_results", "test_case_results", "coverage_snapshots"}) {
		t.Fatalf("gitlab/cicd descriptor=%+v ok=%v", on, ok)
	}
	github, _ := (CompleteRouteSwitches{GitlabCICD: true}).Descriptor("github", "cicd")
	if github.RouteEnabled {
		t.Fatalf("gitlab switch opened github/cicd: %+v", github)
	}
}

// TestGitHubPRSocialPairsRequireIndependentSwitches pins the D16 boundary:
// every PR-social identity is ready only after the complete review-enriched
// route landed, while each identity still requires its own default-off switch.
func TestGitHubPRSocialPairsRequireIndependentSwitches(t *testing.T) {
	t.Parallel()
	probes := []struct {
		dataset  string
		switches CompleteRouteSwitches
	}{
		{"prs", CompleteRouteSwitches{GithubPRs: true}},
		{"pr-reviews", CompleteRouteSwitches{GithubPRReviews: true}},
		{"pr-comments", CompleteRouteSwitches{GithubPRComments: true}},
	}
	for _, probe := range probes {
		descriptor, ok := probe.switches.Descriptor("github", probe.dataset)
		if !ok || descriptor.Executor != ExecutorNativeGo || !descriptor.RouteReady ||
			!descriptor.RouteEnabled || !slices.Equal(
			descriptor.Destinations, githubPRSocialRouteDestinations(),
		) {
			t.Fatalf("github/%s descriptor=%+v ok=%v", probe.dataset, descriptor, ok)
		}
		for _, other := range []string{"prs", "pr-reviews", "pr-comments"} {
			if other == probe.dataset {
				continue
			}
			off, _ := probe.switches.Descriptor("github", other)
			if off.RouteEnabled {
				t.Fatalf("github/%s switch opened github/%s: %+v", probe.dataset, other, off)
			}
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
