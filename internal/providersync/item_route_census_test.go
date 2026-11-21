package providersync

import (
	"sort"
	"strings"
	"testing"
)

// inlineDecodeExceptions are functions that decode and return the bare ErrNormalizationInvalid on the decode failure (an
// item decoder by signature is an allowed origin and is not listed), keyed "file:function", each with its reason. A shared
// helper listed here is an origin only: every function that returns what it returns is a finding of its own, so a new caller
// that returns the bare error must be listed as an edge in itemRouteEdgeExceptions.
var inlineDecodeExceptions = map[string]string{
	"launchdarkly_references_clickhouse.go:parseLaunchDarklyCodeReferences": "decodes the whole code-references document, not a page item (the caller launchdarkly_route.go:189 wraps RouteFailure with the code-references route; the resolver at launchdarkly_references_clickhouse.go:48 gets the payload that call already parsed)",
	"jira_incidents_route.go:decodeJiraIncidentJSON":                        "shared helper; the body caller jira_incidents_route.go:426 wraps ResponseInvalid(BodyDecode) and the item caller :343 wraps RouteFailure",
	"jira_work_items_route.go:decodeJiraJSON":                               "shared helper; the body callers jira_work_items_route.go:644 and jira_dev_status.go:95 wrap ResponseInvalid(BodyDecode), the graphql body caller jira_atlassian_route.go:1116 wraps it too, and the item callers (jira_work_items_route.go:429, :494, jira_atlassian_route.go, jira_team_catalog_route.go) wrap RouteFailure",
	"native_rest.go:NativeRESTHandler.normalizeGitHubWorkItems":             "cannot fail on decode: filterWorkItemWindow (native_rest.go:691) already dropped every item that does not decode into the same workItemPayload; the check shares one condition with the number and title validation",
	"native_rest.go:NativeRESTHandler.normalizeGitLabWorkItems":             "cannot fail on decode: filterWorkItemWindow (native_rest.go:691) already dropped every item that does not decode into the same workItemPayload; the check shares one condition with the iid and title validation",
	"native_rest.go:NativeRESTHandler.fetchGitHubChildren":                  "cannot fail on decode: the parent came through filterWorkItemWindow (native_rest.go:691); the check shares one condition with the number validation",
	"native_rest.go:NativeRESTHandler.fetchGitLabChildren":                  "cannot fail on decode: the parent came through filterWorkItemWindow (native_rest.go:691); the check shares one condition with the iid validation",
	"repository_shadow.go:ClickHouseRepositoryShadowSource.Load":            "decodes the settings JSON of a ClickHouse row, not a provider response",
	"github_tests_chunked_route.go:GitHubTestsRouteHandler.CollectChunks":   "the job item decode in collectRunDetail stays bare: its error reaches githubTestsOpenRunSkip, which compares it by identity (github_tests_run_listing.go:66)",
	"github_tests_run_listing.go:fetchGitHubTestsRun":                       "decodes the whole single-run body, not a page item; its error reaches githubTestsOpenRunSkip, which compares it by identity (github_tests_run_listing.go:66)",
	"gitlab_feature_flags_route.go:ensureJSONEOF":                           "trailing-data check inside the item decoder decodeGitLabFeatureFlagObject; its callers (gitlab_feature_flags_route.go:217, :255) already name the route",
}

// itemRouteAbsorbed are the inline exceptions that cannot fail: the analysis does not follow their bare error to their callers.
var itemRouteAbsorbed = map[string]bool{
	"native_rest.go:NativeRESTHandler.normalizeGitHubWorkItems":  true,
	"native_rest.go:NativeRESTHandler.normalizeGitLabWorkItems":  true,
	"native_rest.go:NativeRESTHandler.fetchGitHubChildren":       true,
	"native_rest.go:NativeRESTHandler.fetchGitLabChildren":       true,
	"repository_shadow.go:ClickHouseRepositoryShadowSource.Load": true,
}

// itemRouteEdgeExceptions are call edges "file:caller>callee" that return the bare error of a decode failure, each with its reason.
// An exception applies to the named edge only: a new caller of the same callee that returns the bare error is a finding.
var itemRouteEdgeExceptions = map[string]string{
	"github_tests_chunked_route.go:GitHubTestsRouteHandler.CollectChunks>fetchGitHubTestsRun":                                                  "the single-run body decode stays bare for githubTestsOpenRunSkip, which compares it by identity (github_tests_run_listing.go:66)",
	"github_tests_chunked_route.go:GitHubTestsRouteHandler.CollectChunks>GitHubTestsRouteHandler.CollectChunks":                                "the run-detail closure of CollectChunks returns the job item decode bare to the same identity compare (github_tests_run_listing.go:66)",
	"launchdarkly_references_clickhouse.go:LaunchDarklyClickHouseReferences.ResolveLaunchDarklyCodeReferences>parseLaunchDarklyCodeReferences": "the resolver gets the payload that launchdarkly_route.go:189 already parsed and routed, so this parse cannot fail on decode",
}

// itemRouteVerdict compares the findings with the two tables: unlisted findings and listed entries that no longer match.
func itemRouteVerdict(a *itemRouteAnalysis, inline, edges map[string]string) []string {
	var bad []string
	found := map[string]bool{}
	for _, finding := range a.findings() {
		found[finding.Key] = true
		if inline[finding.Key] == "" && edges[finding.Key] == "" {
			bad = append(bad, finding.Key+" at "+finding.Pos)
		}
	}
	for _, table := range []map[string]string{inline, edges} {
		for key := range table {
			if !found[key] {
				bad = append(bad, key+" (listed, no finding left: drop it)")
			}
		}
	}
	sort.Strings(bad)
	return bad
}

// TestItemDecodeFailuresCarryTheRoute: the SSA census (item_route_ssa_test.go) follows the bare
// providerfoundation.ErrNormalizationInvalid of a decode failure to every return of the function and of its callers. A
// function that returns it without a RouteFailure/ResponseFailure/ResponseInvalid is a finding unless it is an item decoder by
// signature (`raw json.RawMessage` first) or is listed with its reason (CHAOS-14292). Forms outside the bound of
// itemRouteAnalysis are not pinned.
func TestItemDecodeFailuresCarryTheRoute(t *testing.T) {
	t.Parallel()
	pkg, err := loadItemRoutePackage("./internal/providersync")
	if err != nil {
		t.Fatal(err)
	}
	a := newItemRouteAnalysis(pkg, itemRouteAbsorbed)
	if decoders := a.decoders(); len(decoders) < 77 {
		t.Fatalf("found %d item decoders, want at least the 77 of CHAOS-14292: %v", len(decoders), decoders)
	}
	if edges := a.edges(); edges < 97 {
		t.Fatalf("found %d call edges into an item decoder, want at least the 97 of CHAOS-14292", edges)
	}
	if bad := itemRouteVerdict(a, inlineDecodeExceptions, itemRouteEdgeExceptions); len(bad) > 0 {
		t.Fatalf("decode failures that reach a return as the bare sentinel, not listed:\n%s", strings.Join(bad, "\n"))
	}
}

// The bypass fixture holds one function per form of CHAOS-14292 that the AST census accepted; each must be a finding.
func TestItemRouteCensusRefusesEveryBypassForm(t *testing.T) {
	t.Parallel()
	pkg, err := loadItemRoutePackage("./internal/providersync/testdata/itemroute/bypass")
	if err != nil {
		t.Fatal(err)
	}
	a := newItemRouteAnalysis(pkg, nil)
	// sharedHelper is a listed origin: its callers are the findings.
	var got []string
	for _, finding := range a.findings() {
		if finding.Key != "bypass.go:sharedHelper" {
			got = append(got, finding.Key)
		}
	}
	want := []string{
		"bypass.go:assignDecode", "bypass.go:assignElse", "bypass.go:assignSuccessFirst", "bypass.go:assignSwitch",
		"bypass.go:callerDirect>decodeItem", "bypass.go:callerRouteConditional>decodeItem", "bypass.go:callerRouteDropped>decodeItem",
		"bypass.go:callerVar>decodeItem", "bypass.go:init>decodeItem", "bypass.go:newSharedCaller>sharedHelper",
		"bypass.go:phiMerge", "bypass.go:sentinelLocal", "bypass.go:sentinelNested",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("bypass findings:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

// The control fixture holds legal code (a decode routed through every exit shape, a plain sentinel next to a routed decode, a
// decode whose failure is ignored, the listed forms); the census must accept all of it.
func TestItemRouteCensusAcceptsLegalCode(t *testing.T) {
	t.Parallel()
	pkg, err := loadItemRoutePackage("./internal/providersync/testdata/itemroute/control")
	if err != nil {
		t.Fatal(err)
	}
	inline := map[string]string{
		"control.go:sharedHelper": "listed origin", "control.go:cannotFail": "cannot fail",
	}
	edges := map[string]string{"control.go:identityEdge>sharedHelper": "identity compare"}
	a := newItemRouteAnalysis(pkg, map[string]bool{"control.go:cannotFail": true})
	if bad := itemRouteVerdict(a, inline, edges); len(bad) > 0 {
		t.Fatalf("legal code refused:\n%s", strings.Join(bad, "\n"))
	}
}
