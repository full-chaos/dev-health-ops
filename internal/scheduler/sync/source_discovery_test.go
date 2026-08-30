package sync

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// TestEveryProviderDatasetFamilyHasADecidedSourceDiscoveryStance is the
// CHAOS-4602 registry-level test: every provider with a registered dataset
// family (supportedProviderDatasets, planner.go) must have an EXPLICIT,
// recorded source-discovery stance -- either it is discovered from the
// provider's own API (sourceDiscoveryProviders) or it is exempt for a
// documented reason (sourceDiscoveryExemptProviders). A future provider
// dataset family added without updating either map fails this test, the same
// drift-gate shape CHAOS-4433's ledger generator uses for River kinds and
// bridge routes.
func TestEveryProviderDatasetFamilyHasADecidedSourceDiscoveryStance(t *testing.T) {
	for provider := range supportedProviderDatasets {
		if sourceDiscoveryProviders[provider] || sourceDiscoveryExemptProviders[provider] {
			continue
		}
		t.Errorf(
			"provider %q has registered datasets (supportedProviderDatasets) but no "+
				"source-discovery decision: add it to sourceDiscoveryProviders (its sources "+
				"are discovered from the provider's own API) or sourceDiscoveryExemptProviders "+
				"(with a reason, CHAOS-4602)",
			provider,
		)
	}
	for provider := range sourceDiscoveryProviders {
		if sourceDiscoveryExemptProviders[provider] {
			t.Errorf("provider %q is in both sourceDiscoveryProviders and sourceDiscoveryExemptProviders", provider)
		}
	}
}

func TestGithubDiscoveryOptions(t *testing.T) {
	cases := []struct {
		name          string
		options       map[string]any
		wantAllRepos  bool
		wantOwner     string
		wantPattern   string
		wantNamespace string
	}{
		{name: "empty options: no owner, no API call (see discoverGitHub)", options: map[string]any{}, wantAllRepos: false, wantOwner: "", wantPattern: "*", wantNamespace: ""},
		{name: "explicit owner only", options: map[string]any{"owner": "acme"}, wantAllRepos: false, wantOwner: "acme", wantPattern: "*", wantNamespace: ""},
		{name: "owner/pattern search OVERWRITES owner even when set", options: map[string]any{"owner": "ignored", "search": "acme/api-*"}, wantAllRepos: false, wantOwner: "acme", wantPattern: "api-*", wantNamespace: ""},
		{name: "bare search with no owner becomes the owner", options: map[string]any{"search": "acme"}, wantAllRepos: false, wantOwner: "acme", wantPattern: "*", wantNamespace: ""},
		{name: "all_repos with owner sets namespace, no search", options: map[string]any{"all_repos": true, "owner": "acme"}, wantAllRepos: true, wantOwner: "acme", wantPattern: "*", wantNamespace: "acme"},
		{name: "all_repos with bare search is a pattern only, namespace stays whatever owner resolved", options: map[string]any{"all_repos": true, "search": "api-*"}, wantAllRepos: true, wantOwner: "", wantPattern: "api-*", wantNamespace: ""},
		{name: "all_repos with owner/pattern search fills namespace+pattern", options: map[string]any{"all_repos": true, "search": "acme/api-*"}, wantAllRepos: true, wantOwner: "", wantPattern: "api-*", wantNamespace: "acme"},
		{name: "all_repos with no owner/search: unbounded, no namespace filter", options: map[string]any{"all_repos": true}, wantAllRepos: true, wantOwner: "", wantPattern: "*", wantNamespace: ""},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			allRepos, owner, pattern, namespace := githubDiscoveryOptions(testCase.options)
			if allRepos != testCase.wantAllRepos || owner != testCase.wantOwner || pattern != testCase.wantPattern || namespace != testCase.wantNamespace {
				t.Fatalf("githubDiscoveryOptions(%v) = (%v,%q,%q,%q), want (%v,%q,%q,%q)",
					testCase.options, allRepos, owner, pattern, namespace,
					testCase.wantAllRepos, testCase.wantOwner, testCase.wantPattern, testCase.wantNamespace)
			}
		})
	}
}

func TestGitlabDiscoveryOptions(t *testing.T) {
	cases := []struct {
		name          string
		options       map[string]any
		wantAllRepos  bool
		wantGroup     string
		wantPattern   string
		wantNamespace string
	}{
		{name: "empty options: no group, no API call (see discoverGitLab)", options: map[string]any{}, wantAllRepos: false, wantGroup: "", wantPattern: "*", wantNamespace: ""},
		{name: "explicit group only", options: map[string]any{"group": "acme"}, wantAllRepos: false, wantGroup: "acme", wantPattern: "*", wantNamespace: ""},
		// Python's discover_gitlab_repos only reads sync_options.owner as a
		// NAMESPACE FILTER inside the all_repos branch -- the non-all_repos
		// branch has no owner-falls-back-to-group behavior at all (unlike
		// github, where owner IS the scope). owner alone here resolves to
		// no group, so discoverGitLab makes no API call at all.
		{name: "owner alone (non-all_repos) does not become the group", options: map[string]any{"owner": "acme"}, wantAllRepos: false, wantGroup: "", wantPattern: "*", wantNamespace: ""},
		{name: "non-all_repos nested search splits on the FIRST slash, unlike all_repos", options: map[string]any{"search": "acme/platform/api-*"}, wantAllRepos: false, wantGroup: "acme", wantPattern: "platform/api-*", wantNamespace: ""},
		{name: "bare search with no group becomes the group", options: map[string]any{"search": "acme"}, wantAllRepos: false, wantGroup: "acme", wantPattern: "*", wantNamespace: ""},
		{name: "all_repos nested subgroup search splits on the LAST slash", options: map[string]any{"all_repos": true, "search": "acme/platform/api-*"}, wantAllRepos: true, wantGroup: "", wantPattern: "api-*", wantNamespace: "acme/platform"},
		{name: "all_repos with group sets namespace, no search", options: map[string]any{"all_repos": true, "group": "acme"}, wantAllRepos: true, wantGroup: "acme", wantPattern: "*", wantNamespace: "acme"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			allRepos, group, pattern, namespace := gitlabDiscoveryOptions(testCase.options)
			if allRepos != testCase.wantAllRepos || group != testCase.wantGroup || pattern != testCase.wantPattern || namespace != testCase.wantNamespace {
				t.Fatalf("gitlabDiscoveryOptions(%v) = (%v,%q,%q,%q), want (%v,%q,%q,%q)",
					testCase.options, allRepos, group, pattern, namespace,
					testCase.wantAllRepos, testCase.wantGroup, testCase.wantPattern, testCase.wantNamespace)
			}
		})
	}
}

// fakeSourceDiscoveryDoer returns the same canned JSON body for every
// request, mirroring the existing gitHubRepositoryDoer test convention
// (internal/providersync/github_repository_route_test.go).
type fakeSourceDiscoveryDoer struct {
	t     *testing.T
	body  string
	paths []string
	urls  []string
}

func (doer *fakeSourceDiscoveryDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	doer.paths = append(doer.paths, request.URL.Path)
	doer.urls = append(doer.urls, request.URL.String())
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(doer.body)),
		Request:    request,
	}, nil
}

// sequencedSourceDiscoveryDoer returns one canned (status, body) pair per
// call, in order, holding the last one for any further calls -- used to
// simulate a primary endpoint failing and a fallback succeeding.
type sequencedSourceDiscoveryDoer struct {
	t        *testing.T
	statuses []int
	bodies   []string
	paths    []string
}

func (doer *sequencedSourceDiscoveryDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	doer.paths = append(doer.paths, request.URL.Path)
	index := len(doer.paths) - 1
	if index >= len(doer.statuses) {
		index = len(doer.statuses) - 1
	}
	return &http.Response{
		StatusCode: doer.statuses[index],
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(doer.bodies[index])),
		Request:    request,
	}, nil
}

func fastRetry() providerfoundation.RetryPolicy {
	return providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond}
}

func TestDiscoverGitHubMapsRepositoriesAndFiltersByPattern(t *testing.T) {
	doer := &fakeSourceDiscoveryDoer{t: t, body: `[
		{"name":"api","full_name":"acme/api","owner":{"login":"acme"}},
		{"name":"web","full_name":"acme/web","owner":{"login":"acme"}}
	]`}
	credential, err := providerfoundation.Credential{Provider: "github"}.WithEphemeralSecret("token", secrets.NewValue("gh-token"))
	if err != nil {
		t.Fatal(err)
	}
	service := &NativeSourceDiscoveryService{doer: doer, retry: fastRetry(), telemetry: newSourceDiscoveryTelemetry(), now: time.Now}
	sources, err := service.discoverGitHub(context.Background(), credential, map[string]any{"owner": "acme", "search": "acme/api"})
	if err != nil {
		t.Fatal(err)
	}
	if len(sources) != 1 {
		t.Fatalf("discoverGitHub() = %#v, want exactly the pattern-matched repo", sources)
	}
	got := sources[0]
	if got.ExternalID != "acme/api" || got.SourceType != "repository" || got.Name != "api" || got.FullName != "acme/api" {
		t.Fatalf("discoverGitHub() source = %#v", got)
	}
	if got.Metadata["owner"] != "acme" {
		t.Fatalf("discoverGitHub() metadata = %#v, want owner=acme", got.Metadata)
	}
	if len(doer.paths) != 1 || !strings.HasPrefix(doer.paths[0], "/orgs/acme/repos") {
		t.Fatalf("discoverGitHub() requested paths = %v, want /orgs/acme/repos", doer.paths)
	}
}

func TestDiscoverGitHubNonAllReposWithNoOwnerMakesNoAPICall(t *testing.T) {
	// Python's discover_github_repos: `if not owner: return []` -- no scope
	// resolved at all is an explicit empty result, never an unbounded
	// /user/repos listing (codex review round 1, P1).
	doer := &fakeSourceDiscoveryDoer{t: t, body: `[]`}
	credential, err := providerfoundation.Credential{Provider: "github"}.WithEphemeralSecret("token", secrets.NewValue("gh-token"))
	if err != nil {
		t.Fatal(err)
	}
	service := &NativeSourceDiscoveryService{doer: doer, retry: fastRetry(), telemetry: newSourceDiscoveryTelemetry(), now: time.Now}
	sources, err := service.discoverGitHub(context.Background(), credential, map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if len(sources) != 0 {
		t.Fatalf("discoverGitHub() = %#v, want no sources", sources)
	}
	if len(doer.paths) != 0 {
		t.Fatalf("discoverGitHub() made %d API calls with no owner and no all_repos, want 0: %v", len(doer.paths), doer.paths)
	}
}

func TestDiscoverGitHubAllReposListsUserReposAndFiltersByNamespace(t *testing.T) {
	doer := &fakeSourceDiscoveryDoer{t: t, body: `[
		{"name":"api","full_name":"acme/api","owner":{"login":"acme"}},
		{"name":"other","full_name":"other-owner/other","owner":{"login":"other-owner"}}
	]`}
	credential, err := providerfoundation.Credential{Provider: "github"}.WithEphemeralSecret("token", secrets.NewValue("gh-token"))
	if err != nil {
		t.Fatal(err)
	}
	service := &NativeSourceDiscoveryService{doer: doer, retry: fastRetry(), telemetry: newSourceDiscoveryTelemetry(), now: time.Now}
	sources, err := service.discoverGitHub(context.Background(), credential, map[string]any{"all_repos": true, "owner": "acme"})
	if err != nil {
		t.Fatal(err)
	}
	if len(sources) != 1 || sources[0].FullName != "acme/api" {
		t.Fatalf("discoverGitHub() all_repos = %#v, want only the acme-owned repo", sources)
	}
	if len(doer.paths) != 1 || doer.paths[0] != "/user/repos" {
		t.Fatalf("discoverGitHub() all_repos requested paths = %v, want /user/repos", doer.paths)
	}
}

// TestDiscoverGitHubAppAuthUsesInstallationRepositoriesEndpoint proves the
// codex round-2 P2 fix: an all_repos GitHub App-authenticated credential (no
// "token" secret at all) must list via /installation/repositories, never
// /user/repos (which 401s for an App installation token -- it has no
// authenticated-USER surface). This fix previously had NO test coverage at
// all; added closing that gap (team-lead review, CHAOS-4602 proof ledger).
func TestDiscoverGitHubAppAuthUsesInstallationRepositoriesEndpoint(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatal(err)
	}
	privateKey := string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))
	credential, err := providerfoundation.Credential{Provider: "github"}.WithEphemeralSecret("app_id", secrets.NewValue("1"))
	if err != nil {
		t.Fatal(err)
	}
	credential, err = credential.WithEphemeralSecret("private_key", secrets.NewValue(privateKey))
	if err != nil {
		t.Fatal(err)
	}
	credential, err = credential.WithEphemeralSecret("installation_id", secrets.NewValue("2"))
	if err != nil {
		t.Fatal(err)
	}
	// Call 1: GitHubAppAuth minting its installation token. Call 2: the
	// actual /installation/repositories listing, wrapped in a
	// "repositories" key (unlike /user/repos' bare array).
	doer := &sequencedSourceDiscoveryDoer{t: t,
		statuses: []int{http.StatusOK, http.StatusOK},
		bodies: []string{
			`{"token":"installation-token","expires_at":"2099-01-01T00:00:00Z"}`,
			`{"repositories":[{"name":"api","full_name":"acme/api","owner":{"login":"acme"}}]}`,
		},
	}
	service := &NativeSourceDiscoveryService{doer: doer, retry: fastRetry(), telemetry: newSourceDiscoveryTelemetry(), now: time.Now}
	sources, err := service.discoverGitHub(context.Background(), credential, map[string]any{"all_repos": true})
	if err != nil {
		t.Fatal(err)
	}
	if len(sources) != 1 || sources[0].FullName != "acme/api" {
		t.Fatalf("discoverGitHub() app-auth all_repos = %#v, want the one repo", sources)
	}
	if len(doer.paths) != 2 || doer.paths[1] != "/installation/repositories" {
		t.Fatalf("discoverGitHub() app-auth requested paths = %v, want [.../access_tokens, /installation/repositories]", doer.paths)
	}
}

// TestFnmatchLikeTranslatesBracketNegationAndEmptyPattern proves the codex
// round-2 P2 fixes: fnmatch's "[!abc]" bracket-negation syntax (path.Match's
// own is "[^abc]"), and an EMPTY pattern matching only an empty name
// (Go's path.Match already gets this right on its own -- the bug was an
// earlier version of this function's own special-casing that incorrectly
// treated "" as "match everything"). Neither fix previously had a dedicated
// test.
func TestFnmatchLikeTranslatesBracketNegationAndEmptyPattern(t *testing.T) {
	for _, test := range []struct {
		name, pattern, candidate string
		want                     bool
	}{
		{"bracket negation excludes listed chars", "[!ab]*", "cathedral", true},
		{"bracket negation matches a listed char is false", "[!ab]*", "apple", false},
		{"empty pattern matches only empty name", "", "", true},
		{"empty pattern does not match a non-empty name", "", "anything", false},
		{"star matches everything", "*", "anything", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := fnmatchLike(test.pattern, test.candidate); got != test.want {
				t.Errorf("fnmatchLike(%q, %q) = %v, want %v", test.pattern, test.candidate, got, test.want)
			}
		})
	}
}

func TestDiscoverGitLabNonAllReposListsOneGroupWithoutIncludeSubgroups(t *testing.T) {
	// The real /api/v4/groups/{group}/projects endpoint is already scoped
	// server-side, so this test's fixture returns only what that endpoint
	// would realistically return -- no client-side namespace filter is
	// applied (or needed) for the non-all_repos path, unlike all_repos below.
	doer := &fakeSourceDiscoveryDoer{t: t, body: `[
		{"id":101,"name":"api","path_with_namespace":"acme/api"}
	]`}
	credential, err := providerfoundation.Credential{Provider: "gitlab"}.WithEphemeralSecret("token", secrets.NewValue("gl-token"))
	if err != nil {
		t.Fatal(err)
	}
	service := &NativeSourceDiscoveryService{doer: doer, retry: fastRetry(), telemetry: newSourceDiscoveryTelemetry(), now: time.Now}
	sources, err := service.discoverGitLab(context.Background(), credential, map[string]any{"group": "acme"})
	if err != nil {
		t.Fatal(err)
	}
	if len(sources) != 1 {
		t.Fatalf("discoverGitLab() = %#v, want the one group project", sources)
	}
	got := sources[0]
	if got.ExternalID != "101" || got.SourceType != "project" || got.Name != "api" || got.FullName != "acme/api" {
		t.Fatalf("discoverGitLab() source = %#v", got)
	}
	if got.Metadata["path_with_namespace"] != "acme/api" {
		t.Fatalf("discoverGitLab() metadata = %#v", got.Metadata)
	}
	if len(doer.urls) != 1 || !strings.HasPrefix(doer.paths[0], "/api/v4/groups/acme/projects") {
		t.Fatalf("discoverGitLab() requested paths = %v, want /api/v4/groups/acme/projects", doer.paths)
	}
	if strings.Contains(doer.urls[0], "include_subgroups") {
		t.Fatalf("discoverGitLab() non-all_repos request included include_subgroups, Python's non-all_repos branch never does: %s", doer.urls[0])
	}
}

func TestDiscoverGitLabNonAllReposWithNoGroupMakesNoAPICall(t *testing.T) {
	doer := &fakeSourceDiscoveryDoer{t: t, body: `[]`}
	credential, err := providerfoundation.Credential{Provider: "gitlab"}.WithEphemeralSecret("token", secrets.NewValue("gl-token"))
	if err != nil {
		t.Fatal(err)
	}
	service := &NativeSourceDiscoveryService{doer: doer, retry: fastRetry(), telemetry: newSourceDiscoveryTelemetry(), now: time.Now}
	sources, err := service.discoverGitLab(context.Background(), credential, map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if len(sources) != 0 {
		t.Fatalf("discoverGitLab() = %#v, want no sources", sources)
	}
	if len(doer.paths) != 0 {
		t.Fatalf("discoverGitLab() made %d API calls with no group and no all_repos, want 0: %v", len(doer.paths), doer.paths)
	}
}

func TestDiscoverGitLabAllReposFiltersByNamespaceClientSide(t *testing.T) {
	doer := &fakeSourceDiscoveryDoer{t: t, body: `[
		{"id":101,"name":"api","path_with_namespace":"acme/api"},
		{"id":102,"name":"other","path_with_namespace":"other-group/other"}
	]`}
	credential, err := providerfoundation.Credential{Provider: "gitlab"}.WithEphemeralSecret("token", secrets.NewValue("gl-token"))
	if err != nil {
		t.Fatal(err)
	}
	service := &NativeSourceDiscoveryService{doer: doer, retry: fastRetry(), telemetry: newSourceDiscoveryTelemetry(), now: time.Now}
	sources, err := service.discoverGitLab(context.Background(), credential, map[string]any{"all_repos": true, "group": "acme"})
	if err != nil {
		t.Fatal(err)
	}
	if len(sources) != 1 || sources[0].FullName != "acme/api" {
		t.Fatalf("discoverGitLab() all_repos = %#v, want only the acme-namespaced project", sources)
	}
	if len(doer.paths) != 1 || doer.paths[0] != "/api/v4/projects" {
		t.Fatalf("discoverGitLab() all_repos requested paths = %v, want /api/v4/projects", doer.paths)
	}
}

func TestDiscoverGitLabHonorsSyncOptionsGitlabURL(t *testing.T) {
	doer := &fakeSourceDiscoveryDoer{t: t, body: `[]`}
	credential, err := providerfoundation.Credential{Provider: "gitlab"}.WithEphemeralSecret("token", secrets.NewValue("gl-token"))
	if err != nil {
		t.Fatal(err)
	}
	service := &NativeSourceDiscoveryService{doer: doer, retry: fastRetry(), telemetry: newSourceDiscoveryTelemetry(), now: time.Now}
	_, err = service.discoverGitLab(context.Background(), credential, map[string]any{
		"group": "acme", "gitlab_url": "https://gitlab.example.internal",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(doer.urls) != 1 || !strings.HasPrefix(doer.urls[0], "https://gitlab.example.internal/") {
		t.Fatalf("discoverGitLab() request URL = %v, want it to target sync_options.gitlab_url, not gitlab.com", doer.urls)
	}
}

func TestDiscoverJiraMapsProjectsByKey(t *testing.T) {
	doer := &fakeSourceDiscoveryDoer{t: t, body: `{
		"values":[{"id":"10001","key":"CHAOS","name":"Chaos Engineering"}],
		"isLast":true
	}`}
	credential, err := providerfoundation.Credential{Provider: "jira"}.WithEphemeralSecret("email", secrets.NewValue("bot@example.com"))
	if err != nil {
		t.Fatal(err)
	}
	credential, err = credential.WithEphemeralSecret("api_token", secrets.NewValue("jira-token"))
	if err != nil {
		t.Fatal(err)
	}
	credential, err = credential.WithEphemeralSecret("base_url", secrets.NewValue("https://acme.atlassian.net"))
	if err != nil {
		t.Fatal(err)
	}
	service := &NativeSourceDiscoveryService{doer: doer, retry: fastRetry(), telemetry: newSourceDiscoveryTelemetry(), now: time.Now}
	sources, err := service.discoverJira(context.Background(), credential)
	if err != nil {
		t.Fatal(err)
	}
	if len(sources) != 1 {
		t.Fatalf("discoverJira() = %#v, want exactly one project", sources)
	}
	got := sources[0]
	if got.ExternalID != "CHAOS" || got.SourceType != "project" || got.FullName != "Chaos Engineering" {
		t.Fatalf("discoverJira() source = %#v", got)
	}
	if got.Metadata["project_id"] != "10001" {
		t.Fatalf("discoverJira() metadata = %#v", got.Metadata)
	}
	if len(doer.paths) != 1 || !strings.HasPrefix(doer.paths[0], "/rest/api/3/project/search") {
		t.Fatalf("discoverJira() requested paths = %v", doer.paths)
	}
}

// TestDiscoverJiraFallsBackToLegacyProjectEndpoint proves the CHAOS-4602
// codex round 1 (P2) fix: a Jira deployment that 404s the enhanced
// /rest/api/3/project/search endpoint still discovers projects via the
// legacy, unpaginated /rest/api/3/project, mirroring the existing Python
// client's own fallback.
func TestDiscoverJiraFallsBackToLegacyProjectEndpoint(t *testing.T) {
	doer := &sequencedSourceDiscoveryDoer{
		t:        t,
		statuses: []int{http.StatusNotFound, http.StatusOK},
		bodies: []string{
			`{"errorMessages":["not found"]}`,
			`[{"id":"10001","key":"CHAOS","name":"Chaos Engineering"}]`,
		},
	}
	credential, err := providerfoundation.Credential{Provider: "jira"}.WithEphemeralSecret("email", secrets.NewValue("bot@example.com"))
	if err != nil {
		t.Fatal(err)
	}
	credential, err = credential.WithEphemeralSecret("api_token", secrets.NewValue("jira-token"))
	if err != nil {
		t.Fatal(err)
	}
	credential, err = credential.WithEphemeralSecret("base_url", secrets.NewValue("https://acme.atlassian.net"))
	if err != nil {
		t.Fatal(err)
	}
	service := &NativeSourceDiscoveryService{doer: doer, retry: fastRetry(), telemetry: newSourceDiscoveryTelemetry(), now: time.Now}
	sources, err := service.discoverJira(context.Background(), credential)
	if err != nil {
		t.Fatal(err)
	}
	if len(sources) != 1 || sources[0].ExternalID != "CHAOS" {
		t.Fatalf("discoverJira() legacy fallback = %#v, want the CHAOS project", sources)
	}
	if len(doer.paths) != 2 || doer.paths[0] != "/rest/api/3/project/search" || doer.paths[1] != "/rest/api/3/project" {
		t.Fatalf("discoverJira() requested paths = %v, want [search, legacy]", doer.paths)
	}
}

// TestDiscoverSkipsExplicitScopeAndUnknownProviders proves the two
// no-network, no-DB early-outs: an explicit-scope config, and any provider
// outside sourceDiscoveryProviders. Neither reaches the credential resolver
// (which is nil on this service), so a panic there would fail the test.
func TestDiscoverSkipsExplicitScopeAndUnknownProviders(t *testing.T) {
	service := &NativeSourceDiscoveryService{telemetry: newSourceDiscoveryTelemetry(), now: time.Now}

	report, err := service.Discover(context.Background(), SourceDiscoveryArgs{
		Provider: "jira", ExplicitScope: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Outcome != SourceDiscoveryOutcomeSkipped {
		t.Fatalf("explicit-scope Discover() outcome = %q, want skipped", report.Outcome)
	}

	report, err = service.Discover(context.Background(), SourceDiscoveryArgs{Provider: "pagerduty"})
	if err != nil {
		t.Fatal(err)
	}
	if report.Outcome != SourceDiscoveryOutcomeSkipped {
		t.Fatalf("non-source-type-scope provider Discover() outcome = %q, want skipped", report.Outcome)
	}
}

// TestDiscoverSkipsWhenCredentialIDIsNil proves the CHAOS-4602 codex round 1
// (P1) fix: a NULL Integration.credential_id (environment auth) must never
// resolve to the org's unrelated default stored credential. Discover must
// return before ever calling the (nil, on this service) credential
// resolver, which a panic here would prove it didn't.
func TestDiscoverSkipsWhenCredentialIDIsNil(t *testing.T) {
	service := &NativeSourceDiscoveryService{telemetry: newSourceDiscoveryTelemetry(), now: time.Now}

	report, err := service.Discover(context.Background(), SourceDiscoveryArgs{
		Provider: "github", CredentialID: nil,
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Outcome != SourceDiscoveryOutcomeSkipped {
		t.Fatalf("nil-CredentialID Discover() outcome = %q, want skipped", report.Outcome)
	}
}

// TestDiscoverRecordsErrorOutcomeTelemetryOnAnyInternalFailure answers a
// team-lead review question directly: when a codex round-2 fix (3d7285b59)
// briefly shipped upsertSources' existing-tag check using jsonb's `?`
// operator against a plain json column (fixed in e3d77a856; "operator does
// not exist: json ? unknown" on every planner-managed discovery call), did
// that failure ever go silently unrecorded? No -- Discover's every internal
// error branch (credential resolution, provider fetch, upsertSources) shares
// one shape: observe(outcome=error) BEFORE returning the error. This proves
// that shape for the credential-resolution branch (the cheapest to trigger
// deterministically, with no real HTTP or Postgres needed: a zero-value
// CredentialResolver's own Resolve fails closed on nil Repository/Decryptor)
// -- the upsertSources branch three lines later in the same function is
// byte-identical in structure. The pre-existing (not new)
// TestUpsertSourcesIsIdempotentAndNeverFlipsIsEnabled integration test is
// what actually caught the `?` bug itself, by calling upsertSources with
// plannerManaged=true + a real Postgres connection -- exactly the condition
// that query runs under.
func TestDiscoverRecordsErrorOutcomeTelemetryOnAnyInternalFailure(t *testing.T) {
	service := &NativeSourceDiscoveryService{
		telemetry:   newSourceDiscoveryTelemetry(),
		credentials: providerfoundation.CredentialResolver{}, // zero value: Resolve fails closed
		now:         time.Now,
	}
	credentialID := "cred-1"
	_, err := service.Discover(context.Background(), SourceDiscoveryArgs{
		Provider: "jira", CredentialID: &credentialID,
		OrgID: "org", IntegrationID: "integration",
	})
	if err == nil {
		t.Fatal("Discover() with an unusable credential resolver = nil error, want a resolve failure")
	}
	var buffer bytes.Buffer
	if err := service.telemetry.WritePrometheus(&buffer); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buffer.String(), `provider_source_discovery_total{provider="jira",outcome="error"} 1`) {
		t.Fatalf("an internal Discover() failure was NOT recorded as outcome=error -- a silent-failure defect:\n%s", buffer.String())
	}
}

func TestSourceDiscoveryTelemetryWritesPrometheus(t *testing.T) {
	telemetry := newSourceDiscoveryTelemetry()
	telemetry.observe("jira", SourceDiscoveryOutcomeCreated)
	telemetry.observe("jira", SourceDiscoveryOutcomeCreated)
	telemetry.observe("github", SourceDiscoveryOutcomeSkipped)

	var buffer bytes.Buffer
	if err := telemetry.WritePrometheus(&buffer); err != nil {
		t.Fatal(err)
	}
	output := buffer.String()
	if !strings.Contains(output, `provider_source_discovery_total{provider="jira",outcome="created"} 2`) {
		t.Fatalf("WritePrometheus() output missing jira/created=2:\n%s", output)
	}
	if !strings.Contains(output, `provider_source_discovery_total{provider="github",outcome="skipped"} 1`) {
		t.Fatalf("WritePrometheus() output missing github/skipped=1:\n%s", output)
	}
	// Pre-seeded zero series -- every (provider,outcome) pair for every
	// registered provider must be present even before it is ever observed,
	// so an operator can alert on the series existing at all.
	if !strings.Contains(output, `provider_source_discovery_total{provider="gitlab",outcome="error"} 0`) {
		t.Fatalf("WritePrometheus() output missing pre-seeded gitlab/error=0:\n%s", output)
	}
}
