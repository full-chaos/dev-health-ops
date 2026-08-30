package sync

import (
	"bytes"
	"context"
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

func TestGithubDiscoveryScope(t *testing.T) {
	cases := []struct {
		name        string
		options     map[string]any
		wantOwner   string
		wantPattern string
	}{
		{name: "empty options discovers everything visible", options: map[string]any{}, wantOwner: "", wantPattern: "*"},
		{name: "explicit owner only", options: map[string]any{"owner": "acme"}, wantOwner: "acme", wantPattern: "*"},
		{name: "owner/pattern search fills pattern", options: map[string]any{"owner": "acme", "search": "acme/api-*"}, wantOwner: "acme", wantPattern: "api-*"},
		{name: "bare search with no owner becomes the owner", options: map[string]any{"search": "acme"}, wantOwner: "acme", wantPattern: "*"},
		{name: "search supplies both halves when owner is unset", options: map[string]any{"search": "acme/api-*"}, wantOwner: "acme", wantPattern: "api-*"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			owner, pattern := githubDiscoveryScope(testCase.options)
			if owner != testCase.wantOwner || pattern != testCase.wantPattern {
				t.Fatalf("githubDiscoveryScope(%v) = (%q,%q), want (%q,%q)", testCase.options, owner, pattern, testCase.wantOwner, testCase.wantPattern)
			}
		})
	}
}

func TestGitlabDiscoveryScope(t *testing.T) {
	cases := []struct {
		name        string
		options     map[string]any
		wantGroup   string
		wantPattern string
	}{
		{name: "empty options discovers everything visible", options: map[string]any{}, wantGroup: "", wantPattern: "*"},
		{name: "explicit group only", options: map[string]any{"group": "acme"}, wantGroup: "acme", wantPattern: "*"},
		{name: "owner falls back as group", options: map[string]any{"owner": "acme"}, wantGroup: "acme", wantPattern: "*"},
		{name: "nested subgroup search splits on the LAST slash", options: map[string]any{"search": "acme/platform/api-*"}, wantGroup: "acme/platform", wantPattern: "api-*"},
		{name: "bare search with no group becomes the group", options: map[string]any{"search": "acme"}, wantGroup: "acme", wantPattern: "*"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			group, pattern := gitlabDiscoveryScope(testCase.options)
			if group != testCase.wantGroup || pattern != testCase.wantPattern {
				t.Fatalf("gitlabDiscoveryScope(%v) = (%q,%q), want (%q,%q)", testCase.options, group, pattern, testCase.wantGroup, testCase.wantPattern)
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
}

func (doer *fakeSourceDiscoveryDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	doer.paths = append(doer.paths, request.URL.Path)
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(doer.body)),
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

func TestDiscoverGitLabMapsProjectsAndAppliesGroupScope(t *testing.T) {
	doer := &fakeSourceDiscoveryDoer{t: t, body: `[
		{"id":101,"name":"api","path_with_namespace":"acme/api"},
		{"id":102,"name":"other","path_with_namespace":"other-group/other"}
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
		t.Fatalf("discoverGitLab() = %#v, want only the acme-namespaced project", sources)
	}
	got := sources[0]
	if got.ExternalID != "101" || got.SourceType != "project" || got.Name != "api" || got.FullName != "acme/api" {
		t.Fatalf("discoverGitLab() source = %#v", got)
	}
	if got.Metadata["path_with_namespace"] != "acme/api" {
		t.Fatalf("discoverGitLab() metadata = %#v", got.Metadata)
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
