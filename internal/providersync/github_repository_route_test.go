package providersync

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const gitHubRepositoryFixture = `{
  "id": 4567,
  "name": "api",
  "full_name": "Acme/API",
  "html_url": "https://github.com/Acme/API",
  "default_branch": "main",
  "language": "Go",
  "archived": false,
  "updated_at": "2026-07-20T10:00:00Z",
  "pushed_at": "2026-07-21T10:00:00Z"
}`

type gitHubRepositoryDoer struct {
	t        *testing.T
	body     string
	status   int
	requests int
	paths    []string
}

func (doer *gitHubRepositoryDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	doer.requests++
	doer.paths = append(doer.paths, request.URL.Path)
	status := doer.status
	if status == 0 {
		status = http.StatusOK
	}
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(doer.body)),
		Request:    request,
	}, nil
}

func gitHubRepositoryClient(
	t *testing.T,
	doer providerfoundation.HTTPDoer,
	base string,
) *providerfoundation.HTTPClient {
	t.Helper()
	client, err := providerfoundation.NewHTTPClient(
		"github", base, doer,
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{
			MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
		},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestGitHubRepositoryRouteEmitsOneBoundedReposEffect(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	doer := &gitHubRepositoryDoer{t: t, body: gitHubRepositoryFixture}
	client := gitHubRepositoryClient(t, doer, "https://api.github.com")
	claim := nativeTestClaim("github", "repo-metadata")
	batch, err := (GitHubRepositoryRouteHandler{
		Now: func() time.Time { return now },
	}).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, ok := (CompleteRouteSwitches{}).Descriptor("github", "repo-metadata")
	if !ok {
		t.Fatal("github/repo-metadata has no canonical descriptor")
	}
	if err := batch.validate(descriptor); err != nil {
		t.Fatalf("batch does not satisfy the canonical destination manifest: %v", err)
	}
	if doer.requests != 1 || doer.paths[0] != "/repos/acme/api" {
		t.Fatalf("requests=%d paths=%v", doer.requests, doer.paths)
	}
	// repo-metadata is WatermarkNone: a reference dataset never advances a
	// cursor, and a wrong watermark would silently truncate later windows.
	if batch.Watermark != nil {
		t.Fatalf("watermark=%v", batch.Watermark)
	}
	if batch.Evidence.Records != 1 || batch.Evidence.Requests != 1 ||
		batch.Evidence.CapReached {
		t.Fatalf("evidence=%+v", batch.Evidence)
	}
	if len(batch.Effects) != 1 || batch.Effects[0].Destination != "repos" ||
		batch.Effects[0].Recovery != EffectReplaySafe ||
		len(batch.Effects[0].Rows) != 1 {
		t.Fatalf("effects=%+v", batch.Effects)
	}
	var row repositoryRow
	if err := json.Unmarshal(batch.Effects[0].Rows[0], &row); err != nil {
		t.Fatal(err)
	}
	if row.OrgID != claim.OrgID || row.Repo != "Acme/API" ||
		row.Provider != "github" || row.Ref != nil ||
		!row.LastSynced.Equal(now) || !row.CreatedAt.Equal(now) {
		t.Fatalf("row=%+v", row)
	}
	if row.Settings != `{"source":"github","github_instance_url":"github.com",`+
		`"repo_id":4567,"url":"https://github.com/Acme/API","default_branch":"main"}` {
		t.Fatalf("settings=%s", row.Settings)
	}
	if row.Tags != `["github","Go"]` {
		t.Fatalf("tags=%s", row.Tags)
	}
}

// TestRepositoryIdentityMatchesPythonDerivation pins the repo_id derivation to
// Python's get_repo_uuid_from_repo. A divergence here silently forks every
// downstream repo_id foreign key.
func TestRepositoryIdentityMatchesPythonDerivation(t *testing.T) {
	t.Parallel()
	// python: uuid.UUID(bytes=hashlib.sha256("acme/api".encode()).digest()[:16])
	for input, want := range map[string]string{
		"acme/api":  "c7198fbc-1945-3717-05d8-eb78866b4e79",
		"Acme/API":  "c7198fbc-1945-3717-05d8-eb78866b4e79",
		" acme/api": "c7198fbc-1945-3717-05d8-eb78866b4e79",
	} {
		if got := repositoryIdentity(input); got != want {
			t.Errorf("repositoryIdentity(%q) = %q want %q", input, got, want)
		}
	}
}

func TestGitHubRepositoryRouteFailsClosedOnScopeAndPayloadFaults(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	handler := GitHubRepositoryRouteHandler{Now: func() time.Time { return now }}
	client := gitHubRepositoryClient(
		t, &gitHubRepositoryDoer{t: t, body: gitHubRepositoryFixture},
		"https://api.github.com",
	)
	for name, claim := range map[string]Claim{
		"wrong dataset":  nativeTestClaim("github", "commits"),
		"wrong provider": nativeTestClaim("gitlab", "repo-metadata"),
	} {
		if _, err := handler.Collect(
			context.Background(), claim, providerfoundation.Credential{}, client, now,
		); err != ErrInvalidConfiguration {
			t.Errorf("%s error=%v", name, err)
		}
	}
	malformed := gitHubRepositoryClient(
		t, &gitHubRepositoryDoer{t: t, body: `{"id":"not-a-number"}`},
		"https://api.github.com",
	)
	if _, err := handler.Collect(
		context.Background(), nativeTestClaim("github", "repo-metadata"),
		providerfoundation.Credential{}, malformed, now,
	); err == nil {
		t.Error("malformed repository payload was accepted")
	}
}

func TestNormalizedProviderInstanceMirrorsPython(t *testing.T) {
	t.Parallel()
	for raw, want := range map[string]string{
		"https://api.github.com":       "github.com",
		"https://github.com":           "github.com",
		"https://ghe.acme.test":        "ghe.acme.test",
		"https://ghe.acme.test:443":    "ghe.acme.test",
		"https://ghe.acme.test:8443":   "ghe.acme.test:8443",
		"http://ghe.acme.test:80":      "ghe.acme.test",
		"https://GHE.Acme.Test/api/v3": "ghe.acme.test",
	} {
		parsed, err := url.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		got, ok := normalizedProviderInstance("github", parsed)
		if !ok || got != want {
			t.Errorf("normalizedProviderInstance(%q) = %q,%v want %q", raw, got, ok, want)
		}
	}
	parsed, _ := url.Parse("https://-bad-.test")
	if _, ok := normalizedProviderInstance("github", parsed); ok {
		t.Error("invalid host label was accepted")
	}
}

// TestGitHubRepositoryLiveParityHarness is the live-parity harness stub
// required before (github, repo-metadata) may become RouteReady. It is skipped
// until an operator points it at a real credentialed repository; a skipped run
// is never parity evidence (plan §5 false-pass rules).
func TestGitHubRepositoryLiveParityHarness(t *testing.T) {
	repository := os.Getenv("PROVIDER_LIVE_PARITY_GITHUB_REPO")
	token := os.Getenv("PROVIDER_LIVE_PARITY_GITHUB_TOKEN")
	if repository == "" || token == "" {
		t.Skip(
			"live parity requires PROVIDER_LIVE_PARITY_GITHUB_REPO and " +
				"PROVIDER_LIVE_PARITY_GITHUB_TOKEN; a skip is not parity evidence",
		)
	}
	t.Fatal(
		"live parity harness is not implemented: the comparison against the " +
			"Python-written repos row runs in the CUT-09 live-parity lane",
	)
}
