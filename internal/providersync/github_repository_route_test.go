package providersync

import (
	"context"
	"encoding/json"
	"errors"
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

// TestGitHubRepositoryRouteEmitsOneBoundedReposEffect also carries the CHAOS-
// 3123 parity evidence for RouteReady: the expected id/settings/tags below
// were independently produced by running the real Python collector against
// this exact fixture --
//
//	repo_info = _repo_from_item(fixture)  # code_client.py
//	instance = normalized_operational_provider_instance("github", "https://api.github.com")
//	settings = {"source": "github", "github_instance_url": instance,
//	            "repo_id": repo_info.id, "url": repo_info.url,
//	            "default_branch": repo_info.default_branch}
//	tags = ["github", repo_info.language]  # repo_info.language == "Go"
//	repo_id = get_repo_uuid_from_repo(repo_info.full_name)
//
// which printed repo_uuid=c7198fbc-1945-3717-05d8-eb78866b4e79,
// settings={"source": "github", "github_instance_url": "github.com",
// "repo_id": 4567, "url": "https://github.com/Acme/API",
// "default_branch": "main"}, tags=["github", "Go"] -- field-for-field
// identical to the assertions below (whitespace aside, which is not part of
// the persisted semantics). This is fixture parity, not live parity: it
// proves Go and Python agree on this input, not that either agrees with a
// live GitHub API response.
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
	descriptor, ok := Descriptor("github", "repo-metadata")
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
	// Readback-fenced, not replay-safe: ReplacingMergeTree dedup is
	// asynchronous, so a blind recovery reinsert is visible to raw readers.
	if len(batch.Effects) != 1 || batch.Effects[0].Destination != "repos" ||
		batch.Effects[0].Recovery != EffectReadbackRequired ||
		len(batch.Effects[0].Rows) != 1 {
		t.Fatalf("effects=%+v", batch.Effects)
	}
	var row repositoryRow
	if err := json.Unmarshal(batch.Effects[0].Rows[0], &row); err != nil {
		t.Fatal(err)
	}
	if row.OrgID != claim.OrgID || row.Repo != "Acme/API" ||
		row.Provider != "github" || row.Ref != nil ||
		row.ID != "c7198fbc-1945-3717-05d8-eb78866b4e79" ||
		!row.LastSynced.Equal(now) || !row.CreatedAt.Equal(now) {
		t.Fatalf("row=%+v", row)
	}
	if row.Settings != `{"default_branch":"main","github_instance_url":"github.com",`+
		`"repo_id":4567,"source":"github","url":"https://github.com/Acme/API"}` {
		t.Fatalf("settings=%s", row.Settings)
	}
	if row.Tags != `["github","Go"]` {
		t.Fatalf("tags=%s", row.Tags)
	}
}

// TestRepositoryIdentityMatchesPythonDerivation pins the repo_id derivation to
// Python's get_repo_uuid_from_repo for the ASCII names GitHub issues. A
// divergence here silently forks every downstream repo_id foreign key.
func TestRepositoryIdentityMatchesPythonDerivation(t *testing.T) {
	// Not parallel: asserts behavior under a process-global env var.
	// python: uuid.UUID(bytes=hashlib.sha256("acme/api".encode()).digest()[:16])
	for input, want := range map[string]string{
		"acme/api":  "c7198fbc-1945-3717-05d8-eb78866b4e79",
		"Acme/API":  "c7198fbc-1945-3717-05d8-eb78866b4e79",
		" acme/api": "c7198fbc-1945-3717-05d8-eb78866b4e79",
	} {
		got, err := repositoryIdentity(input)
		if err != nil || got != want {
			t.Errorf("repositoryIdentity(%q) = %q,%v want %q", input, got, err, want)
		}
	}
}

// TestRepositoryIdentityFailsClosedOnDocumentedPythonDivergences covers the two
// cases Go deliberately refuses rather than guesses. Writing a repo_id Python
// would not have written is worse than failing the unit.
func TestRepositoryIdentityFailsClosedOnDocumentedPythonDivergences(t *testing.T) {
	// Python's get_repo_uuid_from_repo honours a process-global REPO_UUID
	// override (models/git.py). Go never sources identity from process state.
	t.Setenv("REPO_UUID", "11111111-1111-4111-8111-111111111111")
	if _, err := repositoryIdentity("acme/api"); !errors.Is(
		err, ErrRepositoryIdentityAmbiguous,
	) {
		t.Errorf("REPO_UUID override error=%v", err)
	}
	// Python truthiness-checks the variable, so an empty value is not an
	// override and must still hash normally.
	t.Setenv("REPO_UUID", "")
	if got, err := repositoryIdentity("acme/api"); err != nil ||
		got != "c7198fbc-1945-3717-05d8-eb78866b4e79" {
		t.Errorf("empty REPO_UUID = %q,%v", got, err)
	}
	os.Unsetenv("REPO_UUID")

	// Python str.lower() applies full Unicode case mapping (U+0130 lowers to
	// "i" plus U+0307); Go strings.ToLower applies simple per-rune mapping.
	// GitHub names are [A-Za-z0-9._-], so a non-ASCII name is already invalid.
	for _, input := range []string{"acme/AP\u0130", "\u00fcmlaut/api"} {
		if _, err := repositoryIdentity(input); !errors.Is(
			err, ErrRepositoryIdentityAmbiguous,
		) {
			t.Errorf("non-ascii %q error=%v", input, err)
		}
	}
	if _, err := repositoryIdentity("   "); err == nil {
		t.Error("blank repository identifier was accepted")
	}
}

// TestGitHubRepositoryRoutePreservesEnterpriseBasePath is the GHE regression:
// an absolute request path would make url.Parse replace the configured
// /api/v3 prefix instead of extending it, sending the request to a non-API
// route.
func TestGitHubRepositoryRoutePreservesEnterpriseBasePath(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	doer := &gitHubRepositoryDoer{t: t, body: gitHubRepositoryFixture}
	client := gitHubRepositoryClient(t, doer, "https://ghe.acme.test/api/v3")
	batch, err := (GitHubRepositoryRouteHandler{
		Now: func() time.Time { return now },
	}).Collect(
		context.Background(), nativeTestClaim("github", "repo-metadata"),
		providerfoundation.Credential{}, client, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	if doer.paths[0] != "/api/v3/repos/acme/api" {
		t.Fatalf("request path=%q want /api/v3/repos/acme/api", doer.paths[0])
	}
	var row repositoryRow
	if err := json.Unmarshal(batch.Effects[0].Rows[0], &row); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(row.Settings, `"github_instance_url":"ghe.acme.test"`) {
		t.Fatalf("settings=%s", row.Settings)
	}
}

// TestGitHubRepositoryRouteDefaultsBranchLikePython covers the empty
// default_branch coercion Python performs in _repo_from_item.
func TestGitHubRepositoryRouteDefaultsBranchLikePython(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	client := gitHubRepositoryClient(t, &gitHubRepositoryDoer{
		t: t, body: `{"id":4567,"full_name":"Acme/API","html_url":"https://github.com/Acme/API"}`,
	}, "https://api.github.com")
	batch, err := (GitHubRepositoryRouteHandler{
		Now: func() time.Time { return now },
	}).Collect(
		context.Background(), nativeTestClaim("github", "repo-metadata"),
		providerfoundation.Credential{}, client, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	var row repositoryRow
	if err := json.Unmarshal(batch.Effects[0].Rows[0], &row); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(row.Settings, `"default_branch":"main"`) {
		t.Fatalf("settings=%s", row.Settings)
	}
	if batch.Result["default_branch"] != "main" {
		t.Fatalf("result=%+v", batch.Result)
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
		"https://ghe.acme.test:65535":  "ghe.acme.test:65535",
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
	for _, raw := range []string{
		"https://-bad-.test",
		// Python's urlsplit.port rejects a value outside 1-65535.
		"https://ghe.acme.test:65536",
		// Documented fail-closed divergence: Python accepts Unicode alphanumeric
		// labels via str.isalnum(); Go refuses rather than persist an instance
		// identifier it cannot prove matches.
		"https://m\u00fcnchen.example",
	} {
		parsed, err := url.Parse(raw)
		if err != nil {
			continue
		}
		if got, ok := normalizedProviderInstance("github", parsed); ok {
			t.Errorf("normalizedProviderInstance(%q) accepted as %q", raw, got)
		}
	}
}

// TestGitHubRepositoryLiveParityHarness is the live-parity harness stub.
// CHAOS-3123 flipped (github, repo-metadata) to RouteReady on fixture-level
// field parity against the production Python collector instead — canary
// staging and live-traffic parity are waived for this program (no production
// users). Live parity against a real credentialed repository is still not
// captured and is NOT required for RouteReady under the waiver, but remains
// valuable operational evidence before the GithubRepoMetadata switch is ever
// turned on. It is skipped until an operator points it at a real credentialed
// repository; a skipped run is never parity evidence (plan §5 false-pass
// rules).
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
