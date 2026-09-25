package sync

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// countingDoer answers every request with one status and headers and records
// the path of each call: the stub call sequence.
type countingDoer struct {
	status  int
	headers http.Header
	paths   []string
}

func (doer *countingDoer) Do(request *http.Request) (*http.Response, error) {
	doer.paths = append(doer.paths, request.URL.Path)
	header := doer.headers.Clone()
	if header == nil {
		header = http.Header{}
	}
	header.Set("Content-Type", "application/json")
	return &http.Response{StatusCode: doer.status, Header: header, Body: io.NopCloser(strings.NewReader(`{"message":"stub"}`)), Request: request}, nil
}

// TestDiscoveryRetryShapeIsTheGoPolicyNotThePythonLibraryDefaults pins CHAOS-6784's
// named divergence: how many times source discovery calls a provider that keeps
// failing. The Go count is the shared providerfoundation policy
// (DefaultRetryPolicy: 3 attempts per call, the Go clients' one budget); the
// Python counts are the DEFAULTS OF ITS LIBRARIES, not a platform decision:
//
//	PyGithub 2.x  Github(token): retry=GithubRetry(total=10), status_forcelist
//	              403 and 500-599, Retry-After / rate-limit reset honoured, no
//	              backoff between other retries. 11 attempts per call, and the
//	              /orgs -> /users fallback doubles it: 22 for a failing owner.
//	python-gitlab Gitlab(...): retry_transient_errors=False (a 5xx is one
//	              attempt), obey_rate_limit=True with 10 retries (a 429 is 11).
//
// Observed against the recorded stub (bigboy-stub pass 6, Python vs Go, same
// stub): GitHub owner 429/500/403 primary/403 secondary/599: Go 6, Python 22;
// GitLab group 429: Go 3, Python 11; 500/599: Go 3, Python 1; 403: 1 and 1.
//
// The reason the counts are not ported: the answer never depends on them
// (Python answers the empty 202 for any provider failure, Go the 503 or the
// authentication 422 of CHAOS-6753), so they show only as latency (Python
// blocks the admin request ~21 s on a rate-limited owner) and provider call
// volume; the platform's own retry budget is one deliberate number shared by
// every Go provider client, and this route runs inside an HTTP request.
// Changing DefaultRetryPolicy must revisit this named divergence: this test
// fails until the expectations below are re-derived.
func TestDiscoveryRetryShapeIsTheGoPolicyNotThePythonLibraryDefaults(t *testing.T) {
	policy := providerfoundation.DefaultRetryPolicy()
	if policy.MaxAttempts != 3 {
		t.Fatalf("DefaultRetryPolicy().MaxAttempts = %d: the discovery retry shape below (and the named divergence in this test's doc comment) assumes 3", policy.MaxAttempts)
	}
	fast := providerfoundation.RetryPolicy{MaxAttempts: policy.MaxAttempts, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond}
	rateLimited := http.Header{"X-Ratelimit-Remaining": []string{"0"}, "Retry-After": []string{"0"}}

	type shape struct {
		name    string
		status  int
		headers http.Header
		calls   int
	}
	attempts := policy.MaxAttempts
	github := []shape{
		{"429", 429, http.Header{"Retry-After": []string{"0"}}, 2 * attempts},
		{"500", 500, nil, 2 * attempts},
		{"599", 599, nil, 2 * attempts},
		{"403 primary rate limit", 403, rateLimited, 2 * attempts},
		{"401 is not retried", 401, nil, 2},
		{"404 is not retried", 404, nil, 2},
	}
	gitlab := []shape{
		{"429", 429, http.Header{"Retry-After": []string{"0"}}, attempts},
		{"500", 500, nil, attempts},
		{"599", 599, nil, attempts},
		{"403 is not retried", 403, nil, 1},
		{"404 is not retried", 404, nil, 1},
	}
	for _, provider := range []struct {
		name   string
		shapes []shape
		run    func(*NativeSourceDiscoveryService, providerfoundation.Credential) error
	}{
		{"github", github, func(service *NativeSourceDiscoveryService, credential providerfoundation.Credential) error {
			_, err := service.discoverGitHub(context.Background(), credential, map[string]any{"owner": "zz-owner"})
			return err
		}},
		{"gitlab", gitlab, func(service *NativeSourceDiscoveryService, credential providerfoundation.Credential) error {
			_, err := service.discoverGitLab(context.Background(), credential, map[string]any{"group": "zz-group"})
			return err
		}},
	} {
		credential, err := providerfoundation.Credential{Provider: provider.name}.WithEphemeralSecret("token", secrets.NewValue("stub-token"))
		if err != nil {
			t.Fatal(err)
		}
		for _, test := range provider.shapes {
			doer := &countingDoer{status: test.status, headers: test.headers}
			service := &NativeSourceDiscoveryService{doer: doer, retry: fast, telemetry: newSourceDiscoveryTelemetry(), now: time.Now}
			if err := provider.run(service, credential); err == nil {
				t.Errorf("%s %s: a failing provider must surface an error", provider.name, test.name)
			}
			if len(doer.paths) != test.calls {
				t.Errorf("%s %s: %d provider calls %v, want %d", provider.name, test.name, len(doer.paths), doer.paths, test.calls)
			}
		}
	}
}
