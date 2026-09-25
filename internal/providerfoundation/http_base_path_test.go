package providerfoundation

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type recordingDoer struct{ urls []string }

func (d *recordingDoer) Do(request *http.Request) (*http.Response, error) {
	d.urls = append(d.urls, request.URL.String())
	return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("[]")), Request: request}, nil
}

func baseURLClient(t *testing.T, base string, doer HTTPDoer) *HTTPClient {
	t.Helper()
	client, err := NewHTTPClient("gitlab", base, doer, func(*http.Request) error { return nil },
		RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// TestDoKeepsTheBaseURLPath is CHAOS-6752: a request path is joined UNDER the
// credential base URL's path (a self-hosted GitLab under /gitlab, a GitHub
// Enterprise /api/v3 prefix), as Python's httpx merges a base_url; url.Parse
// dropped it for a leading-slash path.
func TestDoKeepsTheBaseURLPath(t *testing.T) {
	cases := []struct{ name, base, path, want string }{
		{"sub-path base, absolute path (the ticket repro)", "https://gitlab.example.com/gitlab", "/api/v4/projects", "https://gitlab.example.com/gitlab/api/v4/projects"},
		{"trailing slash on the base", "https://gitlab.example.com/gitlab/", "/api/v4/projects", "https://gitlab.example.com/gitlab/api/v4/projects"},
		{"relative path", "https://gitlab.example.com/gitlab", "api/v4/projects", "https://gitlab.example.com/gitlab/api/v4/projects"},
		{"GHE prefix with a query", "https://ghe.example.com/api/v3", "/orgs/acme/repos?per_page=100", "https://ghe.example.com/api/v3/orgs/acme/repos?per_page=100"},
		{"two-level prefix", "https://host/c/ok", "/user/repos", "https://host/c/ok/user/repos"},
		{"no base path", "https://api.github.com", "/user/repos", "https://api.github.com/user/repos"},
		{"escaped segment stays escaped", "https://host/a%2Fb", "/x%2Fy", "https://host/a%2Fb/x%2Fy"},
		{"an absolute next URL is used as it is", "https://ghe.example.com/api/v3", "https://ghe.example.com/api/v3/orgs/acme/repos?page=2", "https://ghe.example.com/api/v3/orgs/acme/repos?page=2"},
		{"empty path is the base", "https://host/c/ok", "", "https://host/c/ok"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			doer := &recordingDoer{}
			response, err := baseURLClient(t, tc.base, doer).Do(context.Background(), http.MethodGet, tc.path, nil)
			if err != nil {
				t.Fatal(err)
			}
			_ = response.Body.Close()
			if len(doer.urls) != 1 || doer.urls[0] != tc.want {
				t.Fatalf("requested %v, want %s", doer.urls, tc.want)
			}
		})
	}
}

func TestDoRefusesAnotherOriginWhateverThePathForm(t *testing.T) {
	for _, path := range []string{"https://evil.example/x", "http://gitlab.example.com/x", "//evil.example/x", "https://gitlab.example.com:8443/x"} {
		doer := &recordingDoer{}
		if _, err := baseURLClient(t, "https://gitlab.example.com/gitlab", doer).Do(context.Background(), http.MethodGet, path, nil); err == nil || len(doer.urls) != 0 {
			t.Fatalf("path %q: err %v, requests %v; a request must never leave the base origin", path, err, doer.urls)
		}
	}
}
