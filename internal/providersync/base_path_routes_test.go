package providersync

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type urlRecordingDoer struct{ urls []string }

func (d *urlRecordingDoer) Do(request *http.Request) (*http.Response, error) {
	d.urls = append(d.urls, request.URL.String())
	return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("{}")), Request: request}, nil
}

func prefixedClient(t *testing.T, provider, base string, doer providerfoundation.HTTPDoer) *providerfoundation.HTTPClient {
	t.Helper()
	client, err := providerfoundation.NewHTTPClient(provider, base, doer, func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// TestRoutePathsKeepTheCredentialBasePathExactlyOnce (CHAOS-6752): every path
// helper the routes use lands on base path + request path -- never the bare
// path (the old url.Parse drop) and never the base path twice (a helper that
// prefixed it itself on top of HTTPClient.Do's join).
func TestRoutePathsKeepTheCredentialBasePathExactlyOnce(t *testing.T) {
	cases := []struct {
		name, provider, base string
		path                 func(*providerfoundation.HTTPClient) string
		want                 string
	}{
		{"github repo route, GHE prefix", "github", "https://ghe.example/api/v3", func(c *providerfoundation.HTTPClient) string { return providerRelativePath(c, "repos", "acme", "api") }, "https://ghe.example/api/v3/repos/acme/api"},
		{"github repo route, no prefix", "github", "https://api.github.com", func(c *providerfoundation.HTTPClient) string { return providerRelativePath(c, "repos", "acme", "api") }, "https://api.github.com/repos/acme/api"},
		{"gitlab project route, sub-path", "gitlab", "https://host/c/ok", func(c *providerfoundation.HTTPClient) string {
			return providerRelativePath(c, "api", "v4", "projects", "77")
		}, "https://host/c/ok/api/v4/projects/77"},
		{"segments are escaped", "github", "https://ghe.example/api/v3", func(c *providerfoundation.HTTPClient) string { return providerRelativePath(c, "repos", "a b", "c/d") }, "https://ghe.example/api/v3/repos/a%20b/c%2Fd"},
		// The GraphQL endpoint sits BESIDE a GHE /api/v3 base, so it is passed as
		// an absolute URL and must not be joined under the base path.
		{"github graphql beside /api/v3", "github", "https://ghe.example/api/v3", gitHubGraphQLURL, "https://ghe.example/api/graphql"},
		{"github graphql, github.com", "github", "https://api.github.com", gitHubGraphQLURL, "https://api.github.com/graphql"},
		{"gitlab graphql, sub-path", "gitlab", "https://host/c/ok", gitLabGraphQLURL, "https://host/c/ok/api/graphql"},
		{"gitlab graphql, /api/v4 base", "gitlab", "https://host/api/v4", gitLabGraphQLURL, "https://host/api/graphql"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			doer := &urlRecordingDoer{}
			client := prefixedClient(t, tc.provider, tc.base, doer)
			response, err := client.Do(context.Background(), http.MethodGet, tc.path(client), nil)
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
