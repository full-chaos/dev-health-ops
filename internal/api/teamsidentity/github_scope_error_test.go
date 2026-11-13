package teamsidentity

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

type githubScopeDoer struct {
	status  int
	headers http.Header
	body    string
}

func (d githubScopeDoer) Do(request *http.Request) (*http.Response, error) {
	headers := http.Header{}
	for key, values := range d.headers {
		headers[key] = append([]string(nil), values...)
	}
	headers.Set("Content-Type", "application/json")
	return &http.Response{StatusCode: d.status, Header: headers,
		Body: io.NopCloser(strings.NewReader(d.body)), Request: request}, nil
}

// TestGitHubTokenWithoutScopeIsAClearClientError is the CHAOS-2302 red cell:
// a GitHub 403 "Resource not accessible" (a token without the needed scope)
// must answer a clear 4xx that names the provider status, not the
// generic 500.
func TestGitHubTokenWithoutScopeIsAClearClientError(t *testing.T) {
	cases := []struct {
		name   string
		status int
		doer   githubScopeDoer
	}{
		{"403 missing scope", http.StatusForbidden, githubScopeDoer{
			status:  http.StatusForbidden,
			headers: http.Header{"X-Oauth-Scopes": {"repo"}, "X-Accepted-Oauth-Scopes": {"admin:org, read:org"}},
			body:    `{"message":"Resource not accessible by personal access token","documentation_url":"https://docs.github.com/rest/teams/teams#list-teams","status":"403"}`,
		}},
		{"401 bad token", http.StatusUnauthorized, githubScopeDoer{
			status: http.StatusUnauthorized,
			body:   `{"message":"Bad credentials","status":"401"}`,
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			withDiscoveryClient(t, fakehttp.Client(tc.doer))
			_, err := discoverGitHub(context.Background(), githubTestCredential(), "acme")
			if err == nil {
				t.Fatal("want the provider refusal as an error")
			}
			rec := answerProviderFailure(err)
			if rec.Code != http.StatusUnprocessableEntity {
				t.Fatalf("status = %d, want 422 (clear client error, not 500)\nbody: %s", rec.Code, rec.Body.String())
			}
			body := rec.Body.String()
			if !strings.Contains(body, strconv.Itoa(tc.status)) {
				t.Errorf("body must name the provider status %d: %s", tc.status, body)
			}
			for _, leak := range []string{"github-token", "Resource not accessible", "Bad credentials", "acme", "docs.github.com"} {
				if strings.Contains(body, leak) {
					t.Errorf("body echoes %q: %s", leak, body)
				}
			}
		})
	}
}

// TestGitHubGoodTokenDiscoveryControl passes on main: a 200 with an empty
// team list is a success with no error.
func TestGitHubGoodTokenDiscoveryControl(t *testing.T) {
	withDiscoveryClient(t, fakehttp.Client(githubScopeDoer{status: http.StatusOK, body: `[]`}))
	teams, err := discoverGitHub(context.Background(), githubTestCredential(), "acme")
	if err != nil {
		t.Fatalf("discoverGitHub: %v", err)
	}
	if len(teams) != 0 {
		t.Fatalf("teams = %d, want 0", len(teams))
	}
}
