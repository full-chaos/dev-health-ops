package teamsidentity

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// refusedWant is the exact answer text pinned by CHAOS-2302; the fix constant must equal it.
const refusedWant = "The provider refused the stored credential: it is invalid, expired, or lacks the access this request needs. Check the token scopes or the app permissions"

type refusedDoer struct{ status int }

func (d refusedDoer) Do(request *http.Request) (*http.Response, error) {
	response := &http.Response{Request: request}
	response.StatusCode = d.status
	response.Header = http.Header{"Content-Type": {"application/json"}, "X-Oauth-Scopes": {"repo"}, "X-Accepted-Oauth-Scopes": {"admin:org, read:org"}}
	response.Body = io.NopCloser(strings.NewReader(`{"message":"Resource not accessible by personal access token ` + upstreamLeakText + `"}`))
	return response, nil
}

// TestProviderCredentialRefusedIsA422ForEveryProviderRoute pins CHAOS-2302: a
// provider that refuses the stored credential (401, or a 403 that is not a
// rate limit, such as a GitHub token without the read:org scope) is a 422 with
// one fixed text, never a 500 and never the upstream body.
func TestProviderCredentialRefusedIsA422ForEveryProviderRoute(t *testing.T) {
	token := secrets.NewValue("token")
	routes := []struct {
		name string
		run  func(context.Context) error
	}{
		{"github discover", func(ctx context.Context) error {
			_, err := discoverGitHub(ctx, githubTestCredential(), "acme")
			return err
		}},
		{"github members", func(ctx context.Context) error {
			_, err := discoverMembersGitHub(ctx, token, "acme", "eng")
			return err
		}},
		{"gitlab discover", func(ctx context.Context) error {
			_, _, _, err := discoverGitLab(ctx, gitlabTestCredential(), "acme/g")
			return err
		}},
		{"gitlab members", func(ctx context.Context) error { _, err := discoverMembersGitLab(ctx, token, "", "acme/g"); return err }},
		{"jira members", func(ctx context.Context) error {
			_, err := discoverMembersJira(ctx, jiraTestCredential(), "IMP")
			return err
		}},
		{"jira infer", func(ctx context.Context) error {
			_, err := inferJiraMembers(ctx, jiraTestCredential(), "IMP", 30)
			return err
		}},
	}
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		for _, route := range routes {
			t.Run(route.name+"/"+http.StatusText(status), func(t *testing.T) {
				withDiscoveryClient(t, fakehttp.Client(refusedDoer{status: status}))
				err := route.run(context.Background())
				if err == nil {
					t.Fatalf("want the provider's %d as an error", status)
				}
				rec := answerProviderFailure(err)
				if rec.Code != http.StatusUnprocessableEntity {
					t.Fatalf("status = %d, want 422\nbody: %s\nerr: %v", rec.Code, rec.Body.String(), err)
				}
				want := `{"detail":"` + refusedWant + `"}`
				if rec.Body.String() != want {
					t.Errorf("body = %s, want %s", rec.Body.String(), want)
				}
				for _, leak := range []string{upstreamLeakText, "Resource not accessible", "admin:org", "read:org", "github-token", "gitlab-token", "IMP", "acme", "/rest/api", "http"} {
					if strings.Contains(rec.Body.String(), leak) {
						t.Errorf("body echoes %q: %s", leak, rec.Body.String())
					}
				}
			})
		}
	}
}

// TestRateLimitedForbiddenIsNotACredentialRefusal pins the other side of CHAOS-2302: a GitHub 403 that is a
// rate limit is a different class and keeps today's answer (500), never the 422.
func TestRateLimitedForbiddenIsNotACredentialRefusal(t *testing.T) {
	headers := http.Header{"X-Ratelimit-Remaining": {"0"}}
	for _, err := range []error{
		providerfoundation.ClassifyHTTPWithMessage("github", http.StatusForbidden, headers, ""),
		providerfoundation.ClassifyHTTPWithMessage("github", http.StatusForbidden, http.Header{}, "You have exceeded a secondary rate limit"),
	} {
		if rec := answerProviderFailure(err); rec.Code != http.StatusInternalServerError {
			t.Errorf("%v: status = %d, want 500 and not 422", err, rec.Code)
		}
	}
}

type goodTokenDoer struct{}

func (goodTokenDoer) Do(request *http.Request) (*http.Response, error) {
	response := &http.Response{Request: request}
	response.StatusCode = http.StatusOK
	response.Header = http.Header{"Content-Type": {"application/json"}}
	response.Body = io.NopCloser(strings.NewReader(`[]`))
	return response, nil
}

// TestGoodTokenDiscoveryControl passes on main: a 200 with an empty team list is a success.
func TestGoodTokenDiscoveryControl(t *testing.T) {
	withDiscoveryClient(t, fakehttp.Client(goodTokenDoer{}))
	teams, err := discoverGitHub(context.Background(), githubTestCredential(), "acme")
	if err != nil || len(teams) != 0 {
		t.Fatalf("teams = %d, err = %v; want 0 teams and no error", len(teams), err)
	}
}

// TestRefusedCredentialWarnLineHasFixedFieldsOnly pins the log line of the 422: step, provider and
// provider_status, never the provider's body.
func TestRefusedCredentialWarnLineHasFixedFieldsOnly(t *testing.T) {
	var logs bytes.Buffer
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/teams/discover", nil)
	err := &providerfoundation.ProviderError{Class: providerfoundation.ErrorAuthentication, StatusCode: http.StatusForbidden, Body: upstreamLeakText}
	handlers{logger: slog.New(slog.NewTextHandler(&logs, nil))}.providerFailure(rec, req, "github", "discover teams", err)
	line := logs.String()
	for _, want := range []string{"step=\"discover teams\"", "provider=github", "provider_status=403"} {
		if !strings.Contains(line, want) {
			t.Errorf("log line lacks %s: %s", want, line)
		}
	}
	if strings.Contains(line, upstreamLeakText) {
		t.Errorf("log line carries the provider body: %s", line)
	}
}

func TestRefusedCredentialWarnLineLogsAnUnknownProviderAsOther(t *testing.T) {
	for _, provider := range []string{"evil\nprovider=github", "GitHub", ""} {
		var logs bytes.Buffer
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/teams/discover", nil)
		err := &providerfoundation.ProviderError{Class: providerfoundation.ErrorAuthentication, StatusCode: http.StatusUnauthorized}
		handlers{logger: slog.New(slog.NewTextHandler(&logs, nil))}.providerFailure(rec, req, provider, "discover teams", err)
		line := logs.String()
		if !strings.Contains(line, "provider=other") {
			t.Errorf("provider %q: log line lacks provider=other: %s", provider, line)
		}
		if strings.Count(line, "provider=") != 1 {
			t.Errorf("provider %q: log line carries the request's provider text: %s", provider, line)
		}
	}
}
