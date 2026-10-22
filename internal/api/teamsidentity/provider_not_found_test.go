package teamsidentity

import (
	"context"
	"errors"
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

const upstreamLeakText = "UPSTREAM-BODY-LEAK-9014"

type notFoundDoer struct{}

func (notFoundDoer) Do(request *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: http.StatusNotFound, Header: http.Header{"Content-Type": {"application/json"}},
		Body: io.NopCloser(strings.NewReader(`{"errorMessages":["` + upstreamLeakText + `"]}`)), Request: request}, nil
}

func answerProviderFailure(err error) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/teams/IMP/discover-members", nil)
	handlers{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}.providerFailure(rec, req, "discover team members", err)
	return rec
}

// TestProviderNotFoundIsA404ForEveryProviderRoute pins CHAOS-9014: a provider
// that answers 404 (an unknown Jira project, GitHub org or team, GitLab group)
// is a 404 with one fixed text, never a 500 and never the upstream body, for
// every provider route of the discover, member discover and infer routes.
func TestProviderNotFoundIsA404ForEveryProviderRoute(t *testing.T) {
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
	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			withDiscoveryClient(t, fakehttp.Client(notFoundDoer{}))
			err := route.run(context.Background())
			if err == nil {
				t.Fatal("want the provider's 404 as an error")
			}
			rec := answerProviderFailure(err)
			if rec.Code != http.StatusNotFound {
				t.Fatalf("status = %d, want 404\nbody: %s\nerr: %v", rec.Code, rec.Body.String(), err)
			}
			want := `{"detail":"` + providerNotFoundDetail + `"}`
			if rec.Body.String() != want {
				t.Errorf("body = %s, want %s", rec.Body.String(), want)
			}
			for _, leak := range []string{upstreamLeakText, "IMP", "acme", "/rest/api", "http"} {
				if strings.Contains(rec.Body.String(), leak) {
					t.Errorf("body echoes %q: %s", leak, rec.Body.String())
				}
			}
		})
	}
}

// TestProviderFailureKeepsOtherErrorsAs500 guards the other side: only a
// provider "not found" becomes a client error.
func TestProviderFailureKeepsOtherErrorsAs500(t *testing.T) {
	for _, err := range []error{
		errors.New("boom"),
		&providerfoundation.ProviderError{Class: providerfoundation.ErrorAuthentication, StatusCode: 401},
		&providerfoundation.ProviderError{Class: providerfoundation.ErrorTransient, StatusCode: 503},
	} {
		if rec := answerProviderFailure(err); rec.Code != http.StatusInternalServerError {
			t.Errorf("%v: status = %d, want 500", err, rec.Code)
		}
	}
}
