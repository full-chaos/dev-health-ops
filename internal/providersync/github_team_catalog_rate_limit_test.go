package providersync

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

type teamRosterStatusDoer struct{ rosterStatus int }

func (d teamRosterStatusDoer) Do(request *http.Request) (*http.Response, error) {
	status, body := http.StatusOK, `[]`
	switch request.URL.Path {
	case "/orgs/acme/teams":
		body = `[{"slug":"platform","name":"Platform"},{"slug":"ops","name":"Operations"}]`
	case "/orgs/acme/teams/platform/members":
		status, body = d.rosterStatus, `{"message":"limited"}`
	case "/orgs/acme/teams/ops/members":
		body = `[{"login":"octocat"}]`
	}
	return &http.Response{
		StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}},
		Body: io.NopCloser(strings.NewReader(body)), Request: request,
	}, nil
}

// A rate-limited roster read ends the unit so the handler defers it; it is not
// a skipped team.
func TestGitHubTeamCatalogRosterRateLimitEndsTheUnitInsteadOfSkippingTheTeam(t *testing.T) {
	t.Parallel()
	collector := GitHubTeamCatalogRouteHandler{
		Client: githubTeamCatalogTestClient(t, fakehttp.Client(teamRosterStatusDoer{http.StatusTooManyRequests})), OrgName: "acme",
		Now: func() time.Time { return time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC) },
	}
	_, evidence, err := collector.Collect(context.Background(), "org-1", true, true)
	var providerErr *providerfoundation.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != providerfoundation.ErrorRateLimited {
		t.Fatalf("err=%v, want the rate_limited error", err)
	}
	if evidence.SkippedTeamMemberships != 0 {
		t.Fatalf("evidence=%+v, a rate-limited roster must not count as skipped", evidence)
	}
}

// Any other class stays non-fatal: a 404 roster skips only that team.
func TestGitHubTeamCatalogRosterNotFoundStillSkipsTheTeam(t *testing.T) {
	t.Parallel()
	collector := GitHubTeamCatalogRouteHandler{
		Client: githubTeamCatalogTestClient(t, fakehttp.Client(teamRosterStatusDoer{http.StatusNotFound})), OrgName: "acme",
		Now: func() time.Time { return time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC) },
	}
	rows, evidence, err := collector.Collect(context.Background(), "org-1", true, true)
	if err != nil || evidence.SkippedTeamMemberships != 1 || len(rows.Memberships) != 1 {
		t.Fatalf("err=%v evidence=%+v memberships=%d, want the platform team skipped and ops kept", err, evidence, len(rows.Memberships))
	}
}
