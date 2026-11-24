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

type teamRosterRetryAfterDoer struct{ teamRosterStatusDoer }

func (d teamRosterRetryAfterDoer) Do(request *http.Request) (*http.Response, error) {
	response, err := d.teamRosterStatusDoer.Do(request)
	if err == nil && response.StatusCode == http.StatusTooManyRequests {
		response.Header.Set("Retry-After", "25")
	}
	return response, err
}

// A rate-limited roster whose retry wait was cut short by the deadline is the
// base's deadline error: it ends the run and the team is not counted skipped.
func TestGitHubTeamCatalogRosterDeadlineShortenedRateLimitEndsTheRun(t *testing.T) {
	t.Parallel()
	client, err := providerfoundation.NewHTTPClient(
		"github", "https://api.github.com", fakehttp.Client(teamRosterRetryAfterDoer{teamRosterStatusDoer{http.StatusTooManyRequests}}),
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 3, InitialWait: 25 * time.Second, MaxWait: 25 * time.Second},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	collector := GitHubTeamCatalogRouteHandler{
		Client: client, OrgName: "acme", Now: func() time.Time { return time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC) },
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, evidence, err := collector.Collect(ctx, "org-1", true, true)
	var providerErr *providerfoundation.ProviderError
	if !errors.Is(err, context.DeadlineExceeded) || !errors.As(err, &providerErr) || providerErr.Class != providerfoundation.ErrorRateLimited {
		t.Fatalf("err=%v, want the rate_limited deadline error", err)
	}
	if evidence.SkippedTeamMemberships != 0 {
		t.Fatalf("evidence=%+v, a run ended by the deadline must not count a skipped team", evidence)
	}
}

// An exhausted rate limit with time left stays what the base did: the team's
// roster is skipped and the run goes on.
func TestGitHubTeamCatalogRosterExhaustedRateLimitStillSkipsTheTeam(t *testing.T) {
	t.Parallel()
	collector := GitHubTeamCatalogRouteHandler{
		Client: githubTeamCatalogTestClient(t, fakehttp.Client(teamRosterStatusDoer{http.StatusTooManyRequests})), OrgName: "acme",
		Now: func() time.Time { return time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC) },
	}
	rows, evidence, err := collector.Collect(context.Background(), "org-1", true, true)
	if err != nil || evidence.SkippedTeamMemberships != 1 || len(rows.Memberships) != 1 {
		t.Fatalf("err=%v evidence=%+v memberships=%d, want the platform team skipped and ops kept", err, evidence, len(rows.Memberships))
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
