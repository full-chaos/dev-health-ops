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
)

type rateLimitedRosterDoer struct {
	rosterStatus int
	rosterBody   string
	userStatus   int
	headers      http.Header
	limitedBody  string
}

func (d rateLimitedRosterDoer) Do(request *http.Request) (*http.Response, error) {
	status, body, headers := http.StatusOK, `[]`, http.Header{"Content-Type": {"application/json"}}
	limited := func(code int) {
		status, body = code, `{"message":"limited"}`
		if d.limitedBody != "" {
			body = d.limitedBody
		}
		for key, values := range d.headers {
			headers[key] = values
		}
	}
	switch {
	case request.URL.Path == "/orgs/acme/teams":
		body = `[{"slug":"platform","name":"Platform"}]`
	case request.URL.Path == "/orgs/acme/teams/platform/members":
		if d.rosterStatus != 0 {
			limited(d.rosterStatus)
		} else {
			body = d.rosterBody
		}
	case strings.HasPrefix(request.URL.Path, "/users/"):
		if d.userStatus != 0 {
			limited(d.userStatus)
		} else {
			body = `{"login":"octocat"}`
		}
	}
	return &http.Response{StatusCode: status, Header: headers, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

// A classified rate limit on a team roster page or a member email lookup ends the non-strict run with that error and
// no rows, so the caller can snooze; any other roster failure is skipped as before, and the strict run returns
// every error.
func TestGitHubTeamCatalogRateLimitedRosterEndsTheRun(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	retryAfter := http.Header{"Retry-After": {"120"}}
	for _, cell := range []struct {
		name        string
		doer        rateLimitedRosterDoer
		email       bool
		strict      bool
		wantLimited bool
		wantSkipped int
	}{
		{"roster 429, non-strict", rateLimitedRosterDoer{rosterStatus: 429, headers: retryAfter}, false, false, true, 0},
		{"roster 403 remaining 0, non-strict", rateLimitedRosterDoer{rosterStatus: 403, headers: http.Header{"X-Ratelimit-Remaining": {"0"}}}, false, false, true, 0},
		{"email lookup 429, non-strict", rateLimitedRosterDoer{rosterBody: `[{"login":"octocat"}]`, userStatus: 429, headers: retryAfter}, true, false, true, 0},
		{"control: roster 429, strict", rateLimitedRosterDoer{rosterStatus: 429, headers: retryAfter}, false, true, true, 0},
		{"control: roster 500, non-strict", rateLimitedRosterDoer{rosterStatus: 500}, false, false, false, 1},
		{"control: roster 403 permission, non-strict", rateLimitedRosterDoer{rosterStatus: 403, limitedBody: `{"message":"Resource not accessible by integration"}`}, false, false, false, 1},
	} {
		t.Run(cell.name, func(t *testing.T) {
			t.Parallel()
			collector := GitHubTeamCatalogRouteHandler{
				Client: githubTeamCatalogTestClient(t, cell.doer), OrgName: "acme",
				Now: func() time.Time { return now }, ResolveEmail: cell.email, Strict: cell.strict,
			}
			rows, evidence, err := collector.Collect(context.Background(), "org-1", true, true)
			var providerErr *providerfoundation.ProviderError
			limited := errors.As(err, &providerErr) && providerErr.Class == providerfoundation.ErrorRateLimited
			if limited != cell.wantLimited || evidence.SkippedTeamMemberships != cell.wantSkipped {
				t.Fatalf("err=%v rate_limited=%v skipped=%d, want rate_limited=%v skipped=%d", err, limited, evidence.SkippedTeamMemberships, cell.wantLimited, cell.wantSkipped)
			}
			if cell.wantLimited && (len(rows.Memberships) != 0 || len(rows.Teams) != 0 || len(rows.ObservedMembershipTeamIDs) != 0) {
				t.Fatalf("rows teams=%d memberships=%d observed=%v, want no rows from a run that ended in error", len(rows.Teams), len(rows.Memberships), rows.ObservedMembershipTeamIDs)
			}
			if !cell.wantLimited && err != nil {
				t.Fatalf("err=%v, want a skipped roster and a nil error", err)
			}
		})
	}
}
