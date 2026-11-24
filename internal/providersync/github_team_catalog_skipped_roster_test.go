package providersync

import (
	"context"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

type skippedRosterDoer struct{}

func (skippedRosterDoer) Do(request *http.Request) (*http.Response, error) {
	status, body := http.StatusOK, `[]`
	switch request.URL.Path {
	case "/orgs/acme/teams":
		body = `[{"slug":"platform","name":"Platform"},{"slug":"ops","name":"Operations"}]`
	case "/orgs/acme/teams/platform/members":
		status, body = http.StatusTooManyRequests, `{"message":"limited"}`
	case "/orgs/acme/teams/ops/members":
		body = `[{"login":"octocat"}]`
	}
	return &http.Response{
		StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}},
		Body: io.NopCloser(strings.NewReader(body)), Request: request,
	}, nil
}

// A team whose roster read is skipped (here an exhausted 429) closes no
// membership of that team: it is not in the observed set, so nothing of it is
// retired, and the other team is written.
func TestGitHubTeamCatalogSkippedRosterClosesNoMembershipOfThatTeam(t *testing.T) {
	t.Parallel()
	collector := GitHubTeamCatalogRouteHandler{
		Client: githubTeamCatalogTestClient(t, fakehttp.Client(skippedRosterDoer{})), OrgName: "acme",
		Now: func() time.Time { return time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC) },
	}
	rows, evidence, err := collector.Collect(context.Background(), "org-1", true, true)
	if err != nil || evidence.SkippedTeamMemberships != 1 {
		t.Fatalf("err=%v evidence=%+v, want the platform roster skipped and counted", err, evidence)
	}
	if slices.Contains(rows.ObservedMembershipTeamIDs, "gh:platform") || !slices.Contains(rows.ObservedMembershipTeamIDs, "gh:ops") {
		t.Fatalf("observed=%v, a skipped roster must not be observed", rows.ObservedMembershipTeamIDs)
	}
	for _, membership := range rows.Memberships {
		if membership.TeamID == "gh:platform" {
			t.Fatalf("membership of the skipped team was written: %+v", membership)
		}
	}
}
