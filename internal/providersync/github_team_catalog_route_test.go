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

// githubTeamCatalogFixtureDoer routes by exact path match. Each response is
// served once per path unless repeated; teams/repos/members lists here are
// deliberately single-page (Link header absent) since pagination itself is
// providerfoundation.CollectGitHubLinkPages's own tested contract.
type githubTeamCatalogFixtureDoer struct {
	t        *testing.T
	byPath   map[string]string
	statuses map[string]int
	requests []string
}

func (doer *githubTeamCatalogFixtureDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	path := request.URL.Path
	if request.URL.RawQuery != "" {
		path = path + "?" + request.URL.RawQuery
	}
	doer.requests = append(doer.requests, request.URL.Path)
	body, ok := doer.byPath[request.URL.Path]
	if !ok {
		doer.t.Fatalf("unexpected request path %q (query=%q)", request.URL.Path, request.URL.RawQuery)
	}
	status := doer.statuses[request.URL.Path]
	if status == 0 {
		status = http.StatusOK
	}
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    request,
	}, nil
}

func githubTeamCatalogTestClient(t *testing.T, doer providerfoundation.HTTPDoer) *providerfoundation.HTTPClient {
	t.Helper()
	client, err := providerfoundation.NewHTTPClient(
		"github", "https://api.github.com", doer,
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestGitHubTeamCatalogCollectWritesTeamsAndMemberships(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	doer := &githubTeamCatalogFixtureDoer{t: t, byPath: map[string]string{
		"/orgs/acme/teams":                  `[{"slug":"platform","name":"Platform","description":"Platform team"}]`,
		"/orgs/acme/teams/platform/repos":   `[{"name":"api"},{"name":"web"}]`,
		"/orgs/acme/teams/platform/members": `[{"login":"octocat"},{"login":"monalisa"}]`,
	}}
	collector := GitHubTeamCatalogRouteHandler{
		Client: githubTeamCatalogTestClient(t, doer), OrgName: "acme",
		Now: func() time.Time { return now }, ResolveEmail: false,
	}
	rows, evidence, err := collector.Collect(context.Background(), "org-1", true, true)
	if err != nil {
		t.Fatal(err)
	}
	if !evidence.Complete || evidence.TeamsObserved != 1 || evidence.MembersObserved != 2 ||
		evidence.SkippedTeamMemberships != 0 {
		t.Fatalf("evidence=%+v", evidence)
	}
	if len(rows.Teams) != 1 {
		t.Fatalf("teams=%+v", rows.Teams)
	}
	team := rows.Teams[0]
	if team.ID != "gh:platform" || team.OrgID != "org-1" || team.Provider != "github" ||
		team.Name != "Platform" || team.Description == nil || *team.Description != "Platform team" ||
		len(team.RepoPatterns) != 2 || team.RepoPatterns[0] != "acme/api" || team.RepoPatterns[1] != "acme/web" ||
		len(team.Members) != 2 || team.Members[0] != "github:octocat" || team.Members[1] != "github:monalisa" {
		t.Fatalf("team=%+v", team)
	}
	if len(rows.Memberships) != 2 {
		t.Fatalf("memberships=%+v", rows.Memberships)
	}
	for _, membership := range rows.Memberships {
		if membership.TeamID != "gh:platform" || membership.OrgID != "org-1" ||
			membership.Provider != "github" || membership.Source != "provider_access" {
			t.Fatalf("membership=%+v", membership)
		}
	}
	if rows.Memberships[0].MemberID != "gh:octocat" || rows.Memberships[1].MemberID != "gh:monalisa" {
		t.Fatalf("memberships=%+v", rows.Memberships)
	}
	if len(rows.RepoOwnership) != 2 {
		t.Fatalf("repo ownership=%+v", rows.RepoOwnership)
	}
	for _, ownership := range rows.RepoOwnership {
		if ownership.TeamID != "gh:platform" || ownership.OrgID != "org-1" || ownership.Provider != "github" ||
			ownership.Source != "provider_access" || ownership.MatchType != "exact" || ownership.RepoID != nil {
			t.Fatalf("ownership=%+v", ownership)
		}
	}
	if rows.RepoOwnership[0].RepoFullName != "acme/api" || rows.RepoOwnership[1].RepoFullName != "acme/web" {
		t.Fatalf("repo ownership=%+v", rows.RepoOwnership)
	}
}

func TestGitHubTeamCatalogCollectMembersOnlySkipsRepoFetch(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	doer := &githubTeamCatalogFixtureDoer{t: t, byPath: map[string]string{
		"/orgs/acme/teams":                  `[{"slug":"platform","name":"Platform"}]`,
		"/orgs/acme/teams/platform/members": `[{"login":"octocat"}]`,
	}}
	collector := GitHubTeamCatalogRouteHandler{
		Client: githubTeamCatalogTestClient(t, doer), OrgName: "acme",
		Now: func() time.Time { return now }, ResolveEmail: false,
	}
	rows, _, err := collector.Collect(context.Background(), "org-1", false, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows.Teams) != 0 {
		t.Fatalf("teams should stay empty when teams is not selected: %+v", rows.Teams)
	}
	if len(rows.Memberships) != 1 {
		t.Fatalf("memberships=%+v", rows.Memberships)
	}
	for _, path := range doer.requests {
		if path == "/orgs/acme/teams/platform/repos" {
			t.Fatalf("repos fetched even though teams was not selected: requests=%v", doer.requests)
		}
	}
}

func TestGitHubTeamCatalogCollectTeamsOnlySkipsMembersFetch(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	doer := &githubTeamCatalogFixtureDoer{t: t, byPath: map[string]string{
		"/orgs/acme/teams":                "[{\"slug\":\"platform\",\"name\":\"Platform\"}]",
		"/orgs/acme/teams/platform/repos": "[]",
	}}
	collector := GitHubTeamCatalogRouteHandler{
		Client: githubTeamCatalogTestClient(t, doer), OrgName: "acme",
		Now: func() time.Time { return now }, ResolveEmail: false,
	}
	rows, _, err := collector.Collect(context.Background(), "org-1", true, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows.Memberships) != 0 {
		t.Fatalf("memberships should stay empty when members is not selected: %+v", rows.Memberships)
	}
	if len(rows.Teams) != 1 || len(rows.Teams[0].Members) != 0 {
		t.Fatalf("teams=%+v", rows.Teams)
	}
	for _, path := range doer.requests {
		if path == "/orgs/acme/teams/platform/members" {
			t.Fatalf("members fetched even though members was not selected: requests=%v", doer.requests)
		}
	}
}

// TestGitHubTeamCatalogCollectSkipsTeamOnMemberFetchFailure mirrors
// team_autoimport_github.py's _membership_rows: a member-list fetch failure
// for one team is caught and skipped (continue), never a whole-org abort.
func TestGitHubTeamCatalogCollectSkipsTeamOnMemberFetchFailure(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	doer := &githubTeamCatalogFixtureDoer{
		t: t,
		byPath: map[string]string{
			"/orgs/acme/teams":                  `[{"slug":"platform","name":"Platform"},{"slug":"ops","name":"Operations"}]`,
			"/orgs/acme/teams/platform/repos":   `[]`,
			"/orgs/acme/teams/ops/repos":        `[]`,
			"/orgs/acme/teams/platform/members": `not json`,
			"/orgs/acme/teams/ops/members":      `[{"login":"octocat"}]`,
		},
	}
	collector := GitHubTeamCatalogRouteHandler{
		Client: githubTeamCatalogTestClient(t, doer), OrgName: "acme",
		Now: func() time.Time { return now }, ResolveEmail: false,
	}
	rows, evidence, err := collector.Collect(context.Background(), "org-1", true, true)
	if err != nil {
		t.Fatal(err)
	}
	if evidence.SkippedTeamMemberships != 1 {
		t.Fatalf("evidence=%+v", evidence)
	}
	if len(rows.Teams) != 2 {
		t.Fatalf("both teams must still be written even though one team's memberships failed: teams=%+v", rows.Teams)
	}
	if len(rows.Memberships) != 1 || rows.Memberships[0].TeamID != "gh:ops" {
		t.Fatalf("memberships=%+v", rows.Memberships)
	}
}

func TestGitHubTeamCatalogCollectFailsClosedOnInvalidInput(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	doer := &githubTeamCatalogFixtureDoer{t: t, byPath: map[string]string{
		"/orgs/acme/teams": `[]`,
	}}
	collector := GitHubTeamCatalogRouteHandler{
		Client: githubTeamCatalogTestClient(t, doer), OrgName: "acme",
		Now: func() time.Time { return now },
	}
	if _, _, err := collector.Collect(context.Background(), "", true, true); err != ErrInvalidConfiguration {
		t.Errorf("empty org id: err=%v", err)
	}
	if _, _, err := collector.Collect(context.Background(), "org-1", false, false); err != ErrInvalidConfiguration {
		t.Errorf("neither teams nor members selected: err=%v", err)
	}
	noOrgName := collector
	noOrgName.OrgName = ""
	if _, _, err := noOrgName.Collect(context.Background(), "org-1", true, true); err != ErrInvalidConfiguration {
		t.Errorf("empty org name: err=%v", err)
	}
}

func TestGitHubTeamCatalogCollectResolvesMemberEmail(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	doer := &githubTeamCatalogFixtureDoer{t: t, byPath: map[string]string{
		"/orgs/acme/teams":                  `[{"slug":"platform","name":"Platform"}]`,
		"/orgs/acme/teams/platform/repos":   `[]`,
		"/orgs/acme/teams/platform/members": `[{"login":"octocat"},{"login":"octocat"}]`,
		"/users/octocat":                    `{"login":"octocat","email":"octocat@example.com"}`,
	}}
	collector := GitHubTeamCatalogRouteHandler{
		Client: githubTeamCatalogTestClient(t, doer), OrgName: "acme",
		Now: func() time.Time { return now }, ResolveEmail: true,
	}
	rows, _, err := collector.Collect(context.Background(), "org-1", true, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows.Memberships) != 1 {
		// The two "octocat" logins on the members page dedupe to one row.
		t.Fatalf("memberships=%+v", rows.Memberships)
	}
	membership := rows.Memberships[0]
	if membership.RawEmail == nil || *membership.RawEmail != "octocat@example.com" ||
		len(membership.IdentityFacets) != 2 || membership.IdentityFacets[1] != "octocat@example.com" {
		t.Fatalf("membership=%+v", membership)
	}
	usersRequests := 0
	for _, path := range doer.requests {
		if path == "/users/octocat" {
			usersRequests++
		}
	}
	if usersRequests != 1 {
		t.Fatalf("email lookup should be cached per login within one collection pass: requests=%v", doer.requests)
	}
}
