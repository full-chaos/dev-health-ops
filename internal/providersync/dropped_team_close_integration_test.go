//go:build integration

package providersync

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// A team that the provider stops listing keeps its open ownership and
// membership rows until a run proves the team gone (CHAOS-9102). The catalog
// writers of GitHub, GitLab and Linear do not deactivate such a team, so no
// reader's inactive-team rule hides it. The provider matrix:
//
//	github  ownership + memberships   TestGitHubTeamThatIsNoLongerListed...
//	gitlab  ownership + memberships   TestGitLabTeamThatIsNoLongerListed...
//	linear  memberships (+ ownership) TestLinearTeamThatIsNoLongerListed...
//	jira    Atlassian Teams: deactivates the team and closes its rows
//	        (internal/atlassianteams write_integration_test.go: a team deleted upstream)

var droppedAt = []time.Time{
	time.Date(2026, 10, 10, 8, 0, 0, 0, time.UTC),
	time.Date(2026, 10, 10, 9, 0, 0, 0, time.UTC),
	time.Date(2026, 10, 10, 10, 0, 0, 0, time.UTC),
	time.Date(2026, 10, 10, 11, 0, 0, 0, time.UTC),
	time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC),
}

// teamLookupAnswer is what the provider answers to the direct read of one team.
type teamLookupAnswer struct {
	status int
	body   string
}

const (
	githubNoTeam = `{"message":"Not Found","documentation_url":"https://docs.github.com/rest/teams/teams#get-a-team-by-name","status":"404"}`
	gitlabNoTeam = `{"message":"404 Group Not Found"}`
)

// ---- GitHub -------------------------------------------------------------------

// droppedGitHubOrg is the GitHub organization "acme" the test edits between runs.
type droppedGitHubOrg struct {
	mu       sync.Mutex
	listed   []string            // slugs the team listing returns
	twoPages bool                // the listing takes two responses
	repos    map[string][]string // slug -> repository names
	members  map[string][]string // slug -> logins
	// teamLookup is what GET /orgs/acme/teams/{slug} answers; a slug with no
	// entry gets GitHub's 404.
	teamLookup  map[string]teamLookupAnswer
	teamReads   int
	memberReads int
	grantReads  int
	emptyList   bool
}

func newDroppedGitHubOrg() *droppedGitHubOrg {
	return &droppedGitHubOrg{
		listed:     []string{"platform", "ops"},
		repos:      map[string][]string{"platform": {"api", "web"}, "ops": {"infra"}},
		members:    map[string][]string{"platform": {"mona"}, "ops": {"hubot"}},
		teamLookup: map[string]teamLookupAnswer{},
	}
}

func (org *droppedGitHubOrg) Do(request *http.Request) (*http.Response, error) {
	org.mu.Lock()
	defer org.mu.Unlock()
	parts := strings.Split(strings.Trim(request.URL.Path, "/"), "/") // orgs acme teams {slug} ...
	if len(parts) < 3 || parts[0] != "orgs" || parts[1] != "acme" || parts[2] != "teams" {
		return jsonResponse(request, 404, githubNoTeam, nil), nil
	}
	nodes := func(items []string, key string) string {
		out := make([]string, 0, len(items))
		for _, item := range items {
			out = append(out, `{"`+key+`":"`+item+`","name":"`+item+`"}`)
		}
		return `[` + strings.Join(out, ",") + `]`
	}
	switch {
	case len(parts) == 3: // the team listing
		if org.emptyList {
			return jsonResponse(request, 200, `[]`, nil), nil
		}
		if org.twoPages {
			if request.URL.Query().Get("page") == "2" {
				return jsonResponse(request, 200, `[]`, nil), nil
			}
			header := http.Header{}
			header.Set("Link", `<https://api.github.com/orgs/acme/teams?per_page=100&page=2>; rel="next"`)
			return jsonResponse(request, 200, nodes(org.listed, "slug"), header), nil
		}
		return jsonResponse(request, 200, nodes(org.listed, "slug"), nil), nil
	case len(parts) == 4: // one team
		org.teamReads++
		answer, ok := org.teamLookup[parts[3]]
		if !ok {
			return jsonResponse(request, 404, githubNoTeam, nil), nil
		}
		return jsonResponse(request, answer.status, answer.body, nil), nil
	case len(parts) == 5 && parts[4] == "repos":
		return jsonResponse(request, 200, nodes(org.repos[parts[3]], "name"), nil), nil
	case len(parts) == 5 && parts[4] == "members":
		return jsonResponse(request, 200, nodes(org.members[parts[3]], "login"), nil), nil
	case len(parts) == 6 && parts[4] == "memberships":
		org.memberReads++
		return jsonResponse(request, 404, githubNoTeam, nil), nil
	case len(parts) == 7 && parts[4] == "repos":
		org.grantReads++
		return jsonResponse(request, 404, githubNoTeam, nil), nil
	}
	return jsonResponse(request, 404, githubNoTeam, nil), nil
}

func (org *droppedGitHubOrg) lookups() (team, member, grant int) {
	org.mu.Lock()
	defer org.mu.Unlock()
	return org.teamReads, org.memberReads, org.grantReads
}

func (org *droppedGitHubOrg) edit(change func(*droppedGitHubOrg)) {
	org.mu.Lock()
	defer org.mu.Unlock()
	change(org)
}

func githubDroppedRun(
	ctx context.Context, t *testing.T, conn driver.Conn, orgID string, org *droppedGitHubOrg, selections TeamCatalogSelections,
	at time.Time, census OwnershipScopeCensus,
) TeamCatalogResult {
	t.Helper()
	adapter := GitHubTeamCatalogCollector{Sink: GitHubTeamCatalogClickHouseEffects{Conn: conn}, ScopeCensus: census}
	result, err := adapter.CollectTeamCatalog(ctx, TeamCatalogReference{OrgID: orgID, SyncRunID: "run", IntegrationID: "integration-a"},
		providerfoundation.Credential{Provider: "github", Config: map[string]string{"org": "acme"}},
		githubTeamCatalogAdapterClient(t, fakehttp.Client(org)), selections, at)
	if err != nil {
		t.Fatalf("sync at %s: %v", at, err)
	}
	return result
}

// closedRepoOwnership is the closed rows of a provider after FINAL, per fact
// "<team>|<repo>": the valid_to of each closed row.
func closedRepoOwnership(ctx context.Context, t *testing.T, conn driver.Conn, orgID, provider string) map[string]time.Time {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT team_id, repo_full_name, valid_to FROM team_repo_ownership FINAL WHERE org_id = ? AND provider = ? AND valid_to IS NOT NULL`, orgID, provider)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	closed := map[string]time.Time{}
	for rows.Next() {
		var team, repo string
		var to *time.Time
		if err := rows.Scan(&team, &repo, &to); err != nil {
			t.Fatal(err)
		}
		closed[team+"|"+repo] = to.UTC()
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return closed
}

func seedRepoOwnership(ctx context.Context, t *testing.T, conn driver.Conn, orgID, provider, team, repo, source string, from time.Time) {
	t.Helper()
	if err := conn.Exec(ctx, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, updated_at)
VALUES (?, ?, ?, ?, 'exact', ?, 0, 100, 10, ?, ?)`, orgID, provider, team, repo, source, from, from); err != nil {
		t.Fatal(err)
	}
}

// requireOpenFacts holds the open memberships to exactly the facts named.
func requireOpenFacts(t *testing.T, label string, got map[string][]time.Time, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s: open memberships = %v, want %v", label, got, want)
		return
	}
	for _, fact := range want {
		if len(got[fact]) != 1 {
			t.Errorf("%s: open memberships = %v, want %v", label, got, want)
		}
	}
}

func legReasons(result TeamCatalogResult) string {
	var reasons []string
	for _, leg := range result.DegradedLegs {
		reasons = append(reasons, leg.Dataset+"/"+leg.Reason)
	}
	return strings.Join(reasons, ",")
}

func TestGitHubTeamThatIsNoLongerListedKeepsItsRowsUntilTheRunProvesItGone(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	both := TeamCatalogSelections{Teams: true, Members: true}
	const (
		opsInfra = "gh:ops|acme/infra"
		mona     = "gh:platform|gh:mona"
		hubot    = "gh:ops|gh:hubot"
	)
	seed := func(t *testing.T, name string, org *droppedGitHubOrg) string {
		t.Helper()
		orgID := "org-9102-github-" + name
		githubDroppedRun(ctx, t, conn, orgID, org, both, droppedAt[0], staticScopeCensus{})
		requireRepoFacts(t, name+": seeded", openRepoOwnership(ctx, t, conn, orgID, "github"),
			repoOwnershipFact{"gh:platform", "acme/api", "provider_access"}, repoOwnershipFact{"gh:platform", "acme/web", "provider_access"},
			repoOwnershipFact{"gh:ops", "acme/infra", "provider_access"})
		requireOpenFacts(t, name+": seeded", openMembershipFacts(ctx, t, conn, orgID, "github"), mona, hubot)
		return orgID
	}
	platformOnly := func(org *droppedGitHubOrg) { org.listed = []string{"platform"} }
	opsStays := []repoOwnershipFact{{"gh:ops", "acme/infra", "provider_access"}}
	platformRows := []repoOwnershipFact{{"gh:platform", "acme/api", "provider_access"}, {"gh:platform", "acme/web", "provider_access"}}

	t.Run("a listing of one response proves the absence: both datasets close and nothing is asked", func(t *testing.T) {
		org := newDroppedGitHubOrg()
		orgID := seed(t, "one-response", org)
		logs := captureMemberLogs(t)
		org.edit(platformOnly)
		result := githubDroppedRun(ctx, t, conn, orgID, org, both, droppedAt[1], staticScopeCensus{})
		requireRepoFacts(t, "after the team left", openRepoOwnership(ctx, t, conn, orgID, "github"), platformRows...)
		requireOpenFacts(t, "after the team left", openMembershipFacts(ctx, t, conn, orgID, "github"), mona)
		closed := closedRepoOwnership(ctx, t, conn, orgID, "github")
		if got := closed[opsInfra]; !got.Equal(droppedAt[1]) {
			t.Errorf("the grant of the dropped team closed at %v, want the run time %v", got, droppedAt[1])
		}
		requireClosed(t, "after the team left", closedMembershipFacts(ctx, t, conn, orgID, "github"), hubot,
			droppedAt[0].Format(time.RFC3339)+"->"+droppedAt[1].Format(time.RFC3339))
		if team, member, grant := org.lookups(); team+member+grant != 0 {
			t.Errorf("the provider was asked %d/%d/%d times (team/member/grant), want 0: a listing of one response is the proof", team, member, grant)
		}
		if result.MembershipsClosed != 1 || len(result.DegradedLegs) != 0 {
			t.Errorf("MembershipsClosed = %d, legs = %q, want 1 and no leg", result.MembershipsClosed, legReasons(result))
		}
		lines := logs.lines("team_catalog_dropped_teams")
		if len(lines) != 2 {
			t.Fatalf("want one dropped-team line for each close, got %d", len(lines))
		}
		for _, line := range lines {
			if line["teams_dropped"] != float64(1) || line["teams_gone"] != float64(1) {
				t.Errorf("dropped-team line = %v, want 1 dropped and 1 gone", line)
			}
		}
		if strings.Contains(logs.text(), "ops") && strings.Contains(logs.text(), "gh:ops") {
			t.Errorf("a log line names the team: counts, no ids")
		}
	})

	t.Run("a listing of two responses closes on the provider's own answer: gone", func(t *testing.T) {
		org := newDroppedGitHubOrg()
		orgID := seed(t, "two-pages-gone", org)
		org.edit(func(o *droppedGitHubOrg) { platformOnly(o); o.twoPages = true })
		result := githubDroppedRun(ctx, t, conn, orgID, org, both, droppedAt[1], staticScopeCensus{})
		requireRepoFacts(t, "after the lookup", openRepoOwnership(ctx, t, conn, orgID, "github"), platformRows...)
		requireOpenFacts(t, "after the lookup", openMembershipFacts(ctx, t, conn, orgID, "github"), mona)
		team, member, grant := org.lookups()
		if team != 1 || member != 0 || grant != 0 {
			t.Errorf("the provider was asked %d/%d/%d times (team/member/grant), want 1/0/0: one answer for the team serves both closes and its members need none", team, member, grant)
		}
		if legReasons(result) != "" {
			t.Errorf("legs = %q, want none", legReasons(result))
		}
	})

	t.Run("a listing that lost a team that still exists closes nothing and says so", func(t *testing.T) {
		org := newDroppedGitHubOrg()
		orgID := seed(t, "two-pages-still-there", org)
		org.edit(func(o *droppedGitHubOrg) {
			platformOnly(o)
			o.twoPages = true
			o.teamLookup["ops"] = teamLookupAnswer{200, `{"slug":"ops","name":"ops"}`}
		})
		logs := captureMemberLogs(t)
		result := githubDroppedRun(ctx, t, conn, orgID, org, both, droppedAt[1], staticScopeCensus{})
		requireRepoFacts(t, "team still there", openRepoOwnership(ctx, t, conn, orgID, "github"), append(platformRows, opsStays...)...)
		requireOpenFacts(t, "team still there", openMembershipFacts(ctx, t, conn, orgID, "github"), mona, hubot)
		if got := legReasons(result); !strings.Contains(got, "team_ownership/team_absent_from_listing_still_there") ||
			!strings.Contains(got, "team_memberships/team_absent_from_listing_still_there") {
			t.Errorf("legs = %q, want the still-there reason on both datasets", got)
		}
		if team, _, _ := org.lookups(); team != 1 {
			t.Errorf("the team was asked %d times, want once for the run", team)
		}
		for _, line := range logs.lines("team_catalog_dropped_teams") {
			if line["teams_still_there"] != float64(1) || line["teams_gone"] != float64(0) {
				t.Errorf("dropped-team line = %v, want 1 still there and 0 gone", line)
			}
		}
		// The listing sees the team again: its rows stay one open row each, on their first date.
		org.edit(func(o *droppedGitHubOrg) { o.listed = []string{"platform", "ops"}; o.twoPages = false })
		githubDroppedRun(ctx, t, conn, orgID, org, both, droppedAt[2], staticScopeCensus{})
		requireOpenFacts(t, "listed again", openMembershipFacts(ctx, t, conn, orgID, "github"), mona, hubot)
		if from := openMembershipFacts(ctx, t, conn, orgID, "github")[hubot]; len(from) == 1 && !from[0].Equal(droppedAt[0]) {
			t.Errorf("the member of a team that was lost by one listing has valid_from %v, want the first date %v", from[0], droppedAt[0])
		}
	})

	t.Run("an answer that is not the provider's own no closes nothing", func(t *testing.T) {
		for name, answer := range map[string]teamLookupAnswer{
			"a 500":                  {500, `{"message":"Server Error"}`},
			"a 403":                  {403, `{"message":"Forbidden"}`},
			"a 200 of another slug":  {200, `{"slug":"operations"}`},
			"a 200 that is no team":  {200, `[]`},
			"a 404 of a gateway":     {404, `<html>404 Not Found</html>`},
			"a 404 of another thing": {404, `{"message":"Resource not accessible by integration"}`},
		} {
			t.Run(name, func(t *testing.T) {
				org := newDroppedGitHubOrg()
				orgID := seed(t, "not-an-answer-"+strings.ReplaceAll(name, " ", "-"), org)
				org.edit(func(o *droppedGitHubOrg) {
					platformOnly(o)
					o.twoPages = true
					o.teamLookup["ops"] = answer
				})
				result := githubDroppedRun(ctx, t, conn, orgID, org, both, droppedAt[1], staticScopeCensus{})
				requireRepoFacts(t, name, openRepoOwnership(ctx, t, conn, orgID, "github"), append(platformRows, opsStays...)...)
				requireOpenFacts(t, name, openMembershipFacts(ctx, t, conn, orgID, "github"), mona, hubot)
				if got := legReasons(result); !strings.Contains(got, "team_absence_not_proven") {
					t.Errorf("legs = %q, want the not-proven reason", got)
				}
			})
		}
	})

	t.Run("another active integration keeps every dropped team open and asks nothing", func(t *testing.T) {
		org := newDroppedGitHubOrg()
		orgID := seed(t, "shared-scope", org)
		org.edit(func(o *droppedGitHubOrg) { platformOnly(o); o.twoPages = true })
		result := githubDroppedRun(ctx, t, conn, orgID, org, both, droppedAt[1], staticScopeCensus{siblings: 1})
		requireRepoFacts(t, "shared scope", openRepoOwnership(ctx, t, conn, orgID, "github"), append(platformRows, opsStays...)...)
		requireOpenFacts(t, "shared scope", openMembershipFacts(ctx, t, conn, orgID, "github"), mona, hubot)
		if team, _, _ := org.lookups(); team != 0 {
			t.Errorf("the provider was asked for a team %d times, want 0: nothing can close in a shared scope", team)
		}
		if got := legReasons(result); !strings.Contains(got, "scope_shared") {
			t.Errorf("legs = %q, want the shared-scope reason", got)
		}
	})

	t.Run("a listing that returned no team closes nothing", func(t *testing.T) {
		org := newDroppedGitHubOrg()
		orgID := seed(t, "empty-listing", org)
		org.edit(func(o *droppedGitHubOrg) { o.emptyList = true })
		result := githubDroppedRun(ctx, t, conn, orgID, org, both, droppedAt[1], staticScopeCensus{})
		requireRepoFacts(t, "empty listing", openRepoOwnership(ctx, t, conn, orgID, "github"),
			append(append([]repoOwnershipFact{}, platformRows...), opsStays...)...)
		requireOpenFacts(t, "empty listing", openMembershipFacts(ctx, t, conn, orgID, "github"), mona, hubot)
		if got := legReasons(result); !strings.Contains(got, "no_team_listed") {
			t.Errorf("legs = %q, want the no-team-listed reason", got)
		}
		if team, _, _ := org.lookups(); team != 0 {
			t.Errorf("the provider was asked for a team %d times, want 0", team)
		}
	})

	t.Run("a run that selects one dataset closes that dataset only", func(t *testing.T) {
		org := newDroppedGitHubOrg()
		orgID := seed(t, "one-dataset", org)
		org.edit(platformOnly)
		githubDroppedRun(ctx, t, conn, orgID, org, TeamCatalogSelections{Teams: true}, droppedAt[1], staticScopeCensus{})
		requireRepoFacts(t, "teams only", openRepoOwnership(ctx, t, conn, orgID, "github"), platformRows...)
		requireOpenFacts(t, "teams only", openMembershipFacts(ctx, t, conn, orgID, "github"), mona, hubot)
		githubDroppedRun(ctx, t, conn, orgID, org, TeamCatalogSelections{Members: true}, droppedAt[2], staticScopeCensus{})
		requireOpenFacts(t, "then members only", openMembershipFacts(ctx, t, conn, orgID, "github"), mona)
	})

	t.Run("rows of another organization, provider or source are never closed", func(t *testing.T) {
		org := newDroppedGitHubOrg()
		orgID := seed(t, "other-rows", org)
		seedRepoOwnership(ctx, t, conn, orgID, "github", "gh:ops", "elsewhere/infra", "provider_access", droppedAt[0])
		seedRepoOwnership(ctx, t, conn, orgID, "github", "gh:ops", "acme/infra", "manual", droppedAt[0])
		seedRepoOwnership(ctx, t, conn, orgID, "gitlab", "gh:ops", "acme/infra", "provider_access", droppedAt[0])
		seedMembership(ctx, t, conn, orgID, "github", "gh:ops", "gh:manual", "manual", droppedAt[0], nil, droppedAt[0])
		seedMembership(ctx, t, conn, orgID, "gitlab", "gh:ops", "gh:other", "provider_access", droppedAt[0], nil, droppedAt[0])
		seedMembership(ctx, t, conn, orgID, "github", "custom:ops", "gh:custom", "provider_access", droppedAt[0], nil, droppedAt[0])
		org.edit(platformOnly)
		githubDroppedRun(ctx, t, conn, orgID, org, both, droppedAt[1], staticScopeCensus{})
		requireRepoFacts(t, "github rows", openRepoOwnership(ctx, t, conn, orgID, "github"),
			append(append([]repoOwnershipFact{}, platformRows...),
				repoOwnershipFact{"gh:ops", "elsewhere/infra", "provider_access"}, repoOwnershipFact{"gh:ops", "acme/infra", "manual"})...)
		requireRepoFacts(t, "gitlab rows", openRepoOwnership(ctx, t, conn, orgID, "gitlab"), repoOwnershipFact{"gh:ops", "acme/infra", "provider_access"})
		facts := openMembershipFacts(ctx, t, conn, orgID, "github")
		requireOpenFacts(t, "github memberships", facts, mona, "gh:ops|gh:manual", "custom:ops|gh:custom")
		requireOpenFacts(t, "gitlab memberships", openMembershipFacts(ctx, t, conn, orgID, "gitlab"), "gh:ops|gh:other")
	})

	t.Run("a team that comes back is a new fact", func(t *testing.T) {
		org := newDroppedGitHubOrg()
		orgID := seed(t, "comes-back", org)
		org.edit(platformOnly)
		githubDroppedRun(ctx, t, conn, orgID, org, both, droppedAt[1], staticScopeCensus{})
		org.edit(func(o *droppedGitHubOrg) { o.listed = []string{"platform", "ops"} })
		githubDroppedRun(ctx, t, conn, orgID, org, both, droppedAt[2], staticScopeCensus{})
		requireRepoFacts(t, "back", openRepoOwnership(ctx, t, conn, orgID, "github"), append(platformRows, opsStays...)...)
		if from := openMembershipFacts(ctx, t, conn, orgID, "github")[hubot]; len(from) != 1 || !from[0].Equal(droppedAt[2]) {
			t.Errorf("the member of the team that came back has valid_from %v, want the new first date %v", from, droppedAt[2])
		}
	})
}

// ---- GitLab -------------------------------------------------------------------

// droppedGitLabGroups is the GitLab group "org" and its subgroups.
type droppedGitLabGroups struct {
	*httptest.Server
	mu         sync.Mutex
	subgroups  []string
	twoPages   bool
	emptyList  bool
	projects   map[string][]string
	members    map[string][]string
	teamLookup map[string]teamLookupAnswer // full path -> answer; none: GitLab's 404
	reads      struct{ group, member int }
}

func newDroppedGitLabGroups(t *testing.T) *droppedGitLabGroups {
	t.Helper()
	fake := &droppedGitLabGroups{
		subgroups:  []string{"org/team-a", "org/team-b"},
		projects:   map[string][]string{"org/team-a": {"org/team-a/api"}, "org/team-b": {"org/team-b/infra"}},
		members:    map[string][]string{"org/team-a": {"alice"}, "org/team-b": {"bob"}},
		teamLookup: map[string]teamLookupAnswer{},
	}
	group := func(id int, fullPath string) map[string]any {
		return map[string]any{"id": id, "full_path": fullPath, "name": fullPath, "description": nil}
	}
	project := func(path string) map[string]any {
		return map[string]any{"id": len(path) + 1000, "path_with_namespace": path, "name": path, "archived": false, "web_url": "https://gitlab.example.com/" + path}
	}
	fake.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		path := r.URL.EscapedPath()
		w.Header()["X-Next-Page"] = []string{""}
		switch {
		case path == "/api/v4/groups/org":
			writeGitLabTeamCatalogJSON(t, w, group(1, "org"))
		case path == "/api/v4/groups/org/subgroups":
			out := []map[string]any{}
			if fake.emptyList {
				writeGitLabTeamCatalogJSON(t, w, out)
				return
			}
			if fake.twoPages && r.URL.Query().Get("page") == "2" {
				writeGitLabTeamCatalogJSON(t, w, out)
				return
			}
			for index, sub := range fake.subgroups {
				out = append(out, group(10+index, sub))
			}
			if fake.twoPages {
				w.Header()["X-Next-Page"] = []string{"2"}
			}
			writeGitLabTeamCatalogJSON(t, w, out)
		case strings.HasSuffix(path, "/projects"):
			groupPath := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(path, "/api/v4/groups/"), "/projects"), "%2F", "/")
			out := []map[string]any{}
			if r.URL.Query().Get("include_subgroups") == "true" {
				for _, list := range fake.projects {
					for _, p := range list {
						out = append(out, project(p))
					}
				}
			} else {
				for _, p := range fake.projects[groupPath] {
					out = append(out, project(p))
				}
			}
			writeGitLabTeamCatalogJSON(t, w, out)
		case strings.HasSuffix(path, "/members"):
			groupPath := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(path, "/api/v4/groups/"), "/members"), "%2F", "/")
			if r.URL.Query().Get("query") != "" {
				fake.reads.member++
			}
			out := []map[string]any{}
			for _, username := range fake.members[groupPath] {
				out = append(out, map[string]any{"username": username, "name": username, "email": username + "@example.com"})
			}
			writeGitLabTeamCatalogJSON(t, w, out)
		case strings.HasPrefix(path, "/api/v4/groups/"):
			fake.reads.group++
			groupPath := strings.ReplaceAll(strings.TrimPrefix(path, "/api/v4/groups/"), "%2F", "/")
			answer, ok := fake.teamLookup[groupPath]
			if !ok {
				w.WriteHeader(404)
				_, _ = w.Write([]byte(gitlabNoTeam))
				return
			}
			w.WriteHeader(answer.status)
			_, _ = w.Write([]byte(answer.body))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(fake.Close)
	return fake
}

func (fake *droppedGitLabGroups) edit(change func(*droppedGitLabGroups)) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	change(fake)
}

func (fake *droppedGitLabGroups) lookups() (group, member int) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return fake.reads.group, fake.reads.member
}

func gitlabDroppedRun(
	ctx context.Context, t *testing.T, conn driver.Conn, orgID string, fake *droppedGitLabGroups, selections TeamCatalogSelections,
	at time.Time, census OwnershipScopeCensus,
) TeamCatalogResult {
	t.Helper()
	collector := GitLabTeamCatalogCollector{Sink: GitLabTeamCatalogClickHouseEffects{
		Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	}, ScopeCensus: census}
	result, err := collector.CollectTeamCatalog(ctx, TeamCatalogReference{OrgID: orgID, SyncRunID: "run", IntegrationID: "integration-a"},
		providerfoundation.Credential{Provider: "gitlab", Config: map[string]string{"group_path": "org"}},
		gitlabTeamCatalogTestClient(t, fake.URL), selections, at)
	if err != nil {
		t.Fatalf("sync at %s: %v", at, err)
	}
	return result
}

func seedProjectOwnership(ctx context.Context, t *testing.T, conn driver.Conn, orgID, provider, team, project, source string, from time.Time) {
	t.Helper()
	if err := conn.Exec(ctx, `INSERT INTO team_project_ownership (org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, updated_at)
VALUES (?, ?, ?, ?, ?, ?, 0, 100, 10, ?, ?)`, orgID, provider, team, project, project, source, from, from); err != nil {
		t.Fatal(err)
	}
}

func TestGitLabTeamThatIsNoLongerListedKeepsItsRowsUntilTheRunProvesItGone(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	all := TeamCatalogSelections{Teams: true, Projects: true, Members: true}
	const alice, bob = "gl:org/team-a|gl:alice", "gl:org/team-b|gl:bob"
	teamA := gitlabOwnershipFact{"gl:org/team-a", "org/team-a/api", "provider_access"}
	teamB := gitlabOwnershipFact{"gl:org/team-b", "org/team-b/infra", "provider_access"}
	seed := func(t *testing.T, name string, fake *droppedGitLabGroups) string {
		t.Helper()
		orgID := "org-9102-gitlab-" + name
		gitlabDroppedRun(ctx, t, conn, orgID, fake, all, droppedAt[0], staticScopeCensus{})
		requireGitLabFacts(t, name+": seeded", openGitLabOwnership(ctx, t, conn, orgID, "gitlab"), teamA, teamB)
		requireOpenFacts(t, name+": seeded", openMembershipFacts(ctx, t, conn, orgID, "gitlab"), alice, bob)
		return orgID
	}
	withoutB := func(fake *droppedGitLabGroups) { fake.subgroups = []string{"org/team-a"} }

	t.Run("a listing of one response proves the absence: both datasets close and nothing is asked", func(t *testing.T) {
		fake := newDroppedGitLabGroups(t)
		orgID := seed(t, "one-response", fake)
		logs := captureMemberLogs(t)
		fake.edit(withoutB)
		gitlabDroppedRun(ctx, t, conn, orgID, fake, all, droppedAt[1], staticScopeCensus{})
		requireGitLabFacts(t, "after the group left", openGitLabOwnership(ctx, t, conn, orgID, "gitlab"), teamA)
		requireOpenFacts(t, "after the group left", openMembershipFacts(ctx, t, conn, orgID, "gitlab"), alice)
		requireClosed(t, "after the group left", closedMembershipFacts(ctx, t, conn, orgID, "gitlab"), bob,
			droppedAt[0].Format(time.RFC3339)+"->"+droppedAt[1].Format(time.RFC3339))
		if group, member := fake.lookups(); group+member != 0 {
			t.Errorf("the provider was asked %d/%d times (group/member), want 0", group, member)
		}
		for _, line := range logs.lines("team_catalog_dropped_teams", "ownership_close_skipped", "gitlab_team_catalog_ownership_snapshot") {
			if strings.Contains(fmt.Sprint(line), "team-b") {
				t.Errorf("a log line of the dropped-team close names the group: %v (counts, no ids)", line)
			}
		}
	})

	t.Run("a listing of two responses closes on the provider's own answer: GitLab's 404 of a group", func(t *testing.T) {
		fake := newDroppedGitLabGroups(t)
		orgID := seed(t, "two-pages-gone", fake)
		fake.edit(func(f *droppedGitLabGroups) { withoutB(f); f.twoPages = true })
		result := gitlabDroppedRun(ctx, t, conn, orgID, fake, all, droppedAt[1], staticScopeCensus{})
		requireGitLabFacts(t, "after the lookup", openGitLabOwnership(ctx, t, conn, orgID, "gitlab"), teamA)
		requireOpenFacts(t, "after the lookup", openMembershipFacts(ctx, t, conn, orgID, "gitlab"), alice)
		if group, member := fake.lookups(); group != 1 || member != 0 {
			t.Errorf("the provider was asked %d/%d times (group/member), want 1/0", group, member)
		}
		if legReasons(result) != "" {
			t.Errorf("legs = %q, want none", legReasons(result))
		}
	})

	t.Run("a listing that lost a group that still exists closes nothing and says so", func(t *testing.T) {
		fake := newDroppedGitLabGroups(t)
		orgID := seed(t, "two-pages-still-there", fake)
		fake.edit(func(f *droppedGitLabGroups) {
			withoutB(f)
			f.twoPages = true
			f.teamLookup["org/team-b"] = teamLookupAnswer{200, `{"id":11,"full_path":"org/team-b","name":"Team B"}`}
		})
		result := gitlabDroppedRun(ctx, t, conn, orgID, fake, all, droppedAt[1], staticScopeCensus{})
		requireGitLabFacts(t, "group still there", openGitLabOwnership(ctx, t, conn, orgID, "gitlab"), teamA, teamB)
		requireOpenFacts(t, "group still there", openMembershipFacts(ctx, t, conn, orgID, "gitlab"), alice, bob)
		if got := legReasons(result); !strings.Contains(got, "team_ownership/team_absent_from_listing_still_there") ||
			!strings.Contains(got, "team_memberships/team_absent_from_listing_still_there") {
			t.Errorf("legs = %q, want the still-there reason on both datasets", got)
		}
	})

	t.Run("an answer that is not the provider's own no closes nothing", func(t *testing.T) {
		for name, answer := range map[string]teamLookupAnswer{
			"a 500":                       {500, `{"message":"500 Internal Server Error"}`},
			"a 403":                       {403, `{"message":"403 Forbidden"}`},
			"a 404 of a gateway":          {404, `<html>404</html>`},
			"a 404 of a project":          {404, `{"message":"404 Project Not Found"}`},
			"a 200 of another group path": {200, `{"full_path":"org/team-c"}`},
		} {
			t.Run(name, func(t *testing.T) {
				fake := newDroppedGitLabGroups(t)
				orgID := seed(t, "not-an-answer-"+strings.ReplaceAll(name, " ", "-"), fake)
				fake.edit(func(f *droppedGitLabGroups) { withoutB(f); f.twoPages = true; f.teamLookup["org/team-b"] = answer })
				result := gitlabDroppedRun(ctx, t, conn, orgID, fake, all, droppedAt[1], staticScopeCensus{})
				requireGitLabFacts(t, name, openGitLabOwnership(ctx, t, conn, orgID, "gitlab"), teamA, teamB)
				requireOpenFacts(t, name, openMembershipFacts(ctx, t, conn, orgID, "gitlab"), alice, bob)
				if got := legReasons(result); !strings.Contains(got, "team_absence_not_proven") {
					t.Errorf("legs = %q, want the not-proven reason", got)
				}
			})
		}
	})

	t.Run("another active integration keeps every dropped group open and asks nothing", func(t *testing.T) {
		fake := newDroppedGitLabGroups(t)
		orgID := seed(t, "shared-scope", fake)
		fake.edit(func(f *droppedGitLabGroups) { withoutB(f); f.twoPages = true })
		gitlabDroppedRun(ctx, t, conn, orgID, fake, all, droppedAt[1], staticScopeCensus{siblings: 1})
		requireGitLabFacts(t, "shared scope", openGitLabOwnership(ctx, t, conn, orgID, "gitlab"), teamA, teamB)
		requireOpenFacts(t, "shared scope", openMembershipFacts(ctx, t, conn, orgID, "gitlab"), alice, bob)
		if group, _ := fake.lookups(); group != 0 {
			t.Errorf("the provider was asked for a group %d times, want 0", group)
		}
	})

	t.Run("a subgroup listing with no subgroup still lists the root group: the groups that are gone close", func(t *testing.T) {
		fake := newDroppedGitLabGroups(t)
		orgID := seed(t, "no-subgroup", fake)
		fake.edit(func(f *droppedGitLabGroups) { f.emptyList = true })
		gitlabDroppedRun(ctx, t, conn, orgID, fake, all, droppedAt[1], staticScopeCensus{})
		requireGitLabFacts(t, "no subgroup listed", openGitLabOwnership(ctx, t, conn, orgID, "gitlab"))
		requireOpenFacts(t, "no subgroup listed", openMembershipFacts(ctx, t, conn, orgID, "gitlab"))
	})

	t.Run("rows of another root group, provider or source are never closed", func(t *testing.T) {
		fake := newDroppedGitLabGroups(t)
		orgID := seed(t, "other-rows", fake)
		seedProjectOwnership(ctx, t, conn, orgID, "gitlab", "gl:other/team-b", "other/team-b/x", "provider_access", droppedAt[0])
		seedProjectOwnership(ctx, t, conn, orgID, "gitlab", "gl:org/team-b", "org/team-b/manual", "manual", droppedAt[0])
		seedProjectOwnership(ctx, t, conn, orgID, "github", "gl:org/team-b", "org/team-b/infra", "provider_access", droppedAt[0])
		seedMembership(ctx, t, conn, orgID, "gitlab", "gl:other/team-b", "gl:zed", "provider_access", droppedAt[0], nil, droppedAt[0])
		seedMembership(ctx, t, conn, orgID, "gitlab", "gl:org/team-b", "gl:manual", "manual", droppedAt[0], nil, droppedAt[0])
		fake.edit(withoutB)
		gitlabDroppedRun(ctx, t, conn, orgID, fake, all, droppedAt[1], staticScopeCensus{})
		requireGitLabFacts(t, "gitlab rows", openGitLabOwnership(ctx, t, conn, orgID, "gitlab"), teamA,
			gitlabOwnershipFact{"gl:other/team-b", "other/team-b/x", "provider_access"},
			gitlabOwnershipFact{"gl:org/team-b", "org/team-b/manual", "manual"})
		requireGitLabFacts(t, "github rows", openGitLabOwnership(ctx, t, conn, orgID, "github"),
			gitlabOwnershipFact{"gl:org/team-b", "org/team-b/infra", "provider_access"})
		requireOpenFacts(t, "memberships", openMembershipFacts(ctx, t, conn, orgID, "gitlab"), alice, "gl:other/team-b|gl:zed", "gl:org/team-b|gl:manual")
	})
}

// ---- Linear -------------------------------------------------------------------

func TestLinearTeamThatIsNoLongerListedClosesItsMembershipsAndItsOwnershipRow(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	end := `{"hasNextPage":false,"endCursor":null}`
	team := func(key, email string) string {
		return `{"id":"team-raw-` + strings.ToLower(key) + `","key":"` + key + `","name":"Team ` + key + `","members":{"nodes":[` +
			`{"id":"user-` + strings.ToLower(key) + `","name":"` + key + `","email":"` + email + `","active":true}],"pageInfo":` + end + `}}`
	}
	teams := func(nodes ...string) string {
		return `{"data":{"teams":{"nodes":[` + strings.Join(nodes, ",") + `],"pageInfo":` + end + `}}}`
	}
	cycles := `{"data":{"cycles":{"nodes":[],"pageInfo":` + end + `}}}`
	projects := `{"data":{"projects":{"nodes":[],"pageInfo":` + end + `}}}`
	run := func(at time.Time, siblings int, teamsResponse string) error {
		t.Helper()
		collector := LinearTeamCatalogCollector{
			ScopeCensus: staticScopeCensus{siblings: siblings},
			Handler:     LinearReferenceCatalogRouteHandler{PerPage: 50, MaxPages: 10},
			Sink:        LinearReferenceCatalogClickHouseEffects{Conn: f.conn, Lease: carrySeamLease()},
		}
		claim := nativeTestClaim("linear", "work-items")
		claim.OrgID = f.orgID
		ref := teamCatalogRefFromClaim(claim)
		ref.Strict = true
		ref.IntegrationID = "integration-a"
		// One cycles answer for each team the walk returns, then the projects.
		responses := []string{teamsResponse}
		for index := 0; index < strings.Count(teamsResponse, `"key":`); index++ {
			responses = append(responses, cycles)
		}
		responses = append(responses, projects)
		_, err := collector.CollectTeamCatalog(f.ctx, ref,
			providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
			linearWorkItemsClient(t, fakehttp.Client(&linearWorkItemsDoer{responses: responses})),
			TeamCatalogSelections{Teams: true, Members: true, Projects: true}, at)
		return err
	}
	const eng, ops = "linear:ENG|linear:eng@example.com", "linear:OPS|linear:ops@example.com"
	if err := run(droppedAt[0], 0, teams(team("ENG", "eng@example.com"), team("OPS", "ops@example.com"))); err != nil {
		t.Fatal(err)
	}
	requireOpenFacts(t, "seeded", openMembershipFacts(f.ctx, t, f.conn, f.orgID, "linear"), eng, ops)
	teamKeyRows := func() []string {
		t.Helper()
		rows, err := f.conn.Query(f.ctx, `SELECT team_id FROM team_project_ownership FINAL WHERE org_id = ? AND provider = 'linear' AND valid_to IS NULL ORDER BY team_id`, f.orgID)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			out = append(out, id)
		}
		return out
	}
	if got := fmt.Sprint(teamKeyRows()); got != "[linear:ENG linear:OPS]" {
		t.Fatalf("seeded team-key ownership = %s, want both teams", got)
	}

	// Another active integration of the provider: nothing closes.
	if err := run(droppedAt[1], 1, teams(team("ENG", "eng@example.com"))); err != nil {
		t.Fatal(err)
	}
	requireOpenFacts(t, "shared scope", openMembershipFacts(f.ctx, t, f.conn, f.orgID, "linear"), eng, ops)
	// A team walk that returned no team closes no membership.
	if err := run(droppedAt[2], 0, teams()); err != nil {
		t.Fatal(err)
	}
	requireOpenFacts(t, "empty team walk", openMembershipFacts(f.ctx, t, f.conn, f.orgID, "linear"), eng, ops)
	// The walk reached its end and the team is not in it: its memberships and its
	// team-key ownership row close; the other team stays.
	if err := run(droppedAt[3], 0, teams(team("ENG", "eng@example.com"))); err != nil {
		t.Fatal(err)
	}
	requireOpenFacts(t, "team dropped", openMembershipFacts(f.ctx, t, f.conn, f.orgID, "linear"), eng)
	requireClosed(t, "team dropped", closedMembershipFacts(f.ctx, t, f.conn, f.orgID, "linear"), ops,
		droppedAt[0].Format(time.RFC3339)+"->"+droppedAt[3].Format(time.RFC3339))
	if got := fmt.Sprint(teamKeyRows()); got != "[linear:ENG]" {
		t.Errorf("team-key ownership after the drop = %s, want only the team that is listed", got)
	}
	// The team comes back: a new fact.
	if err := run(droppedAt[4], 0, teams(team("ENG", "eng@example.com"), team("OPS", "ops@example.com"))); err != nil {
		t.Fatal(err)
	}
	if from := openMembershipFacts(f.ctx, t, f.conn, f.orgID, "linear")[ops]; len(from) != 1 || !from[0].Equal(droppedAt[4]) {
		t.Errorf("the member of the team that came back has valid_from %v, want %v", from, droppedAt[4])
	}
}
