//go:build integration

package providersync

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// CHAOS-9079: a member absent from the COMPLETE member read of their team is
// closed (valid_to = the run time) by the run that reads it; a read that did
// not prove its end, a failed read and a scope another integration may share
// close nothing; a member who comes back is a new fact with a new valid_from.

var departureAt = []time.Time{
	time.Date(2026, 10, 2, 8, 0, 0, 0, time.UTC),
	time.Date(2026, 10, 2, 9, 0, 0, 0, time.UTC),
	time.Date(2026, 10, 2, 10, 0, 0, 0, time.UTC),
	time.Date(2026, 10, 2, 11, 0, 0, 0, time.UTC),
	time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC),
}

// closedMembershipFacts is the closed rows of a provider after FINAL, per fact
// "<team>|<member>": "<valid_from>-><valid_to>" of each closed row.
func closedMembershipFacts(ctx context.Context, t *testing.T, conn driver.Conn, orgID, provider string) map[string][]string {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT team_id, member_id, valid_from, valid_to FROM team_memberships FINAL WHERE org_id = ? AND provider = ? AND valid_to IS NOT NULL`, orgID, provider)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	facts := map[string][]string{}
	for rows.Next() {
		var team, member string
		var from time.Time
		var to *time.Time
		if err := rows.Scan(&team, &member, &from, &to); err != nil {
			t.Fatal(err)
		}
		facts[team+"|"+member] = append(facts[team+"|"+member], from.UTC().Format(time.RFC3339)+"->"+to.UTC().Format(time.RFC3339))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return facts
}

func requireOpen(t *testing.T, label string, facts map[string][]time.Time, fact string, from time.Time) {
	t.Helper()
	rows := facts[fact]
	if len(rows) != 1 || !rows[0].Equal(from) {
		t.Errorf("%s: fact %s open rows = %v, want one open row valid_from %s", label, fact, rows, from)
	}
}

func requireNotOpen(t *testing.T, label string, facts map[string][]time.Time, fact string) {
	t.Helper()
	if rows := facts[fact]; len(rows) != 0 {
		t.Errorf("%s: fact %s is still open %v, want closed", label, fact, rows)
	}
}

func requireClosed(t *testing.T, label string, closed map[string][]string, fact, want string) {
	t.Helper()
	if got := closed[fact]; len(got) != 1 || got[0] != want {
		t.Errorf("%s: fact %s closed rows = %v, want [%s]", label, fact, got, want)
	}
}

func TestGitHubDepartedMemberIsClosedByTheCompleteReadAndReopenedAsANewFact(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	const orgID = "org-9079-github"
	credential := providerfoundation.Credential{Provider: "github", Config: map[string]string{"org": "acme"}}
	sync := func(at time.Time, siblings int, membersStatus int, members string) {
		t.Helper()
		doer := &githubTeamCatalogFixtureDoer{t: t, byPath: map[string]string{
			"/orgs/acme/teams":                  `[{"slug":"platform","name":"Platform","description":"Platform team"}]`,
			"/orgs/acme/teams/platform/repos":   `[{"name":"api"}]`,
			"/orgs/acme/teams/platform/members": members,
			// the provider's direct lookup: hubot is not a member of the team
			"/orgs/acme/teams/platform/memberships/hubot": `{"message":"Not Found"}`,
		}, statuses: map[string]int{
			"/orgs/acme/teams/platform/members":           membersStatus,
			"/orgs/acme/teams/platform/memberships/hubot": http.StatusNotFound,
		}}
		adapter := GitHubTeamCatalogCollector{Sink: GitHubTeamCatalogClickHouseEffects{Conn: conn}, ScopeCensus: staticScopeCensus{siblings: siblings}}
		if _, err := adapter.CollectTeamCatalog(ctx, TeamCatalogReference{OrgID: orgID, SyncRunID: "run", IntegrationID: "integration-a"},
			credential, githubTeamCatalogAdapterClient(t, fakehttp.Client(doer)),
			TeamCatalogSelections{Teams: true, Members: true}, at); err != nil {
			t.Fatalf("sync at %s: %v", at, err)
		}
	}
	both := `[{"login":"octocat"},{"login":"hubot"}]`
	only := `[{"login":"octocat"}]`
	const octocat, hubot = "gh:platform|gh:octocat", "gh:platform|gh:hubot"

	sync(departureAt[0], 0, http.StatusOK, both)
	// A read that FAILED closes nothing: hubot is absent from the answer, but there is no answer.
	sync(departureAt[1], 0, http.StatusInternalServerError, only)
	facts := openMembershipFacts(ctx, t, conn, orgID, "github")
	requireOpen(t, "after a failed read", facts, hubot, departureAt[0])
	requireOpen(t, "after a failed read", facts, octocat, departureAt[0])
	// A scope another integration shares closes nothing.
	sync(departureAt[2], 1, http.StatusOK, only)
	requireOpen(t, "shared scope", openMembershipFacts(ctx, t, conn, orgID, "github"), hubot, departureAt[0])
	// The complete read closes the member who left.
	sync(departureAt[3], 0, http.StatusOK, only)
	facts = openMembershipFacts(ctx, t, conn, orgID, "github")
	requireNotOpen(t, "after the complete read", facts, hubot)
	requireOpen(t, "after the complete read", facts, octocat, departureAt[0])
	requireClosed(t, "after the complete read", closedMembershipFacts(ctx, t, conn, orgID, "github"), hubot,
		departureAt[0].Format(time.RFC3339)+"->"+departureAt[3].Format(time.RFC3339))
	// A member who comes back is a new fact with a new valid_from.
	sync(departureAt[4], 0, http.StatusOK, both)
	facts = openMembershipFacts(ctx, t, conn, orgID, "github")
	requireOpen(t, "after the return", facts, hubot, departureAt[4])
	requireOpen(t, "after the return", facts, octocat, departureAt[0])
	requireClosed(t, "after the return", closedMembershipFacts(ctx, t, conn, orgID, "github"), hubot,
		departureAt[0].Format(time.RFC3339)+"->"+departureAt[3].Format(time.RFC3339))
}

func TestGitLabDepartedMemberIsClosedByTheCompleteReadAndATruncatedReadClosesNothing(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	const orgID = "org-9079-gitlab"
	fake := newGitLabMembersServer(t, "alice", "carol")
	credential := providerfoundation.Credential{Provider: "gitlab", Config: map[string]string{"group_path": "org"}}
	sync := func(at time.Time, siblings int) error {
		t.Helper()
		collector := GitLabTeamCatalogCollector{Sink: GitLabTeamCatalogClickHouseEffects{
			Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
		}, ScopeCensus: staticScopeCensus{siblings: siblings}}
		ref := TeamCatalogReference{OrgID: orgID, SyncRunID: "run", IntegrationID: "integration-a"}
		_, err := collector.CollectTeamCatalog(ctx, ref, credential, gitlabTeamCatalogTestClient(t, fake.URL),
			TeamCatalogSelections{Teams: true, Members: true}, at)
		return err
	}
	mustSync := func(at time.Time, siblings int) {
		t.Helper()
		if err := sync(at, siblings); err != nil {
			t.Fatalf("sync at %s: %v", at, err)
		}
	}
	const alice, carol = "gl:org/team-a|gl:alice", "gl:org/team-a|gl:carol"

	mustSync(departureAt[0], 0)
	// A read cut by the page budget (the provider never said it ended) fails the
	// run closed before any write (gitlab_team_catalog_route.go:607-613) and
	// closes nothing, although alice and carol are not in the partial answer.
	fake.setEndless(true)
	if err := sync(departureAt[1], 0); err == nil {
		t.Fatal("a member read cut by the page budget must fail the run")
	}
	fake.setEndless(false)
	facts := openMembershipFacts(ctx, t, conn, orgID, "gitlab")
	requireOpen(t, "after a truncated read", facts, alice, departureAt[0])
	requireOpen(t, "after a truncated read", facts, carol, departureAt[0])
	if len(closedMembershipFacts(ctx, t, conn, orgID, "gitlab")) != 0 {
		t.Fatalf("a truncated read closed %v", closedMembershipFacts(ctx, t, conn, orgID, "gitlab"))
	}
	// A scope another integration shares closes nothing.
	fake.setMembers("alice")
	mustSync(departureAt[2], 1)
	requireOpen(t, "shared scope", openMembershipFacts(ctx, t, conn, orgID, "gitlab"), carol, departureAt[0])
	// The complete read: carol left.
	fake.setMembers("alice")
	mustSync(departureAt[3], 0)
	facts = openMembershipFacts(ctx, t, conn, orgID, "gitlab")
	requireOpen(t, "after the complete read", facts, alice, departureAt[0])
	requireNotOpen(t, "after the complete read", facts, carol)
	requireClosed(t, "after the complete read", closedMembershipFacts(ctx, t, conn, orgID, "gitlab"), carol,
		departureAt[0].Format(time.RFC3339)+"->"+departureAt[3].Format(time.RFC3339))
	// Carol comes back: a new fact.
	fake.setMembers("alice", "carol")
	mustSync(departureAt[4], 0)
	requireOpen(t, "after the return", openMembershipFacts(ctx, t, conn, orgID, "gitlab"), carol, departureAt[4])
}

func TestLinearDepartedMemberIsClosedByTheCompleteReadAndAPartialReadClosesNothing(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	alice := `{"id":"user-1","name":"Alice","email":"alice@example.com","active":true}`
	bob := `{"id":"user-2","name":"Bob","email":"bob@example.com","active":true}`
	bobInactive := `{"id":"user-2","name":"Bob","email":"bob@example.com","active":false}`
	teamWith := func(members, pageInfo string) string {
		return `{"data":{"teams":{"nodes":[{"id":"team-raw-eng","key":"ENG","name":"Provider Eng","members":{"nodes":[` + members + `],"pageInfo":` + pageInfo + `}}],` +
			`"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`
	}
	end := `{"hasNextPage":false,"endCursor":null}`
	more := `{"hasNextPage":true,"endCursor":"cursor-1"}`
	sync := func(at time.Time, siblings int, responses ...string) error {
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
		_, err := collector.CollectTeamCatalog(f.ctx, ref,
			providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
			linearWorkItemsClient(t, fakehttp.Client(&linearWorkItemsDoer{responses: responses})),
			TeamCatalogSelections{Teams: true, Members: true, Projects: true}, at)
		return err
	}
	rest := []string{
		`{"data":{"cycles":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
		`{"data":{"projects":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
	}
	run := func(at time.Time, siblings int, teamResponse string) error {
		return sync(at, siblings, append([]string{teamResponse}, rest...)...)
	}
	const aliceFact, bobFact = "linear:ENG|linear:alice@example.com", "linear:ENG|linear:bob@example.com"

	if err := run(departureAt[0], 0, teamWith(alice+","+bob, end)); err != nil {
		t.Fatal(err)
	}
	// A partial read: the member list does not end and its continuation fails, so
	// the whole run fails before any write and nobody is closed.
	if err := sync(departureAt[1], 0, teamWith(alice, more)); err == nil {
		t.Fatal("a member read that does not end must fail the run")
	}
	facts := openMembershipFacts(f.ctx, t, f.conn, f.orgID, "linear")
	requireOpen(t, "after a partial read", facts, aliceFact, departureAt[0])
	requireOpen(t, "after a partial read", facts, bobFact, departureAt[0])
	// A scope another integration shares closes nothing.
	if err := run(departureAt[2], 1, teamWith(alice, end)); err != nil {
		t.Fatal(err)
	}
	requireOpen(t, "shared scope", openMembershipFacts(f.ctx, t, f.conn, f.orgID, "linear"), bobFact, departureAt[0])
	// The complete read: bob is deactivated (inactive members are not members): closed.
	if err := run(departureAt[3], 0, teamWith(alice+","+bobInactive, end)); err != nil {
		t.Fatal(err)
	}
	facts = openMembershipFacts(f.ctx, t, f.conn, f.orgID, "linear")
	requireOpen(t, "after the complete read", facts, aliceFact, departureAt[0])
	requireNotOpen(t, "after the complete read", facts, bobFact)
	requireClosed(t, "after the complete read", closedMembershipFacts(f.ctx, t, f.conn, f.orgID, "linear"), bobFact,
		departureAt[0].Format(time.RFC3339)+"->"+departureAt[3].Format(time.RFC3339))
	// Bob comes back: a new fact.
	if err := run(departureAt[4], 0, teamWith(alice+","+bob, end)); err != nil {
		t.Fatal(err)
	}
	requireOpen(t, "after the return", openMembershipFacts(f.ctx, t, f.conn, f.orgID, "linear"), bobFact, departureAt[4])
}

// The cases that must NOT close, and the empty list both ways: an
// unusable member node, a truncated read and a failed read leave a departed
// member open; a list that is empty AND proved its end closes every member of
// the team, a list that is empty without the provider's end signal closes none.

func TestGitHubUnusableNodeAndTruncatedReadCloseNothingAndAnEmptyCompleteListClosesAll(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	const orgID = "org-9079-github-edge"
	credential := providerfoundation.Credential{Provider: "github", Config: map[string]string{"org": "acme"}}
	const octocat, hubot = "gh:platform|gh:octocat", "gh:platform|gh:hubot"
	sync := func(at time.Time, maxPages int, members string, links map[string]string, extra map[string]string) {
		t.Helper()
		byPath := map[string]string{
			"/orgs/acme/teams":                  `[{"slug":"platform","name":"Platform","description":"Platform team"}]`,
			"/orgs/acme/teams/platform/repos":   `[{"name":"api"}]`,
			"/orgs/acme/teams/platform/members": members,
			// the provider's direct lookup: neither is a member of the team
			"/orgs/acme/teams/platform/memberships/octocat": `{}`,
			"/orgs/acme/teams/platform/memberships/hubot":   `{}`,
		}
		for key, body := range extra {
			byPath[key] = body
		}
		doer := &githubTeamCatalogFixtureDoer{t: t, byPath: byPath, links: links, statuses: map[string]int{
			"/orgs/acme/teams/platform/memberships/octocat": http.StatusNotFound,
			"/orgs/acme/teams/platform/memberships/hubot":   http.StatusNotFound,
		}}
		adapter := GitHubTeamCatalogCollector{Client: GitHubTeamCatalogRouteHandler{MaxPages: maxPages},
			Sink: GitHubTeamCatalogClickHouseEffects{Conn: conn}, ScopeCensus: staticScopeCensus{}}
		if _, err := adapter.CollectTeamCatalog(ctx, TeamCatalogReference{OrgID: orgID, SyncRunID: "run", IntegrationID: "integration-a"},
			credential, githubTeamCatalogAdapterClient(t, fakehttp.Client(doer)),
			TeamCatalogSelections{Teams: true, Members: true}, at); err != nil {
			t.Fatalf("sync at %s: %v", at, err)
		}
	}
	sync(departureAt[0], 0, `[{"login":"octocat"},{"login":"hubot"}]`, nil, nil)
	// An unusable node (no login): hubot is absent but the list is not known to be complete.
	sync(departureAt[1], 0, `[{"login":"octocat"},{"login":""}]`, nil, nil)
	facts := openMembershipFacts(ctx, t, conn, orgID, "github")
	requireOpen(t, "after an unusable node", facts, hubot, departureAt[0])
	// A read cut by the page budget: the run skips the team's memberships, closes nothing.
	sync(departureAt[2], 1, `[{"login":"octocat"}]`,
		map[string]string{"/orgs/acme/teams/platform/members": `<https://api.github.com/orgs/acme/teams/platform/members?page=2>; rel="next"`},
		map[string]string{"/orgs/acme/teams/platform/members?page=2": `[{"login":"hubot"}]`})
	requireOpen(t, "after a truncated read", openMembershipFacts(ctx, t, conn, orgID, "github"), hubot, departureAt[0])
	// An empty list that proved its end: every member of the team closes.
	sync(departureAt[3], 0, `[]`, nil, nil)
	facts = openMembershipFacts(ctx, t, conn, orgID, "github")
	if len(facts) != 0 {
		t.Errorf("an empty complete member list left %v open, want none", facts)
	}
	closed := closedMembershipFacts(ctx, t, conn, orgID, "github")
	requireClosed(t, "after the empty complete list", closed, octocat, departureAt[0].Format(time.RFC3339)+"->"+departureAt[3].Format(time.RFC3339))
	requireClosed(t, "after the empty complete list", closed, hubot, departureAt[0].Format(time.RFC3339)+"->"+departureAt[3].Format(time.RFC3339))
}

func TestGitLabUnusableNodeAndAnEmptyListWithoutAnEndSignalCloseNothing(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	const orgID = "org-9079-gitlab-edge"
	fake := newGitLabMembersServer(t, "alice", "carol")
	credential := providerfoundation.Credential{Provider: "gitlab", Config: map[string]string{"group_path": "org"}}
	sync := func(at time.Time) {
		t.Helper()
		collector := GitLabTeamCatalogCollector{Sink: GitLabTeamCatalogClickHouseEffects{
			Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
		}, ScopeCensus: staticScopeCensus{}}
		ref := TeamCatalogReference{OrgID: orgID, SyncRunID: "run", IntegrationID: "integration-a"}
		if _, err := collector.CollectTeamCatalog(ctx, ref, credential, gitlabTeamCatalogTestClient(t, fake.URL),
			TeamCatalogSelections{Teams: true, Members: true}, at); err != nil {
			t.Fatalf("sync at %s: %v", at, err)
		}
	}
	const alice, carol = "gl:org/team-a|gl:alice", "gl:org/team-a|gl:carol"
	sync(departureAt[0])
	// A member node the normalizer rejects (no username): carol is absent, the list is not known complete.
	fake.setRaw([]map[string]any{{"username": "alice", "name": "alice", "email": "alice@example.com"}, {"name": "ghost"}}, false)
	sync(departureAt[1])
	requireOpen(t, "after an unusable node", openMembershipFacts(ctx, t, conn, orgID, "gitlab"), carol, departureAt[0])
	// An empty list WITHOUT the provider's end signal closes nothing.
	fake.setRaw([]map[string]any{}, true)
	sync(departureAt[2])
	facts := openMembershipFacts(ctx, t, conn, orgID, "gitlab")
	requireOpen(t, "after an empty list without an end signal", facts, alice, departureAt[0])
	requireOpen(t, "after an empty list without an end signal", facts, carol, departureAt[0])
	// An empty list that proved its end, and the provider's own lookup agrees
	// nobody is a member: every member of the group closes.
	fake.setRaw([]map[string]any{}, false)
	fake.setLookup([]string{}, 0)
	sync(departureAt[3])
	if facts := openMembershipFacts(ctx, t, conn, orgID, "gitlab"); len(facts) != 0 {
		t.Errorf("an empty complete member list left %v open, want none", facts)
	}
}

func TestLinearUnusableNodeCloseNothingAndAnEmptyCompleteListClosesEveryMemberKeyedById(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	// Members with no email are keyed by their Linear user id, which is stable.
	alice := `{"id":"user-1","name":"Alice","active":true}`
	bob := `{"id":"user-2","name":"Bob","active":true}`
	ghost := `{"name":"Ghost","active":true}`
	run := func(at time.Time, members string) {
		t.Helper()
		responses := []string{
			`{"data":{"teams":{"nodes":[{"id":"team-raw-eng","key":"ENG","name":"Provider Eng","members":{"nodes":[` + members + `],"pageInfo":{"hasNextPage":false,"endCursor":null}}}],` +
				`"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
			`{"data":{"cycles":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
			`{"data":{"projects":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
		}
		collector := LinearTeamCatalogCollector{
			ScopeCensus: staticScopeCensus{},
			Handler:     LinearReferenceCatalogRouteHandler{PerPage: 50, MaxPages: 10},
			Sink:        LinearReferenceCatalogClickHouseEffects{Conn: f.conn, Lease: carrySeamLease()},
		}
		claim := nativeTestClaim("linear", "work-items")
		claim.OrgID = f.orgID
		ref := teamCatalogRefFromClaim(claim)
		ref.Strict = true
		ref.IntegrationID = "integration-a"
		if _, err := collector.CollectTeamCatalog(f.ctx, ref,
			providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
			linearWorkItemsClient(t, fakehttp.Client(&linearWorkItemsDoer{responses: responses})),
			TeamCatalogSelections{Teams: true, Members: true, Projects: true}, at); err != nil {
			t.Fatalf("sync at %s: %v", at, err)
		}
	}
	const aliceFact, bobFact = "linear:ENG|linear:user-1", "linear:ENG|linear:user-2"
	run(departureAt[0], alice+","+bob)
	// A node with neither id nor email: bob is absent, the team's list is not known complete.
	run(departureAt[1], alice+","+ghost)
	facts := openMembershipFacts(f.ctx, t, f.conn, f.orgID, "linear")
	requireOpen(t, "after an unusable node", facts, bobFact, departureAt[0])
	requireOpen(t, "after an unusable node", facts, aliceFact, departureAt[0])
	// An empty list that proved its end closes every member of the team.
	run(departureAt[2], "")
	if facts := openMembershipFacts(f.ctx, t, f.conn, f.orgID, "linear"); len(facts) != 0 {
		t.Errorf("an empty complete member list left %v open, want none", facts)
	}
}
