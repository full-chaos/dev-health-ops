//go:build integration

package providersync

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// CHAOS-9144: a member whose login (GitHub, GitLab) or email (Linear) changed is
// the same person: the provider's own stable user id, stored on the membership,
// says so, and the open row CONTINUES (the new member id keeps its valid_from, the
// old one is retired): no leave, no join. member_id, the identity facets and
// raw_provider_user_id are not changed by it. A row with no stored id (written
// before the column) is never matched, and a different id is a different person.

// providerUserIDs reads provider_user_id of the OPEN rows of a provider, by member id.
func providerUserIDs(ctx context.Context, t *testing.T, conn driver.Conn, orgID, provider string) map[string]string {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT member_id, provider_user_id FROM team_memberships FINAL WHERE org_id = ? AND provider = ? AND valid_to IS NULL`, orgID, provider)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var member string
		var id *string
		if err := rows.Scan(&member, &id); err != nil {
			t.Fatal(err)
		}
		if id == nil {
			out[member] = "<NULL>"
		} else {
			out[member] = *id
		}
	}
	return out
}

func requireProviderUserIDs(t *testing.T, label string, got map[string]string, want map[string]string) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s: provider_user_id of the open rows = %v, want %v", label, got, want)
		return
	}
	for member, id := range want {
		if got[member] != id {
			t.Errorf("%s: provider_user_id of %s = %q, want %q (all: %v)", label, member, got[member], id, got)
		}
	}
}

func TestGitHubRenamedLoginContinuesTheOpenRowAndIsNotALeaveAndAJoin(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	const orgID = "org-9144-github"
	logs := captureMemberLogs(t)
	credential := providerfoundation.Credential{Provider: "github", Config: map[string]string{"org": "acme"}}
	sync := func(at time.Time, members string, lookups map[string]int) TeamCatalogResult {
		t.Helper()
		byPath := map[string]string{
			"/orgs/acme/teams":                  `[{"slug":"platform","name":"Platform"}]`,
			"/orgs/acme/teams/platform/repos":   `[]`,
			"/orgs/acme/teams/platform/members": members,
		}
		statuses := map[string]int{}
		for login, status := range lookups {
			path := "/orgs/acme/teams/platform/memberships/" + login
			byPath[path], statuses[path] = `{"message":"Not Found"}`, status
		}
		doer := &githubTeamCatalogFixtureDoer{t: t, byPath: byPath, statuses: statuses}
		adapter := GitHubTeamCatalogCollector{Sink: GitHubTeamCatalogClickHouseEffects{Conn: conn}, ScopeCensus: staticScopeCensus{}}
		result, err := adapter.CollectTeamCatalog(ctx, TeamCatalogReference{OrgID: orgID, SyncRunID: "run", IntegrationID: "integration-a"},
			credential, githubTeamCatalogAdapterClient(t, fakehttp.Client(doer)), TeamCatalogSelections{Teams: true, Members: true}, at)
		if err != nil {
			t.Fatalf("sync at %s: %v", at, err)
		}
		return result
	}
	const octocat, oldHubot, newHubot = "gh:platform|gh:octocat", "gh:platform|gh:old-hubot", "gh:platform|gh:new-hubot"

	// First run: both members are seen, and their numeric ids are stored.
	sync(departureAt[0], `[{"login":"octocat","id":1},{"login":"old-hubot","id":2}]`, nil)
	requireProviderUserIDs(t, "after the first run", providerUserIDs(ctx, t, conn, orgID, "github"),
		map[string]string{"gh:octocat": "1", "gh:old-hubot": "2"})

	// The second run returns id 2 under a new login. No lookup is made for the old login
	// (a lookup would be an unexpected request): the person did not leave.
	result := sync(departureAt[1], `[{"login":"octocat","id":1},{"login":"new-hubot","id":2}]`, nil)
	facts := openMembershipFacts(ctx, t, conn, orgID, "github")
	requireOpen(t, "after the rename", facts, newHubot, departureAt[0]) // the old valid_from: no new join date
	requireOpen(t, "after the rename", facts, octocat, departureAt[0])
	requireNotOpen(t, "after the rename", facts, oldHubot)
	if len(facts) != 2 {
		t.Errorf("open memberships after the rename = %v, want exactly octocat and new-hubot", facts)
	}
	requireClosed(t, "after the rename", closedMembershipFacts(ctx, t, conn, orgID, "github"), oldHubot,
		departureAt[0].Format(time.RFC3339)+"->"+departureAt[1].Format(time.RFC3339))
	if result.MembershipsClosed != 0 {
		t.Errorf("the rename counted %d departure(s), want 0", result.MembershipsClosed)
	}
	requireProviderUserIDs(t, "after the rename", providerUserIDs(ctx, t, conn, orgID, "github"),
		map[string]string{"gh:octocat": "1", "gh:new-hubot": "2"})
	// What the logs say: a rename is counted, no login is named.
	renamed := logs.lines("team_membership_renamed")
	if len(renamed) != 1 || renamed[0]["team"] != "platform" || renamed[0]["provider"] != "github" || renamed[0]["renamed"] != float64(1) {
		t.Errorf("rename log lines = %v, want one line of team=platform provider=github renamed=1", renamed)
	}
	if len(logs.lines("team_membership_closed")) != 0 {
		t.Errorf("a rename wrote a close line: %v", logs.lines("team_membership_closed"))
	}
	if strings.Contains(logs.text(), "hubot") {
		t.Error("a log line holds a member login")
	}
	// The next run changes nothing: the facts keep the first-seen date.
	sync(departureAt[2], `[{"login":"octocat","id":1},{"login":"new-hubot","id":2}]`, nil)
	facts = openMembershipFacts(ctx, t, conn, orgID, "github")
	requireOpen(t, "the run after the rename", facts, newHubot, departureAt[0])
	requireOpen(t, "the run after the rename", facts, octocat, departureAt[0])
}

func TestGitHubALoginUnderAnotherIdIsAnotherPersonAndAnOldRowWithNoIdIsNeverMatched(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	const orgID = "org-9144-github-other"
	credential := providerfoundation.Credential{Provider: "github", Config: map[string]string{"org": "acme"}}
	sync := func(at time.Time, members string, gone ...string) {
		t.Helper()
		byPath := map[string]string{
			"/orgs/acme/teams":                  `[{"slug":"platform","name":"Platform"}]`,
			"/orgs/acme/teams/platform/repos":   `[]`,
			"/orgs/acme/teams/platform/members": members,
		}
		statuses := map[string]int{}
		for _, login := range gone { // the provider's own lookup: this login is not a member
			path := "/orgs/acme/teams/platform/memberships/" + login
			byPath[path], statuses[path] = `{"message":"Not Found"}`, http.StatusNotFound
		}
		doer := &githubTeamCatalogFixtureDoer{t: t, byPath: byPath, statuses: statuses}
		adapter := GitHubTeamCatalogCollector{Sink: GitHubTeamCatalogClickHouseEffects{Conn: conn}, ScopeCensus: staticScopeCensus{}}
		if _, err := adapter.CollectTeamCatalog(ctx, TeamCatalogReference{OrgID: orgID, SyncRunID: "run", IntegrationID: "integration-a"},
			credential, githubTeamCatalogAdapterClient(t, fakehttp.Client(doer)), TeamCatalogSelections{Teams: true, Members: true}, at); err != nil {
			t.Fatalf("sync at %s: %v", at, err)
		}
	}
	sync(departureAt[0], `[{"login":"octocat","id":1},{"login":"hubot","id":2}]`)
	// An open row written BEFORE the column: no stored id (NULL).
	seedMembership(ctx, t, conn, orgID, "github", "gh:platform", "gh:legacy", githubTeamCatalogSource, departureAt[0], nil, departureAt[0])
	// A different id under the new login is a new person: the old login leaves (the lookup says so) and the
	// new one joins now. The legacy row has no id: its member is not matched by a login that appears.
	sync(departureAt[1], `[{"login":"octocat","id":1},{"login":"other-hubot","id":3},{"login":"legacy-renamed","id":9}]`, "hubot", "legacy")
	facts := openMembershipFacts(ctx, t, conn, orgID, "github")
	requireNotOpen(t, "another id", facts, "gh:platform|gh:hubot")
	requireOpen(t, "another id", facts, "gh:platform|gh:other-hubot", departureAt[1])
	requireNotOpen(t, "a row with no stored id", facts, "gh:platform|gh:legacy")
	requireOpen(t, "a row with no stored id", facts, "gh:platform|gh:legacy-renamed", departureAt[1])
}

func TestGitLabRenamedUsernameContinuesTheOpenRow(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	const orgID = "org-9144-gitlab"
	fake := newGitLabMembersServer(t, "alice")
	credential := providerfoundation.Credential{Provider: "gitlab", Config: map[string]string{"group_path": "org"}}
	sync := func(at time.Time) TeamCatalogResult {
		t.Helper()
		collector := GitLabTeamCatalogCollector{Sink: GitLabTeamCatalogClickHouseEffects{
			Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
		}, ScopeCensus: staticScopeCensus{}}
		result, err := collector.CollectTeamCatalog(ctx, TeamCatalogReference{OrgID: orgID, SyncRunID: "run", IntegrationID: "integration-a"},
			credential, gitlabTeamCatalogTestClient(t, fake.URL), TeamCatalogSelections{Teams: true, Members: true}, at)
		if err != nil {
			t.Fatalf("sync at %s: %v", at, err)
		}
		return result
	}
	member := func(id int, username string) map[string]any {
		return map[string]any{"id": id, "username": username, "name": username}
	}
	const carol, oldDan, newDan = "gl:org/team-a|gl:carol", "gl:org/team-a|gl:old-dan", "gl:org/team-a|gl:new-dan"

	fake.setRaw([]map[string]any{member(11, "carol"), member(12, "old-dan")}, false)
	sync(departureAt[0])
	requireProviderUserIDs(t, "after the first run", providerUserIDs(ctx, t, conn, orgID, "gitlab"),
		map[string]string{"gl:carol": "11", "gl:old-dan": "12"})
	// The member id 12 is now new-dan. The lookup for old-dan would answer "not a member"; a rename must not need it.
	fake.setRaw([]map[string]any{member(11, "carol"), member(12, "new-dan")}, false)
	fake.setLookup(nil, http.StatusInternalServerError)
	result := sync(departureAt[1])
	facts := openMembershipFacts(ctx, t, conn, orgID, "gitlab")
	requireOpen(t, "after the rename", facts, newDan, departureAt[0])
	requireOpen(t, "after the rename", facts, carol, departureAt[0])
	requireNotOpen(t, "after the rename", facts, oldDan)
	if result.MembershipsClosed != 0 {
		t.Errorf("the rename counted %d departure(s), want 0", result.MembershipsClosed)
	}
	requireClosed(t, "after the rename", closedMembershipFacts(ctx, t, conn, orgID, "gitlab"), oldDan,
		departureAt[0].Format(time.RFC3339)+"->"+departureAt[1].Format(time.RFC3339))
	requireProviderUserIDs(t, "after the rename", providerUserIDs(ctx, t, conn, orgID, "gitlab"),
		map[string]string{"gl:carol": "11", "gl:new-dan": "12"})
}

func TestLinearChangedEmailContinuesTheOpenRowInsteadOfLeavingTwoOpenRows(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	teamWith := func(members string) string {
		return `{"data":{"teams":{"nodes":[{"id":"team-raw-eng","key":"ENG","name":"Provider Eng","members":{"nodes":[` + members + `],"pageInfo":{"hasNextPage":false,"endCursor":null}}}],` +
			`"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`
	}
	rest := []string{
		`{"data":{"cycles":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
		`{"data":{"projects":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
	}
	run := func(at time.Time, members string) {
		t.Helper()
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
		responses := append([]string{teamWith(members)}, rest...)
		if _, err := collector.CollectTeamCatalog(f.ctx, ref, providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
			linearWorkItemsClient(t, fakehttp.Client(&linearWorkItemsDoer{responses: responses})),
			TeamCatalogSelections{Teams: true, Members: true, Projects: true}, at); err != nil {
			t.Fatalf("sync at %s: %v", at, err)
		}
	}
	const aliceFact, oldBob, newBob = "linear:ENG|linear:alice@example.com", "linear:ENG|linear:bob@example.com", "linear:ENG|linear:robert@example.com"
	alice := `{"id":"user-1","name":"Alice","email":"alice@example.com","active":true}`
	run(departureAt[0], alice+`,{"id":"user-2","name":"Bob","email":"bob@example.com","active":true}`)
	requireProviderUserIDs(t, "after the first run", providerUserIDs(f.ctx, t, f.conn, f.orgID, "linear"),
		map[string]string{"linear:alice@example.com": "user-1", "linear:bob@example.com": "user-2"})
	// Bob changes his email: the member id is made from the email, the Linear id stays.
	run(departureAt[1], alice+`,{"id":"user-2","name":"Bob","email":"robert@example.com","active":true}`)
	facts := openMembershipFacts(f.ctx, t, f.conn, f.orgID, "linear")
	requireOpen(t, "after the email change", facts, newBob, departureAt[0])
	requireOpen(t, "after the email change", facts, aliceFact, departureAt[0])
	requireNotOpen(t, "after the email change", facts, oldBob)
	if len(facts) != 2 {
		t.Errorf("open memberships after the email change = %v, want two (never two open rows of one person)", facts)
	}
	requireProviderUserIDs(t, "after the email change", providerUserIDs(f.ctx, t, f.conn, f.orgID, "linear"),
		map[string]string{"linear:alice@example.com": "user-1", "linear:robert@example.com": "user-2"})
}

// A rename does not wait for the proof a departure needs: a team whose member read has no
// end signal (a full page and no Link) still replaces the old login's open row, so the
// person does not hold two open rows (round 122 r3).
func TestGitHubRenameIsAppliedEvenWhenTheMemberReadHasNoEndSignal(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	const orgID = "org-9144-github-unproven"
	credential := providerfoundation.Credential{Provider: "github", Config: map[string]string{"org": "acme"}}
	run := func(at time.Time, members string, perPage int) TeamCatalogResult {
		t.Helper()
		doer := &githubTeamCatalogFixtureDoer{t: t, byPath: map[string]string{
			"/orgs/acme/teams":                  `[{"slug":"platform","name":"Platform"}]`,
			"/orgs/acme/teams/platform/repos":   `[]`,
			"/orgs/acme/teams/platform/members": members,
		}}
		adapter := GitHubTeamCatalogCollector{Client: GitHubTeamCatalogRouteHandler{PerPage: perPage},
			Sink: GitHubTeamCatalogClickHouseEffects{Conn: conn}, ScopeCensus: staticScopeCensus{}}
		result, err := adapter.CollectTeamCatalog(ctx, TeamCatalogReference{OrgID: orgID, SyncRunID: "run", IntegrationID: "integration-a"},
			credential, githubTeamCatalogAdapterClient(t, fakehttp.Client(doer)), TeamCatalogSelections{Teams: true, Members: true}, at)
		if err != nil {
			t.Fatalf("sync at %s: %v", at, err)
		}
		return result
	}
	run(departureAt[0], `[{"login":"octocat","id":1},{"login":"old-hubot","id":2}]`, 0)
	// per_page 2 and two members back with no Link: the read is not known to have ended.
	result := run(departureAt[1], `[{"login":"octocat","id":1},{"login":"new-hubot","id":2}]`, 2)
	facts := openMembershipFacts(ctx, t, conn, orgID, "github")
	requireOpen(t, "an unproven read", facts, "gh:platform|gh:new-hubot", departureAt[0])
	requireOpen(t, "an unproven read", facts, "gh:platform|gh:octocat", departureAt[0])
	requireNotOpen(t, "an unproven read", facts, "gh:platform|gh:old-hubot")
	if len(facts) != 2 || result.MembershipsClosed != 0 {
		t.Errorf("open = %v, departures counted = %d, want octocat and new-hubot and 0", facts, result.MembershipsClosed)
	}
}
