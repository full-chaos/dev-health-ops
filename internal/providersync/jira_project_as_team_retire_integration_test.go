//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

type retireFixture struct {
	t     *testing.T
	ctx   context.Context
	conn  driver.Conn
	orgID string
	old   time.Time
}

func (f retireFixture) team(provider, id, nativeKey string, manualMembers []string) {
	f.t.Helper()
	var key *string
	if nativeKey != "" {
		key = &nativeKey
	}
	if manualMembers == nil {
		manualMembers = []string{}
	}
	if err := f.conn.Exec(f.ctx, `INSERT INTO teams (id, team_uuid, name, members, manual_members, project_keys, repo_patterns, is_active, updated_at, org_id, provider, native_team_key) VALUES (?, ?, ?, [], ?, ?, [], 1, ?, ?, ?, ?)`,
		id, uuid.New(), "team "+id, manualMembers, []string{id}, f.old, f.orgID, provider, key); err != nil {
		f.t.Fatalf("insert team %s: %v", id, err)
	}
}

func (f retireFixture) ownership(provider, teamID, projectID, projectKey, source string) {
	f.t.Helper()
	if err := f.conn.Exec(f.ctx, `INSERT INTO team_project_ownership (org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, updated_at) VALUES (?, ?, ?, ?, ?, ?, 1, 100, 10, ?, ?)`,
		f.orgID, provider, teamID, projectID, projectKey, source, f.old, f.old); err != nil {
		f.t.Fatalf("insert ownership %s/%s: %v", teamID, projectID, err)
	}
}

func (f retireFixture) membership(provider, teamID, memberID string) {
	f.t.Helper()
	if err := f.conn.Exec(f.ctx, `INSERT INTO team_memberships (org_id, provider, team_id, member_id, identity_facets, source, is_primary, specificity, priority, valid_from, updated_at) VALUES (?, ?, ?, ?, [], 'native', 1, 100, 10, ?, ?)`,
		f.orgID, provider, teamID, memberID, f.old, f.old); err != nil {
		f.t.Fatalf("insert membership %s/%s: %v", teamID, memberID, err)
	}
}

func (f retireFixture) activeTeam(provider, id string) bool {
	f.t.Helper()
	return countRows(f.t, f.ctx, f.conn, `SELECT count() FROM teams FINAL WHERE org_id = ? AND provider = ? AND id = ? AND is_active = 1`, f.orgID, provider, id) == 1
}

func (f retireFixture) openOwnership(provider, teamID string) uint64 {
	f.t.Helper()
	return countRows(f.t, f.ctx, f.conn, `SELECT count() FROM team_project_ownership FINAL WHERE org_id = ? AND provider = ? AND team_id = ? AND valid_to IS NULL`, f.orgID, provider, teamID)
}

func (f retireFixture) openMembership(provider, teamID string) uint64 {
	f.t.Helper()
	return countRows(f.t, f.ctx, f.conn, `SELECT count() FROM team_memberships FINAL WHERE org_id = ? AND provider = ? AND team_id = ? AND valid_to IS NULL`, f.orgID, provider, teamID)
}

// The store holds what the old Jira catalog left (a team per project, with
// its ownership row and its lead) beside every row class that must stay: an
// Atlassian team with a connected space, a legacy link, the teams and project
// ownership of the three other providers, a row of another organization, and
// two rows built to sit as near the retired shape as a row of another class
// can: an Atlassian team whose id reads like a project key, and a team whose
// id and native key are the same team ARI.
func TestRetireJiraProjectAsTeamRowsClosesOnlyThatClass(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := retireFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString(), old: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)}
	other := retireFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString(), old: f.old}
	at := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	const atlassian = "0b1f3b0a-6d0c-4f5e-9a55-1c2d3e4f5a6b"
	const ari = "ari:cloud:identity::team/"

	// The class to retire: two projects; one has manual members and a policy.
	f.team("jira", "OPS", "OPS", nil)
	f.team("jira", "SEC", "SEC", []string{"person@example.test"})
	f.ownership("jira", "OPS", "10001", "OPS", "native")
	f.ownership("jira", "SEC", "10002", "SEC", "native")
	f.membership("jira", "OPS", "jira:lead-1")
	f.membership("jira", "SEC", "jira:lead-2")
	if err := conn.Exec(ctx, `INSERT INTO team_sync_policies (org_id, team_id, sync_policy, updated_at) VALUES (?, 'SEC', 1, ?)`, f.orgID, f.old); err != nil {
		t.Fatalf("insert policy: %v", err)
	}

	// What stays.
	f.team("jira", atlassian, ari+atlassian, nil)
	f.ownership("jira", atlassian, "10001", "OPS", "native")
	f.membership("jira", atlassian, "jira:member-1")
	f.ownership("jira", "ops-team-7", "10002", "SEC", "jira_legacy")
	f.team("jira", "KEYLIKE", ari+"0c2f3b0a-6d0c-4f5e-9a55-1c2d3e4f5a6b", nil)
	f.ownership("jira", "KEYLIKE", "10003", "KEYLIKE", "native")
	f.membership("jira", "KEYLIKE", "jira:member-2")
	f.team("jira", ari+"same", ari+"same", nil)
	f.membership("jira", ari+"same", "jira:member-3")
	f.team("jira", "ADMINMADE", "", nil)
	for _, provider := range []string{"github", "gitlab", "linear"} {
		// teams is keyed (org_id, id): one id per provider. Each row has the
		// retired shape in every column but the provider.
		id := "ENG" + provider
		f.team(provider, id, id, nil)
		f.ownership(provider, id, "p-1", id, "native")
		f.membership(provider, id, provider+":member")
	}
	other.team("jira", "OPS", "OPS", nil)
	other.ownership("jira", "OPS", "10001", "OPS", "native")
	other.membership("jira", "OPS", "jira:lead-1")

	stays := func(stage string) {
		t.Helper()
		for _, id := range []string{atlassian, "KEYLIKE", ari + "same", "ADMINMADE"} {
			if !f.activeTeam("jira", id) {
				t.Fatalf("%s: jira team %q is not active any more", stage, id)
			}
		}
		if f.openOwnership("jira", atlassian) != 1 || f.openOwnership("jira", "ops-team-7") != 1 || f.openOwnership("jira", "KEYLIKE") != 1 {
			t.Fatalf("%s: an ownership row of another class was closed", stage)
		}
		if f.openMembership("jira", atlassian) != 1 || f.openMembership("jira", "KEYLIKE") != 1 || f.openMembership("jira", ari+"same") != 1 {
			t.Fatalf("%s: a membership row of another class was closed", stage)
		}
		for _, provider := range []string{"github", "gitlab", "linear"} {
			id := "ENG" + provider
			if !f.activeTeam(provider, id) || f.openOwnership(provider, id) != 1 || f.openMembership(provider, id) != 1 {
				t.Fatalf("%s: a %s row changed", stage, provider)
			}
		}
		if !other.activeTeam("jira", "OPS") || other.openOwnership("jira", "OPS") != 1 || other.openMembership("jira", "OPS") != 1 {
			t.Fatalf("%s: a row of another organization changed", stage)
		}
	}

	dry, err := RetireJiraProjectAsTeamRows(ctx, conn, f.orgID, at, true)
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	want := JiraProjectAsTeamRetireOutcome{DryRun: true, Teams: 2, TeamsWithManualMembers: 1, TeamsWithSyncPolicy: 1, OwnershipRows: 2, MembershipRows: 2}
	if dry != want {
		t.Fatalf("dry run = %+v, want %+v", dry, want)
	}
	if !f.activeTeam("jira", "OPS") || f.openOwnership("jira", "OPS") != 1 || f.openMembership("jira", "OPS") != 1 {
		t.Fatal("the dry run wrote")
	}
	stays("after the dry run")

	real, err := RetireJiraProjectAsTeamRows(ctx, conn, f.orgID, at, false)
	if err != nil {
		t.Fatalf("real run: %v", err)
	}
	want.DryRun, want.TeamsRetired, want.OwnershipClosed, want.MembershipClosed = false, 2, 2, 2
	if real != want {
		t.Fatalf("real run = %+v, want %+v", real, want)
	}
	for _, id := range []string{"OPS", "SEC"} {
		if f.activeTeam("jira", id) || f.openOwnership("jira", id) != 0 || f.openMembership("jira", id) != 0 {
			t.Fatalf("project-as-team %s is not retired", id)
		}
		// Retired, not removed: the team row and its rows are still there,
		// on the valid_from they had.
		if n := countRows(t, ctx, conn, `SELECT count() FROM teams FINAL WHERE org_id = ? AND id = ? AND is_active = 0 AND project_keys = [?]`, f.orgID, id, id); n != 1 {
			t.Fatalf("team %s: %d inactive rows, want 1", id, n)
		}
		if n := countRows(t, ctx, conn, `SELECT count() FROM team_project_ownership FINAL WHERE org_id = ? AND team_id = ? AND valid_from = ? AND valid_to = ?`, f.orgID, id, f.old, at); n != 1 {
			t.Fatalf("ownership of %s: %d closed rows on the first valid_from, want 1", id, n)
		}
		if n := countRows(t, ctx, conn, `SELECT count() FROM team_memberships FINAL WHERE org_id = ? AND team_id = ? AND valid_from = ? AND valid_to = ?`, f.orgID, id, f.old, at); n != 1 {
			t.Fatalf("membership of %s: %d closed rows on the first valid_from, want 1", id, n)
		}
	}
	if n := countRows(t, ctx, conn, `SELECT count() FROM teams FINAL WHERE org_id = ? AND id = 'SEC' AND manual_members = ['person@example.test']`, f.orgID); n != 1 {
		t.Fatal("the retired team row lost its manual members")
	}
	stays("after the real run")

	second, err := RetireJiraProjectAsTeamRows(ctx, conn, f.orgID, at.Add(time.Hour), false)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if second != (JiraProjectAsTeamRetireOutcome{}) {
		t.Fatalf("second run = %+v, want all zero", second)
	}
	stays("after the second run")
}

func TestRetireJiraProjectAsTeamRowsRefusesAnEmptyOrganization(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	for _, org := range []string{"", "  "} {
		if _, err := RetireJiraProjectAsTeamRows(ctx, conn, org, time.Now(), false); err != ErrInvalidConfiguration {
			t.Fatalf("org %q: err = %v, want ErrInvalidConfiguration", org, err)
		}
	}
}

// The repositories a project-as-team owned were derived from its project
// ownership (team_repo_ownership, source 'inferred'). The derivation closes a
// derived row only while the organization has an open project link, so in an
// organization whose only project links were the retired class it closes
// none. The retire closes them itself: a retired team owns no repository.
//
// Both derived rows come from the real producer. The Atlassian team's row and
// a hand-written row of another source stay.
func TestRetireJiraProjectAsTeamRowsClosesTheDerivedRepoOwnershipOfARetiredTeam(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := retireFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString(), old: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)}
	const atlassian = "0b1f3b0a-6d0c-4f5e-9a55-1c2d3e4f5a6b"

	opsRepo, platRepo := uuid.New(), uuid.New()
	seedTeamRepoOwnershipRepos(t, ctx, conn, f.orgID, map[uuid.UUID]string{opsRepo: "acme/ops", platRepo: "acme/plat"})
	f.team("jira", "OPS", "OPS", nil)
	f.ownership("jira", "OPS", "10001", "OPS", "native")
	f.team("jira", atlassian, jiraAtlassianTeamARIPrefix+atlassian, nil)
	f.ownership("jira", atlassian, "10002", "PLAT", "native")
	seedWorkItem(t, ctx, conn, f.orgID, "jira:OPS-1", "jira", opsRepo, "10001", f.old)
	seedWorkItem(t, ctx, conn, f.orgID, "jira:PLAT-1", "jira", platRepo, "10002", f.old)

	service := TeamRepoOwnershipDerivationService{Conn: conn}
	if written, _, ready, _, err := service.Derive(ctx, f.orgID); err != nil || !ready || written != 2 {
		t.Fatalf("first derivation: written=%d ready=%v err=%v, want the two derived rows", written, ready, err)
	}
	// A row of another source for the retired team's id: not derived, so not
	// this step's to close.
	if err := conn.Exec(ctx, teamRepoOwnershipInsert+` VALUES (?, 'github', 'OPS', ?, 'acme/manual', 'exact', 'manual', 0, 100, 0, ?, NULL, ?)`,
		f.orgID, uuid.New(), f.old, f.old); err != nil {
		t.Fatalf("insert manual repo ownership: %v", err)
	}
	openRepoRows := func(teamID, source string) uint64 {
		t.Helper()
		return countRows(t, ctx, conn, `SELECT count() FROM team_repo_ownership FINAL WHERE org_id = ? AND team_id = ? AND source = ? AND valid_to IS NULL`, f.orgID, teamID, source)
	}
	if openRepoRows("OPS", "inferred") != 1 || openRepoRows(atlassian, "inferred") != 1 {
		t.Fatal("the derivation did not write one open derived row for each team: the test measures nothing")
	}

	// The derivation stamps its rows with the wall clock: the retire time is
	// taken after it.
	time.Sleep(5 * time.Millisecond)
	at := time.Now().UTC().Truncate(time.Millisecond)
	dry, err := RetireJiraProjectAsTeamRows(ctx, conn, f.orgID, at, true)
	if err != nil || dry.RepoOwnershipRows != 1 || dry.RepoOwnershipClosed != 0 || openRepoRows("OPS", "inferred") != 1 {
		t.Fatalf("dry run = %+v, %v; want 1 derived repo row found and none closed", dry, err)
	}
	real, err := RetireJiraProjectAsTeamRows(ctx, conn, f.orgID, at, false)
	if err != nil || real.RepoOwnershipRows != 1 || real.RepoOwnershipClosed != 1 {
		t.Fatalf("real run = %+v, %v; want 1 derived repo row closed", real, err)
	}
	if got := openRepoRows("OPS", "inferred"); got != 0 {
		t.Fatalf("the retired team still has %d open derived repo rows, want 0", got)
	}
	if n := countRows(t, ctx, conn, `SELECT count() FROM team_repo_ownership FINAL WHERE org_id = ? AND team_id = 'OPS' AND source = 'inferred' AND repo_full_name = 'acme/ops' AND valid_to IS NOT NULL AND toUnixTimestamp64Milli(assumeNotNull(valid_to)) = ?`, f.orgID, at.UnixMilli()); n != 1 {
		t.Fatalf("%d closed derived rows of the retired team at the retire time, want 1 (closed, not removed)", n)
	}
	if openRepoRows(atlassian, "inferred") != 1 || openRepoRows("OPS", "manual") != 1 {
		t.Fatal("a repo ownership row of another class was closed")
	}
	// The derivation after the retire agrees: it opens no row for the retired
	// team again and keeps the Atlassian team's.
	if _, _, _, _, err := service.Derive(ctx, f.orgID); err != nil {
		t.Fatalf("derivation after the retire: %v", err)
	}
	if openRepoRows("OPS", "inferred") != 0 || openRepoRows(atlassian, "inferred") != 1 {
		t.Fatalf("after the next derivation: retired team %d open derived rows, Atlassian team %d; want 0 and 1",
			openRepoRows("OPS", "inferred"), openRepoRows(atlassian, "inferred"))
	}
	if second, err := RetireJiraProjectAsTeamRows(ctx, conn, f.orgID, at.Add(time.Second), false); err != nil || second != (JiraProjectAsTeamRetireOutcome{}) {
		t.Fatalf("second run = %+v, %v; want all zero", second, err)
	}
}

// The reason the retire closes the derived repo rows itself: with no open
// project link left in the organization, the derivation closes nothing.
func TestTheRepoOwnershipDerivationAloneLeavesARetiredTeamsDerivedRowsOpen(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := retireFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString(), old: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)}
	repo := uuid.New()
	seedTeamRepoOwnershipRepos(t, ctx, conn, f.orgID, map[uuid.UUID]string{repo: "acme/ops"})
	f.team("jira", "OPS", "OPS", nil)
	f.ownership("jira", "OPS", "10001", "OPS", "native")
	seedWorkItem(t, ctx, conn, f.orgID, "jira:OPS-1", "jira", repo, "10001", f.old)
	service := TeamRepoOwnershipDerivationService{Conn: conn}
	if written, _, _, _, err := service.Derive(ctx, f.orgID); err != nil || written != 1 {
		t.Fatalf("first derivation: written=%d err=%v, want 1", written, err)
	}
	// Close the project link only, as the retire did before it closed the
	// derived rows too.
	if err := conn.Exec(ctx, jiraProjectAsTeamCloseOwnership, clickhouse.Named("org_id", f.orgID),
		clickhouse.Named("at", time.Now().UTC().Add(-time.Minute).Format("2006-01-02 15:04:05.000"))); err != nil {
		t.Fatalf("close the project link: %v", err)
	}
	_, retracted, ready, _, err := service.Derive(ctx, f.orgID)
	if err != nil {
		t.Fatalf("derivation after the close: %v", err)
	}
	open := countRows(t, ctx, conn, `SELECT count() FROM team_repo_ownership FINAL WHERE org_id = ? AND team_id = 'OPS' AND source = 'inferred' AND valid_to IS NULL`, f.orgID)
	if ready || retracted != 0 || open != 1 {
		t.Fatalf("ready=%v retracted=%d open=%d: the derivation now closes these rows by itself; "+
			"the close in RetireJiraProjectAsTeamRows may be a second writer of the same fact", ready, retracted, open)
	}
}
