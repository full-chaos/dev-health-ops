//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

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
