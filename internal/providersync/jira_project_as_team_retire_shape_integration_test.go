//go:build integration

package providersync

import (
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
)

// teamAt writes a team row again, newer than the one in the store: teams holds
// one row per id, so this row is the team from then on.
func (f retireFixture) teamAt(provider, id, nativeKey string, updatedAt time.Time) {
	f.t.Helper()
	var key *string
	if nativeKey != "" {
		key = &nativeKey
	}
	if err := f.conn.Exec(f.ctx, `INSERT INTO teams (id, team_uuid, name, members, manual_members, project_keys, repo_patterns, is_active, updated_at, org_id, provider, native_team_key) VALUES (?, ?, ?, [], [], ?, [], 1, ?, ?, ?, ?)`,
		id, uuid.New(), "team "+id, []string{id}, updatedAt, f.orgID, provider, key); err != nil {
		f.t.Fatalf("write team %s again: %v", id, err)
	}
}

func (f retireFixture) openRepoRows(teamID string) uint64 {
	f.t.Helper()
	return countRows(f.t, f.ctx, f.conn, `SELECT count() FROM team_repo_ownership FINAL WHERE org_id = ? AND team_id = ? AND source = 'inferred' AND valid_to IS NULL`, f.orgID, teamID)
}

func (f retireFixture) openLeadRows(provider, teamID string) uint64 {
	f.t.Helper()
	return countRows(f.t, f.ctx, f.conn, `SELECT count() FROM team_memberships FINAL WHERE org_id = ? AND provider = ? AND team_id = ? AND source = 'native' AND valid_to IS NULL`, f.orgID, provider, teamID)
}

// derived runs the real repository-ownership derivation for a project link
// and a work item of that project, so the inferred row is the one the
// producer writes.
func (f retireFixture) derived(provider, workItem, projectID string, repo uuid.UUID, fullName string) {
	f.t.Helper()
	seedTeamRepoOwnershipRepos(f.t, f.ctx, f.conn, f.orgID, map[uuid.UUID]string{repo: fullName})
	seedWorkItem(f.t, f.ctx, f.conn, f.orgID, workItem, provider, repo, projectID, f.old)
}

func (f retireFixture) derive() {
	f.t.Helper()
	if _, _, _, _, err := (TeamRepoOwnershipDerivationService{Conn: f.conn}).Derive(f.ctx, f.orgID); err != nil {
		f.t.Fatalf("derive: %v", err)
	}
}

// What the old catalog wrote for a project (a team, its lead and its project
// link) is retired by the SHAPE of the project link, not by the team row as it
// reads today: the row may be written again after the catalog wrote it, and
// the lead and the derived repository rows must go with the link.
func TestRetireJiraProjectAsTeamRowsFollowsTheOwnershipShapeNotTheCurrentTeamRow(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	old := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	newer := old.Add(24 * time.Hour)
	at := time.Now().UTC().Add(time.Second).Truncate(time.Millisecond)
	newFixture := func() retireFixture {
		return retireFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString(), old: old}
	}
	retire := func(f retireFixture) JiraProjectAsTeamRetireOutcome {
		t.Helper()
		out, err := RetireJiraProjectAsTeamRows(ctx, conn, f.orgID, at, false)
		if err != nil {
			t.Fatalf("retire: %v", err)
		}
		return out
	}

	t.Run("an admin edited team keeps itself and loses the Jira rows of the project", func(t *testing.T) {
		f := newFixture()
		repo := uuid.New()
		f.team("jira", "OPS", "OPS", nil)
		f.ownership("jira", "OPS", "10001", "OPS", "native")
		f.membership("jira", "OPS", "jira:lead-ops")
		f.derived("jira", "jira:OPS-1", "10001", repo, "acme/r1")
		f.derive()
		if f.openRepoRows("OPS") != 1 {
			t.Fatal("the derivation wrote no derived row: the test measures nothing")
		}
		// The admin edits the team: the row is written again with an empty provider.
		f.teamAt("", "OPS", "", newer)
		// Links that do not hold the derived row open: a closed legacy link of
		// the team, and an open link of a team with the same id in another
		// organization.
		f.ownership("jira", "OPS", "10098", "OPSL", "jira_legacy")
		if err := conn.Exec(ctx, `INSERT INTO team_project_ownership (org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, 'jira', 'OPS', '10098', 'OPSL', 'jira_legacy', 1, 100, 10, ?, ?, ?)`,
			f.orgID, old, old.Add(time.Hour), old.Add(2*time.Hour)); err != nil {
			t.Fatalf("close the legacy link: %v", err)
		}
		newFixture().ownership("jira", "OPS", "10099", "OPSL", "jira_legacy")

		out := retire(f)
		if out.Teams != 0 || out.OwnershipClosed != 1 || out.MembershipClosed != 1 || out.RepoOwnershipClosed != 1 {
			t.Fatalf("outcome = %+v, want 0 teams, and 1 ownership, 1 membership, 1 derived repo row closed", out)
		}
		if f.openLeadRows("jira", "OPS") != 0 || f.openRepoRows("OPS") != 0 || f.openOwnership("jira", "OPS") != 0 {
			t.Fatal("a Jira row of the project is still open")
		}
		if !f.activeTeam("", "OPS") {
			t.Fatal("the admin's team was made inactive: only its Jira rows retire")
		}
		if again := retire(f); again != (JiraProjectAsTeamRetireOutcome{}) {
			t.Fatalf("second run = %+v, want all zero", again)
		}
	})

	t.Run("a team of another provider with the project key keeps its rows", func(t *testing.T) {
		f := newFixture()
		linearRepo := uuid.New()
		f.team("jira", "ENG", "ENG", nil)
		f.ownership("jira", "ENG", "10001", "ENG", "native")
		f.membership("jira", "ENG", "jira:lead-eng")
		// The Linear team of the same key is the row from now on.
		f.teamAt("linear", "ENG", "ENG", newer)
		f.ownership("linear", "ENG", "p-1", "ENG", "native")
		f.membership("linear", "ENG", "linear:member-1")
		f.derived("linear", "linear:ENG-1", "p-1", linearRepo, "acme/linear")
		f.derive()
		if f.openRepoRows("ENG") == 0 {
			t.Fatal("the derivation wrote no derived row for the Linear team: the test measures nothing")
		}
		repoRowsBefore := f.openRepoRows("ENG")

		out := retire(f)
		if out.MembershipClosed != 1 || out.OwnershipClosed != 1 || out.RepoOwnershipClosed != 0 || out.TeamsRetired != 0 {
			t.Fatalf("outcome = %+v, want the Jira lead and the Jira project link closed, no derived row, no team", out)
		}
		if f.openLeadRows("jira", "ENG") != 0 {
			t.Fatal("the Jira lead stays open on the team of the same key")
		}
		if f.openLeadRows("linear", "ENG") != 1 || f.openOwnership("linear", "ENG") != 1 || !f.activeTeam("linear", "ENG") {
			t.Fatal("a row of the Linear team was closed")
		}
		if got := f.openRepoRows("ENG"); got != repoRowsBefore {
			t.Fatalf("derived repo rows of team ENG: %d open, want %d: the Linear team's rows stay", got, repoRowsBefore)
		}
	})

	t.Run("a run after the project link closed still closes the lead and the derived row", func(t *testing.T) {
		f := newFixture()
		repo := uuid.New()
		f.team("jira", "OPS", "OPS", nil)
		f.ownership("jira", "OPS", "10001", "OPS", "native")
		f.membership("jira", "OPS", "jira:lead-ops")
		f.derived("jira", "jira:OPS-1", "10001", repo, "acme/r1")
		f.derive()
		f.teamAt("", "OPS", "", newer)
		// The part way failure: the link closed, the lead and the derived row did not.
		if err := conn.Exec(ctx, jiraProjectAsTeamCloseOwnership, clickhouse.Named("org_id", f.orgID),
			clickhouse.Named("at", at.Add(-time.Minute).Format("2006-01-02 15:04:05.000"))); err != nil {
			t.Fatalf("close the link: %v", err)
		}
		if f.openLeadRows("jira", "OPS") != 1 || f.openRepoRows("OPS") != 1 {
			t.Fatal("the setup closed more than the link")
		}
		out := retire(f)
		if out.OwnershipRows != 0 || out.MembershipClosed != 1 || out.RepoOwnershipClosed != 1 {
			t.Fatalf("outcome = %+v, want the lead and the derived row closed with no link left", out)
		}
	})

	t.Run("a project-as-team row with no project link still takes its lead and derived row with it", func(t *testing.T) {
		f := newFixture()
		repo := uuid.New()
		f.team("jira", "NOLINK", "NOLINK", nil)
		f.membership("jira", "NOLINK", "jira:lead-nolink")
		seedTeamRepoOwnershipRepos(t, ctx, conn, f.orgID, map[uuid.UUID]string{repo: "acme/nolink"})
		if err := conn.Exec(ctx, teamRepoOwnershipInsert+` VALUES (?, 'github', 'NOLINK', ?, 'acme/nolink', 'exact', 'inferred', 0, 100, 0, ?, NULL, ?)`,
			f.orgID, repo, old, old); err != nil {
			t.Fatalf("insert derived row: %v", err)
		}
		out := retire(f)
		if out.TeamsRetired != 1 || out.MembershipClosed != 1 || out.RepoOwnershipClosed != 1 {
			t.Fatalf("outcome = %+v, want the team, its lead and its derived row retired", out)
		}
	})

	t.Run("a team that keeps another open link keeps its derived rows across runs", func(t *testing.T) {
		f := newFixture()
		retiredRepo, keptRepo := uuid.New(), uuid.New()
		f.team("jira", "OPS", "OPS", nil)
		f.ownership("jira", "OPS", "10001", "OPS", "native")
		f.membership("jira", "OPS", "jira:lead-ops")
		f.ownership("jira", "OPS", "10099", "OPSL", "jira_legacy")
		f.derived("jira", "jira:OPS-1", "10001", retiredRepo, "acme/retired")
		f.derived("jira", "jira:OPSL-1", "10099", keptRepo, "acme/kept")
		f.derive()
		if f.openRepoRows("OPS") != 2 {
			t.Fatal("the derivation did not write the two derived rows: the test measures nothing")
		}
		f.teamAt("", "OPS", "", newer)

		for cycle := 1; cycle <= 3; cycle++ {
			out := retire(f)
			if out.RepoOwnershipClosed != 0 {
				t.Fatalf("cycle %d: the retire closed %d derived rows of a team that keeps a project link", cycle, out.RepoOwnershipClosed)
			}
			f.derive()
			if got := countRows(t, ctx, conn, `SELECT count() FROM team_repo_ownership FINAL WHERE org_id = ? AND team_id = 'OPS' AND repo_full_name = 'acme/kept' AND source = 'inferred' AND valid_to IS NULL`, f.orgID); got != 1 {
				t.Fatalf("cycle %d: the row of the kept link is not open after the derivation (%d)", cycle, got)
			}
		}
		if got := countRows(t, ctx, conn, `SELECT count() FROM team_repo_ownership FINAL WHERE org_id = ? AND team_id = 'OPS' AND repo_full_name = 'acme/retired' AND source = 'inferred' AND valid_to IS NULL`, f.orgID); got != 0 {
			t.Fatalf("the derivation left the row of the retired link open (%d)", got)
		}
		if f.openLeadRows("jira", "OPS") != 0 || f.openOwnership("jira", "OPS") != 1 || !f.activeTeam("", "OPS") {
			t.Fatal("the lead did not close, the kept link closed, or the admin's team went inactive")
		}
	})

	t.Run("a team of another provider with the project key keeps its derived rows when it has no link", func(t *testing.T) {
		f := newFixture()
		for _, provider := range []string{"linear", "github", "gitlab"} {
			id := "K" + provider
			repo := uuid.New()
			f.team("jira", id, id, nil)
			f.ownership("jira", id, "1"+provider, id, "native")
			f.teamAt(provider, id, id, newer)
			seedTeamRepoOwnershipRepos(t, ctx, conn, f.orgID, map[uuid.UUID]string{repo: "acme/" + provider})
			if err := conn.Exec(ctx, teamRepoOwnershipInsert+` VALUES (?, 'github', ?, ?, ?, 'exact', 'inferred', 0, 100, 0, ?, NULL, ?)`,
				f.orgID, id, repo, "acme/"+provider, old, old); err != nil {
				t.Fatalf("insert derived row of the %s team: %v", provider, err)
			}
		}
		out := retire(f)
		if out.RepoOwnershipRows != 0 || out.RepoOwnershipClosed != 0 {
			t.Fatalf("outcome = %+v, want no derived row found", out)
		}
		for _, provider := range []string{"linear", "github", "gitlab"} {
			if f.openRepoRows("K"+provider) != 1 {
				t.Fatalf("the derived row of the %s team was closed", provider)
			}
		}
	})

	t.Run("rows one clause away from the shape stay", func(t *testing.T) {
		f := newFixture()
		other := newFixture()
		const ari = "ari:cloud:identity::team/"
		const atlassian = "0b1f3b0a-6d0c-4f5e-9a55-1c2d3e4f5a6b"
		// A legacy link whose team id is its project key (source), an admin
		// team whose id is the shape only in another organization (org), and
		// an Atlassian-looking team whose id reads like a project key (ARI).
		f.team("", "LEG", "", nil)
		f.ownership("jira", "LEG", "10010", "LEG", "jira_legacy")
		f.membership("jira", "LEG", "jira:lead-leg")
		f.team("", "ZED", "", nil)
		f.membership("jira", "ZED", "jira:lead-zed")
		other.ownership("jira", "ZED", "10011", "ZED", "native")
		f.teamAt("jira", "KEYLIKE", ari+atlassian, newer)
		f.ownership("jira", "KEYLIKE", "10012", "KEYLIKE", "native")
		f.membership("jira", "KEYLIKE", "jira:member-keylike")
		// A native link of a team id that is not its project key (an ops team).
		f.team("", "OPSTEAM", "", nil)
		f.ownership("jira", "OPSTEAM", "10013", "OPS", "native")
		f.membership("jira", "OPSTEAM", "jira:member-opsteam")
		// A membership of another provider or source on a project-as-team.
		f.team("jira", "SEC", "SEC", nil)
		f.ownership("jira", "SEC", "10014", "SEC", "native")
		f.membership("github", "SEC", "github:member")

		out := retire(f)
		if out.MembershipClosed != 0 {
			t.Fatalf("outcome = %+v, want no membership closed", out)
		}
		for _, id := range []string{"LEG", "ZED", "KEYLIKE", "OPSTEAM"} {
			if f.openLeadRows("jira", id) != 1 {
				t.Fatalf("the Jira membership of %s was closed", id)
			}
		}
		if f.openMembership("github", "SEC") != 1 {
			t.Fatal("a github membership was closed")
		}
	})
}
