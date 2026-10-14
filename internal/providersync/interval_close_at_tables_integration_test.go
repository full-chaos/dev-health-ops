//go:build integration

package providersync

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func emptyIntervals(t *testing.T, f carryFixture, table string) uint64 {
	t.Helper()
	return f.count("SELECT count() FROM " + table + " FINAL WHERE org_id = ? AND valid_to IS NOT NULL AND valid_to <= valid_from")
}

// The Jira retire closes the memberships and project links of a retired team
// that open after the run, 1 ms past their open, never at it.
func TestJiraRetireOfFutureMembershipAndProjectLinkWritesNoEmptyInterval(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	at := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	f := retireFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString(), old: at.Add(time.Hour)}
	f.team("jira", "OPS", "OPS", nil)
	f.ownership("jira", "OPS", "10001", "OPS", "native")
	f.membership("jira", "OPS", "lead-1")
	out, err := RetireJiraProjectAsTeamRows(ctx, conn, f.orgID, at, false)
	if err != nil || out.MembershipClosed != 1 || out.OwnershipClosed != 1 {
		t.Fatalf("retire = %+v, %v; want the membership and the project link closed", out, err)
	}
	for _, table := range []string{"team_memberships", "team_project_ownership"} {
		if n := countRows(t, ctx, conn, "SELECT count() FROM "+table+" FINAL WHERE org_id = ? AND valid_to IS NOT NULL AND valid_to <= valid_from", f.orgID); n != 0 {
			t.Errorf("%s: %d empty intervals after the retire", table, n)
		}
	}
}

// The team-id carry closes the bare-id memberships and project links that open
// after the run, 1 ms past their open, never at it.
func TestTeamIDCarryOfFutureMembershipAndProjectLinkWritesNoEmptyInterval(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	f.team("linear", "ENG", carryPtr("ENG"), nil, 1, carryOld, nil, nil)
	future := carryAt.Add(time.Hour)
	f.membership("linear", "ENG", "m1", future, nil)
	f.exec(`INSERT INTO team_project_ownership (org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, updated_at) VALUES (?, 'linear', 'ENG', 'p1', NULL, 'native', 1, 100, 10, ?, ?)`,
		f.orgID, future, carryOld)
	out, err := CarryTeamIDs(ctx, conn, f.orgID, carryAt, false)
	if err != nil || out.Memberships != 1 || out.ProjectOwnership != 1 {
		t.Fatalf("carry = %+v, %v; want the membership and the project link moved", out, err)
	}
	for _, table := range []string{"team_memberships", "team_project_ownership"} {
		if n := emptyIntervals(t, f, table); n != 0 {
			t.Errorf("%s: %d empty intervals after the carry", table, n)
		}
	}
}

// An empty interval covers no instant, so the GitLab provider_access readers
// never read one as open, even when it opens in the future.
func TestGitLabProviderAccessReadersSkipAFutureEmptyInterval(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	const org = "org-gl-future-empty"
	future := time.Now().UTC().Add(24 * time.Hour).Truncate(time.Millisecond)
	if err := conn.Exec(ctx, `INSERT INTO team_project_ownership (org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, ?, 'gl:grp/sub', 'p1', NULL, ?, 1, 100, 10, ?, ?, ?)`,
		org, gitlabTeamCatalogProvider, gitlabTeamCatalogSource, future, future, future); err != nil {
		t.Fatal(err)
	}
	sink := GitLabTeamCatalogClickHouseEffects{Conn: conn}
	open, err := sink.openProviderAccessOwnership(ctx, org, []string{"gl:grp/sub"})
	if err != nil {
		t.Fatal(err)
	}
	if len(open) != 0 {
		t.Fatalf("openProviderAccessOwnership read %d empty interval(s) as open", len(open))
	}
	teams, err := sink.OpenOwnershipTeamsNotListed(ctx, org, "gl:grp", []string{"gl:other"})
	if err != nil {
		t.Fatal(err)
	}
	if len(teams) != 0 {
		t.Fatalf("OpenOwnershipTeamsNotListed read %d empty interval(s) as open", len(teams))
	}
}
