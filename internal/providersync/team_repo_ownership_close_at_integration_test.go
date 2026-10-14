//go:build integration

package providersync

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

// A retraction at the instant a row opened writes an interval that is not
// empty, and the tenure reader counts that 1 ms row as a real interval while
// it skips the supersede tombstone (valid_to == valid_from).
func TestRetractAtOpenInstantKeepsARealIntervalAndTombstoneIsSkipped(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	const org = "org-close-at"
	opened := time.Date(2026, 6, 7, 8, 9, 10, 0, time.UTC)
	insert := func(team string, validTo time.Time) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, 'github', ?, NULL, 'acme/repo', 'exact', 'inferred', 1, 100, 10, ?, ?, ?)`,
			org, team, opened, validTo, opened); err != nil {
			t.Fatal(err)
		}
	}
	insert("team-tombstone", opened)

	row := teamRepoOwnershipActiveRow{
		Provider: "github", TeamID: "team-retracted", RepoID: uuid.New(), RepoFullName: "acme/repo",
		MatchType: "exact", IsPrimary: 1, Specificity: 100, Priority: 10, ValidFrom: opened,
	}
	if _, err := retractTeamRepoOwnershipRows(ctx, conn, org, opened, []teamRepoOwnershipActiveRow{row}); err != nil {
		t.Fatal(err)
	}

	var empty uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM team_repo_ownership FINAL WHERE org_id = ? AND team_id = 'team-retracted' AND valid_to IS NOT NULL AND valid_to <= valid_from`, org).Scan(&empty); err != nil {
		t.Fatal(err)
	}
	if empty != 0 {
		t.Fatalf("retract wrote %d empty intervals", empty)
	}

	tenure, err := loadTeamRepoOwnershipRetractedUntil(ctx, conn, org)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := tenure.retractedUntil[teamRepoOwnershipClaim{"github", "acme/repo", "team-tombstone"}]; ok {
		t.Error("a tombstone row must not read as a retraction")
	}
	got, ok := tenure.retractedUntil[teamRepoOwnershipClaim{"github", "acme/repo", "team-retracted"}]
	if !ok || !got.Equal(opened.Add(time.Millisecond)) {
		t.Errorf("the 1 ms row must read as a real interval ending %v, got %v (found=%v)", opened.Add(time.Millisecond), got, ok)
	}
}
