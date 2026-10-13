//go:build integration

package providersync

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

const emptyInferredIntervals = `SELECT count() FROM team_repo_ownership FINAL WHERE org_id = ? AND source = 'inferred' AND valid_to IS NOT NULL AND valid_to <= valid_from`

// Every closer of an inferred row, not only the derivation, keeps the close
// after the open: a row that opens after the closing run is closed 1 ms past
// its open, never at it.
func TestJiraRetireOfAFutureInferredRowWritesNoEmptyInterval(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := retireFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString(), old: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)}
	f.team("jira", "OPS", "OPS", nil)
	f.ownership("jira", "OPS", "10001", "OPS", "native")
	at := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	future := at.Add(time.Hour)
	if err := conn.Exec(ctx, teamRepoOwnershipInsert+` VALUES (?, 'github', 'OPS', ?, 'acme/ops', 'exact', 'inferred', 1, 100, 10, ?, NULL, ?)`,
		f.orgID, uuid.New(), future, f.old); err != nil {
		t.Fatal(err)
	}
	if out, err := RetireJiraProjectAsTeamRows(ctx, conn, f.orgID, at, false); err != nil || out.RepoOwnershipClosed != 1 {
		t.Fatalf("retire = %+v, %v; want the future row closed", out, err)
	}
	if n := countRows(t, ctx, conn, emptyInferredIntervals, f.orgID); n != 0 {
		t.Fatalf("%d empty inferred intervals after the retire", n)
	}
}

func TestTeamIDCarryOfAFutureInferredRowWritesNoEmptyInterval(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	f.team("linear", "ENG", carryPtr("ENG"), nil, 1, carryOld, nil, nil)
	future := carryAt.Add(time.Hour)
	f.exec(`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, updated_at) VALUES (?, 'github', 'ENG', NULL, 'acme/api', 'exact', 'inferred', 1, 100, 10, ?, ?)`,
		f.orgID, future, carryOld)
	if out, err := CarryTeamIDs(ctx, conn, f.orgID, carryAt, false); err != nil || out.RepoOwnership != 1 {
		t.Fatalf("carry = %+v, %v; want the future row moved", out, err)
	}
	if n := f.count(emptyInferredIntervals); n != 0 {
		t.Fatalf("%d empty inferred intervals after the carry", n)
	}
}

// An empty interval covers no instant, so the derivation's open-row read never
// counts one, even when it opens in the future.
func TestDerivationActiveReaderSkipsAFutureEmptyInterval(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	const org = "org-future-empty"
	future := time.Now().UTC().Add(24 * time.Hour).Truncate(time.Millisecond)
	if err := conn.Exec(ctx, teamRepoOwnershipInsert+` VALUES (?, 'github', 'future-empty', ?, 'acme/api', 'exact', 'inferred', 1, 100, 10, ?, ?, ?)`,
		org, uuid.New(), future, future, future); err != nil {
		t.Fatal(err)
	}
	rows, err := loadTeamRepoOwnershipActiveInferredRows(ctx, conn, org)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("active reader admitted %d empty interval(s)", len(rows))
	}
}
