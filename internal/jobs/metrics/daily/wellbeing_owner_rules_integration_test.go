//go:build integration

package daily

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// The rules of the ownership-first team of team_wellbeing that the first tests
// did not pin: an inactive owner never wins, the ownership read error fails
// the family (never an empty map), the pattern fallback, and the stale-key
// scope. They use the helpers of wellbeing_ownership_integration_test.go.

func ownerRulesTeam(t *testing.T, ctx context.Context, conn driver.Conn, id string, active uint8, patterns []string, at time.Time) {
	t.Helper()
	wellbeingOwnershipExec(t, ctx, conn, "insert team "+id, `INSERT INTO teams
    (id, team_uuid, name, members, repo_patterns, updated_at, last_synced, org_id, provider, is_active)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, uuid.NewSHA1(uuid.NameSpaceURL, []byte("team:"+id)), "Team "+id, []string{}, patterns, at, at, wellbeingOwnershipOrg, "github", active)
}

func ownerRulesOwns(t *testing.T, ctx context.Context, conn driver.Conn, teamID string, repo uuid.UUID, name string, primary uint8, spec uint16, at time.Time) {
	t.Helper()
	wellbeingOwnershipExec(t, ctx, conn, "insert ownership "+teamID, `INSERT INTO team_repo_ownership
    (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, valid_from, updated_at)
    VALUES (?, 'github', ?, ?, ?, 'exact', 'native', ?, ?, ?, ?)`,
		wellbeingOwnershipOrg, teamID, repo, name, primary, spec, at, at)
}

// 5. an INACTIVE owner team must not give the team.
func TestWellbeingInactiveOwnerNeverWins(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	t0 := wellbeingOwnershipDay.Add(-72 * time.Hour)
	r1 := uuid.MustParse("00000000-0000-4000-8000-0000000000d1")
	r2 := uuid.MustParse("00000000-0000-4000-8000-0000000000d2")
	ownerRulesTeam(t, ctx, conn, "old-bare-id", 0, nil, t0)
	ownerRulesTeam(t, ctx, conn, "github:active", 1, nil, t0)
	wellbeingOwnershipRepo(t, ctx, conn, "github", "acme/one", r1, "a@example.com")
	wellbeingOwnershipRepo(t, ctx, conn, "github", "acme/two", r2, "a@example.com")
	// r1: inactive team ranks first, active team ranks lower -> active wins.
	ownerRulesOwns(t, ctx, conn, "old-bare-id", r1, "acme/one", 1, 10, t0)
	ownerRulesOwns(t, ctx, conn, "github:active", r1, "acme/one", 0, 5, t0)
	// r2: only the inactive team -> unassigned, never the old id.
	ownerRulesOwns(t, ctx, conn, "old-bare-id", r2, "acme/two", 1, 10, t0)

	wellbeingOwnershipCompute(t, ctx, conn, []uuid.UUID{r1, r2}, wellbeingOwnershipDay.Add(30*time.Hour))
	got := wellbeingOwnershipRead(t, ctx, conn)
	want := map[string]uint32{"github:active|" + r1.String(): 2, "unassigned|" + r2.String(): 2}
	if len(got) != len(want) {
		t.Errorf("rows = %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("row %s = %d, want %d (all %v)", k, got[k], v, got)
		}
	}
}

// 3. ownership outranks repo_patterns; repo_patterns still serves an unowned repo.
func TestWellbeingOwnershipOutranksPatternAndPatternStillFallsBack(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	t0 := wellbeingOwnershipDay.Add(-72 * time.Hour)
	owned := uuid.MustParse("00000000-0000-4000-8000-0000000000e1")
	patterned := uuid.MustParse("00000000-0000-4000-8000-0000000000e2")
	ownerRulesTeam(t, ctx, conn, "github:owner", 1, nil, t0)
	ownerRulesTeam(t, ctx, conn, "github:pattern", 1, []string{"acme/owned", "acme/patterned"}, t0)
	wellbeingOwnershipRepo(t, ctx, conn, "github", "acme/owned", owned, "a@example.com")
	wellbeingOwnershipRepo(t, ctx, conn, "github", "acme/patterned", patterned, "a@example.com")
	ownerRulesOwns(t, ctx, conn, "github:owner", owned, "acme/owned", 1, 10, t0)

	wellbeingOwnershipCompute(t, ctx, conn, []uuid.UUID{owned, patterned}, wellbeingOwnershipDay.Add(30*time.Hour))
	got := wellbeingOwnershipRead(t, ctx, conn)
	want := map[string]uint32{"github:owner|" + owned.String(): 2, "github:pattern|" + patterned.String(): 2}
	if len(got) != len(want) {
		t.Errorf("rows = %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("row %s = %d, want %d (all %v)", k, got[k], v, got)
		}
	}
}

// 2d. a failed ownership read fails the family and writes nothing.
func TestWellbeingOwnershipReadErrorFailsTheFamily(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	t0 := wellbeingOwnershipDay.Add(-72 * time.Hour)
	repo := uuid.MustParse("00000000-0000-4000-8000-0000000000f1")
	ownerRulesTeam(t, ctx, conn, "github:owner", 1, nil, t0)
	wellbeingOwnershipRepo(t, ctx, conn, "github", "acme/x", repo, "a@example.com")
	ownerRulesOwns(t, ctx, conn, "github:owner", repo, "acme/x", 1, 10, t0)
	wellbeingOwnershipExec(t, ctx, conn, "drop ownership", "DROP TABLE team_repo_ownership")

	run := Run{ID: uuid.NewString(), OrganizationID: wellbeingOwnershipOrg, TargetDay: wellbeingOwnershipDay}
	executor, err := NewTeamWellbeingExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	_, err = executor.ComputeFamily(ctx, run, Partition{ID: uuid.NewString(), RunID: run.ID, RepoIDs: []RepositoryID{RepositoryID(repo.String())}})
	if err == nil || !strings.Contains(err.Error(), "ownership") {
		t.Fatalf("a failed ownership read must fail the family, got err=%v", err)
	}
	if got := wellbeingOwnershipRead(t, ctx, conn); len(got) != 0 {
		t.Errorf("rows were written despite the failed read: %v", got)
	}
}

// 4. a recompute retracts the stale unassigned key only for the partition repos.
func TestWellbeingRecomputeRetractsOnlyPartitionRepos(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	t0 := wellbeingOwnershipDay.Add(-72 * time.Hour)
	r5 := uuid.MustParse("00000000-0000-4000-8000-000000000051")
	r6 := uuid.MustParse("00000000-0000-4000-8000-000000000061")
	ownerRulesTeam(t, ctx, conn, "github:owner", 1, nil, t0)
	wellbeingOwnershipRepo(t, ctx, conn, "github", "acme/five", r5, "a@example.com")
	wellbeingOwnershipRepo(t, ctx, conn, "github", "acme/six", r6, "a@example.com")
	wellbeingOwnershipCompute(t, ctx, conn, []uuid.UUID{r5, r6}, wellbeingOwnershipDay.Add(30*time.Hour))
	ownerRulesOwns(t, ctx, conn, "github:owner", r5, "acme/five", 1, 10, t0)
	ownerRulesOwns(t, ctx, conn, "github:owner", r6, "acme/six", 1, 10, t0)
	wellbeingOwnershipCompute(t, ctx, conn, []uuid.UUID{r5}, wellbeingOwnershipDay.Add(40*time.Hour))
	got := wellbeingOwnershipRead(t, ctx, conn)
	t.Logf("rows after recompute of [r5] only: %v", got)
	if got["github:owner|"+r5.String()] != 2 || got["unassigned|"+r5.String()] != 0 {
		t.Errorf("partition repo r5 not repaired: %v", got)
	}
	if got["unassigned|"+r6.String()] != 2 {
		t.Errorf("repo r6 outside the partition was touched (expected stale unassigned=2 to remain): %v", got)
	}
	wellbeingOwnershipCompute(t, ctx, conn, []uuid.UUID{r5, r6}, wellbeingOwnershipDay.Add(50*time.Hour))
	got = wellbeingOwnershipRead(t, ctx, conn)
	t.Logf("rows after recompute of [r5,r6]: %v", got)
	if got["github:owner|"+r6.String()] != 2 || got["unassigned|"+r6.String()] != 0 {
		t.Errorf("full recompute did not repair r6: %v", got)
	}
}
