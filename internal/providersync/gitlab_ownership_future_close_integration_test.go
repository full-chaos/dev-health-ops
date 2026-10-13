//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// CHAOS-9548: an open row whose valid_to is still in the future is not a repeat of a fresh row, which has no
// close time. The fresh row is written, as before the change, and the row is open again.
func TestGitLabOwnershipRowThatClosesInTheFutureIsNotRepeated(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	sink := GitLabTeamCatalogClickHouseEffects{Conn: conn, Lease: lease}
	const org = "gitlab-future-close-org"
	at := time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)
	seeded := mustGitLabOwnershipRow(org, "gl:org", "PLAT", gitlabTeamCatalogBaseSpecificity, at)

	snapshot := func(fresh gitlabTeamCatalogOwnershipRow) ([]gitlabTeamCatalogOwnershipRow, SnapshotPlan) {
		t.Helper()
		grants := GitLabGroupProjectGrantKind([]string{fresh.TeamID}).testSnapshot(testSoleScope(), testProof(true))
		rows, plan, err := sink.SnapshotOwnership(ctx, org, []gitlabTeamCatalogOwnershipRow{fresh}, []string{fresh.TeamID}, grants, at.Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		return rows, plan
	}

	if err := sink.writeOwnership(ctx, []gitlabTeamCatalogOwnershipRow{seeded}); err != nil {
		t.Fatal(err)
	}
	if rows, plan := snapshot(seeded); len(rows) != 0 || plan.UnchangedCount() != 1 {
		t.Fatalf("control: an open row with no close time: rows=%d unchanged=%d, want 0 written and 1 repeat", len(rows), plan.UnchangedCount())
	}

	future := time.Now().UTC().Add(time.Hour)
	closing := seeded
	closing.ValidTo = &future
	closing.UpdatedAt = at.Add(time.Minute)
	if err := sink.writeOwnership(ctx, []gitlabTeamCatalogOwnershipRow{closing}); err != nil {
		t.Fatal(err)
	}
	rows, plan := snapshot(seeded)
	if len(rows) != 1 || rows[0].ValidTo != nil || plan.UnchangedCount() != 0 {
		t.Fatalf("a row that closes in the future: rows=%+v unchanged=%d, want the fresh open row written", rows, plan.UnchangedCount())
	}
}
