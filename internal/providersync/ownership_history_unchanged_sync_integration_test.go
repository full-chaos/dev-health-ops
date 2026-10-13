//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// An unchanged sync appends no ownership row (CHAOS-9548). The Linear writer is
// run three times over the same two facts: the table must hold two rows, one per
// fact, and the open facts must keep their first valid_from.
func TestUnchangedOwnershipSyncAppendsNoProjectOwnershipRow(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	if err := conn.Exec(ctx, "SYSTEM STOP MERGES team_project_ownership"); err != nil {
		t.Fatal(err)
	}
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	sink := LinearReferenceCatalogClickHouseEffects{Conn: conn, Lease: lease}
	const org = "ownership-history-org"
	base := time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)

	for n := 0; n < 3; n++ {
		at := base.Add(time.Duration(n) * time.Hour)
		var fresh []linearReferenceOwnershipRow
		for _, project := range []string{"p1", "p2"} {
			row := linearOwnershipTestRow(t, "linear:ENG", project, at)
			row.OrgID = org
			fresh = append(fresh, row)
		}
		rows, _, err := sink.SnapshotOwnership(ctx, org, fresh, at, linearOwnershipKindSnapshots(ctx, org, testSoleScope(),
			LinearReferenceCatalogEvidence{TeamsComplete: true, ProjectsComplete: true}, LinearReferenceCatalogResult{}, linearAskEveryAbsentRowGone)...)
		if err != nil {
			t.Fatal(err)
		}
		if err := sink.writeOwnership(ctx, rows); err != nil {
			t.Fatal(err)
		}
	}
	var stored uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM team_project_ownership WHERE org_id = ?", org).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != 2 {
		t.Fatalf("after 3 unchanged syncs of 2 facts the table holds %d rows, want 2", stored)
	}
}
