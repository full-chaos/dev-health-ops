//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// TestLinearOwnershipThreeSyncsLeaveOneOpenRowPerFact runs the real sink three
// times (CHAOS-8885 shape: a new open row at every sync) and reads back what
// the readers see.
func TestLinearOwnershipThreeSyncsLeaveOneOpenRowPerFact(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	sink := LinearReferenceCatalogClickHouseEffects{Conn: conn, Lease: lease}
	const org = "linear-snapshot-org"
	base := time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)

	sync := func(n int, projects []string, complete bool) {
		t.Helper()
		at := base.Add(time.Duration(n) * time.Hour)
		var fresh []linearReferenceOwnershipRow
		for _, project := range projects {
			row := linearOwnershipTestRow(t, "linear:ENG", project, at)
			row.OrgID = org
			fresh = append(fresh, row)
		}
		rows, _, err := sink.SnapshotOwnership(ctx, org, fresh, at, complete)
		if err != nil {
			t.Fatal(err)
		}
		if err := sink.writeOwnership(ctx, rows); err != nil {
			t.Fatal(err)
		}
	}
	open := func() map[string]int {
		t.Helper()
		out := map[string]int{}
		for _, row := range openGitLabOwnership(ctx, t, conn, org, "linear") {
			out[row.Project]++
		}
		return out
	}

	sync(0, []string{"p1", "p2"}, true)
	sync(1, []string{"p1", "p2"}, true)
	sync(2, []string{"p1", "p2"}, true)
	if got := open(); len(got) != 2 || got["p1"] != 1 || got["p2"] != 1 {
		t.Fatalf("after 3 syncs open rows = %v, want one per fact", got)
	}
	sync(3, []string{"p1"}, false)
	if got := open(); got["p2"] != 1 {
		t.Fatalf("an incomplete run closed a row: %v", got)
	}
	sync(4, []string{"p1"}, true)
	if got := open(); len(got) != 1 || got["p1"] != 1 {
		t.Fatalf("a complete run did not close the lost fact: %v", got)
	}
}
