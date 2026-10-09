//go:build integration

package providersync

import (
	"reflect"
	"testing"
)

// TestLinearCollectorReportsAnAbandonedCloseByKindAndReason pins the loud half
// of an abandoned close through the real collector and a real store: the
// result says the snapshot was not complete, the shared counter names the fact
// kind and the reason, and the Linear counter names the cause. Not parallel:
// it reads process-wide counters.
func TestLinearCollectorReportsAnAbandonedCloseByKindAndReason(t *testing.T) {
	unstated := `{"id":"unstated","name":"P","description":"","status":{"id":"s","name":"Active","type":"started"},"trashed":false,"targetDate":"","archivedAt":null,"url":"","lead":null,"teams":{"nodes":[{"id":"team-raw-1","key":"QA"}]}}`
	for _, c := range []struct {
		name         string
		projectNodes string
		wantShared   map[string]int64
		wantLinear   map[string]int64
	}{
		{"zero project nodes and a team present", ``,
			map[string]int64{"linear/linear_project_ownership/" + SnapshotEmptyAnswer: 1},
			map[string]int64{SnapshotEmptyAnswer: 1}},
		{"a project whose teams page end is not stated", linearProjectNodeJSON("other", "QA") + `,` + unstated,
			map[string]int64{"linear/linear_project_ownership/" + linearSnapshotProjectsNotRead: 1},
			map[string]int64{"project_teams_page_end_not_stated": 1}},
		{"a project-team link without a key", linearProjectNodeJSON("other", "QA") + `,` + linearProjectNodeJSON("nokey", ""),
			map[string]int64{"linear/linear_project_ownership/" + linearSnapshotKeylessLink: 1},
			map[string]int64{linearSnapshotKeylessLink: 1}},
		{"control: a complete run that closes the seeded row reports nothing", linearProjectNodeJSON("other", "QA"),
			map[string]int64{}, map[string]int64{}},
	} {
		t.Run(c.name, func(t *testing.T) {
			shared, linear := snapshotAbandonedCounts(t), linearIncompleteCounts(t)
			open, result := runLinearCollectorOverSeededOwnership(t, c.projectNodes)
			abandoned := len(c.wantShared) > 0
			if result.OwnershipSnapshotIncomplete != abandoned || (abandoned && (open["keep"] != 1 || result.OwnershipRetracted != 0)) {
				t.Fatalf("open=%v retracted=%d incomplete=%v, want incomplete=%v and the seeded row kept when it is",
					open, result.OwnershipRetracted, result.OwnershipSnapshotIncomplete, abandoned)
			}
			if got := snapshotAbandonedMoved(t, shared); !reflect.DeepEqual(got, c.wantShared) {
				t.Errorf("%s moved %v, want %v", snapshotCloseAbandonedName, got, c.wantShared)
			}
			moved := map[string]int64{}
			for reason, n := range linearIncompleteCounts(t) {
				if d := n - linear[reason]; d != 0 {
					moved[reason] = d
				}
			}
			if !reflect.DeepEqual(moved, c.wantLinear) {
				t.Errorf("%s moved %v, want %v", linearOwnershipSnapshotIncompleteName, moved, c.wantLinear)
			}
		})
	}
}
