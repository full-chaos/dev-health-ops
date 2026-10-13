package providersync

import (
	"testing"
	"time"
)

// CHAOS-9548: the shared rule marks a fresh row that repeats the open row of its fact, on the first-seen
// valid_from, with the same shape. A change of a column, a fact with no open row, a fact with an unknown
// shape, and a repeat of a later duplicate are not repeats.
func TestPlanOwnershipSnapshotMarksOnlyARepeatOfTheFirstSeenOpenRow(t *testing.T) {
	first := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	later := first.Add(time.Hour)
	now := first.Add(48 * time.Hour)
	key := "OPS"
	shape := ProjectOwnershipShape(&key, 1, 100, 10)
	row := func(project string, validFrom time.Time, shape string) OwnershipSnapshotRow {
		return OwnershipSnapshotRow{TeamID: "t", ProjectID: testPID(project), Source: "native", ValidFrom: validFrom, Shape: shape}
	}
	kind := testEveryRowKind(EmptyIsAnAnswer).testSnapshot(testSoleScope(), testProof(true))
	cases := map[string]struct {
		fresh OwnershipSnapshotRow
		open  []OwnershipSnapshotRow
		want  bool
	}{
		"same shape on the open row":           {row("1", now, shape), []OwnershipSnapshotRow{row("1", first, shape)}, true},
		"another shape":                        {row("1", now, ProjectOwnershipShape(&key, 1, 100, 11)), []OwnershipSnapshotRow{row("1", first, shape)}, false},
		"no open row":                          {row("1", now, shape), []OwnershipSnapshotRow{row("2", first, shape)}, false},
		"unknown fresh shape":                  {row("1", now, ""), []OwnershipSnapshotRow{row("1", first, shape)}, false},
		"unknown open shape":                   {row("1", now, shape), []OwnershipSnapshotRow{row("1", first, "")}, false},
		"the first-seen row repeats":           {row("1", now, shape), []OwnershipSnapshotRow{row("1", later, shape), row("1", first, shape)}, true},
		"the first-seen row has another shape": {row("1", now, shape), []OwnershipSnapshotRow{row("1", later, shape), row("1", first, "x")}, false},
	}
	for name, tc := range cases {
		plan := PlanOwnershipSnapshot([]OwnershipSnapshotRow{tc.fresh}, tc.open, now, kind)
		if got := plan.Unchanged[0]; got != tc.want {
			t.Errorf("%s: unchanged = %v, want %v", name, got, tc.want)
		}
		if plan.UnchangedCount() != map[bool]int{true: 1, false: 0}[tc.want] {
			t.Errorf("%s: count = %d", name, plan.UnchangedCount())
		}
	}
}

func TestOwnershipShapesSeparateEveryColumnAndNullFromEmpty(t *testing.T) {
	empty, key := "", "K"
	repo := "11111111-1111-4111-8111-111111111111"
	base := RepoOwnershipShape(&repo, "exact", 1, 100, 10)
	for name, other := range map[string]string{
		"repo_id": RepoOwnershipShape(nil, "exact", 1, 100, 10), "match_type": RepoOwnershipShape(&repo, "pattern", 1, 100, 10),
		"is_primary": RepoOwnershipShape(&repo, "exact", 2, 100, 10), "specificity": RepoOwnershipShape(&repo, "exact", 1, 101, 10),
		"priority": RepoOwnershipShape(&repo, "exact", 1, 100, 11),
	} {
		if other == base {
			t.Errorf("repo shape ignores %s", name)
		}
	}
	if ProjectOwnershipShape(nil, 1, 1, 1) == ProjectOwnershipShape(&empty, 1, 1, 1) || ProjectOwnershipShape(&key, 1, 1, 1) == ProjectOwnershipShape(&empty, 1, 1, 1) {
		t.Error("project shape confuses a NULL, an empty and a set project key")
	}
}
