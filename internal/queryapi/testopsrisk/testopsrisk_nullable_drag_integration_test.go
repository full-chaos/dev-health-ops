//go:build integration

package testopsrisk

import (
	"fmt"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

const nullHours = "NULL"

// dragRow inserts one testops_quality_drag row; a part given as nullHours is NULL.
func dragRow(t *testing.T, run func(string), repo, day, drag, failure, flake, queue, retry, computed string) {
	t.Helper()
	run(fmt.Sprintf(`INSERT INTO testops_quality_drag (repo_id, day, drag_hours, failure_rework_hours, flake_investigation_hours, queue_wait_hours, retry_overhead_hours, org_id, computed_at) SELECT '%s', '%s', %s, %s, %s, %s, %s, 'org-1', '%s'`, repo, day, drag, failure, flake, queue, retry, computed))
}

func breakdownByCategory(got *model.TestOpsRiskResult) map[string]float64 {
	out := map[string]float64{}
	for _, item := range got.QualityDragBreakdown {
		out[item.Category] = item.Hours
	}
	return out
}

// A NULL part of the latest row is never read as 0 and never replaced by an
// older row's value: a breakdown item whose sum is absent is omitted.
func TestRealClickHouse_TestopsRiskDragNullParts(t *testing.T) {
	ctx, conn, client := startStore(t)
	run := func(q string) {
		t.Helper()
		if err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	// Merges would collapse the old and new row of a repo-day and hide the stale read.
	run(`SYSTEM STOP MERGES testops_quality_drag`)
	const n = nullHours
	// 1: two repos, flake NULL in both
	dragRow(t, run, rA, "2026-01-01", "3", "1", n, "1", "1", "2026-01-01 02:00:00")
	dragRow(t, run, rB, "2026-01-01", "4", "2", n, "1", "1", "2026-01-01 02:00:00")
	// 2: one NULL flake, one known
	dragRow(t, run, rA, "2026-01-02", "3", "1", n, "1", "1", "2026-01-02 02:00:00")
	dragRow(t, run, rB, "2026-01-02", "4", "1", "1", "1", "1", "2026-01-02 02:00:00")
	// 5: recompute measured -> unmeasured
	dragRow(t, run, rA, "2026-01-05", "5", "1", "2", "1", "1", "2026-01-05 01:00:00")
	dragRow(t, run, rA, "2026-01-05", "3", "1", n, "1", "1", "2026-01-05 02:00:00")
	// 6: known zero
	dragRow(t, run, rA, "2026-01-06", "3", "1", "0", "1", "1", "2026-01-06 02:00:00")
	// 7: older NULL, newer known
	dragRow(t, run, rA, "2026-01-07", "3", "1", n, "1", "1", "2026-01-07 01:00:00")
	dragRow(t, run, rA, "2026-01-07", "4", "1", "1.5", "1", "1", "2026-01-07 02:00:00")
	// 8: recompute whose drag_hours and failure part turn NULL
	dragRow(t, run, rA, "2026-01-08", "5", "2", "1", "1", "1", "2026-01-08 01:00:00")
	dragRow(t, run, rA, "2026-01-08", n, n, "1", "1", "1", "2026-01-08 02:00:00")
	// 9: every part NULL
	dragRow(t, run, rA, "2026-01-09", n, n, n, n, n, "2026-01-09 02:00:00")

	type want struct {
		drag      *float64
		breakdown map[string]float64
	}
	h := func(v float64) *float64 { return &v }
	cells := []struct {
		name, day string
		want      want
	}{
		{"1 all flake NULL", "2026-01-01", want{h(7), map[string]float64{"Failure Rework": 3, "Queue Wait": 2, "Retry Overhead": 2}}},
		{"2 some flake NULL", "2026-01-02", want{h(7), map[string]float64{"Failure Rework": 2, "Flake Investigation": 1, "Queue Wait": 2, "Retry Overhead": 2}}},
		{"4 no rows", "2026-01-04", want{nil, map[string]float64{}}},
		{"5 recompute measured to unmeasured", "2026-01-05", want{h(3), map[string]float64{"Failure Rework": 1, "Queue Wait": 1, "Retry Overhead": 1}}},
		{"6 known zero", "2026-01-06", want{h(3), map[string]float64{"Failure Rework": 1, "Flake Investigation": 0, "Queue Wait": 1, "Retry Overhead": 1}}},
		{"7 older NULL newer known", "2026-01-07", want{h(4), map[string]float64{"Failure Rework": 1, "Flake Investigation": 1.5, "Queue Wait": 1, "Retry Overhead": 1}}},
		{"8 newer drag and failure NULL", "2026-01-08", want{nil, map[string]float64{}}},
		{"9 all parts NULL", "2026-01-09", want{nil, map[string]float64{}}},
	}
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			got, err := Resolve(ctx, client, "org-1", model.TestOpsRiskInput{StartDate: date(c.day), EndDate: date(c.day)})
			if err != nil {
				t.Fatal(err)
			}
			if (got.QualityDragHours == nil) != (c.want.drag == nil) || (c.want.drag != nil && *got.QualityDragHours != *c.want.drag) {
				t.Errorf("QualityDragHours %v, want %v", deref(got.QualityDragHours), deref(c.want.drag))
			}
			gotB := breakdownByCategory(got)
			if len(gotB) != len(c.want.breakdown) {
				t.Errorf("breakdown %v, want %v", gotB, c.want.breakdown)
				return
			}
			for k, v := range c.want.breakdown {
				if g, ok := gotB[k]; !ok || g != v {
					t.Errorf("breakdown %v, want %v", gotB, c.want.breakdown)
					return
				}
			}
		})
	}
}

func deref(v *float64) string {
	if v == nil {
		return "absent"
	}
	return fmt.Sprintf("%v", *v)
}
