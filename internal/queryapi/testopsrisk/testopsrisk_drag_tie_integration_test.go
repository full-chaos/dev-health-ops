//go:build integration

package testopsrisk

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// Two rows of one repo-day with the same computed_at second: the reader shows
// the one row the table keeps (the last inserted), never an older value and
// never columns of both rows.
func TestRealClickHouse_TestopsRiskDragSameSecondTie(t *testing.T) {
	ctx, conn, client := startStore(t)
	run := func(q string) {
		t.Helper()
		if err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	// Merges would collapse the tied rows before the reader sees them.
	run(`SYSTEM STOP MERGES testops_quality_drag`)
	const n = nullHours
	const tied = "2026-01-01 02:00:00"
	read := func(day string) *model.TestOpsRiskResult {
		t.Helper()
		got, err := Resolve(ctx, client, "org-1", model.TestOpsRiskInput{StartDate: date(day), EndDate: date(day)})
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	want := func(t *testing.T, got *model.TestOpsRiskResult, drag float64, breakdown map[string]float64) {
		t.Helper()
		if got.QualityDragHours == nil || *got.QualityDragHours != drag {
			t.Errorf("QualityDragHours %v, want %v", deref(got.QualityDragHours), drag)
		}
		if g := breakdownByCategory(got); !reflect.DeepEqual(g, breakdown) {
			t.Errorf("breakdown %v, want %v", g, breakdown)
		}
	}
	lastRow := map[string]float64{"Failure Rework": 1, "Queue Wait": 1, "Retry Overhead": 1}

	// a: known row first, NULL-flake row last
	dragRow(t, run, rA, "2026-01-01", "5", "1", "2", "1", "1", tied)
	dragRow(t, run, rA, "2026-01-01", "3", "1", n, "1", "1", tied)
	t.Run("a tie known then NULL", func(t *testing.T) { want(t, read("2026-01-01"), 3, lastRow) })

	// b: the per-column pick used to take columns from both rows
	dragRow(t, run, rA, "2026-01-02", "5", "1", "2", "9", "1", tied)
	dragRow(t, run, rA, "2026-01-02", "3", "8", "4", "2", "7", tied)
	t.Run("b mixed columns", func(t *testing.T) {
		want(t, read("2026-01-02"), 3, map[string]float64{"Failure Rework": 8, "Flake Investigation": 4, "Queue Wait": 2, "Retry Overhead": 7})
	})

	// d: both rows in one INSERT (one part): the later row of the part is kept
	run(fmt.Sprintf(`INSERT INTO testops_quality_drag (repo_id, day, drag_hours, failure_rework_hours, flake_investigation_hours, queue_wait_hours, retry_overhead_hours, org_id, computed_at) VALUES ('%s','2026-01-03',5,1,2,1,1,'org-1','%s'),('%s','2026-01-03',3,1,NULL,1,1,'org-1','%s')`, rA, tied, rA, tied))
	t.Run("d tie in one part", func(t *testing.T) { want(t, read("2026-01-03"), 3, lastRow) })

	// e: the answer does not change when a merge runs
	dragRow(t, run, rA, "2026-01-04", "5", "1", "2", "9", "1", tied)
	dragRow(t, run, rA, "2026-01-04", "3", "8", "4", "2", "7", tied)
	t.Run("e same answer before and after OPTIMIZE FINAL", func(t *testing.T) {
		before := read("2026-01-04")
		run(`SYSTEM START MERGES testops_quality_drag`)
		run(`OPTIMIZE TABLE testops_quality_drag FINAL`)
		after := read("2026-01-04")
		if !reflect.DeepEqual(before.QualityDragHours, after.QualityDragHours) || !reflect.DeepEqual(before.QualityDragBreakdown, after.QualityDragBreakdown) {
			t.Errorf("merge changed the answer: before %v %v, after %v %v", deref(before.QualityDragHours), before.QualityDragBreakdown, deref(after.QualityDragHours), after.QualityDragBreakdown)
		}
		want(t, after, 3, map[string]float64{"Failure Rework": 8, "Flake Investigation": 4, "Queue Wait": 2, "Retry Overhead": 7})
	})
}
