//go:build integration

package testopsrisk

import (
	"encoding/json"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// partialOf reads qualityDragPartial from the JSON the field resolves to:
// "absent" when the result carries no such key.
func partialOf(t *testing.T, got *model.TestOpsRiskResult) string {
	t.Helper()
	raw, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	v, ok := m["qualityDragPartial"]
	if !ok {
		return "absent"
	}
	if b, ok := v.(bool); ok && b {
		return "true"
	}
	return "false"
}

// qualityDragPartial is true when any repo row of the latest drag day has a
// NULL failure-rework, flake-investigation, queue-wait or retry-overhead part.
func TestRealClickHouse_TestopsRiskQualityDragPartial(t *testing.T) {
	ctx, conn, client := startStore(t)
	run := func(q string) {
		t.Helper()
		if err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	run(`SYSTEM STOP MERGES testops_quality_drag`)
	const n = nullHours
	const at = "2026-01-01 02:00:00"
	d := func(repo, day, drag, fr, fl, qw, ro string) { dragRow(t, run, repo, day, drag, fr, fl, qw, ro, at) }

	d(rA, "2026-01-01", "4", "1", "1", "1", "1") // P1 all known, two repos
	d(rB, "2026-01-01", "4", "1", "1", "1", "1")
	d(rA, "2026-01-02", "3", "1", n, "1", "1") // P2 one flake NULL among known
	d(rB, "2026-01-02", "4", "1", "1", "1", "1")
	d(rA, "2026-01-03", "3", "1", n, "1", "1") // P3 every flake NULL
	d(rB, "2026-01-03", "3", "1", n, "1", "1")
	d(rA, "2026-01-04", "3", n, "1", "1", "1") // P4 failure part NULL
	d(rA, "2026-01-05", "3", "1", "1", n, "1") // P4 queue part NULL
	d(rA, "2026-01-06", "3", "1", "1", "1", n) // P4 retry part NULL
	d(rA, "2026-01-07", "3", "1", "0", "1", "1") // P5 known 0
	d(rA, "2026-01-09", n, n, n, n, n) // P7 every part NULL, drag NULL
	d(rA, "2026-01-10", "3", n, "1", "1", "1") // P8 earlier day has a NULL part
	d(rA, "2026-01-11", "4", "1", "1", "1", "1") // P8 latest day known
	d(rA, "2026-01-12", "3", "1", n, "1", "1") // P9a older row NULL ...
	d(rA, "2026-01-12", "4", "1", "1", "1", "1") // ... last-inserted row known
	d(rA, "2026-01-13", "4", "1", "1", "1", "1") // P9b older row known ...
	d(rA, "2026-01-13", "3", "1", n, "1", "1") // ... last-inserted row NULL

	cells := []struct {
		name, start, end string
		drag             *float64
		partial          string
	}{
		{"P1 all known", "2026-01-01", "2026-01-01", f64(8), "false"},
		{"P2 one NULL among known", "2026-01-02", "2026-01-02", f64(7), "true"},
		{"P3 every flake NULL", "2026-01-03", "2026-01-03", f64(6), "true"},
		{"P4 failure NULL", "2026-01-04", "2026-01-04", f64(3), "true"},
		{"P4 queue NULL", "2026-01-05", "2026-01-05", f64(3), "true"},
		{"P4 retry NULL", "2026-01-06", "2026-01-06", f64(3), "true"},
		{"P5 known zero", "2026-01-07", "2026-01-07", f64(3), "false"},
		{"P6 no rows", "2026-01-08", "2026-01-08", nil, "false"},
		{"P7 every part NULL", "2026-01-09", "2026-01-09", nil, "false"},
		{"P8 earlier day NULL, latest day known", "2026-01-10", "2026-01-11", f64(4), "false"},
		{"P9a tie, kept row known", "2026-01-12", "2026-01-12", f64(4), "false"},
		{"P9b tie, kept row NULL", "2026-01-13", "2026-01-13", f64(3), "true"},
	}
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			got, err := Resolve(ctx, client, "org-1", model.TestOpsRiskInput{StartDate: date(c.start), EndDate: date(c.end)})
			if err != nil {
				t.Fatal(err)
			}
			if (got.QualityDragHours == nil) != (c.drag == nil) || (c.drag != nil && *got.QualityDragHours != *c.drag) {
				t.Errorf("QualityDragHours %v, want %v", deref(got.QualityDragHours), deref(c.drag))
			}
			if p := partialOf(t, got); p != c.partial {
				t.Errorf("qualityDragPartial %s, want %s", p, c.partial)
			}
		})
	}
}

func f64(v float64) *float64 { return &v }
