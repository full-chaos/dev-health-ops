package goapiproof

import (
	"os"
	"testing"
)

// This file exercises KeyedDirectionShape (keyeddirection.go) directly
// through small, self-contained keyed lists, plus one real captured GET
// /api/v1/heatmap response pair (a production deployed-vs-deployed prove run) -- the
// shape-specific admission that replaces a blanket "any difference under
// data.cells is covered" rule for a strictly additive row-dedup
// mechanism whose affected list element can sum many unrelated source
// rows (so no clean multiplier is provable in general, unlike
// SankeyRepoFanoutShape's own mechanism).

func heatmapCellsOptions(ticket string) Options {
	return Options{
		OrderInsensitiveLists: []OrderInsensitiveList{
			{Path: "data.cells", KeyFields: []string{"x", "y"}, Reason: "test fixture", Ticket: "CHAOS-TEST-ORDER"},
		},
		BaselineDefects: []BaselineDefect{{
			Ticket: ticket, Reason: "test fixture",
			Paths:        []string{"data.cells"},
			Intermittent: true, IntermittentReason: "test fixture",
			KeyedDirectionShape: &KeyedDirectionShape{
				ListPath:   "data.cells",
				ValueField: "value",
				ValuePath:  "data.cells.value",
				KeyFields:  []string{"x", "y"},
			},
		}},
	}
}

func heatmapCellsBody(cells string) string {
	return `{"data":{"cells":[` + cells + `]}}`
}

func heatmapCell(x, y string, value float64) string {
	return `{"x":"` + x + `","y":"` + y + `","value":` + jsonFloat(value) + `}`
}

// The mechanism's own direction -- baseline strictly greater, an
// unmerged table row can only be counted extra, never dropped -- is
// admitted with no magnitude bound: a tiny shift and a large one are
// both explained the same way.
func TestKeyedDirectionShape_BaselineGreaterIsAdmitted(t *testing.T) {
	cells := heatmapCell("Mon", "09", 1)
	baseline := snapshotFromJSON(t, heatmapCellsBody(heatmapCell("Mon", "09", 1000000)))
	candidate := snapshotFromJSON(t, heatmapCellsBody(cells))

	result := Compare(baseline, candidate, heatmapCellsOptions("CHAOS-TEST-DIRECTION"))
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- a large shift is explained the same as a small one: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	if !equalStrings(result.BaselineDefectsMatched, []string{"CHAOS-TEST-DIRECTION"}) {
		t.Fatalf("matched = %v, want [CHAOS-TEST-DIRECTION]", result.BaselineDefectsMatched)
	}
}

// Blind-spot pin: a candidate value ABOVE baseline at the same key is
// the one shape this direction cannot produce -- a Go-side regression
// that happens to move the same key the "wrong" way stays uncovered, by
// design, exactly as documented.
func TestKeyedDirectionShape_CandidateGreaterIsNotAdmitted(t *testing.T) {
	baseline := snapshotFromJSON(t, heatmapCellsBody(heatmapCell("Mon", "09", 5)))
	candidate := snapshotFromJSON(t, heatmapCellsBody(heatmapCell("Mon", "09", 9)))

	result := Compare(baseline, candidate, heatmapCellsOptions("CHAOS-TEST-DIRECTION"))
	if result.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("outside = %d, want 1 -- a candidate-greater shift must never be admitted by a direction-only shape: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// Keys, when declared, restricts admission to exactly those tuples --
// modelling sankeyCycleTimesDedupParity's single fixed edge. A
// same-direction difference at an UNDECLARED key is not this mechanism
// and stays outside the citation.
func TestKeyedDirectionShape_RestrictedKeysExcludesOtherKeys(t *testing.T) {
	opts := heatmapCellsOptions("CHAOS-TEST-DIRECTION")
	opts.BaselineDefects[0].KeyedDirectionShape.Keys = [][]string{{"Mon", "09"}}

	baseline := snapshotFromJSON(t, heatmapCellsBody(heatmapCell("Mon", "09", 10)+","+heatmapCell("Tue", "10", 10)))
	candidate := snapshotFromJSON(t, heatmapCellsBody(heatmapCell("Mon", "09", 5)+","+heatmapCell("Tue", "10", 5)))

	result := Compare(baseline, candidate, opts)
	if result.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("outside = %d, want 1 -- (Tue,10) is not a declared key and must stay uncovered even though it moves the same direction: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

func heatmapDirectionSnapshotFromFile(t *testing.T, path string) Snapshot {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture %s: %v", path, err)
	}
	snapshot, err := DecodeRESTSnapshot(body)
	if err != nil {
		t.Fatalf("decode REST fixture %s: %v", path, err)
	}
	return snapshot
}

// TestKeyedDirectionShape_RealHeatmapCapture pins a real instance
// captured from a production deployed-vs-deployed prove run (GET
// /api/v1/heatmap review_wait_density_org): two cells ((03,Fri) and
// (19,Sun)) each show baseline exactly 2x candidate; every other one of
// the capture's 32 shared cell keys already agrees.
func TestKeyedDirectionShape_RealHeatmapCapture(t *testing.T) {
	baseline := heatmapDirectionSnapshotFromFile(t, "testdata/heatmap_review_wait_direction_baseline_09956ee8.json")
	candidate := heatmapDirectionSnapshotFromFile(t, "testdata/heatmap_review_wait_direction_candidate_3b684469.json")

	result := Compare(baseline, candidate, heatmapCellsOptions("CHAOS-TEST-DIRECTION"))
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch", result.TerminalState)
	}
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- covered %v outside %v findings %+v",
			result.DifferencesOutsideBaselineDefect, result.CoveredByShape, result.OutsideByShape, result.Findings)
	}
	if !equalStrings(result.BaselineDefectsMatched, []string{"CHAOS-TEST-DIRECTION"}) {
		t.Fatalf("matched = %v, want [CHAOS-TEST-DIRECTION] -- idle %v stale %v", result.BaselineDefectsMatched, result.IdleIntermittentBaselineDefects, result.StaleBaselineDefects)
	}
}
