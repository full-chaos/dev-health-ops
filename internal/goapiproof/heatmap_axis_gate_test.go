package goapiproof

import (
	"fmt"
	"reflect"
	"sort"
	"testing"
)

// heatmapAxisAdmissionPaths is every BaselineDefect shape that can admit
// a data.axes.y position, each with the axis admission of its own plan.
// TestHeatmapAxisAdmissionPathsAreGated pins it to the shapes the
// BaselineDefect type actually carries.
var heatmapAxisAdmissionPaths = map[string]func(defect BaselineDefect, baselineData, candidateData any, finding Finding) (applies, admitted bool){
	"HeatmapAxisTieGroupShape": func(d BaselineDefect, b, c any, f Finding) (bool, bool) {
		if d.HeatmapAxisTieGroupShape == nil {
			return false, false
		}
		return true, buildHeatmapAxisTieGroupPlan(d.HeatmapAxisTieGroupShape, b, c).admits(f)
	},
	"HeatmapAxisRepoOrderShape": func(d BaselineDefect, b, c any, f Finding) (bool, bool) {
		if d.HeatmapAxisRepoOrderShape == nil {
			return false, false
		}
		return true, buildHeatmapAxisRepoOrderPlan(d.HeatmapAxisRepoOrderShape, b, c).admits(f)
	},
	"HeatmapCellBoundaryShape": func(d BaselineDefect, b, c any, f Finding) (bool, bool) {
		if d.HeatmapCellBoundaryShape == nil {
			return false, false
		}
		return true, buildHeatmapCellBoundaryPlan(d.HeatmapCellBoundaryShape, b, c).admits(f)
	},
}

// heatmapAxisAdmitters lists which BaselineDefect entries of opts admit
// the axis finding at idx, by shape name.
func heatmapAxisAdmitters(opts Options, baseline, candidate Snapshot, idx int) []string {
	finding := Finding{Kind: FindingMismatch, Path: fmt.Sprintf("$.data.axes.y[%d]", idx), Shape: ShapeValue}
	var out []string
	for _, defect := range opts.BaselineDefects {
		for name, path := range heatmapAxisAdmissionPaths {
			if applies, admitted := path(defect, baseline.Data, candidate.Data, finding); applies && admitted {
				out = append(out, name)
			}
		}
	}
	sort.Strings(out)
	return out
}

// Every shape type on BaselineDefect with an AxisListPath is an axis
// admission path; the table above must list exactly those, so a new
// axis-admitting shape fails here until it is gated and listed.
func TestHeatmapAxisAdmissionPathsAreGated(t *testing.T) {
	var generated []string
	defectType := reflect.TypeOf(BaselineDefect{})
	for i := 0; i < defectType.NumField(); i++ {
		field := defectType.Field(i).Type
		if field.Kind() != reflect.Pointer || field.Elem().Kind() != reflect.Struct {
			continue
		}
		if _, ok := field.Elem().FieldByName("AxisListPath"); ok {
			generated = append(generated, field.Elem().Name())
		}
	}
	sort.Strings(generated)
	var listed []string
	for name := range heatmapAxisAdmissionPaths {
		listed = append(listed, name)
	}
	sort.Strings(listed)
	if !reflect.DeepEqual(generated, listed) {
		t.Fatalf("axis admission paths on BaselineDefect = %v, gated table = %v", generated, listed)
	}

	// Each path admits its own mechanism's axis move with the candidate
	// in axisOrder's order, and admits nothing once an unmoved tie run
	// of the candidate is misordered identically on both legs.
	for _, tc := range []struct {
		path   string
		opts   Options
		build  func(misordered bool) (Snapshot, Snapshot)
		admits int
	}{
		{"HeatmapAxisTieGroupShape", heatmapHotspotRiskParity, gateTieGroupPair(t), 0},
		{"HeatmapAxisRepoOrderShape", heatmapRepoTouchpointsParity, gateRepoOrderPair(t), 0},
		{"HeatmapCellBoundaryShape", heatmapHotspotRiskParity, gateCellBoundaryPair(t), 19},
	} {
		for _, misordered := range []bool{false, true} {
			baseline, candidate := tc.build(misordered)
			got := false
			for _, name := range heatmapAxisAdmitters(tc.opts, baseline, candidate, tc.admits) {
				if name == tc.path {
					got = true
				}
			}
			if got == misordered {
				t.Errorf("%s, candidate misordered=%t: axis position %d admitted=%t", tc.path, misordered, tc.admits, got)
			}
		}
	}
}

// gateTieGroupPair: a/b tie at 4 and c/d tie at 2 on both legs; the
// baseline swaps a/b. Misordered: both legs list d before c.
func gateTieGroupPair(t *testing.T) func(bool) (Snapshot, Snapshot) {
	return func(misordered bool) (Snapshot, Snapshot) {
		cells := []string{heatmapBoundaryCell("w1", "o/r:a.go", 4), heatmapBoundaryCell("w1", "o/r:b.go", 4), heatmapBoundaryCell("w1", "o/r:c.go", 2), heatmapBoundaryCell("w1", "o/r:d.go", 2)}
		low := []string{"o/r:c.go", "o/r:d.go"}
		if misordered {
			low = []string{"o/r:d.go", "o/r:c.go"}
		}
		return heatmapAxisSnapshotsFromCells(t, cells, cells, append([]string{"o/r:b.go", "o/r:a.go"}, low...), append([]string{"o/r:a.go", "o/r:b.go"}, low...))
	}
}

// gateRepoOrderPair: org/a fans out at k=2 and moves above org/b; org/c
// and org/d tie untouched. Misordered: both legs list org/d first.
func gateRepoOrderPair(t *testing.T) func(bool) (Snapshot, Snapshot) {
	return func(misordered bool) (Snapshot, Snapshot) {
		base := []string{heatmapBoundaryCell("w1", "org/a", 4), heatmapBoundaryCell("w2", "org/a", 4), heatmapBoundaryCell("w1", "org/b", 6), heatmapBoundaryCell("w1", "org/c", 1), heatmapBoundaryCell("w1", "org/d", 1)}
		cand := []string{heatmapBoundaryCell("w1", "org/a", 2), heatmapBoundaryCell("w2", "org/a", 2), heatmapBoundaryCell("w1", "org/b", 6), heatmapBoundaryCell("w1", "org/c", 1), heatmapBoundaryCell("w1", "org/d", 1)}
		low := []string{"org/c", "org/d"}
		if misordered {
			low = []string{"org/d", "org/c"}
		}
		return heatmapAxisSnapshotsFromCells(t, base, cand, append([]string{"org/a", "org/b"}, low...), append([]string{"org/b", "org/a"}, low...))
	}
}

// gateCellBoundaryPair: 20-file lists at the route's Limit, repo-a
// fanned at k=2 (big/mid), a repo-a leaver against a repo-x entrant at
// position 19. Misordered: both legs list two tied padding files in
// reverse name order.
func gateCellBoundaryPair(t *testing.T) func(bool) (Snapshot, Snapshot) {
	return func(misordered bool) (Snapshot, Snapshot) {
		shared := []heatmapAxisEntry{{"repo-a:big.go", 120, 60}, {"repo-a:mid.go", 116, 58}}
		axis := []string{"repo-a:big.go", "repo-a:mid.go"}
		for i := 0; i < 17; i++ {
			name := fmt.Sprintf("ops:f%02d.go", i)
			v := float64(50 - i)
			if i == 16 {
				v = 35 // ties f15
			}
			shared = append(shared, heatmapAxisEntry{name, v, v})
			axis = append(axis, name)
		}
		if misordered {
			axis[17], axis[18] = axis[18], axis[17]
		}
		return heatmapAxisEntrantLeaverFixture(t, shared, "repo-a:leaver.go", 20, "repo-x:entrant.go", 15,
			append(append([]string{}, axis...), "repo-a:leaver.go"), append(append([]string{}, axis...), "repo-x:entrant.go"))
	}
}
