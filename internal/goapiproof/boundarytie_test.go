package goapiproof

import (
	"encoding/json"
	"fmt"
	"testing"
)

// issueItemFixture is one data.items element this file's helpers build,
// mirroring IssueItem's own wire shape (internal/drilldown/issues.go).
type issueItemFixture struct {
	workItemID    string
	completedAt   string // no trailing "Z" -- addZ applies the candidate-side suffix
	leadTimeHours float64
}

// buildIssuesItemsBody marshals items into a `{"items":[...]}` REST body --
// the same whole-payload-is-data shape restSnapshotFromJSON's own callers
// use elsewhere in this package. addZ reproduces the sibling datetime-format
// defect's own declared divergence (Go's RFC 3339 "Z" suffix vs Python's
// naive isoformat) so a fixture pair exercises that already-declared defect
// alongside this file's own boundary-tie mechanism, the same as a live pair
// does.
func buildIssuesItemsBody(t *testing.T, items []issueItemFixture, addZ bool) string {
	t.Helper()
	out := make([]map[string]any, len(items))
	for i, it := range items {
		completedAt := it.completedAt
		if addZ {
			completedAt += "Z"
		}
		out[i] = map[string]any{
			"work_item_id":     it.workItemID,
			"provider":         "linear",
			"status":           "done",
			"team_id":          "team-x",
			"cycle_time_hours": nil,
			"lead_time_hours":  it.leadTimeHours,
			"started_at":       nil,
			"completed_at":     completedAt,
		}
	}
	body, err := json.Marshal(map[string]any{"items": out})
	if err != nil {
		t.Fatalf("marshal fixture body: %v", err)
	}
	return string(body)
}

// stableIssuesPrefix returns n items sharing NOTHING with the boundary
// tie -- distinct, strictly descending completed_at values above
// boundaryTieValue -- so this prefix pairs cleanly by work_item_id on
// both planes and never itself produces a presence finding.
func stableIssuesPrefix(n int) []issueItemFixture {
	items := make([]issueItemFixture, n)
	for i := 0; i < n; i++ {
		items[i] = issueItemFixture{
			workItemID:    fmt.Sprintf("linear:ABC-STABLE-%d", i),
			completedAt:   fmt.Sprintf("2024-02-01T%02d:00:00", 23-i%23),
			leadTimeHours: float64(1000 + i),
		}
	}
	return items
}

const boundaryTieValue = "2024-01-15T10:00:00"

// TestDrilldownIssuesBoundaryTie_TiedGroupLargerThanRemainingSlotsGoesToMatch
// reconstructs the general shape this file's own boundary-tie mechanism
// targets: 47 items both planes agree on (positionally AND by key), then a
// 5-member tie on completed_at at the LIMIT=50 boundary, where baseline
// keeps 3 of the 5 tied members and candidate keeps a DIFFERENT 3 of the
// same 5 -- one member (the same physical row) common to both, at a
// different position each side, and two more members of the tie visible
// on only one plane each. Before drilldownIssuesItemsOrderInsensitiveLists/
// drilldownIssuesBoundaryTie existed, a pair shaped this way reported 6
// outside-declaration leaf findings under positional comparison
// (work_item_id/lead_time_hours at three indices); this test pins the
// current, covered outcome: every difference explained.
func TestDrilldownIssuesBoundaryTie_TiedGroupLargerThanRemainingSlotsGoesToMatch(t *testing.T) {
	stable := stableIssuesPrefix(47)

	baselineItems := append(append([]issueItemFixture{}, stable...),
		issueItemFixture{workItemID: "linear:ABC-101", completedAt: boundaryTieValue, leadTimeHours: 1276.4486277777776},
		issueItemFixture{workItemID: "linear:ABC-102", completedAt: boundaryTieValue, leadTimeHours: 868.4317605555556},
		issueItemFixture{workItemID: "linear:ABC-103", completedAt: boundaryTieValue, leadTimeHours: 868.4341636111111},
	)
	candidateItems := append(append([]issueItemFixture{}, stable...),
		issueItemFixture{workItemID: "linear:ABC-104", completedAt: boundaryTieValue, leadTimeHours: 179.9628675},
		issueItemFixture{workItemID: "linear:ABC-105", completedAt: boundaryTieValue, leadTimeHours: 194.64589916666665},
		issueItemFixture{workItemID: "linear:ABC-101", completedAt: boundaryTieValue, leadTimeHours: 1276.4486277777776},
	)

	baseline := restSnapshotFromJSON(t, buildIssuesItemsBody(t, baselineItems, false))
	candidate := restSnapshotFromJSON(t, buildIssuesItemsBody(t, candidateItems, true))

	result := Compare(baseline, candidate, drilldownIssuesParity)
	// TerminalState stays "mismatch" -- the sibling datetime-format defect
	// fires whenever completed_at is present, on either plane
	// (classifyBaselineDefects never touches TerminalState, by its own
	// doc comment). The metric this test pins is
	// DifferencesOutsideBaselineDefect, the "outside=N" figure a
	// go-api-rest-prove receipt reports.
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %s (refusal %q), want mismatch (the sibling datetime-format defect always fires here)", result.TerminalState, result.StructuralRefusal)
	}
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d (%v), want 0 -- the presence swap must be fully covered", result.DifferencesOutsideBaselineDefect, result.OutsideByShape)
	}
	if len(result.StaleBaselineDefects) != 0 {
		t.Fatalf("stale defects = %v, want none", result.StaleBaselineDefects)
	}
	if len(result.LiveBaselineDefectsUnexplained) != 0 {
		t.Fatalf("live unexplained defects = %v, want none", result.LiveBaselineDefectsUnexplained)
	}
}

// TestDrilldownIssuesBoundaryTie_NonBoundarySwapStaysOutside proves the
// shape never admits a presence swap whose own completed_at does not
// match the tie value shared by every OTHER admitted key: a work_item_id
// present only in baseline, at a completed_at strictly its own, is a
// real, unexplained divergence, not this mechanism.
func TestDrilldownIssuesBoundaryTie_NonBoundarySwapStaysOutside(t *testing.T) {
	stable := stableIssuesPrefix(49)
	baselineItems := append(append([]issueItemFixture{}, stable...),
		issueItemFixture{workItemID: "linear:ABC-LONE", completedAt: "2024-01-10T00:00:00", leadTimeHours: 42},
	)
	candidateItems := append(append([]issueItemFixture{}, stable...),
		issueItemFixture{workItemID: "linear:ABC-OTHER", completedAt: "2024-01-11T00:00:00", leadTimeHours: 43},
	)

	baseline := restSnapshotFromJSON(t, buildIssuesItemsBody(t, baselineItems, false))
	candidate := restSnapshotFromJSON(t, buildIssuesItemsBody(t, candidateItems, true))

	result := Compare(baseline, candidate, drilldownIssuesParity)
	if result.DifferencesOutsideBaselineDefect == 0 {
		t.Fatalf("outside = 0 (%v), want > 0 -- an unrelated presence swap must never be silently admitted", result.OutsideByShape)
	}
	if result.OutsideByShape["presence"] == 0 {
		t.Fatalf("outside_by_shape = %v, want a non-zero \"presence\" count", result.OutsideByShape)
	}
}

// TestDrilldownIssuesBoundaryTie_ShortOfLimitStaysOutside proves the
// shape refuses to admit anything when the lists sit BELOW the request's
// own effective LIMIT: a presence difference there is an ordinary
// missing/extra row, never a rank displacement at a boundary that was
// never actually reached.
func TestDrilldownIssuesBoundaryTie_ShortOfLimitStaysOutside(t *testing.T) {
	baselineItems := []issueItemFixture{
		{workItemID: "linear:ABC-101", completedAt: boundaryTieValue, leadTimeHours: 1276.4486277777776},
		{workItemID: "linear:ABC-102", completedAt: boundaryTieValue, leadTimeHours: 868.4317605555556},
	}
	candidateItems := []issueItemFixture{
		{workItemID: "linear:ABC-101", completedAt: boundaryTieValue, leadTimeHours: 1276.4486277777776},
		{workItemID: "linear:ABC-104", completedAt: boundaryTieValue, leadTimeHours: 179.9628675},
	}

	baseline := restSnapshotFromJSON(t, buildIssuesItemsBody(t, baselineItems, false))
	candidate := restSnapshotFromJSON(t, buildIssuesItemsBody(t, candidateItems, true))

	result := Compare(baseline, candidate, drilldownIssuesParity)
	if result.DifferencesOutsideBaselineDefect == 0 {
		t.Fatalf("outside = 0 (%v), want > 0 -- a 2-item list is nowhere near LIMIT=50, no boundary exists to displace at", result.OutsideByShape)
	}
	if result.OutsideByShape["presence"] == 0 {
		t.Fatalf("outside_by_shape = %v, want a non-zero \"presence\" count", result.OutsideByShape)
	}
}

// TestDrilldownIssuesBoundaryTie_StableRowLeafDifferenceStaysOutside
// proves the shape never reaches past a genuine leaf mismatch on a row
// BOTH planes still return: a key present on both sides with a real
// lead_time_hours divergence (no presence difference at all) is outside
// every declared defect, exactly as before this shape existed.
func TestDrilldownIssuesBoundaryTie_StableRowLeafDifferenceStaysOutside(t *testing.T) {
	baselineItems := append(stableIssuesPrefix(49),
		issueItemFixture{workItemID: "linear:ABC-101", completedAt: boundaryTieValue, leadTimeHours: 1276.4486277777776},
	)
	candidateItems := append(stableIssuesPrefix(49),
		issueItemFixture{workItemID: "linear:ABC-101", completedAt: boundaryTieValue, leadTimeHours: 999.0},
	)

	baseline := restSnapshotFromJSON(t, buildIssuesItemsBody(t, baselineItems, false))
	candidate := restSnapshotFromJSON(t, buildIssuesItemsBody(t, candidateItems, true))

	result := Compare(baseline, candidate, drilldownIssuesParity)
	if result.DifferencesOutsideBaselineDefect == 0 {
		t.Fatalf("outside = 0 (%v), want > 0 -- a real lead_time_hours divergence on a key both planes return must never be covered by this shape", result.OutsideByShape)
	}
}
