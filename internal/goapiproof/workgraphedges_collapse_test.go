package goapiproof

import (
	"encoding/json"
	"fmt"
	"testing"
)

// A workGraphEdges page the request limit does not cut, over a node whose
// edges hold unmerged duplicate versions. Measured on production (one node
// id): baseline 147 rows over 73 distinct edgeIds (72 ids twice, 1 id three
// times), totalCount 147; candidate 73 rows, 73 distinct,
// totalCount 73, the two id sets identical.

type collapseEdge struct {
	id         string
	confidence float64
}

func collapseEdges(n int) []collapseEdge {
	out := make([]collapseEdge, 0, n)
	for i := 0; i < n; i++ {
		// Confidence falls with i, so the order is confidence DESC, id ASC.
		out = append(out, collapseEdge{id: fmt.Sprintf("edge-%03d", i), confidence: 1.0 - float64(i)/1000})
	}
	return out
}

func collapseBody(t *testing.T, edges []collapseEdge, totalCount int) string {
	t.Helper()
	list := make([]map[string]any, 0, len(edges))
	for _, e := range edges {
		list = append(list, map[string]any{
			"edgeId": e.id, "sourceType": "PR", "sourceId": "pr:node", "targetType": "ISSUE",
			"targetId": "issue:" + e.id, "edgeType": "IMPLEMENTS", "confidence": e.confidence, "evidence": "e-" + e.id,
		})
	}
	end := any(nil)
	if len(edges) > 0 {
		end = edges[len(edges)-1].id
	}
	raw, err := json.Marshal(map[string]any{"data": map[string]any{"workGraphEdges": map[string]any{
		"edges": list, "totalCount": totalCount,
		"pageInfo": map[string]any{"hasNextPage": false, "hasPreviousPage": false, "startCursor": edges[0].id, "endCursor": end},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// productionShape repeats 72 ids twice and the last id three times.
func productionShape() (baseline, candidate []collapseEdge) {
	candidate = collapseEdges(73)
	for i, e := range candidate {
		copies := 2
		if i == 72 {
			copies = 3
		}
		for c := 0; c < copies; c++ {
			baseline = append(baseline, e)
		}
	}
	return baseline, candidate
}

func compareCollapse(t *testing.T, baselineBody, candidateBody string) Result {
	t.Helper()
	spec, err := SpecFor("workGraphEdges")
	if err != nil {
		t.Fatal(err)
	}
	return Compare(snapshotFromJSON(t, baselineBody), snapshotFromJSON(t, candidateBody), spec.Parity)
}

func TestWorkGraphEdgesCollapse_ProductionShapeIsCovered(t *testing.T) {
	base, cand := productionShape()
	if len(base) != 147 || len(cand) != 73 {
		t.Fatalf("fixture is %d/%d, want 147/73", len(base), len(cand))
	}
	result := compareCollapse(t, collapseBody(t, base, 147), collapseBody(t, cand, 73))
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d: %v", result.DifferencesOutsideBaselineDefect, result.OutsideByShape)
	}
	seen := false
	for _, ticket := range result.BaselineDefectsMatched {
		if ticket == "CHAOS-6114" {
			seen = true
		}
	}
	if !seen {
		t.Fatalf("CHAOS-6114 not matched: %v", result.BaselineDefectsMatched)
	}
}

func TestWorkGraphEdgesCollapse_StaysOutside(t *testing.T) {
	base, cand := productionShape()
	baseBody := collapseBody(t, base, 147)
	cases := []struct {
		name string
		body string
	}{
		{"go drops one distinct id", collapseBody(t, cand[:72], 72)},
		{"go invents an id", collapseBody(t, append(append([]collapseEdge{}, cand...), collapseEdge{id: "edge-zzz", confidence: 0.001}), 74)},
		{"go totalCount is not its own length", collapseBody(t, cand, 74)},
		{"go order is not confidence descending", func() string {
			swapped := append([]collapseEdge{}, cand...)
			swapped[0], swapped[1] = swapped[1], swapped[0]
			return collapseBody(t, swapped, 73)
		}()},
		{"go repeats an id", collapseBody(t, append(append([]collapseEdge{}, cand[:72]...), cand[71]), 73)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			result := compareCollapse(t, baseBody, c.body)
			if result.DifferencesOutsideBaselineDefect == 0 {
				t.Fatalf("admitted: %v", result.CoveredByShape)
			}
		})
	}
	t.Run("baseline totalCount is not its own length", func(t *testing.T) {
		result := compareCollapse(t, collapseBody(t, base, 150), collapseBody(t, cand, 73))
		if result.DifferencesOutsideBaselineDefect == 0 {
			t.Fatal("admitted a baseline count that is not its list length")
		}
	})
}

// A candidate row that differs from its baseline copies is a real
// regression, never the collapse.
func TestWorkGraphEdgesCollapse_SharedRowDisagreementStaysOutside(t *testing.T) {
	base, cand := productionShape()
	baseBody := collapseBody(t, base, 147)
	altered := collapseBody(t, cand, 73)
	var decoded map[string]any
	if err := json.Unmarshal([]byte(altered), &decoded); err != nil {
		t.Fatal(err)
	}
	edges := decoded["data"].(map[string]any)["workGraphEdges"].(map[string]any)["edges"].([]any)
	edges[5].(map[string]any)["evidence"] = "changed"
	raw, _ := json.Marshal(decoded)
	result := compareCollapse(t, baseBody, string(raw))
	if result.DifferencesOutsideBaselineDefect == 0 {
		t.Fatal("admitted a changed shared row")
	}
}

// The enabled workGraphEdges document's non-cut cases carry the whole-list
// declaration, require a non-empty answer and echo the supplied value.
func TestWorkGraphEdgesNonCutVariantsAreDeclared(t *testing.T) {
	spec, err := SpecFor("workGraphEdges")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]string{"SOURCE_TYPE_SMALL": {"sourceType"}, "NODE_ID_VALID": {"sourceId", "targetId"}}
	seen := 0
	for _, v := range spec.Variants {
		fields, ok := want[v.Name]
		if !ok {
			continue
		}
		seen++
		if v.Instance == nil {
			t.Fatalf("%s carries no run-supplied value", v.Name)
		}
		if len(v.Parity.RequireNonEmpty) != 1 || v.Parity.RequireNonEmpty[0] != "data.workGraphEdges.edges" {
			t.Errorf("%s must require a non-empty edge list: %v", v.Name, v.Parity.RequireNonEmpty)
		}
		hasCollapse := false
		for _, d := range v.Parity.BaselineDefects {
			if d.Ticket == "CHAOS-6114" && d.DuplicateCollapseLengthShape != nil {
				hasCollapse = true
			}
		}
		if !hasCollapse {
			t.Errorf("%s does not carry the whole-list declaration", v.Name)
		}
		echo := v.Instance.Echo("X")
		if len(echo) != 1 || echo[0].List != "data.workGraphEdges.edges" || echo[0].Value != "X" || len(echo[0].Fields) != len(fields) {
			t.Errorf("%s echo %+v", v.Name, echo)
		}
		vars := v.Variables("org", Window{})
		v.Instance.Bind(vars, "X")
		filters := vars["filters"].(map[string]any)
		if filters["limit"] != 200 {
			t.Errorf("%s limit %v", v.Name, filters["limit"])
		}
	}
	if seen != 2 {
		t.Fatalf("found %d of the two non-cut variants", seen)
	}
}

// The order clause alone: every id, row and count agree between the planes,
// only the candidate's order changes, so the plan's own decision is what a
// reordered or mis-tied list must fail.
func TestWorkGraphEdgesCollapse_OrderRuleDecidesAlone(t *testing.T) {
	spec, err := SpecFor("workGraphEdges")
	if err != nil {
		t.Fatal(err)
	}
	var shape *DuplicateCollapseLengthShape
	for _, d := range spec.Parity.BaselineDefects {
		if d.DuplicateCollapseLengthShape != nil && d.DuplicateCollapseLengthShape.OrderField != "" {
			shape = d.DuplicateCollapseLengthShape
		}
	}
	if shape == nil {
		t.Fatal("no whole-list shape with an order rule is declared on workGraphEdges")
	}
	decode := func(edges []collapseEdge, total int) any {
		return snapshotFromJSON(t, collapseBody(t, edges, total)).Data
	}
	// Two ids at one confidence, then a lower one.
	ordered := []collapseEdge{{"edge-a", 0.9}, {"edge-b", 0.9}, {"edge-c", 0.5}}
	base := []collapseEdge{ordered[0], ordered[0], ordered[1], ordered[2]}
	cases := []struct {
		name string
		cand []collapseEdge
		want bool
	}{
		{"confidence descending, ties ascending", ordered, true},
		{"confidence ascending", []collapseEdge{ordered[2], ordered[0], ordered[1]}, false},
		{"ties descending", []collapseEdge{ordered[1], ordered[0], ordered[2]}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			plan := buildDuplicateCollapseLengthPlan(shape, decode(base, 4), decode(c.cand, 3))
			if plan.applies != c.want {
				t.Fatalf("applies = %v, want %v", plan.applies, c.want)
			}
		})
	}
}

// The whole-list plan admits the list's own length finding and the total count
// finding, each only when it applies, and admits nothing else beside them.
func TestWorkGraphEdgesCollapse_PlanAdmitsOnlyLengthAndCount(t *testing.T) {
	spec, err := SpecFor("workGraphEdges")
	if err != nil {
		t.Fatal(err)
	}
	var shape *DuplicateCollapseLengthShape
	for _, d := range spec.Parity.BaselineDefects {
		if d.DuplicateCollapseLengthShape != nil && d.DuplicateCollapseLengthShape.CountPath != "" {
			shape = d.DuplicateCollapseLengthShape
		}
	}
	if shape == nil {
		t.Fatal("no whole-list shape with a count path is declared on workGraphEdges")
	}
	decode := func(edges []collapseEdge, total int) any {
		return snapshotFromJSON(t, collapseBody(t, edges, total)).Data
	}
	edges := collapseEdges(3)
	dup := []collapseEdge{edges[0], edges[0], edges[1], edges[2]}
	lengthFinding := Finding{Kind: FindingMismatch, Path: "$.data.workGraphEdges.edges", Shape: ShapeLength}
	countFinding := Finding{Kind: FindingMismatch, Path: "$.data.workGraphEdges.totalCount", Shape: ShapeValue}
	otherFinding := Finding{Kind: FindingMismatch, Path: "$.data.workGraphEdges.pageInfo.hasNextPage", Shape: ShapeValue}

	applies := buildDuplicateCollapseLengthPlan(shape, decode(dup, 4), decode(edges, 3))
	if !applies.applies || !applies.admits(lengthFinding) || !applies.admits(countFinding) {
		t.Fatalf("plan must apply and admit the length and count findings: applies=%v", applies.applies)
	}
	if applies.admits(otherFinding) {
		t.Fatal("plan admitted a finding that is neither the list length nor the total count")
	}

	// A total count that is not its own list length leaves the count outside
	// while the plan still applies to the length.
	badCount := buildDuplicateCollapseLengthPlan(shape, decode(dup, 4), decode(edges, 9))
	if !badCount.admits(lengthFinding) || badCount.admits(countFinding) {
		t.Fatalf("a candidate count that is not its length must stay outside: length=%v count=%v", badCount.admits(lengthFinding), badCount.admits(countFinding))
	}

	// No duplicate physical row in the baseline: nothing to collapse, nothing admitted.
	noDup := buildDuplicateCollapseLengthPlan(shape, decode(edges, 3), decode(edges, 3))
	if noDup.applies || noDup.admits(lengthFinding) {
		t.Fatal("plan applied to a baseline with no repeated id")
	}
}
