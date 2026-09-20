package goapiproof

import (
	"encoding/json"
	"fmt"
	"slices"
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
	boundFilter := map[string]string{"SOURCE_TYPE_SMALL": "sourceType", "NODE_ID_VALID": "nodeId"}
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
		if len(echo) != 1 || echo[0].List != "data.workGraphEdges.edges" || echo[0].Value != "X" || !slices.Equal(echo[0].Fields, fields) {
			t.Errorf("%s echo %+v, want fields %v", v.Name, echo, fields)
		}
		vars := v.Variables("org", Window{})
		v.Instance.Bind(vars, "X")
		filters := vars["filters"].(map[string]any)
		if filters["limit"] != 200 {
			t.Errorf("%s limit %v", v.Name, filters["limit"])
		}
		if filters[boundFilter[v.Name]] != "X" {
			t.Errorf("%s binds the run-supplied value to %v, want filters.%s", v.Name, filters, boundFilter[v.Name])
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
			// The baseline runs in the candidate's own order, so only the
			// order clause can refuse: the page-ownership sequence rule agrees.
			base := append([]collapseEdge{c.cand[0]}, c.cand...)
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

// Every request that carries the whole-list declaration carries its own page
// limit as the declaration's RequestLimit, and a baseline that reaches that
// limit is a page the limit may have cut, which the declaration refuses.
func TestWorkGraphEdgesCollapse_EveryCaseRefusesACutPage(t *testing.T) {
	carriers := 0
	for _, operation := range []string{"workGraphEdges", "releaseImpact"} {
		spec, err := SpecFor(operation)
		if err != nil {
			t.Fatal(err)
		}
		type requestCase struct {
			name      string
			variables func(string, Window) map[string]any
			parity    Options
		}
		cases := []requestCase{{"base", spec.Variables, spec.Parity}}
		for _, v := range spec.Variants {
			cases = append(cases, requestCase{v.Name, v.Variables, v.Parity})
		}
		for _, c := range cases {
			limit := workGraphEdgesDefaultLimit
			if filters, ok := c.variables("org", Window{})["filters"].(map[string]any); ok {
				if l, ok := filters["limit"].(int); ok {
					limit = l
				}
			}
			for _, d := range c.parity.BaselineDefects {
				shape := d.DuplicateCollapseLengthShape
				if shape == nil {
					continue
				}
				carriers++
				if shape.RequestLimit != limit {
					t.Errorf("%s/%s sends limit %d but the whole-list declaration carries RequestLimit %d", operation, c.name, limit, shape.RequestLimit)
				}
				// The baseline reaches the limit only through its duplicates (its
				// distinct set is below the limit, its raw length is at it): the
				// limit precondition refuses, the page may still be cut.
				edges := collapseEdges(limit)
				var base []collapseEdge
				for _, e := range edges[:limit/2] {
					base = append(base, e, e)
				}
				decode := func(list []collapseEdge, total int) any {
					return snapshotFromJSON(t, collapseBody(t, list, total)).Data
				}
				if len(base) < limit {
					t.Fatalf("fixture has %d baseline rows below limit %d", len(base), limit)
				}
				cut := buildDuplicateCollapseLengthPlan(shape, decode(base, len(base)), decode(edges[:limit/2], limit/2))
				if cut.applies {
					t.Errorf("%s/%s: the declaration applies to a baseline of %d rows at limit %d", operation, c.name, len(base), limit)
				}
				if limit > 2 {
					short := collapseEdges(limit / 2)
					var below []collapseEdge
					for _, e := range short[:limit/2-1] {
						below = append(below, e, e)
					}
					below = append(below, short[limit/2-1])
					if len(below) >= limit {
						t.Fatalf("fixture has %d rows, not below limit %d", len(below), limit)
					}
					if !buildDuplicateCollapseLengthPlan(shape, decode(below, len(below)), decode(short, len(short))).applies {
						t.Errorf("%s/%s: the declaration refuses a baseline of %d rows below limit %d", operation, c.name, len(below), limit)
					}
				}
			}
		}
	}
	// workGraphEdges: base and the two non-cut variants; releaseImpact: base,
	// NODE_ID_VALID and SOURCE_TYPE_POPULATED (LIMIT_ONE carries no defects).
	if carriers != 6 {
		t.Fatalf("found %d requests carrying the whole-list declaration, want 6", carriers)
	}
}

// The time axis: the two legs are read at different moments, so a merge or an
// edge write can land between them. Every cell is decided on the id sets and the
// rows, never on a raw row count.
func TestWorkGraphEdgesCollapse_TimeAxisRows(t *testing.T) {
	spec, err := SpecFor("releaseImpact")
	if err != nil {
		t.Fatal(err)
	}
	var shape *DuplicateCollapseLengthShape
	for _, d := range spec.Parity.BaselineDefects {
		if d.DuplicateCollapseLengthShape != nil {
			shape = d.DuplicateCollapseLengthShape
		}
	}
	if shape == nil {
		t.Fatal("releaseImpact declares no whole-list shape")
	}
	decode := func(edges []collapseEdge, total int) any {
		return snapshotFromJSON(t, collapseBody(t, edges, total)).Data
	}
	edges := collapseEdges(4)
	dup := []collapseEdge{edges[0], edges[0], edges[1], edges[2]}
	cases := []struct {
		name string
		base []collapseEdge
		cand []collapseEdge
		want bool
	}{
		{"same moment: duplicates collapse", dup, edges[:3], true},
		{"a merge ran before the baseline read: no repeated id, nothing to collapse", edges[:3], edges[:3], false},
		{"an edge written after the baseline read: the candidate has an id the baseline lacks", dup, edges, false},
		{"an edge written before the baseline read only: the baseline has an id the candidate lacks", dup, edges[:2], false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			plan := buildDuplicateCollapseLengthPlan(shape, decode(c.base, len(c.base)), decode(c.cand, len(c.cand)))
			if plan.applies != c.want {
				t.Fatalf("applies = %v, want %v", plan.applies, c.want)
			}
		})
	}
}

// One owner per page. For every request that carries the whole-page
// declaration, a page the limit did not cut is owned by that declaration alone
// (the per-edge declaration matches nothing on it), and a page the limit may
// have cut is owned by the per-edge declaration alone. Every combination of
// page cut or not, difference kind and shape present is run through Compare.
func TestWorkGraphEdgesOnePageOneOwner(t *testing.T) {
	const perEdge, wholePage = "CHAOS-5791", "CHAOS-6114"
	matched := func(r Result, ticket string) bool {
		for _, m := range r.BaselineDefectsMatched {
			if m == ticket {
				return true
			}
		}
		return false
	}
	type requestCase struct {
		name   string
		limit  int
		parity Options
	}
	var cases []requestCase
	for _, operation := range []string{"workGraphEdges", "releaseImpact"} {
		spec, err := SpecFor(operation)
		if err != nil {
			t.Fatal(err)
		}
		all := []requestCase{{operation + "/base", workGraphEdgesDefaultLimit, spec.Parity}}
		if operation == "releaseImpact" {
			all[0].limit = 200
		}
		for _, v := range spec.Variants {
			limit := workGraphEdgesDefaultLimit
			if filters, ok := v.Variables("org", Window{})["filters"].(map[string]any); ok {
				if l, ok := filters["limit"].(int); ok {
					limit = l
				}
			}
			all = append(all, requestCase{operation + "/" + v.Name, limit, v.Parity})
		}
		for _, c := range all {
			for _, d := range c.parity.BaselineDefects {
				if d.DuplicateCollapseLengthShape != nil {
					cases = append(cases, c)
					break
				}
			}
		}
	}
	if len(cases) != 6 {
		t.Fatalf("found %d requests carrying the whole-page declaration, want 6", len(cases))
	}
	perEdgeCases, nonCutCases := 0, 0
	compare := func(c requestCase, base, cand []collapseEdge) Result {
		return Compare(snapshotFromJSON(t, collapseBody(t, base, len(base))), snapshotFromJSON(t, collapseBody(t, cand, len(cand))), c.parity)
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			edges := collapseEdges(c.limit + 2)
			// Not cut: four rows, one repeated, against a limit above four.
			nonCutBase := []collapseEdge{edges[0], edges[0], edges[1], edges[2]}
			nonCut := []struct {
				name        string
				cand        []collapseEdge
				wantOutside bool
			}{
				{"collapse of the repeated row", edges[:3], false},
				{"equal length, one id invented", []collapseEdge{edges[0], edges[1], edges[2], edges[3]}, true},
				{"one distinct id dropped", edges[:2], true},
				{"one distinct id replaced", []collapseEdge{edges[0], edges[1], edges[3]}, true},
				{"same ids, other order", []collapseEdge{edges[1], edges[0], edges[2]}, true},
			}
			// The repeated row sits after the last distinct id, so the baseline's
			// trailing cursor names it while the candidate's names its own last
			// element: a cursor difference the whole-page declaration owns.
			trailing := compare(c, []collapseEdge{edges[0], edges[1], edges[2], edges[0]}, edges[:3])
			if trailing.DifferencesOutsideBaselineDefect != 0 || !matched(trailing, wholePage) || matched(trailing, perEdge) {
				t.Errorf("not cut, repeated row after the last id: outside=%d matched=%v", trailing.DifferencesOutsideBaselineDefect, trailing.BaselineDefectsMatched)
			}
			for _, k := range nonCut {
				result := compare(c, nonCutBase, k.cand)
				if (result.DifferencesOutsideBaselineDefect > 0) != k.wantOutside {
					t.Errorf("not cut, %s: outside=%d, want outside=%v (matched %v)", k.name, result.DifferencesOutsideBaselineDefect, k.wantOutside, result.BaselineDefectsMatched)
				}
				if matched(result, perEdge) {
					t.Errorf("not cut, %s: the per-edge declaration matched on a page it does not own", k.name)
				}
				if !k.wantOutside && !matched(result, wholePage) {
					t.Errorf("not cut, %s: the whole-page declaration did not match", k.name)
				}
			}
			// Cut: the baseline reaches the limit through one repeated row.
			cutBase := []collapseEdge{edges[0]}
			for i := 0; i < c.limit-1; i++ {
				cutBase = append(cutBase, edges[i])
			}
			cutCand := edges[:c.limit]
			result := compare(c, cutBase, cutCand)
			if matched(result, wholePage) {
				t.Errorf("cut page: the whole-page declaration matched on a page it refuses")
			}
			hasPerEdge := false
			for _, d := range c.parity.BaselineDefects {
				if d.WorkGraphEdgeDedupShape != nil {
					hasPerEdge = true
				}
			}
			if hasPerEdge {
				perEdgeCases++
				if !matched(result, perEdge) {
					t.Errorf("cut page: the per-edge declaration did not match (outside=%d)", result.DifferencesOutsideBaselineDefect)
				}
			} else {
				// A request whose purpose is a page the limit does not cut
				// carries only the whole-page declaration: a supplied
				// identifier whose page reaches the limit leaves every finding
				// outside, so the run reports it rather than absorbing it.
				nonCutCases++
				if result.DifferencesOutsideBaselineDefect == 0 || matched(result, perEdge) {
					t.Errorf("cut page on a non-cut request: outside=%d matched=%v, want findings left outside", result.DifferencesOutsideBaselineDefect, result.BaselineDefectsMatched)
				}
				// A dropped and an invented id on that cut page are not absorbed either.
				altered := append(append([]collapseEdge{}, cutCand[:c.limit-1]...), edges[c.limit], edges[c.limit+1])
				if res := compare(c, cutBase, altered); res.DifferencesOutsideBaselineDefect == 0 {
					t.Errorf("cut page on a non-cut request: a dropped and an invented id were admitted (matched %v)", res.BaselineDefectsMatched)
				}
			}
		})
	}
	// The two base requests keep both declarations; the four requests whose
	// purpose is a page the limit does not cut carry the whole-page one only.
	if perEdgeCases != 2 || nonCutCases != 4 {
		t.Fatalf("per-edge cases %d, non-cut-only cases %d, want 2 and 4", perEdgeCases, nonCutCases)
	}
}

// The whole-page plan's own rules that the request-level cases above do not
// reach alone: the baseline's distinct ids must run in the candidate's order,
// and the trailing cursor is admitted only when it names the candidate's last
// element.
func TestWorkGraphEdgesCollapse_PageOwnershipRules(t *testing.T) {
	spec, err := SpecFor("releaseImpact")
	if err != nil {
		t.Fatal(err)
	}
	var shape *DuplicateCollapseLengthShape
	for _, d := range spec.Parity.BaselineDefects {
		if d.DuplicateCollapseLengthShape != nil {
			shape = d.DuplicateCollapseLengthShape
		}
	}
	if shape == nil || !shape.OwnsPage || shape.CursorPath == "" {
		t.Fatalf("releaseImpact must declare a page-owning shape with a cursor path: %+v", shape)
	}
	decode := func(edges []collapseEdge) any {
		return snapshotFromJSON(t, collapseBody(t, edges, len(edges))).Data
	}
	e := collapseEdges(3)
	sameOrder := buildDuplicateCollapseLengthPlan(shape, decode([]collapseEdge{e[0], e[0], e[1], e[2]}), decode(e))
	if !sameOrder.applies {
		t.Fatal("plan must apply when the distinct baseline ids run in the candidate's order")
	}
	otherOrder := buildDuplicateCollapseLengthPlan(shape, decode([]collapseEdge{e[1], e[1], e[0], e[2]}), decode(e))
	if otherOrder.applies {
		t.Fatal("plan applied although the baseline's distinct ids run in another order than the candidate's")
	}
	cursor := Finding{Kind: FindingMismatch, Path: "$." + shape.CursorPath, Shape: ShapeValue}
	element := Finding{Kind: FindingMismatch, Path: "$.data.workGraphEdges.edges[2].edgeId", Shape: ShapeValue}
	if !sameOrder.admits(cursor) || !sameOrder.admits(element) {
		t.Fatalf("plan must own the cursor and element findings: cursor=%v element=%v", sameOrder.admits(cursor), sameOrder.admits(element))
	}
	// A candidate whose cursor does not name its own last element.
	wrong := snapshotFromJSON(t, collapseBody(t, e, 3)).Data
	root := wrong.(map[string]any)["workGraphEdges"].(map[string]any)["pageInfo"].(map[string]any)
	root["endCursor"] = e[0].id
	badCursor := buildDuplicateCollapseLengthPlan(shape, decode([]collapseEdge{e[0], e[0], e[1], e[2]}), wrong)
	if !badCursor.applies || badCursor.admits(cursor) {
		t.Fatalf("a cursor that is not the candidate's last id must stay outside: applies=%v admits=%v", badCursor.applies, badCursor.admits(cursor))
	}
}
