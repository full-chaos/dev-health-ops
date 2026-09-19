package goapiproof

import (
	"encoding/json"
	"testing"
)

// The invariant these tests hold (pageboundarycopy.go): a candidate row is
// admitted against baseline copies it does not equal only when its id is
// the id of the last row of a baseline page at its limit, no other id
// shares that row's created_at, every on-page copy of that id agrees with
// the others, and the candidate row differs from them only on the declared
// copy-rule field set; every other row keeps the whole-copy rule.

const (
	boundaryCopyBaseline  = "testdata/drilldownprs_boundarycopy_baseline_57bc6594.json"
	boundaryCopyCandidate = "testdata/drilldownprs_boundarycopy_candidate_1652e62a.json"
)

func scopeDrilldownPRsRequest(t *testing.T, name string) RESTRequest {
	t.Helper()
	spec, err := SpecForREST("REST:POST:/api/v1/drilldown/prs")
	if err != nil {
		t.Fatalf("SpecForREST: %v", err)
	}
	for _, req := range spec.Requests {
		if req.Name == name {
			return req
		}
	}
	t.Fatalf("request %q not found", name)
	return RESTRequest{}
}

// TestDrilldownPRs_CapturedBoundaryCopyHasNothingOutside pins the corpus
// verdict on a captured POST explicit_scope_and_sort pair (limit 25):
// baseline 25 rows carrying 18 distinct pull requests, the last one a
// single copy with merged_at null; the candidate's row for that pull
// request carries merged_at, every other field equal.
func TestDrilldownPRs_CapturedBoundaryCopyHasNothingOutside(t *testing.T) {
	req := scopeDrilldownPRsRequest(t, "explicit_scope_and_sort")
	result := compareAsRunner(t, req, readFixture(t, boundaryCopyBaseline), readFixture(t, boundaryCopyCandidate))
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d (%s), want 0", result.DifferencesOutsideBaselineDefect, outsideSummary(result))
	}
}

// TestDrilldownPRs_CapturedBoundaryCopyAdmittedByTheBoundaryRuleOnly reads
// which rule admits each shared candidate row of the captured pair: the
// boundary pull request by the page-boundary rule, every other row by the
// whole-copy rule. With the boundary's merged_at written onto the
// baseline copy, the whole-copy rule admits it and the boundary rule does
// not fire.
func TestDrilldownPRs_CapturedBoundaryCopyAdmittedByTheBoundaryRuleOnly(t *testing.T) {
	verdicts := func(base map[string]any) map[copyAdmission]int {
		baseSnap, err := DecodeRESTSnapshot(encodeBody(t, base))
		if err != nil {
			t.Fatal(err)
		}
		candSnap, err := DecodeRESTSnapshot(readFixture(t, boundaryCopyCandidate))
		if err != nil {
			t.Fatal(err)
		}
		baseData := InjectRESTDedupKeys(baseSnap.Data, drilldownPRsDedup.ListPath, drilldownPRsDedup.KeyFields)
		candData := InjectRESTDedupKeys(candSnap.Data, drilldownPRsDedup.ListPath, drilldownPRsDedup.KeyFields)
		baseList, _ := listAtDottedPath(baseData, "data.items")
		candList, _ := listAtDottedPath(candData, "data.items")
		boundary := &PageBoundary{SortField: "created_at", Limit: 25, CopyRule: &pullRequestRowCopyRule}
		cutID, cut := boundary.cutBoundaryID(baseList, RESTDedupKeyField)
		if !cut {
			t.Fatal("the captured baseline has no cut boundary id")
		}
		groups := map[string][]map[string]any{}
		for _, element := range baseList {
			object, id, _ := edgeObjectAndID(element, RESTDedupKeyField)
			groups[id] = append(groups[id], object)
		}
		out := map[copyAdmission]int{}
		for _, element := range candList {
			object, id, _ := edgeObjectAndID(element, RESTDedupKeyField)
			if group, ok := groups[id]; ok {
				v := boundary.admitCopy(&pullRequestRowCopyRule, group, object, id == cutID)
				if v == copyCutBoundary && id != cutID {
					t.Fatalf("boundary rule admitted %q, not the cut id %q", id, cutID)
				}
				out[v] = out[v] + 1
			}
		}
		return out
	}
	base := cursorBody(t, boundaryCopyBaseline)
	if got := verdicts(base); got[copyServed] != 17 || got[copyCutBoundary] != 1 || got[copyRefused] != 0 {
		t.Fatalf("captured verdicts = %v, want 17 served and 1 boundary", got)
	}
	items := cursorItems(base)
	boundaryRow := items[len(items)-1].(map[string]any)
	for _, item := range cursorItems(cursorBody(t, boundaryCopyCandidate)) {
		row := item.(map[string]any)
		if row["repo_id"] == boundaryRow["repo_id"] && row["number"] == boundaryRow["number"] {
			merged := row["merged_at"].(string)
			boundaryRow["merged_at"] = merged[:len(merged)-1]
		}
	}
	if got := verdicts(base); got[copyServed] != 18 || got[copyCutBoundary] != 0 {
		t.Fatalf("with the boundary copy equal to the candidate: verdicts = %v, want 18 served", got)
	}
}

// boundaryVariant is the candidate's boundary row relative to its
// on-page copies.
type boundaryVariant string

const (
	variantEqual        boundaryVariant = "equal to the on-page copy"
	variantInsideSet    boundaryVariant = "differs inside the copy-rule set"
	variantOutsideSet   boundaryVariant = "differs outside the copy-rule set"
	variantCopiesDiffer boundaryVariant = "on-page copies disagree"
)

type boundaryCell struct {
	variant    boundaryVariant
	atLimit    bool
	tie        bool
	atBoundary bool
	copies     int
	// order places the two rows that are not judged: in order on both
	// legs, swapped on the baseline only, or tied at one instant and
	// swapped on the candidate only.
	order boundaryOrder
}

type boundaryOrder string

const (
	orderSorted           boundaryOrder = "both legs ordered"
	orderBaselineUnsorted boundaryOrder = "baseline out of order"
	orderTieSwapped       boundaryOrder = "a tie away from the judged row, swapped on the candidate"
)

// boundaryRow renders one pull-request row as a plane serves it.
func boundaryRow(number int, createdAt string, candidate bool, title string, merged, link any) map[string]any {
	suffix := ""
	if candidate {
		suffix = "Z"
		if s, ok := merged.(string); ok {
			merged = s + suffix
		}
	}
	return map[string]any{
		"repo_id": "ABC", "number": number, "title": title, "author": "a",
		"created_at": createdAt + suffix, "merged_at": merged,
		"first_review_at": nil, "review_latency_hours": nil, "link": link,
	}
}

// boundaryBodies builds one cell: ids 1..3 newest first, the judged id
// placed last (atBoundary) or first; its on-page copies are the
// candidate's row variant's counterpart; tie gives the row before the
// judged row the judged row's created_at.
func boundaryBodies(c boundaryCell) (base, cand []any, limit int) {
	times := []string{"2024-01-04T00:00:00", "2024-01-03T00:00:00", "2024-01-02T00:00:00"}
	judged := 3
	if !c.atBoundary {
		judged = 1
	}
	if c.tie {
		times[1] = times[2]
		if !c.atBoundary {
			times[1] = times[0]
		}
	}
	if c.order == orderTieSwapped {
		// The two rows other than the judged one share an instant.
		if c.atBoundary {
			times[1] = times[0]
		} else {
			times[2] = times[1]
		}
	}
	for n := 1; n <= 3; n++ {
		at := times[n-1]
		if n != judged {
			base = append(base, boundaryRow(n, at, false, "t", nil, nil))
			cand = append(cand, boundaryRow(n, at, true, "t", nil, nil))
			continue
		}
		for i := 0; i < c.copies; i++ {
			title := "t"
			if c.variant == variantCopiesDiffer && i > 0 {
				title = "u"
			}
			base = append(base, boundaryRow(n, at, false, title, nil, nil))
		}
		switch c.variant {
		case variantEqual:
			cand = append(cand, boundaryRow(n, at, true, "t", nil, nil))
		case variantInsideSet:
			cand = append(cand, boundaryRow(n, at, true, "t", "2024-01-05T00:00:00", nil))
		case variantOutsideSet:
			cand = append(cand, boundaryRow(n, at, true, "t", nil, "ABC-123"))
		case variantCopiesDiffer:
			cand = append(cand, boundaryRow(n, at, true, "v", nil, nil))
		}
	}
	others := []int{}
	for i, row := range cand {
		if row.(map[string]any)["number"] != judged {
			others = append(others, i)
		}
	}
	switch c.order {
	case orderBaselineUnsorted:
		baseOthers := []int{}
		for i, row := range base {
			if row.(map[string]any)["number"] != judged {
				baseOthers = append(baseOthers, i)
			}
		}
		base[baseOthers[0]], base[baseOthers[1]] = base[baseOthers[1]], base[baseOthers[0]]
	case orderTieSwapped:
		cand[others[0]], cand[others[1]] = cand[others[1]], cand[others[0]]
	}
	limit = len(base)
	if !c.atLimit {
		limit++
	}
	return base, cand, limit
}

// boundaryOracle is the invariant for one cell. A cell whose rows all
// equal their on-page copies is admitted when both legs are ordered, and,
// with a repeated copy on the page, also when the rows are reordered: the
// duplicate-row shape reads no leg's order (its behaviour before the
// boundary rule, kept unchanged). The boundary rule adds exactly one
// admission: the in-set difference on the cut id of an ordered page at
// its limit with no tie at the boundary.
func boundaryOracle(c boundaryCell) bool {
	if c.variant == variantEqual {
		return c.order == orderSorted || c.copies == 2
	}
	return c.variant == variantInsideSet && c.order == orderSorted && c.atLimit && !c.tie && c.atBoundary
}

func boundarySnapshot(t *testing.T, items []any) Snapshot {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"items": items})
	if err != nil {
		t.Fatal(err)
	}
	snap, err := DecodeRESTSnapshot(raw)
	if err != nil {
		t.Fatal(err)
	}
	snap.Data = InjectRESTDedupKeys(snap.Data, drilldownPRsDedup.ListPath, drilldownPRsDedup.KeyFields)
	return snap
}

// TestPageBoundaryCopy_EnumeratedCellsMatchTheInvariant runs every cell of
// variant x {at limit, under} x {tie at the judged instant, no tie} x
// {judged id at the boundary, not} x {1, 2 on-page copies} x {both legs
// ordered, baseline out of order, a tie away from the judged row swapped
// on the candidate} through each pull-request route's
// own bound Options; a cell is admitted (outside=0) exactly when the
// oracle says so.
func TestPageBoundaryCopy_EnumeratedCellsMatchTheInvariant(t *testing.T) {
	routes := map[string]func(limit int) Options{
		"scope route":       drilldownPRsParityWithLimit,
		"person route":      func(limit int) Options { return parityWithPageCutLimit(personDrilldownPRsParity, limit) },
		"team-scoped route": drilldownPRsTeamScopedParityWithLimit,
	}
	variants := []boundaryVariant{variantEqual, variantInsideSet, variantOutsideSet, variantCopiesDiffer}
	admitted, refused := 0, 0
	for routeName, bind := range routes {
		for _, variant := range variants {
			for _, atLimit := range []bool{true, false} {
				for _, tie := range []bool{false, true} {
					for _, atBoundary := range []bool{true, false} {
						for _, copies := range []int{1, 2} {
							for _, order := range []boundaryOrder{orderSorted, orderBaselineUnsorted, orderTieSwapped} {
								c := boundaryCell{variant, atLimit, tie, atBoundary, copies, order}
								if variant == variantCopiesDiffer && copies == 1 || tie && order == orderTieSwapped {
									continue
								}
								base, cand, limit := boundaryBodies(c)
								result := Compare(boundarySnapshot(t, base), boundarySnapshot(t, cand), bind(limit))
								got := result.DifferencesOutsideBaselineDefect == 0 && result.StructuralRefusal == ""
								want := boundaryOracle(c)
								if got != want {
									t.Errorf("%s %+v: admitted = %v, want %v (%s)", routeName, c, got, want, outsideSummary(result))
								}
								if got {
									admitted++
								} else {
									refused++
								}
							}
						}
					}
				}
			}
		}
	}
	t.Logf("cells: admitted %d, refused %d", admitted, refused)
	if admitted == 0 || refused == 0 {
		t.Fatal("the enumeration did not exercise both verdicts")
	}
}

// TestPageBoundaryCopy_CutBoundaryID pins which id the boundary names.
func TestPageBoundaryCopy_CutBoundaryID(t *testing.T) {
	row := func(id, at string) any { return map[string]any{"id": id, "created_at": at} }
	cases := []struct {
		name   string
		b      *PageBoundary
		list   []any
		wantID string
		wantOK bool
	}{
		{"at limit, last id unique at its instant", &PageBoundary{SortField: "created_at", Limit: 2}, []any{row("A", "2024-01-02T00:00:00"), row("B", "2024-01-01T00:00:00")}, "B", true},
		{"over limit", &PageBoundary{SortField: "created_at", Limit: 1}, []any{row("A", "2024-01-02T00:00:00"), row("B", "2024-01-01T00:00:00")}, "B", true},
		{"last id repeated at its instant", &PageBoundary{SortField: "created_at", Limit: 2}, []any{row("B", "2024-01-01T00:00:00"), row("B", "2024-01-01T00:00:00")}, "B", true},
		{"under limit", &PageBoundary{SortField: "created_at", Limit: 3}, []any{row("A", "2024-01-02T00:00:00"), row("B", "2024-01-01T00:00:00")}, "", false},
		{"zero limit", &PageBoundary{SortField: "created_at"}, []any{row("A", "2024-01-02T00:00:00")}, "", false},
		{"nil boundary", nil, []any{row("A", "2024-01-02T00:00:00")}, "", false},
		{"another id ties at the boundary instant", &PageBoundary{SortField: "created_at", Limit: 2}, []any{row("A", "2024-01-01T00:00:00"), row("B", "2024-01-01T00:00:00")}, "", false},
		{"a tie far from the boundary", &PageBoundary{SortField: "created_at", Limit: 3}, []any{row("A", "2024-01-02T00:00:00"), row("C", "2024-01-02T00:00:00"), row("B", "2024-01-01T00:00:00")}, "B", true},
		{"last row without an id", &PageBoundary{SortField: "created_at", Limit: 1}, []any{map[string]any{"created_at": "2024-01-01T00:00:00"}}, "", false},
		{"last row sort value unparseable", &PageBoundary{SortField: "created_at", Limit: 1}, []any{row("A", "ABC-123")}, "", false},
		{"an earlier row without an id", &PageBoundary{SortField: "created_at", Limit: 2}, []any{map[string]any{"created_at": "2024-01-02T00:00:00"}, row("B", "2024-01-01T00:00:00")}, "", false},
		{"page out of order", &PageBoundary{SortField: "created_at", Limit: 3}, []any{row("A", "2024-01-03T00:00:00"), row("C", "2024-01-01T00:00:00"), row("B", "2024-01-02T00:00:00")}, "", false},
		{"an earlier row sort value unparseable", &PageBoundary{SortField: "created_at", Limit: 2}, []any{row("A", "ABC-123"), row("B", "2024-01-01T00:00:00")}, "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			id, ok := c.b.cutBoundaryID(c.list, "id")
			if id != c.wantID || ok != c.wantOK {
				t.Fatalf("cutBoundaryID = %q, %v; want %q, %v", id, ok, c.wantID, c.wantOK)
			}
		})
	}
}

// TestPageBoundaryCopy_CandidateIsCutBoundaryCopy pins the row judgement
// over its input domain.
func TestPageBoundaryCopy_CandidateIsCutBoundaryCopy(t *testing.T) {
	rule := &DuplicateCopyRule{WriteOnceFields: []string{"closed_at"}, RewrittenFields: []string{"title"}}
	row := func(title, closed any, extra ...string) map[string]any {
		out := map[string]any{"id": "ABC-123", "title": title, "closed_at": closed, "state": "open"}
		for i := 0; i+1 < len(extra); i += 2 {
			out[extra[i]] = extra[i+1]
		}
		return out
	}
	cases := []struct {
		name  string
		rule  *DuplicateCopyRule
		group []map[string]any
		cand  map[string]any
		want  bool
	}{
		{"rewritten field differs", rule, []map[string]any{row("a", nil)}, row("b", nil), true},
		{"write-once null on page, populated on candidate", rule, []map[string]any{row("a", nil)}, row("a", "2024-01-01T00:00:00Z"), true},
		{"write-once populated on page, null on candidate", rule, []map[string]any{row("a", "2024-01-01T00:00:00")}, row("a", nil), false},
		{"write-once populated on page, other value on candidate", rule, []map[string]any{row("a", "2024-01-01T00:00:00")}, row("a", "2024-01-02T00:00:00Z"), false},
		{"write-once populated on page, same instant on candidate", rule, []map[string]any{row("a", "2024-01-01T00:00:00")}, row("b", "2024-01-01T00:00:00Z"), true},
		{"field outside the set differs", rule, []map[string]any{row("a", nil)}, row("a", nil, "state", "closed"), false},
		{"candidate carries an extra key", rule, []map[string]any{row("a", nil)}, row("a", nil, "body", "x"), false},
		{"candidate lacks a copy-rule key", rule, []map[string]any{row("a", nil)}, map[string]any{"id": "ABC-123", "closed_at": nil, "state": "open"}, false},
		{"on-page copies disagree", rule, []map[string]any{row("a", nil), row("b", nil)}, row("c", nil), false},
		{"two agreeing on-page copies", rule, []map[string]any{row("a", nil), row("a", nil)}, row("b", nil), true},
		{"empty rule", &DuplicateCopyRule{}, []map[string]any{row("a", nil)}, row("b", nil), false},
		{"nil rule", nil, []map[string]any{row("a", nil)}, row("b", nil), false},
		{"no on-page copy", rule, nil, row("b", nil), false},
		{"no candidate", rule, []map[string]any{row("a", nil)}, nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.rule.candidateIsCutBoundaryCopy(c.group, c.cand); got != c.want {
				t.Fatalf("candidateIsCutBoundaryCopy = %v, want %v", got, c.want)
			}
		})
	}
}

// TestPageBoundaryCopy_AdmitCopyNamesTheRule pins the order: the whole-copy
// rule first, the boundary rule only for the cut id and only where the
// whole-copy rule refuses.
func TestPageBoundaryCopy_AdmitCopyNamesTheRule(t *testing.T) {
	rule := &DuplicateCopyRule{RewrittenFields: []string{"title"}}
	b := &PageBoundary{CopyRule: rule}
	group := []map[string]any{{"id": "ABC-123", "title": "a"}}
	cases := []struct {
		name string
		b    *PageBoundary
		cand map[string]any
		cut  bool
		want copyAdmission
	}{
		{"equal row at the cut boundary", b, map[string]any{"id": "ABC-123", "title": "a"}, true, copyServed},
		{"equal row elsewhere", b, map[string]any{"id": "ABC-123", "title": "a"}, false, copyServed},
		{"rewritten row at the cut boundary", b, map[string]any{"id": "ABC-123", "title": "b"}, true, copyCutBoundary},
		{"rewritten row elsewhere", b, map[string]any{"id": "ABC-123", "title": "b"}, false, copyRefused},
		{"rewritten row, nil boundary", nil, map[string]any{"id": "ABC-123", "title": "b"}, true, copyRefused},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.b.admitCopy(nil, group, c.cand, c.cut); got != c.want {
				t.Fatalf("admitCopy = %v, want %v", got, c.want)
			}
		})
	}
}

// TestPageBoundaryCopy_BoundaryRowDoesNotExcuseOtherViolations keeps the
// family's other accounting rules around an admissible boundary row: a
// missing candidate row, a candidate out of order, and a boundary row that
// differs on a field outside the copy-rule set each stay outside.
func TestPageBoundaryCopy_BoundaryRowDoesNotExcuseOtherViolations(t *testing.T) {
	c := boundaryCell{variant: variantInsideSet, atLimit: true, atBoundary: true, copies: 1}
	cases := []struct {
		name   string
		mutate func(cand []any) []any
	}{
		{"a candidate row missing", func(cand []any) []any { return append([]any{}, cand[1:]...) }},
		{"candidate out of order", func(cand []any) []any { return []any{cand[1], cand[0], cand[2]} }},
		{"boundary row also differs outside the set", func(cand []any) []any {
			cand[2].(map[string]any)["link"] = "ABC-123"
			return cand
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			base, cand, limit := boundaryBodies(c)
			result := Compare(boundarySnapshot(t, base), boundarySnapshot(t, tc.mutate(cand)), drilldownPRsParityWithLimit(limit))
			if result.DifferencesOutsideBaselineDefect == 0 && result.StructuralRefusal == "" {
				t.Fatalf("admitted: %s", outsideSummary(result))
			}
		})
	}
	base, cand, limit := boundaryBodies(c)
	if result := Compare(boundarySnapshot(t, base), boundarySnapshot(t, cand), drilldownPRsParityWithLimit(limit)); result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("the unmutated cell is refused: %s", outsideSummary(result))
	}
}

// TestPageBoundaryCopy_EachShapeLimitsTheRuleToTheCutID holds the two
// shapes that apply the boundary rule on their own, apart from the
// family's accounting gate: an id not at the boundary, differing only
// inside the copy-rule set, is refused by each shape; the same difference
// on the cut id is admitted.
func TestPageBoundaryCopy_EachShapeLimitsTheRuleToTheCutID(t *testing.T) {
	var dedup *WorkGraphEdgeDedupShape
	var pageCut *DuplicateCollapsePageCutShape
	for _, d := range drilldownPRsParity.BaselineDefects {
		if d.WorkGraphEdgeDedupShape != nil && d.WorkGraphEdgeDedupShape.PageBoundary != nil {
			dedup = d.WorkGraphEdgeDedupShape
		}
		if d.DuplicateCollapsePageCutShape != nil {
			pageCut = d.DuplicateCollapsePageCutShape
		}
	}
	if dedup == nil || pageCut == nil {
		t.Fatal("the scope route declares no boundary dedup entry or no page-cut entry")
	}
	for _, atBoundary := range []bool{true, false} {
		base, cand, limit := boundaryBodies(boundaryCell{variant: variantInsideSet, atLimit: true, atBoundary: atBoundary, copies: 2})
		baseSnap, candSnap := boundarySnapshot(t, base), boundarySnapshot(t, cand)
		judgedID := "ABC" + restDedupKeySeparator + "3"
		if !atBoundary {
			judgedID = "ABC" + restDedupKeySeparator + "1"
		}

		d := *dedup
		boundary := *d.PageBoundary
		boundary.Limit = limit
		d.PageBoundary = &boundary
		plan := buildWorkGraphEdgeDedupPlan(&d, baseSnap.Data, candSnap.Data)
		if got := plan.applies && plan.admittedIDs[judgedID]; got != atBoundary {
			t.Errorf("dedup shape, judged id at boundary = %v: admitted = %v", atBoundary, got)
		}

		p := *pageCut
		p.Limit = limit
		if got := buildDuplicateCollapsePageCutPlan(&p, baseSnap.Data, candSnap.Data).applies; got != atBoundary {
			t.Errorf("page-cut shape, judged id at boundary = %v: applies = %v", atBoundary, got)
		}
	}
}

// TestPageBoundaryCopy_UnorderedPageWithoutCopiesStaysOutside: a baseline
// page out of created_at order with no repeated id, the candidate ordered
// with its last row retitled, has no page boundary; every finding stays
// outside.
func TestPageBoundaryCopy_UnorderedPageWithoutCopiesStaysOutside(t *testing.T) {
	base := []any{
		boundaryRow(1, "2024-01-03T00:00:00", false, "t", nil, nil),
		boundaryRow(3, "2024-01-01T00:00:00", false, "t", nil, nil),
		boundaryRow(2, "2024-01-02T00:00:00", false, "t", nil, nil),
	}
	cand := []any{
		boundaryRow(1, "2024-01-03T00:00:00", true, "t", nil, nil),
		boundaryRow(2, "2024-01-02T00:00:00", true, "u", nil, nil),
		boundaryRow(3, "2024-01-01T00:00:00", true, "t", nil, nil),
	}
	result := Compare(boundarySnapshot(t, base), boundarySnapshot(t, cand), drilldownPRsParityWithLimit(3))
	if len(result.CoveredByShape) != 0 || result.DifferencesOutsideBaselineDefect == 0 {
		t.Fatalf("covered %v, outside %d: want every finding outside", result.CoveredByShape, result.DifferencesOutsideBaselineDefect)
	}
}
