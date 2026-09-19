package goapiproof

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"
)

// This file holds GET /api/v1/people/{person_id}/drilldown/prs' next_cursor
// admission to one invariant: a next_cursor difference is admitted only as
// the computed consequence of baseline duplicate copies consuming page
// slots; any other cursor difference is outside.
//
// The captured bodies come from one production proof run. In every
// baseline page, 13 pull requests arrive as two physical copies each, so
// the page's 50 rows carry 37 distinct pull requests; the candidate's 50
// rows are 50 distinct pull requests with dedup(baseline) as their literal
// prefix. Both lists are 50 long, so no length finding exists, and each
// leg's next_cursor is its own last row's created_at: the baseline's names
// a later instant than the candidate's. Request cases share bodies:
// limit_zero_falls_back_to_default returned the default case's pair.
type personDrilldownPRsCursorCase struct {
	request   string
	baseline  string
	candidate string
}

var personDrilldownPRsCursorCases = []personDrilldownPRsCursorCase{
	{"drilldown_prs_default", "testdata/persondrilldownprs_pagecutcursor_baseline_default_4bbbbd15.json", "testdata/persondrilldownprs_pagecutcursor_candidate_default_baa32fdf.json"},
	{"valid_cursor", "testdata/persondrilldownprs_pagecutcursor_baseline_cursor_0402589b.json", "testdata/persondrilldownprs_pagecutcursor_candidate_cursor_06f311a0.json"},
	{"limit_above_ceiling", "testdata/persondrilldownprs_pagecutcursor_baseline_ceiling_0ea58747.json", "testdata/persondrilldownprs_pagecutcursor_candidate_ceiling_5282f06d.json"},
	{"limit_zero_falls_back_to_default", "testdata/persondrilldownprs_pagecutcursor_baseline_default_4bbbbd15.json", "testdata/persondrilldownprs_pagecutcursor_candidate_default_baa32fdf.json"},
}

const personCursorFindingPath = "$.data.next_cursor"

// withoutCursorAdmission returns req with the page-cut entry's CursorPath
// cleared, every other declaration unchanged.
func withoutCursorAdmission(req RESTRequest) RESTRequest {
	defects := make([]BaselineDefect, len(req.Parity.BaselineDefects))
	copy(defects, req.Parity.BaselineDefects)
	for i, defect := range defects {
		if defect.DuplicateCollapsePageCutShape != nil {
			shape := *defect.DuplicateCollapsePageCutShape
			shape.CursorPath = ""
			defect.DuplicateCollapsePageCutShape = &shape
			defects[i] = defect
		}
	}
	req.Parity.BaselineDefects = defects
	return req
}

// TestPersonDrilldownPRs_CapturedPageCutCursorCasesHaveNothingOutside pins
// the corpus verdict on every captured case, and that the cursor is the
// one finding the page-cut entry's cursor rule admits: with CursorPath
// cleared, exactly that finding is outside.
func TestPersonDrilldownPRs_CapturedPageCutCursorCasesHaveNothingOutside(t *testing.T) {
	for _, c := range personDrilldownPRsCursorCases {
		t.Run(c.request, func(t *testing.T) {
			req := personDrilldownPRsRequest(t, c.request)
			base, cand := readFixture(t, c.baseline), readFixture(t, c.candidate)
			result := compareAsRunner(t, req, base, cand)
			if result.DifferencesOutsideBaselineDefect != 0 {
				t.Fatalf("outside = %d, want 0 -- findings %+v", result.DifferencesOutsideBaselineDefect, outsideSummary(result))
			}
			if !hasFinding(result, personCursorFindingPath, ShapeValue) {
				t.Fatal("no next_cursor value finding: the fixture no longer exercises the cursor admission")
			}
			if hasFinding(result, "$.data.items", ShapeLength) {
				t.Fatal("a length finding exists: the fixture no longer exercises the equal-length page cut")
			}
			stripped := compareAsRunner(t, withoutCursorAdmission(req), base, cand)
			if stripped.DifferencesOutsideBaselineDefect != 1 || stripped.OutsideByShape[ShapeValue] != 1 {
				t.Fatalf("without the cursor rule: outside = %d %v, want exactly the one cursor value finding", stripped.DifferencesOutsideBaselineDefect, stripped.OutsideByShape)
			}
		})
	}
}

func outsideSummary(result Result) string {
	return FormatShapeCounts(result.CoveredByShape, result.OutsideByShape)
}

// cursorBody decodes a captured fixture for mutation.
func cursorBody(t *testing.T, path string) map[string]any {
	t.Helper()
	body, _ := retitleItems(t, path)
	return body
}

func cursorItems(body map[string]any) []any { return body["items"].([]any) }

func rowCreatedAt(item any) string { return item.(map[string]any)["created_at"].(string) }

// TestPersonDrilldownPRs_CapturedPageCutCursorRefusals keeps every cell the
// cursor rule refuses on the captured default case: each mutation breaks
// exactly one conjunct, and the next_cursor finding (or the list itself)
// is outside.
func TestPersonDrilldownPRs_CapturedPageCutCursorRefusals(t *testing.T) {
	const basePath = "testdata/persondrilldownprs_pagecutcursor_baseline_default_4bbbbd15.json"
	const candPath = "testdata/persondrilldownprs_pagecutcursor_candidate_default_baa32fdf.json"
	req := personDrilldownPRsRequest(t, "drilldown_prs_default")
	// A cursor conjunct broken alone leaves the cursor finding outside
	// by itself; a broken list rule refuses the whole list through the
	// family's candidate accounting, the list's length beside the cursor.
	cursorOnly := map[string]int{ShapeValue: 1}
	listAndCursor := map[string]int{ShapeLength: 1, ShapeValue: 1}
	cases := []struct {
		name        string
		mutate      func(base, cand map[string]any)
		wantOutside map[string]int
	}{
		{"baseline under its limit", func(base, cand map[string]any) {
			items := cursorItems(base)
			base["items"] = items[:len(items)-1]
			base["next_cursor"] = rowCreatedAt(items[len(items)-2])
		}, listAndCursor},
		{"baseline cursor names a row other than its last", func(base, cand map[string]any) {
			base["next_cursor"] = rowCreatedAt(cursorItems(base)[0])
		}, cursorOnly},
		{"candidate cursor names a row other than its last", func(base, cand map[string]any) {
			items := cursorItems(cand)
			cand["next_cursor"] = rowCreatedAt(items[len(items)-2])
		}, cursorOnly},
		{"candidate cursor later than the baseline cursor", func(base, cand map[string]any) {
			cand["next_cursor"] = "2030-01-01T00:00:00Z"
		}, cursorOnly},
		{"baseline cursor null", func(base, cand map[string]any) {
			base["next_cursor"] = nil
		}, map[string]int{ShapeNull: 1}},
		{"candidate cursor unparseable", func(base, cand map[string]any) {
			cand["next_cursor"] = "ABC-123"
		}, cursorOnly},
		{"candidate row missing", func(base, cand map[string]any) {
			items := cursorItems(cand)
			cand["items"] = append(append([]any{}, items[:5]...), items[6:]...)
		}, listAndCursor},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			base, cand := cursorBody(t, basePath), cursorBody(t, candPath)
			c.mutate(base, cand)
			result := compareAsRunner(t, req, encodeBody(t, base), encodeBody(t, cand))
			if !reflect.DeepEqual(result.OutsideByShape, c.wantOutside) {
				t.Fatalf("outside by shape = %v, want %v", result.OutsideByShape, c.wantOutside)
			}
			if !hasFinding(result, personCursorFindingPath, ShapeValue) && !hasFinding(result, personCursorFindingPath, ShapeNull) {
				t.Fatal("no next_cursor finding")
			}
		})
	}
}

// TestPersonDrilldownPRs_CapturedPageCutCursorRefusesWithoutDuplicates is
// the no-duplicate cell: the baseline replaced by the candidate's own rows
// in the reference plane's rendering (the page a deduplicated reference
// read would serve). With no repeated id there is no page cut, so the two
// pages are the same rows and a cursor difference can only name a row
// other than the baseline's last; it is outside.
func TestPersonDrilldownPRs_CapturedPageCutCursorRefusesWithoutDuplicates(t *testing.T) {
	base := cursorBody(t, "testdata/persondrilldownprs_pagecutcursor_baseline_default_4bbbbd15.json")
	cand := cursorBody(t, "testdata/persondrilldownprs_pagecutcursor_candidate_default_baa32fdf.json")
	candItems := cursorItems(cand)
	distinct := make([]any, 0, len(candItems))
	for _, item := range candItems {
		row := map[string]any{}
		for k, v := range item.(map[string]any) {
			row[k] = v
		}
		for _, field := range []string{"created_at", "merged_at", "first_review_at"} {
			if s, ok := row[field].(string); ok {
				row[field] = s[:len(s)-1]
			}
		}
		distinct = append(distinct, row)
	}
	base["items"] = distinct
	base["next_cursor"] = rowCreatedAt(distinct[36])
	req := personDrilldownPRsRequest(t, "drilldown_prs_default")
	result := compareAsRunner(t, req, encodeBody(t, base), encodeBody(t, cand))
	if result.DifferencesOutsideBaselineDefect != 1 || !hasFinding(result, personCursorFindingPath, ShapeValue) {
		t.Fatalf("outside = %d (%s), want exactly the next_cursor finding", result.DifferencesOutsideBaselineDefect, outsideSummary(result))
	}
}

// cursorChoice is one leg's generated next_cursor: absent (null), the
// leg's own last row, the leg's own first row, an instant later than
// every generated row, or an unparseable string.
type cursorChoice int

const (
	cursorNull cursorChoice = iota
	cursorOwnLast
	cursorOwnFirst
	cursorLater
	cursorMalformed
)

var cursorChoices = []cursorChoice{cursorNull, cursorOwnLast, cursorOwnFirst, cursorLater, cursorMalformed}

// cursorValue renders one choice for a leg, naive on the reference plane
// and with an offset on the candidate's, as each plane serves it.
func cursorValue(choice cursorChoice, rows []enumRow, candidate bool) any {
	suffix := ""
	if candidate {
		suffix = "Z"
	}
	switch choice {
	case cursorOwnLast:
		if len(rows) == 0 {
			return nil
		}
		return rows[len(rows)-1].json(candidate)["created_at"]
	case cursorOwnFirst:
		if len(rows) == 0 {
			return nil
		}
		return rows[0].json(candidate)["created_at"]
	case cursorLater:
		return "2024-02-01T00:00:00" + suffix
	case cursorMalformed:
		return "ABC-123"
	}
	return nil
}

func cursorSnapshot(t *testing.T, rows []enumRow, cursor any, candidate bool) Snapshot {
	t.Helper()
	items := make([]any, len(rows))
	for i, r := range rows {
		items[i] = r.json(candidate)
	}
	raw, err := json.Marshal(map[string]any{"items": items, "next_cursor": cursor})
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

// cursorInstant parses a generated cursor; ok is false for null or an
// unparseable value.
func cursorInstant(value any) (time.Time, bool) {
	s, ok := value.(string)
	if !ok {
		return time.Time{}, false
	}
	return parseTimestamp(s)
}

// cursorOracle is the invariant, written from its statement alone: a
// cursor pair naming different instants is admissible only when the rows
// satisfy the accounting invariant, the baseline fills its limit with at
// least one repeated id, each cursor is its own leg's last row's instant,
// and the candidate's is not later than the baseline's.
func cursorOracle(base, cand []enumRow, baseCursor, candCursor any, limit int) bool {
	if !enumInvariant(base, cand, limit, false) || len(base) != limit || len(cand) == 0 {
		return false
	}
	ids := map[string]bool{}
	repeated := false
	for _, r := range base {
		repeated = repeated || ids[r.id]
		ids[r.id] = true
	}
	if !repeated {
		return false
	}
	b, okB := cursorInstant(baseCursor)
	c, okC := cursorInstant(candCursor)
	if !okB || !okC {
		return false
	}
	baseLast, _ := cursorInstant(base[len(base)-1].json(false)["created_at"])
	candLast, _ := cursorInstant(cand[len(cand)-1].json(true)["created_at"])
	return b.Equal(baseLast) && c.Equal(candLast) && !c.After(b)
}

// TestPersonDrilldownPRs_EnumeratedCursorAdmissionImpliesTheInvariant runs
// every generated baseline against every candidate of up to two rows,
// with and without a page cut, under every pair of cursor choices,
// through the person route's own bound Options. A comparison admitted with
// a next_cursor finding naming a different instant on each leg must
// satisfy cursorOracle. The integration build runs candidates up to three
// rows.
func TestPersonDrilldownPRs_EnumeratedCursorAdmissionImpliesTheInvariant(t *testing.T) {
	runCursorEnumeration(t, 2)
}

func runCursorEnumeration(t *testing.T, maxLen int) {
	baselines := enumBaselines()
	candidates := enumCandidates(maxLen)
	compared, admittedShift, refusedShift := 0, 0, 0
	// Compare never writes to a snapshot's data, so each generated
	// candidate body is decoded once and shared.
	candSnaps := make([][]Snapshot, len(candidates))
	for ci, cand := range candidates {
		for _, choice := range cursorChoices {
			candSnaps[ci] = append(candSnaps[ci], cursorSnapshot(t, cand, cursorValue(choice, cand, true), true))
		}
	}
	for _, cut := range []bool{false, true} {
		for _, base := range baselines {
			limit := drilldownPRsDefaultLimit
			if cut {
				limit = len(base)
			}
			opts := parityWithPageCutLimit(personDrilldownPRsParity, limit)
			for _, baseChoice := range cursorChoices {
				baseCursor := cursorValue(baseChoice, base, false)
				baseSnap := cursorSnapshot(t, base, baseCursor, false)
				for ci, cand := range candidates {
					for choiceIndex, candChoice := range cursorChoices {
						candCursor := cursorValue(candChoice, cand, true)
						b, okB := cursorInstant(baseCursor)
						c, okC := cursorInstant(candCursor)
						if okB && okC && b.Equal(c) {
							// One instant in two renderings: the
							// timestamp-rendering entry's case.
							continue
						}
						compared++
						result := Compare(baseSnap, candSnaps[ci][choiceIndex], opts)
						if !hasFinding(result, personCursorFindingPath, ShapeValue) && !hasFinding(result, personCursorFindingPath, ShapeNull) {
							continue
						}
						if result.DifferencesOutsideBaselineDefect != 0 || result.StructuralRefusal != "" {
							refusedShift++
							continue
						}
						admittedShift++
						if !cursorOracle(base, cand, baseCursor, candCursor, limit) {
							t.Fatalf("cut=%v baseline %s cursor %v, candidate %s cursor %v: admitted, invariant broken", cut, rowsString(base), baseCursor, rowsString(cand), candCursor)
						}
					}
				}
			}
		}
	}
	t.Logf("compared %d cursor-differing pairs: admitted %d, refused %d", compared, admittedShift, refusedShift)
	if admittedShift == 0 || refusedShift == 0 {
		t.Fatalf("admitted %d, refused %d: the enumeration did not exercise both verdicts", admittedShift, refusedShift)
	}
}

// TestPersonDrilldownPRs_CursorChoicesCoverEveryRuleSevenConjunct pins
// the cursor alphabet to rule 7's conjuncts: each choice other than the
// own-last row breaks one of them on a non-empty leg.
func TestPersonDrilldownPRs_CursorChoicesCoverEveryRuleSevenConjunct(t *testing.T) {
	rows := []enumRow{{"A", "1", 1}, {"B", "1", 2}}
	last, _ := cursorInstant(cursorValue(cursorOwnLast, rows, false))
	for _, choice := range cursorChoices {
		got, ok := cursorInstant(cursorValue(choice, rows, false))
		breaks := !ok || !got.Equal(last)
		if (choice == cursorOwnLast) == breaks {
			t.Errorf("%s: breaks rule 7 = %v", fmt.Sprint(choice), breaks)
		}
	}
}

// pageCutCursorPlan builds the person route's page-cut plan over two
// generated bodies whose rows all carry createdAt, the baseline repeating
// its one id: every list rule holds, so rule 7 alone decides.
func pageCutCursorPlan(t *testing.T, createdAt string, baseCursor, candCursor any) *duplicateCollapsePageCutPlan {
	t.Helper()
	row := func(suffix string) map[string]any {
		return map[string]any{"repo_id": "ABC", "number": 1, "title": "t", "author": "a", "created_at": createdAt + suffix}
	}
	snap := func(items []any, cursor any) Snapshot {
		raw, err := json.Marshal(map[string]any{"items": items, "next_cursor": cursor})
		if err != nil {
			t.Fatal(err)
		}
		s, err := DecodeRESTSnapshot(raw)
		if err != nil {
			t.Fatal(err)
		}
		s.Data = InjectRESTDedupKeys(s.Data, drilldownPRsDedup.ListPath, drilldownPRsDedup.KeyFields)
		return s
	}
	var shape *DuplicateCollapsePageCutShape
	for _, d := range parityWithPageCutLimit(personDrilldownPRsParity, 2).BaselineDefects {
		if d.DuplicateCollapsePageCutShape != nil {
			shape = d.DuplicateCollapsePageCutShape
		}
	}
	if shape == nil || shape.CursorPath == "" {
		t.Fatal("the person route declares no page-cut entry with a cursor")
	}
	base := snap([]any{row(""), row("")}, baseCursor)
	cand := snap([]any{row("Z")}, candCursor)
	plan := buildDuplicateCollapsePageCutPlan(shape, base.Data, cand.Data)
	if !plan.applies {
		t.Fatal("the list rules do not hold on the generated bodies")
	}
	return plan
}

// TestDuplicateCollapsePageCutShape_CursorRuleReadsEachCursor pins rule 7's
// parse checks where a failed parse and the rows' own instant would
// otherwise agree: rows at the zero instant, and a cursor that is null or
// unparseable on one leg.
func TestDuplicateCollapsePageCutShape_CursorRuleReadsEachCursor(t *testing.T) {
	const zero = "0001-01-01T00:00:00"
	cases := []struct {
		name                   string
		baseCursor, candCursor any
		want                   bool
	}{
		{"both cursors name the last rows", zero, zero + "Z", true},
		{"baseline cursor unparseable", "ABC-123", zero + "Z", false},
		{"baseline cursor null", nil, zero + "Z", false},
		{"candidate cursor unparseable", zero, "ABC-123", false},
		{"candidate cursor null", zero, nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := pageCutCursorPlan(t, zero, c.baseCursor, c.candCursor).cursorAdmitted; got != c.want {
				t.Fatalf("cursorAdmitted = %v, want %v", got, c.want)
			}
		})
	}
}

// TestPullRequestDrilldownRoutes_PageCutEntryCitesExactlyItsOwnCursor pins
// the one shared page-cut entry per route: the route with a cursor cites
// and binds it; the route without one cites the list alone.
func TestPullRequestDrilldownRoutes_PageCutEntryCitesExactlyItsOwnCursor(t *testing.T) {
	routes := []struct {
		name      string
		opts      Options
		wantPaths []string
	}{
		{"scope route", drilldownPRsParity, []string{"data.items"}},
		{"team-scoped route", drilldownPRsTeamScopedParityWithLimit(drilldownPRsDefaultLimit), []string{"data.items"}},
		{"person route", personDrilldownPRsParity, []string{"data.items", "data.next_cursor"}},
	}
	for _, r := range routes {
		found := 0
		for _, d := range r.opts.BaselineDefects {
			if d.DuplicateCollapsePageCutShape == nil {
				continue
			}
			found++
			if !reflect.DeepEqual(d.Paths, r.wantPaths) {
				t.Errorf("%s: page-cut Paths = %q, want %q", r.name, d.Paths, r.wantPaths)
			}
			wantCursor := ""
			if len(r.wantPaths) > 1 {
				wantCursor = r.wantPaths[1]
			}
			if d.DuplicateCollapsePageCutShape.CursorPath != wantCursor {
				t.Errorf("%s: CursorPath = %q, want %q", r.name, d.DuplicateCollapsePageCutShape.CursorPath, wantCursor)
			}
		}
		if found != 1 {
			t.Errorf("%s: %d page-cut entries, want 1", r.name, found)
		}
	}
}

// TestPersonDrilldownPRs_CursorInheritsTheTailRowLimit pins the cursor
// admission to the list admission and the candidate's own last row: a
// tail row the baseline page never reached carries a value no baseline
// bounds, and the cursor is admitted with it exactly when it names that
// row; a cursor naming anything else, or a list the family refuses,
// stays outside.
func TestPersonDrilldownPRs_CursorInheritsTheTailRowLimit(t *testing.T) {
	const basePath = "testdata/persondrilldownprs_pagecutcursor_baseline_default_4bbbbd15.json"
	const candPath = "testdata/persondrilldownprs_pagecutcursor_candidate_default_baa32fdf.json"
	const earlier = "2000-01-01T00:00:00Z"
	req := personDrilldownPRsRequest(t, "drilldown_prs_default")
	cases := []struct {
		name        string
		mutate      func(cand map[string]any)
		wantOutside int
	}{
		{"tail row moved earlier, cursor names it", func(cand map[string]any) {
			items := cursorItems(cand)
			items[len(items)-1].(map[string]any)["created_at"] = earlier
			cand["next_cursor"] = earlier
		}, 0},
		{"tail row moved earlier, cursor unchanged", func(cand map[string]any) {
			items := cursorItems(cand)
			items[len(items)-1].(map[string]any)["created_at"] = earlier
		}, 1},
		{"list refused, cursor names the candidate's own last row", func(cand map[string]any) {
			items := cursorItems(cand)
			cand["items"] = append(append([]any{}, items[:5]...), items[6:]...)
		}, 2},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			base, cand := cursorBody(t, basePath), cursorBody(t, candPath)
			c.mutate(cand)
			result := compareAsRunner(t, req, encodeBody(t, base), encodeBody(t, cand))
			if result.DifferencesOutsideBaselineDefect != c.wantOutside {
				t.Fatalf("outside = %d (%s), want %d", result.DifferencesOutsideBaselineDefect, outsideSummary(result), c.wantOutside)
			}
			if !hasFinding(result, personCursorFindingPath, ShapeValue) {
				t.Fatal("no next_cursor finding")
			}
		})
	}
}
