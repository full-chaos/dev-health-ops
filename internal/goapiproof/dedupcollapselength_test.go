package goapiproof

import (
	"fmt"
	"testing"
)

// This file exercises DuplicateCollapseLengthShape (dedupcollapselength.go)
// through a real captured GET/POST /api/v1/drilldown/prs team_scoped
// response pair (a production deployed-vs-deployed prove run) plus
// small, self-contained synthetic bodies for the refusal rules the real
// capture never individually isolates.

func dedupCollapseItem(id string, extra string) string {
	return fmt.Sprintf(`{%q:%q,"title":%q}`, RESTDedupKeyField, id, extra)
}

func dedupCollapseBody(items []string) string {
	return `{"items":[` + joinJSON(items) + `]}`
}

// drilldownPRsLengthCollapseTicket reads the ticket of
// drilldownPRsParity's own DuplicateCollapseLengthShape entry, rather
// than a literal copy that could drift out of sync with restcorpus.go.
func drilldownPRsLengthCollapseTicket(t *testing.T) string {
	t.Helper()
	for _, d := range drilldownPRsParity.BaselineDefects {
		if d.DuplicateCollapseLengthShape != nil {
			return d.Ticket
		}
	}
	t.Fatal("drilldownPRsParity declares no DuplicateCollapseLengthShape entry")
	return ""
}

// TestDuplicateCollapseLengthShape_RealTeamScopedCaptureCollapsesExactly
// pins buildDuplicateCollapseLengthPlan's own admission directly against
// the real captured pair (drilldown/prs team_scoped): 6 duplicated
// (repo_id, number) ids in a 42-item baseline page, every one exactly 2
// byte-identical copies, all for the same physically-duplicated
// repository; collapsing them id for id reproduces the candidate's own
// 36-item page exactly.
func TestDuplicateCollapseLengthShape_RealTeamScopedCaptureCollapsesExactly(t *testing.T) {
	baseline := drilldownPRsSnapshotFromFile(t, "testdata/drilldownprs_lengthcollapse_baseline_a5c794d5.json")
	candidate := drilldownPRsSnapshotFromFile(t, "testdata/drilldownprs_lengthcollapse_candidate_fa4ebe78.json")

	baseList, ok := listAtDottedPath(baseline.Data, "data.items")
	if !ok || len(baseList) != 42 {
		t.Fatalf("baseline items = %d, want 42", len(baseList))
	}
	candList, ok := listAtDottedPath(candidate.Data, "data.items")
	if !ok || len(candList) != 36 {
		t.Fatalf("candidate items = %d, want 36", len(candList))
	}

	shape := &DuplicateCollapseLengthShape{ListPath: "data.items", IDField: RESTDedupKeyField}
	plan := buildDuplicateCollapseLengthPlan(shape, baseline.Data, candidate.Data)
	if !plan.applies {
		t.Fatal("plan does not apply against the real captured pair -- collapsing baseline's own duplicates should reproduce candidate's page exactly")
	}
	finding := Finding{Kind: FindingMismatch, Path: "$.data.items", Detail: "length 42 != 36", Shape: ShapeLength}
	if !plan.admits(finding) {
		t.Fatal("plan should admit the list's own length finding")
	}
}

// TestDrilldownPRsTeamScopedParity_RealCaptureHasNothingOutside runs the
// SAME captured pair through the actual registered
// drilldownPRsTeamScopedParity (not a hand-built stand-in): the length
// finding is covered and the new entry reads matched.
func TestDrilldownPRsTeamScopedParity_RealCaptureHasNothingOutside(t *testing.T) {
	baseline := drilldownPRsSnapshotFromFile(t, "testdata/drilldownprs_lengthcollapse_baseline_a5c794d5.json")
	candidate := drilldownPRsSnapshotFromFile(t, "testdata/drilldownprs_lengthcollapse_candidate_fa4ebe78.json")

	result := Compare(baseline, candidate, drilldownPRsTeamScopedParity)
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch", result.TerminalState)
	}
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	wantTicket := drilldownPRsLengthCollapseTicket(t)
	foundMatched := false
	for _, ticket := range result.BaselineDefectsMatched {
		if ticket == wantTicket {
			foundMatched = true
		}
	}
	if !foundMatched {
		t.Fatalf("matched = %v, want %s among them: idle %v stale %v", result.BaselineDefectsMatched, wantTicket, result.IdleIntermittentBaselineDefects, result.StaleBaselineDefects)
	}
}

// TestDuplicateCollapseLengthShape_DisagreeingDuplicateStaysOutside is the
// synthetic guard for rule 1: one id's own two baseline copies disagree
// (a real per-field regression hiding behind a shared id, not a clean
// duplicate row) -- the plan refuses the WHOLE list's length claim rather
// than guessing which copy is the "real" one.
func TestDuplicateCollapseLengthShape_DisagreeingDuplicateStaysOutside(t *testing.T) {
	baseItems := []string{
		dedupCollapseItem("id-1", "same"),
		dedupCollapseItem("id-1", "DIFFERENT"),
		dedupCollapseItem("id-2", "x"),
	}
	candItems := []string{
		dedupCollapseItem("id-1", "same"),
		dedupCollapseItem("id-2", "x"),
	}
	baseline, err := DecodeRESTSnapshot([]byte(dedupCollapseBody(baseItems)))
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := DecodeRESTSnapshot([]byte(dedupCollapseBody(candItems)))
	if err != nil {
		t.Fatal(err)
	}

	shape := &DuplicateCollapseLengthShape{ListPath: "data.items", IDField: RESTDedupKeyField}
	plan := buildDuplicateCollapseLengthPlan(shape, baseline.Data, candidate.Data)
	if plan.applies {
		t.Fatal("plan should not apply -- id-1's own two baseline copies disagree")
	}
	finding := Finding{Kind: FindingMismatch, Path: "$.data.items", Detail: "length 3 != 2", Shape: ShapeLength}
	if plan.admits(finding) {
		t.Fatal("a disagreeing duplicate must never be admitted")
	}
}

// TestDuplicateCollapseLengthShape_CollapsedLengthStillDiffersStaysOutside
// is the synthetic guard for rule 4: every duplicate agrees and every
// shared id agrees, but collapsing baseline's duplicates does NOT
// reproduce candidate's own id set (a genuinely dropped row, unrelated to
// the duplication) -- the plan refuses rather than trusting a bare count
// match it never actually observed.
func TestDuplicateCollapseLengthShape_CollapsedLengthStillDiffersStaysOutside(t *testing.T) {
	baseItems := []string{
		dedupCollapseItem("id-1", "x"),
		dedupCollapseItem("id-1", "x"),
		dedupCollapseItem("id-2", "y"),
		dedupCollapseItem("id-3", "z"),
	}
	candItems := []string{
		dedupCollapseItem("id-1", "x"),
		dedupCollapseItem("id-2", "y"),
	}
	baseline, err := DecodeRESTSnapshot([]byte(dedupCollapseBody(baseItems)))
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := DecodeRESTSnapshot([]byte(dedupCollapseBody(candItems)))
	if err != nil {
		t.Fatal(err)
	}

	shape := &DuplicateCollapseLengthShape{ListPath: "data.items", IDField: RESTDedupKeyField}
	plan := buildDuplicateCollapseLengthPlan(shape, baseline.Data, candidate.Data)
	if plan.applies {
		t.Fatal("plan should not apply -- id-3 is a baseline-only id the duplication never explains")
	}
	finding := Finding{Kind: FindingMismatch, Path: "$.data.items", Detail: "length 3 != 2", Shape: ShapeLength}
	if plan.admits(finding) {
		t.Fatal("a collapsed length that still differs from an unexplained id must never be admitted")
	}
}

// TestDuplicateCollapseLengthShape_AdmitsOnlyTheLengthFinding pins the
// admits() dispatch itself: it never reaches for any OTHER shape's own
// finding (a leaf value, a presence key) even when the plan applies --
// WorkGraphEdgeDedupShape's own sibling entry, not this one, judges
// those.
func TestDuplicateCollapseLengthShape_AdmitsOnlyTheLengthFinding(t *testing.T) {
	baseItems := []string{
		dedupCollapseItem("id-1", "x"),
		dedupCollapseItem("id-1", "x"),
		dedupCollapseItem("id-2", "y"),
	}
	candItems := []string{
		dedupCollapseItem("id-1", "x"),
		dedupCollapseItem("id-2", "y"),
	}
	baseline, err := DecodeRESTSnapshot([]byte(dedupCollapseBody(baseItems)))
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := DecodeRESTSnapshot([]byte(dedupCollapseBody(candItems)))
	if err != nil {
		t.Fatal(err)
	}

	shape := &DuplicateCollapseLengthShape{ListPath: "data.items", IDField: RESTDedupKeyField}
	plan := buildDuplicateCollapseLengthPlan(shape, baseline.Data, candidate.Data)
	if !plan.applies {
		t.Fatal("plan should apply: one clean duplicate, collapsing reproduces candidate exactly")
	}
	leafFinding := Finding{Kind: FindingMismatch, Path: "$.data.items[0].title", Detail: "x != z", Shape: ShapeValue}
	if plan.admits(leafFinding) {
		t.Fatal("this shape must never admit a leaf finding, only the list's own length")
	}
	presenceFinding := Finding{Kind: FindingMismatch, Path: "$.data.items[0]", Detail: `key "id-9" present in baseline, absent in candidate`, Shape: ShapePresence}
	if plan.admits(presenceFinding) {
		t.Fatal("this shape must never admit a presence finding, only the list's own length")
	}
}

// dedupCollapseLimitFixture builds a baseline/candidate pair with n
// distinct ids, every one of them duplicated exactly once (byte-
// identical copies) on the baseline side, and an exact collapse on the
// candidate side -- rules 1-4 all clean, so any refusal this fixture
// produces is rule 0's own RequestLimit precondition alone.
func dedupCollapseLimitFixture(t *testing.T, n int) (baseline, candidate Snapshot) {
	t.Helper()
	var baseItems, candItems []string
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("id-%02d", i)
		baseItems = append(baseItems, dedupCollapseItem(id, "x"), dedupCollapseItem(id, "x"))
		candItems = append(candItems, dedupCollapseItem(id, "x"))
	}
	base, err := DecodeRESTSnapshot([]byte(dedupCollapseBody(baseItems)))
	if err != nil {
		t.Fatal(err)
	}
	cand, err := DecodeRESTSnapshot([]byte(dedupCollapseBody(candItems)))
	if err != nil {
		t.Fatal(err)
	}
	return base, cand
}

// TestDuplicateCollapseLengthShape_RequestLimitZeroPreservesExistingBehavior
// pins RequestLimit's own default: unset (zero), a baseline whose own
// raw length happens to equal what WOULD be a request limit elsewhere
// (10 distinct ids, 20 raw rows) still admits -- every entry declared
// before this field existed keeps its own unchanged behavior.
func TestDuplicateCollapseLengthShape_RequestLimitZeroPreservesExistingBehavior(t *testing.T) {
	baseline, candidate := dedupCollapseLimitFixture(t, 10)
	shape := &DuplicateCollapseLengthShape{ListPath: "data.items", IDField: RESTDedupKeyField}
	plan := buildDuplicateCollapseLengthPlan(shape, baseline.Data, candidate.Data)
	if !plan.applies {
		t.Fatal("plan should apply -- RequestLimit is unset (zero), rule 0 must not run at all")
	}
}

// TestDuplicateCollapseLengthShape_RequestLimitJustBelowAdmits is the
// ceiling-1 cell: baseline's own raw length (20, from 10 duplicated
// ids) sits ONE below RequestLimit (21) -- genuinely short of the
// limit, so ClickHouse itself would have returned more rows had more
// existed. Rules 1-4 are all clean; the plan must apply.
func TestDuplicateCollapseLengthShape_RequestLimitJustBelowAdmits(t *testing.T) {
	baseline, candidate := dedupCollapseLimitFixture(t, 10)
	shape := &DuplicateCollapseLengthShape{ListPath: "data.items", IDField: RESTDedupKeyField, RequestLimit: 21}
	plan := buildDuplicateCollapseLengthPlan(shape, baseline.Data, candidate.Data)
	if !plan.applies {
		t.Fatal("plan should apply -- baseline's own raw length (20) sits strictly below RequestLimit (21)")
	}
}

// TestDuplicateCollapseLengthShape_RequestLimitReachedRefuses is rule
// 0's own isolation cell: baseline's own raw length (20) EQUALS
// RequestLimit (20) exactly, with rules 1-4 otherwise all clean (a
// genuine exact collapse, every id duplicated once, byte-identical).
// The plan must still refuse -- reaching the limit means candidate's
// own leg could equally have been truncated at the SAME boundary, so
// the exact-collapse match alone never proves the whole population was
// compared. Mutation-verified: disabling rule 0 alone flips this test.
func TestDuplicateCollapseLengthShape_RequestLimitReachedRefuses(t *testing.T) {
	baseline, candidate := dedupCollapseLimitFixture(t, 10)
	shape := &DuplicateCollapseLengthShape{ListPath: "data.items", IDField: RESTDedupKeyField, RequestLimit: 20}
	plan := buildDuplicateCollapseLengthPlan(shape, baseline.Data, candidate.Data)
	if plan.applies {
		t.Fatal("plan should refuse -- baseline's own raw length (20) reaches RequestLimit (20) exactly, an unverifiable page boundary")
	}
}

// TestDuplicateCollapseLengthShape_RequestLimitExceededRefuses is the
// ceiling+1 cell, defensive: baseline's own raw length (20) EXCEEDS
// RequestLimit (19), a shape the route's own LIMIT clause should never
// itself produce. The plan must still refuse rather than treat it as a
// clean case to admit.
func TestDuplicateCollapseLengthShape_RequestLimitExceededRefuses(t *testing.T) {
	baseline, candidate := dedupCollapseLimitFixture(t, 10)
	shape := &DuplicateCollapseLengthShape{ListPath: "data.items", IDField: RESTDedupKeyField, RequestLimit: 19}
	plan := buildDuplicateCollapseLengthPlan(shape, baseline.Data, candidate.Data)
	if plan.applies {
		t.Fatal("plan should refuse -- baseline's own raw length (20) exceeds RequestLimit (19)")
	}
}
