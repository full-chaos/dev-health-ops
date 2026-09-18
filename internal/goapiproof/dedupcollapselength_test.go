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
