package goapiproof

import (
	"fmt"
	"testing"
)

// This file exercises DuplicateCollapsePageCutShape
// (duplicatecollapsepagecut.go) through a real captured GET
// /api/v1/drilldown/prs team_scoped response pair (a production
// deployed-vs-deployed prove run) plus small, self-contained synthetic
// bodies for the refusal rules the real capture never individually
// isolates. It never touches dedupcollapselength_test.go or its own
// fixtures.

const (
	drilldownPRsPageCutBaselinePath  = "testdata/drilldownprs_pagecut_baseline_b3dbfda2.json"
	drilldownPRsPageCutCandidatePath = "testdata/drilldownprs_pagecut_candidate_6a9b742e.json"
)

// pageCutTS returns a deterministic, descending RFC 3339 timestamp for
// rank (rank 1 is the most recent; every higher rank is one minute
// older) -- SortField's own wire form, plain enough that a test reading
// two ranks side by side can tell their order at a glance.
func pageCutTS(rank int) string {
	return fmt.Sprintf("2026-08-15T12:%02d:00Z", 59-rank)
}

// pageCutItem builds one synthetic data.items element carrying
// RESTDedupKeyField, a "title" (the field DisagreeingDuplicateRefuses
// and PrefixContentMutationRefuses mutate), and "created_at" at rank
// (see pageCutTS) -- rule 6's own SortField. Two physical copies of the
// same id sharing the SAME rank carry the SAME created_at, matching a
// real duplicate row's own byte-identical copies.
func pageCutItem(id, extra string, rank int) string {
	return fmt.Sprintf(`{%q:%q,"title":%q,"created_at":%q}`, RESTDedupKeyField, id, extra, pageCutTS(rank))
}

func pageCutShape(limit int) *DuplicateCollapsePageCutShape {
	return &DuplicateCollapsePageCutShape{ListPath: "data.items", IDField: RESTDedupKeyField, SortField: "created_at", Limit: limit}
}

// drilldownPRsPageCutTicket reads the ticket of drilldownPRsParity's own
// DuplicateCollapsePageCutShape entry, rather than a literal copy that
// could drift out of sync with restcorpus.go.
func drilldownPRsPageCutTicket(t *testing.T) string {
	t.Helper()
	for _, d := range drilldownPRsParity.BaselineDefects {
		if d.DuplicateCollapsePageCutShape != nil {
			return d.Ticket
		}
	}
	t.Fatal("drilldownPRsParity declares no DuplicateCollapsePageCutShape entry")
	return ""
}

// TestDuplicateCollapsePageCutShape_RealTeamScopedCaptureAdmitsTheShortfall
// pins buildDuplicateCollapsePageCutPlan's own admission directly against
// the real captured pair: baseline is 50 items (the route's own limit),
// 28 distinct ids, 22 of them duplicated exactly 2x each, byte-identical;
// candidate is 36 items, its own 36 distinct ids a strict superset of the
// baseline's 28 (all 28 recurring as candidate's own first 28 elements,
// in the same order); the 8 remaining candidate-only ids fall short of
// the 22 wasted slots because the window's true population (36) is
// itself under the route's own limit (50), and the WHOLE candidate list
// is monotone by its own real created_at values.
func TestDuplicateCollapsePageCutShape_RealTeamScopedCaptureAdmitsTheShortfall(t *testing.T) {
	baseline := drilldownPRsSnapshotFromFile(t, drilldownPRsPageCutBaselinePath)
	candidate := drilldownPRsSnapshotFromFile(t, drilldownPRsPageCutCandidatePath)

	baseList, ok := listAtDottedPath(baseline.Data, "data.items")
	if !ok || len(baseList) != 50 {
		t.Fatalf("baseline items = %d, want 50", len(baseList))
	}
	candList, ok := listAtDottedPath(candidate.Data, "data.items")
	if !ok || len(candList) != 36 {
		t.Fatalf("candidate items = %d, want 36", len(candList))
	}

	shape := pageCutShape(50)
	plan := buildDuplicateCollapsePageCutPlan(shape, baseline.Data, candidate.Data)
	if !plan.applies {
		t.Fatal("plan does not apply against the real captured pair -- the shortfall is exactly what this shape exists to admit")
	}
	finding := Finding{Kind: FindingMismatch, Path: "$.data.items", Detail: "length 50 != 36", Shape: ShapeLength}
	if !plan.admits(finding) {
		t.Fatal("plan should admit the list's own length finding")
	}
}

const (
	drilldownPRsPageCutAllDoubledBaselinePath  = "testdata/drilldownprs_pagecut_alldoubled_baseline_66b26ad9.json"
	drilldownPRsPageCutAllDoubledCandidatePath = "testdata/drilldownprs_pagecut_alldoubled_candidate_fa4ebe78.json"
)

// TestDuplicateCollapsePageCutShape_RealAllDoubledCaptureAdmitsTheShortfall
// pins the SAME shape against a SECOND real captured pair where the
// duplicate-row mechanism covers the baseline page's own population
// TOTALLY, not partially: 50 items, 25 distinct ids ACROSS TWO
// repositories, every single one of the 25 duplicated exactly 2x,
// byte-identical (zero single-copy rows at all); candidate 36 items,
// dedup(baseline)'s own 25 ids as its literal prefix, 11 candidate-only
// ids in the tail. The shape carries no premise that only SOME of a
// page's rows may be doubled -- this capture proves it admits the
// all-doubled case exactly as it does the partial one.
func TestDuplicateCollapsePageCutShape_RealAllDoubledCaptureAdmitsTheShortfall(t *testing.T) {
	baseline := drilldownPRsSnapshotFromFile(t, drilldownPRsPageCutAllDoubledBaselinePath)
	candidate := drilldownPRsSnapshotFromFile(t, drilldownPRsPageCutAllDoubledCandidatePath)

	baseList, ok := listAtDottedPath(baseline.Data, "data.items")
	if !ok || len(baseList) != 50 {
		t.Fatalf("baseline items = %d, want 50", len(baseList))
	}
	candList, ok := listAtDottedPath(candidate.Data, "data.items")
	if !ok || len(candList) != 36 {
		t.Fatalf("candidate items = %d, want 36", len(candList))
	}

	shape := pageCutShape(50)
	plan := buildDuplicateCollapsePageCutPlan(shape, baseline.Data, candidate.Data)
	if !plan.applies {
		t.Fatal("plan does not apply against the all-doubled captured pair -- total duplicate coverage of the baseline page is not a premise this shape requires")
	}
	finding := Finding{Kind: FindingMismatch, Path: "$.data.items", Detail: "length 50 != 36", Shape: ShapeLength}
	if !plan.admits(finding) {
		t.Fatal("plan should admit the list's own length finding")
	}
}

// TestDrilldownPRsTeamScopedParity_RealAllDoubledCaptureHasNothingOutside
// runs the SAME all-doubled captured pair through the actual registered
// drilldownPRsTeamScopedParity.
func TestDrilldownPRsTeamScopedParity_RealAllDoubledCaptureHasNothingOutside(t *testing.T) {
	baseline := drilldownPRsSnapshotFromFile(t, drilldownPRsPageCutAllDoubledBaselinePath)
	candidate := drilldownPRsSnapshotFromFile(t, drilldownPRsPageCutAllDoubledCandidatePath)

	result := Compare(baseline, candidate, drilldownPRsTeamScopedParity)
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch", result.TerminalState)
	}
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	wantTicket := drilldownPRsPageCutTicket(t)
	foundMatched := false
	for _, ticket := range result.BaselineDefectsMatched {
		if ticket == wantTicket {
			foundMatched = true
		}
	}
	if !foundMatched {
		t.Fatalf("matched = %v, want %s among them: idle %v stale %v live-unexplained %v", result.BaselineDefectsMatched, wantTicket, result.IdleIntermittentBaselineDefects, result.StaleBaselineDefects, result.LiveBaselineDefectsUnexplained)
	}
}

// TestDrilldownPRsTeamScopedParity_RealPageCutCaptureHasNothingOutside
// runs the SAME captured pair through the actual registered
// drilldownPRsTeamScopedParity: the length finding is covered by its
// own declared entry and nothing stays outside any declaration.
func TestDrilldownPRsTeamScopedParity_RealPageCutCaptureHasNothingOutside(t *testing.T) {
	baseline := drilldownPRsSnapshotFromFile(t, drilldownPRsPageCutBaselinePath)
	candidate := drilldownPRsSnapshotFromFile(t, drilldownPRsPageCutCandidatePath)

	result := Compare(baseline, candidate, drilldownPRsTeamScopedParity)
	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch", result.TerminalState)
	}
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	wantTicket := drilldownPRsPageCutTicket(t)
	foundMatched := false
	for _, ticket := range result.BaselineDefectsMatched {
		if ticket == wantTicket {
			foundMatched = true
		}
	}
	if !foundMatched {
		t.Fatalf("matched = %v, want %s among them: idle %v stale %v live-unexplained %v", result.BaselineDefectsMatched, wantTicket, result.IdleIntermittentBaselineDefects, result.StaleBaselineDefects, result.LiveBaselineDefectsUnexplained)
	}
}

// TestDrilldownPRsTeamScopedParity_RealPageCutCaptureFailsWithoutTheGuard
// removes the page-cut entry from a copy of drilldownPRsTeamScopedParity
// and re-runs the SAME captured pair: the length finding must now stay
// outside every remaining declaration -- the guard this file exists to
// prove is load-bearing, not redundant with the sibling exact-collapse
// entry (which never reaches this truncated case, per its own rule 4).
func TestDrilldownPRsTeamScopedParity_RealPageCutCaptureFailsWithoutTheGuard(t *testing.T) {
	baseline := drilldownPRsSnapshotFromFile(t, drilldownPRsPageCutBaselinePath)
	candidate := drilldownPRsSnapshotFromFile(t, drilldownPRsPageCutCandidatePath)

	var without []BaselineDefect
	for _, d := range drilldownPRsTeamScopedParity.BaselineDefects {
		if d.DuplicateCollapsePageCutShape != nil {
			continue
		}
		without = append(without, d)
	}
	opts := drilldownPRsTeamScopedParity
	opts.BaselineDefects = without

	result := Compare(baseline, candidate, opts)
	if result.DifferencesOutsideBaselineDefect == 0 {
		t.Fatalf("outside = 0, want > 0 -- removing the page-cut entry must leave the length finding uncovered; findings %+v", result.Findings)
	}
	sawLength := false
	for _, f := range result.Findings {
		if f.Shape == ShapeLength && tieredPath(f.Path) == "data.items" {
			sawLength = true
		}
	}
	if !sawLength {
		t.Fatal("expected an uncovered data.items length finding once the guard is removed")
	}
}

// pageCutMutateCandidateItems loads the real captured pair and returns
// the candidate's own decoded items slice (data.items, []any of
// map[string]any) alongside both snapshots, so a test can mutate it in
// place before running Compare/buildDuplicateCollapsePageCutPlan.
func pageCutMutateCandidateItems(t *testing.T) (baseline, candidate Snapshot, items []any) {
	t.Helper()
	baseline = drilldownPRsSnapshotFromFile(t, drilldownPRsPageCutBaselinePath)
	candidate = drilldownPRsSnapshotFromFile(t, drilldownPRsPageCutCandidatePath)
	items, ok := candidate.Data.(map[string]any)["items"].([]any)
	if !ok || len(items) != 36 {
		t.Fatalf("candidate items = %v (ok=%v), want a 36-element list", items, ok)
	}
	return baseline, candidate, items
}

// TestDuplicateCollapsePageCutShape_NewerTailRowRefuses is the guard for
// rule 6's own monotonicity check: the real captured candidate's own
// LAST (tail, candidate-only) row is given a created_at three days newer
// than every other row -- under the route's own ORDER BY created_at DESC
// this row had to appear ON the baseline's own page, and it now also
// breaks the candidate's own internal monotonicity (its own predecessor
// is older), the ordering violation rule 6 exists to catch.
func TestDuplicateCollapsePageCutShape_NewerTailRowRefuses(t *testing.T) {
	baseline, candidate, items := pageCutMutateCandidateItems(t)
	last, ok := items[len(items)-1].(map[string]any)
	if !ok {
		t.Fatalf("items[%d] is not an object: %#v", len(items)-1, items[len(items)-1])
	}
	last["created_at"] = "2026-08-18T12:00:00Z"

	shape := pageCutShape(50)
	plan := buildDuplicateCollapsePageCutPlan(shape, baseline.Data, candidate.Data)
	if plan.applies {
		t.Fatal("plan should not apply -- the candidate's own tail row is newer than its own predecessor, breaking rule 6's own monotonicity")
	}

	result := Compare(baseline, candidate, drilldownPRsTeamScopedParity)
	if result.DifferencesOutsideBaselineDefect == 0 {
		t.Fatalf("outside = 0, want > 0 -- findings %+v", result.Findings)
	}
}

// TestDuplicateCollapsePageCutShape_ReversedTailRefuses is the second
// guard for rule 6: the real captured candidate's own tail (its 8
// candidate-only rows, positions 28..35) is reversed in place -- the id
// SET at those positions is unchanged (rule 4's own prefix identity
// still holds), but the candidate list is no longer monotone by
// created_at, a genuine ordering regression rule 6 must catch.
func TestDuplicateCollapsePageCutShape_ReversedTailRefuses(t *testing.T) {
	baseline, candidate, items := pageCutMutateCandidateItems(t)
	tail := items[28:36]
	for i, j := 0, len(tail)-1; i < j; i, j = i+1, j-1 {
		tail[i], tail[j] = tail[j], tail[i]
	}

	shape := pageCutShape(50)
	plan := buildDuplicateCollapsePageCutPlan(shape, baseline.Data, candidate.Data)
	if plan.applies {
		t.Fatal("plan should not apply -- the candidate's own tail is reversed, no longer monotone by created_at")
	}

	result := Compare(baseline, candidate, drilldownPRsTeamScopedParity)
	if result.DifferencesOutsideBaselineDefect == 0 {
		t.Fatalf("outside = 0, want > 0 -- findings %+v", result.Findings)
	}
}

// TestDuplicateCollapsePageCutShape_PrefixContentMutationRefuses is the
// guard for rule 4's own content-equality check
// (duplicatecollapsepagecut.go's jsonValuesEqual(dedupRepresentative[id],
// candByID[id])): the real captured candidate's own items[5] -- a SHARED
// PREFIX row, present on both legs -- has its title changed to a value
// the baseline's own representative copy does not carry. Exactly one
// ShapeLength finding stays outside, and the entry reads unmatched.
func TestDuplicateCollapsePageCutShape_PrefixContentMutationRefuses(t *testing.T) {
	baseline, candidate, items := pageCutMutateCandidateItems(t)
	row, ok := items[5].(map[string]any)
	if !ok {
		t.Fatalf("items[5] is not an object: %#v", items[5])
	}
	row["title"] = "WRONG"

	shape := pageCutShape(50)
	plan := buildDuplicateCollapsePageCutPlan(shape, baseline.Data, candidate.Data)
	if plan.applies {
		t.Fatal("plan should not apply -- items[5]'s own title no longer matches its baseline representative")
	}

	result := Compare(baseline, candidate, drilldownPRsTeamScopedParity)
	if result.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("outside = %d, want exactly 1 -- findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	wantTicket := drilldownPRsPageCutTicket(t)
	for _, ticket := range result.BaselineDefectsMatched {
		if ticket == wantTicket {
			t.Fatalf("matched = %v, want %s absent -- the mutated prefix row must leave the entry unmatched", result.BaselineDefectsMatched, wantTicket)
		}
	}
}

// TestDuplicateCollapsePageCutShape_CandidateTailElementNoIDRefuses
// isolates rule 3's own candidate-side edgeObjectAndID gate specifically:
// dedup(baseline)'s own prefix is left intact and well-formed, and a
// SYNTHETIC extra TAIL element (a position rule 4's own prefix loop
// never examines) carries a valid, monotone-correct created_at but NO
// dedup key at all. Rule 4 has no opinion about a tail position, and
// rule 6 accepts the row's own valid created_at, so only rule 3's own
// element decode refuses it -- proven by mutation: removing rule 3's own
// early exit here flips this exact case from refuse to admit.
func TestDuplicateCollapsePageCutShape_CandidateTailElementNoIDRefuses(t *testing.T) {
	baseItems := []string{
		pageCutItem("id-1", "a", 1),
		pageCutItem("id-1", "a", 1),
		pageCutItem("id-2", "b", 2),
	}
	baseline := mustDecodeRESTSnapshot(t, dedupCollapseBody(baseItems))
	candBody := `{"items":[` +
		pageCutItem("id-1", "a", 1) + `,` +
		pageCutItem("id-2", "b", 2) + `,` +
		`{"title":"tail","created_at":"` + pageCutTS(3) + `"}` +
		`]}`
	candidate := mustDecodeRESTSnapshot(t, candBody)

	shape := pageCutShape(3)
	plan := buildDuplicateCollapsePageCutPlan(shape, baseline.Data, candidate.Data)
	if plan.applies {
		t.Fatal("plan should not apply -- the candidate's own tail element carries no dedup key")
	}
}

// TestDuplicateCollapsePageCutShape_BaselineMalformedElementStillRefuses
// pins the observable behaviour for a malformed BASELINE element (the
// real captured baseline's own last element replaced with a bare
// `null`): it refuses, but via rule 4's own id-set mismatch, not a
// dedicated baseline-side decode guard -- duplicatecollapsepagecut.go's
// own comment on this exact point states why no such guard exists (a
// decode failure here always surfaces as a spurious id no well-formed
// candidate prefix can ever match).
func TestDuplicateCollapsePageCutShape_BaselineMalformedElementStillRefuses(t *testing.T) {
	baseline := drilldownPRsSnapshotFromFile(t, drilldownPRsPageCutBaselinePath)
	candidate := drilldownPRsSnapshotFromFile(t, drilldownPRsPageCutCandidatePath)
	items, ok := baseline.Data.(map[string]any)["items"].([]any)
	if !ok || len(items) != 50 {
		t.Fatalf("baseline items = %v (ok=%v), want a 50-element list", items, ok)
	}
	items[len(items)-1] = nil

	shape := pageCutShape(50)
	plan := buildDuplicateCollapsePageCutPlan(shape, baseline.Data, candidate.Data)
	if plan.applies {
		t.Fatal("plan should not apply -- the baseline's own last element is not an object at all")
	}
}

// TestDuplicateCollapsePageCutShape_CandidateShorterThanDedupOrderRefuses
// is the synthetic guard for rule 4's own length precondition
// (duplicatecollapsepagecut.go: len(candOrder) < len(dedupOrder)):
// baseline dedups to 3 distinct ids, but the candidate carries only 1
// item -- shorter than dedup(baseline) itself, so no literal-prefix
// comparison is even well-formed. Without this guard the indexed
// comparison loop right after it would read past the candidate's own
// slice.
func TestDuplicateCollapsePageCutShape_CandidateShorterThanDedupOrderRefuses(t *testing.T) {
	baseItems := []string{
		pageCutItem("id-1", "a", 1),
		pageCutItem("id-1", "a", 1),
		pageCutItem("id-2", "b", 2),
		pageCutItem("id-3", "c", 3),
	}
	candItems := []string{
		pageCutItem("id-1", "a", 1),
	}
	baseline := mustDecodeRESTSnapshot(t, dedupCollapseBody(baseItems))
	candidate := mustDecodeRESTSnapshot(t, dedupCollapseBody(candItems))

	shape := pageCutShape(4)
	plan := buildDuplicateCollapsePageCutPlan(shape, baseline.Data, candidate.Data)
	if plan.applies {
		t.Fatal("plan should not apply -- the candidate (1 item) is shorter than dedup(baseline) (3 distinct ids)")
	}
}

// TestDuplicateCollapsePageCutShape_NonPositiveLimitDoesNotApply pins the
// observable behaviour for a misconfigured declaration carrying a
// non-positive Limit: it refuses, but via rule 1's own length mismatch
// (duplicatecollapsepagecut.go's own comment on the shape.Limit <= 0
// check states why no input isolates that check on its own).
func TestDuplicateCollapsePageCutShape_NonPositiveLimitDoesNotApply(t *testing.T) {
	baseItems := []string{
		pageCutItem("id-1", "x", 1),
		pageCutItem("id-1", "x", 1),
		pageCutItem("id-2", "y", 2),
	}
	candItems := []string{
		pageCutItem("id-1", "x", 1),
		pageCutItem("id-2", "y", 2),
	}
	baseline := mustDecodeRESTSnapshot(t, dedupCollapseBody(baseItems))
	candidate := mustDecodeRESTSnapshot(t, dedupCollapseBody(candItems))

	shape := pageCutShape(0)
	plan := buildDuplicateCollapsePageCutPlan(shape, baseline.Data, candidate.Data)
	if plan.applies {
		t.Fatal("plan should not apply -- Limit 0 carries no page-boundary meaning")
	}
}

// TestDuplicateCollapsePageCutShape_ListPathNotAListDoesNotApply pins
// the observable behaviour when the candidate body's own "items" key is
// an object, not a list, at all: it refuses, but via rule 1's own length
// mismatch (duplicatecollapsepagecut.go's own comment on the listAtDottedPath
// decode states why no input isolates that check on its own either).
func TestDuplicateCollapsePageCutShape_ListPathNotAListDoesNotApply(t *testing.T) {
	baseItems := []string{
		pageCutItem("id-1", "x", 1),
		pageCutItem("id-1", "x", 1),
		pageCutItem("id-2", "y", 2),
	}
	baseline := mustDecodeRESTSnapshot(t, dedupCollapseBody(baseItems))
	candidate := mustDecodeRESTSnapshot(t, `{"items":{"not":"a list"}}`)

	shape := pageCutShape(3)
	plan := buildDuplicateCollapsePageCutPlan(shape, baseline.Data, candidate.Data)
	if plan.applies {
		t.Fatal("plan should not apply -- the candidate's own \"items\" is an object, not a list")
	}
}

// TestDuplicateCollapsePageCutShape_CandidateMissingSortFieldRefuses is
// the guard for rule 6's own element-level SortField decode
// (duplicatecollapsepagecut.go's duplicateCollapsePageCutSortValue,
// called from rule 6's own loop): a candidate TAIL element carries a
// perfectly valid dedup key (rule 3 and rule 4 have no opinion about it)
// but NO created_at field at all -- rule 6 cannot place it in the
// route's own sort order and refuses rather than guessing.
func TestDuplicateCollapsePageCutShape_CandidateMissingSortFieldRefuses(t *testing.T) {
	baseItems := []string{
		pageCutItem("id-1", "a", 1),
		pageCutItem("id-1", "a", 1),
		pageCutItem("id-2", "b", 2),
	}
	baseline := mustDecodeRESTSnapshot(t, dedupCollapseBody(baseItems))
	candBody := `{"items":[` +
		pageCutItem("id-1", "a", 1) + `,` +
		pageCutItem("id-2", "b", 2) + `,` +
		fmt.Sprintf(`{%q:%q,"title":"tail"}`, RESTDedupKeyField, "id-9") +
		`]}`
	candidate := mustDecodeRESTSnapshot(t, candBody)

	shape := pageCutShape(3)
	plan := buildDuplicateCollapsePageCutPlan(shape, baseline.Data, candidate.Data)
	if plan.applies {
		t.Fatal("plan should not apply -- the candidate's own tail element carries no created_at at all")
	}
}

// TestDuplicateCollapsePageCutShape_CandidateNonStringSortFieldRefuses is
// the SAME guard's sibling case: the candidate tail element's own
// created_at is present but not a string (a number), so parseTimestamp
// is never even attempted.
func TestDuplicateCollapsePageCutShape_CandidateNonStringSortFieldRefuses(t *testing.T) {
	baseItems := []string{
		pageCutItem("id-1", "a", 1),
		pageCutItem("id-1", "a", 1),
		pageCutItem("id-2", "b", 2),
	}
	baseline := mustDecodeRESTSnapshot(t, dedupCollapseBody(baseItems))
	candBody := `{"items":[` +
		pageCutItem("id-1", "a", 1) + `,` +
		pageCutItem("id-2", "b", 2) + `,` +
		fmt.Sprintf(`{%q:%q,"title":"tail","created_at":20260815}`, RESTDedupKeyField, "id-9") +
		`]}`
	candidate := mustDecodeRESTSnapshot(t, candBody)

	shape := pageCutShape(3)
	plan := buildDuplicateCollapsePageCutPlan(shape, baseline.Data, candidate.Data)
	if plan.applies {
		t.Fatal("plan should not apply -- the candidate's own tail element's created_at is a number, not a string")
	}
}

// TestDuplicateCollapsePageCutShape_BaselineShortOfLimitDoesNotApply is
// the synthetic guard for rule 1: the baseline page never reaches the
// route's own limit, so there is no truncation for this shape to
// explain -- DuplicateCollapseLengthShape's own exact-collapse rule is
// the correct citation for a case like this, not this one.
func TestDuplicateCollapsePageCutShape_BaselineShortOfLimitDoesNotApply(t *testing.T) {
	baseItems := []string{
		pageCutItem("id-1", "x", 1),
		pageCutItem("id-1", "x", 1),
		pageCutItem("id-2", "y", 2),
	}
	candItems := []string{
		pageCutItem("id-1", "x", 1),
		pageCutItem("id-2", "y", 2),
		pageCutItem("id-3", "z", 3),
	}
	baseline := mustDecodeRESTSnapshot(t, dedupCollapseBody(baseItems))
	candidate := mustDecodeRESTSnapshot(t, dedupCollapseBody(candItems))

	shape := pageCutShape(50)
	plan := buildDuplicateCollapsePageCutPlan(shape, baseline.Data, candidate.Data)
	if plan.applies {
		t.Fatal("plan should not apply -- baseline (3 items) never reaches Limit (50)")
	}
}

// TestDuplicateCollapsePageCutShape_NoDuplicateDoesNotApply is the
// synthetic guard for rule 2's own gate: a baseline page AT the limit
// with no repeated id at all has nothing this shape explains.
func TestDuplicateCollapsePageCutShape_NoDuplicateDoesNotApply(t *testing.T) {
	baseItems := []string{
		pageCutItem("id-1", "x", 1),
		pageCutItem("id-2", "y", 2),
	}
	candItems := []string{
		pageCutItem("id-1", "x", 1),
		pageCutItem("id-2", "y", 2),
	}
	baseline := mustDecodeRESTSnapshot(t, dedupCollapseBody(baseItems))
	candidate := mustDecodeRESTSnapshot(t, dedupCollapseBody(candItems))

	shape := pageCutShape(2)
	plan := buildDuplicateCollapsePageCutPlan(shape, baseline.Data, candidate.Data)
	if plan.applies {
		t.Fatal("plan should not apply -- baseline carries no duplicate id at all")
	}
}

// TestDuplicateCollapsePageCutShape_DisagreeingDuplicateRefuses is the
// synthetic guard for rule 2's own byte-identical requirement: one id's
// two baseline copies disagree, a real per-field regression hiding
// behind a shared id, never a clean duplicate row.
func TestDuplicateCollapsePageCutShape_DisagreeingDuplicateRefuses(t *testing.T) {
	baseItems := []string{
		pageCutItem("id-1", "same", 1),
		pageCutItem("id-1", "DIFFERENT", 1),
		pageCutItem("id-2", "x", 2),
	}
	candItems := []string{
		pageCutItem("id-1", "same", 1),
		pageCutItem("id-2", "x", 2),
		pageCutItem("id-3", "y", 3),
	}
	baseline := mustDecodeRESTSnapshot(t, dedupCollapseBody(baseItems))
	candidate := mustDecodeRESTSnapshot(t, dedupCollapseBody(candItems))

	shape := pageCutShape(3)
	plan := buildDuplicateCollapsePageCutPlan(shape, baseline.Data, candidate.Data)
	if plan.applies {
		t.Fatal("plan should not apply -- id-1's own two baseline copies disagree")
	}
}

// TestDuplicateCollapsePageCutShape_CandidateRepeatedIDRefuses is the
// synthetic guard for rule 3: the candidate itself carries a repeated id
// -- a different, unexplained condition this shape never speaks to.
func TestDuplicateCollapsePageCutShape_CandidateRepeatedIDRefuses(t *testing.T) {
	baseItems := []string{
		pageCutItem("id-1", "x", 1),
		pageCutItem("id-1", "x", 1),
		pageCutItem("id-2", "y", 2),
	}
	candItems := []string{
		pageCutItem("id-1", "x", 1),
		pageCutItem("id-1", "x", 1),
		pageCutItem("id-2", "y", 2),
	}
	baseline := mustDecodeRESTSnapshot(t, dedupCollapseBody(baseItems))
	candidate := mustDecodeRESTSnapshot(t, dedupCollapseBody(candItems))

	shape := pageCutShape(3)
	plan := buildDuplicateCollapsePageCutPlan(shape, baseline.Data, candidate.Data)
	if plan.applies {
		t.Fatal("plan should not apply -- the candidate itself carries a repeated id")
	}
}

// TestDuplicateCollapsePageCutShape_MissingBaselineDistinctIDRefuses is
// the synthetic guard for rule 4: a genuinely dropped Go-side row -- one
// of baseline's own distinct ids never appears in the candidate at all --
// breaks the literal-prefix property outright rather than being silently
// folded into "a candidate-only id ran short".
func TestDuplicateCollapsePageCutShape_MissingBaselineDistinctIDRefuses(t *testing.T) {
	baseItems := []string{
		pageCutItem("id-1", "x", 1),
		pageCutItem("id-1", "x", 1),
		pageCutItem("id-2", "y", 2),
		pageCutItem("id-3", "z", 3),
	}
	// id-2 (a baseline-distinct, non-duplicated id) is dropped entirely --
	// a real Go-side regression, not a page-slot consequence.
	candItems := []string{
		pageCutItem("id-1", "x", 1),
		pageCutItem("id-3", "z", 3),
	}
	baseline := mustDecodeRESTSnapshot(t, dedupCollapseBody(baseItems))
	candidate := mustDecodeRESTSnapshot(t, dedupCollapseBody(candItems))

	shape := pageCutShape(4)
	plan := buildDuplicateCollapsePageCutPlan(shape, baseline.Data, candidate.Data)
	if plan.applies {
		t.Fatal("plan should not apply -- id-2 is missing from the candidate entirely, not merely absent from the baseline's own truncated page")
	}
}

// TestDuplicateCollapsePageCutShape_CandidateOnlyIDInsidePrefixRefuses is
// a SECOND, distinct synthetic guard for rule 4: a candidate-only id is
// spliced INTO the shared prefix's own range (ahead of where baseline's
// own duplication-truncated page reaches) rather than sitting in the
// tail -- a real ordering anomaly this mechanism does not produce, and
// distinct from the missing-id case above (every baseline-distinct id IS
// present in the candidate here, just not contiguously as the prefix).
func TestDuplicateCollapsePageCutShape_CandidateOnlyIDInsidePrefixRefuses(t *testing.T) {
	baseItems := []string{
		pageCutItem("id-1", "a", 1),
		pageCutItem("id-1", "a", 1),
		pageCutItem("id-2", "b", 2),
		pageCutItem("id-3", "c", 4),
	}
	// dedup(baseline) is [id-1, id-2, id-3] in that order; a genuinely new
	// id ("id-9") is inserted BEFORE id-3 instead of after it, at a rank
	// that keeps the candidate's OWN list still monotone by created_at --
	// this test isolates rule 4's own id-position check, not rule 6's.
	candItems := []string{
		pageCutItem("id-1", "a", 1),
		pageCutItem("id-2", "b", 2),
		pageCutItem("id-9", "new", 3),
		pageCutItem("id-3", "c", 4),
	}
	baseline := mustDecodeRESTSnapshot(t, dedupCollapseBody(baseItems))
	candidate := mustDecodeRESTSnapshot(t, dedupCollapseBody(candItems))

	shape := pageCutShape(4)
	plan := buildDuplicateCollapsePageCutPlan(shape, baseline.Data, candidate.Data)
	if plan.applies {
		t.Fatal("plan should not apply -- id-9 sits inside the shared prefix's own range instead of the tail")
	}
}

// TestDuplicateCollapsePageCutShape_CandidateLongerThanLimitRefuses is
// the synthetic guard for rule 5: the candidate list is LONGER than the
// route's own declared Limit -- structurally impossible under the
// route's own shared LIMIT in production, but a real Go-side over-fetch
// must still refuse rather than being silently admitted. This is the
// same case the count argument (C vs E) names as "C > E" -- the type
// doc comment's own rule 5 states why the two are one and the same
// check, not two independent ones.
func TestDuplicateCollapsePageCutShape_CandidateLongerThanLimitRefuses(t *testing.T) {
	// Limit is declared as 5 even though the candidate carries 6 items --
	// this shape never assumes the candidate obeys the declared Limit.
	baseItems := []string{
		pageCutItem("id-1", "a", 1),
		pageCutItem("id-1", "a", 1),
		pageCutItem("id-2", "b", 2),
		pageCutItem("id-3", "c", 3),
		pageCutItem("id-4", "d", 4),
	}
	candItems := []string{
		pageCutItem("id-1", "a", 1),
		pageCutItem("id-2", "b", 2),
		pageCutItem("id-3", "c", 3),
		pageCutItem("id-4", "d", 4),
		pageCutItem("id-8", "new1", 5),
		pageCutItem("id-9", "new2", 6),
	}
	baseline := mustDecodeRESTSnapshot(t, dedupCollapseBody(baseItems))
	candidate := mustDecodeRESTSnapshot(t, dedupCollapseBody(candItems))

	shape := pageCutShape(5)
	plan := buildDuplicateCollapsePageCutPlan(shape, baseline.Data, candidate.Data)
	if plan.applies {
		t.Fatal("plan should not apply -- the candidate (6 items) is longer than the declared Limit (5)")
	}
}

// TestDuplicateCollapsePageCutShape_ShortfallBelowLimitAdmits is the
// positive synthetic counterpart: the candidate carries fewer new ids
// than the duplicate copies wasted, but its own page is genuinely SHORT
// of Limit -- the true population ran out, exactly the real capture's
// own shape (see
// TestDuplicateCollapsePageCutShape_RealTeamScopedCaptureAdmitsTheShortfall).
func TestDuplicateCollapsePageCutShape_ShortfallBelowLimitAdmits(t *testing.T) {
	// Limit 5: baseline 5 items, dedup = [id-1, id-2, id-3]. Candidate
	// carries dedup's own 3 ids plus ONE new id -- 4 items total, short
	// of Limit 5, still monotone by created_at.
	baseItems := []string{
		pageCutItem("id-1", "a", 1),
		pageCutItem("id-1", "a", 1),
		pageCutItem("id-2", "b", 2),
		pageCutItem("id-2", "b", 2),
		pageCutItem("id-3", "c", 3),
	}
	candItems := []string{
		pageCutItem("id-1", "a", 1),
		pageCutItem("id-2", "b", 2),
		pageCutItem("id-3", "c", 3),
		pageCutItem("id-9", "new", 4),
	}
	baseline := mustDecodeRESTSnapshot(t, dedupCollapseBody(baseItems))
	candidate := mustDecodeRESTSnapshot(t, dedupCollapseBody(candItems))

	shape := pageCutShape(5)
	plan := buildDuplicateCollapsePageCutPlan(shape, baseline.Data, candidate.Data)
	if !plan.applies {
		t.Fatal("plan should apply -- the candidate page (4 items) is short of Limit (5), so the true population genuinely ran out")
	}
}

// TestDuplicateCollapsePageCutShape_ExactCollapseAlsoAdmits pins that
// this shape ALSO admits the C == 0 exact-collapse case
// DuplicateCollapseLengthShape already covers -- the two shapes are not
// mutually exclusive by construction, only by what each one's own
// declared entry is proven against; both independently reaching the same
// verdict on an overlapping case is expected, not a conflict.
func TestDuplicateCollapsePageCutShape_ExactCollapseAlsoAdmits(t *testing.T) {
	baseItems := []string{
		pageCutItem("id-1", "a", 1),
		pageCutItem("id-1", "a", 1),
		pageCutItem("id-2", "b", 2),
	}
	candItems := []string{
		pageCutItem("id-1", "a", 1),
		pageCutItem("id-2", "b", 2),
	}
	baseline := mustDecodeRESTSnapshot(t, dedupCollapseBody(baseItems))
	candidate := mustDecodeRESTSnapshot(t, dedupCollapseBody(candItems))

	shape := pageCutShape(3)
	plan := buildDuplicateCollapsePageCutPlan(shape, baseline.Data, candidate.Data)
	if !plan.applies {
		t.Fatal("plan should apply -- one wasted slot, no new id at all, and the candidate page (2 items) is short of Limit (3): admitted by the shortfall branch")
	}
}

// TestDuplicateCollapsePageCutShape_AdmitsOnlyTheLengthFinding pins the
// admits() dispatch itself: it never reaches for any OTHER shape's own
// finding (a leaf value, a presence key) even when the plan applies, and
// never admits a length finding under a DIFFERENT list path.
func TestDuplicateCollapsePageCutShape_AdmitsOnlyTheLengthFinding(t *testing.T) {
	baseItems := []string{
		pageCutItem("id-1", "a", 1),
		pageCutItem("id-1", "a", 1),
		pageCutItem("id-2", "b", 2),
	}
	candItems := []string{
		pageCutItem("id-1", "a", 1),
		pageCutItem("id-2", "b", 2),
		pageCutItem("id-9", "new", 3),
	}
	baseline := mustDecodeRESTSnapshot(t, dedupCollapseBody(baseItems))
	candidate := mustDecodeRESTSnapshot(t, dedupCollapseBody(candItems))

	shape := pageCutShape(3)
	plan := buildDuplicateCollapsePageCutPlan(shape, baseline.Data, candidate.Data)
	if !plan.applies {
		t.Fatal("plan should apply -- one clean shortfall, candidate page short of Limit")
	}
	leafFinding := Finding{Kind: FindingMismatch, Path: "$.data.items[0].title", Detail: "x != z", Shape: ShapeValue}
	if plan.admits(leafFinding) {
		t.Fatal("this shape must never admit a leaf finding, only the list's own length")
	}
	presenceFinding := Finding{Kind: FindingMismatch, Path: "$.data.items[0]", Detail: `key "id-9" present in candidate, absent in baseline`, Shape: ShapePresence}
	if plan.admits(presenceFinding) {
		t.Fatal("this shape must never admit a presence finding, only the list's own length")
	}
	wrongPathFinding := Finding{Kind: FindingMismatch, Path: "$.data.other", Detail: "length 3 != 3", Shape: ShapeLength}
	if plan.admits(wrongPathFinding) {
		t.Fatal("this shape must never admit a length finding under a different list path")
	}
}

// mustDecodeRESTSnapshot decodes a bare REST body with no dedup-key
// injection: pageCutItem/dedupCollapseItem already embed
// RESTDedupKeyField as a literal, the SAME convention
// dedupcollapselength_test.go's own synthetic bodies use.
func mustDecodeRESTSnapshot(t *testing.T, body string) Snapshot {
	t.Helper()
	snapshot, err := DecodeRESTSnapshot([]byte(body))
	if err != nil {
		t.Fatalf("decode REST body: %v", err)
	}
	return snapshot
}

// TestDuplicateCollapsePageCutShape_TailOnlyFieldMutationStillAdmits
// pins the type doc comment's own "What this shape CANNOT catch"
// paragraph as an accepted, present fact, not a silent gap: the real
// captured candidate's own LAST row -- a CANDIDATE-ONLY tail row, past
// dedup(baseline)'s own 28-element prefix, with no baseline counterpart
// anywhere in this comparison -- has its title changed to a value this
// shape has no baseline data to check it against. The plan still
// applies: IDField and SortField (rules 3, 4, 6) are unaffected by a
// title change, and no rule reads any other field of a candidate-only
// row. This is the shape's own documented scope, reproduced here so a
// future change to that scope shows up as a failing assertion, not a
// silent behavior change.
func TestDuplicateCollapsePageCutShape_TailOnlyFieldMutationStillAdmits(t *testing.T) {
	baseline, candidate, items := pageCutMutateCandidateItems(t)
	last := len(items) - 1
	row, ok := items[last].(map[string]any)
	if !ok {
		t.Fatalf("items[%d] is not an object: %#v", last, items[last])
	}
	if _, hasID := row[RESTDedupKeyField]; !hasID {
		t.Fatalf("items[%d] carries no %s -- fixture assumption broken", last, RESTDedupKeyField)
	}
	row["title"] = "TAIL-ONLY-FIELD-THIS-SHAPE-DOES-NOT-CHECK"

	shape := pageCutShape(50)
	plan := buildDuplicateCollapsePageCutPlan(shape, baseline.Data, candidate.Data)
	if !plan.applies {
		t.Fatal("plan should still apply -- title is neither IDField nor SortField, and items[35] is candidate-only (no baseline counterpart to check it against)")
	}
}

// TestDuplicateCollapsePageCutShape_BaselineNonMonotoneRawOrderRefuses
// pins rule 2b: the baseline's own RAW physical sequence (duplicates in
// their own captured position, not dedup(baseline)'s first-occurrence
// order) carries a strict, non-tied INCREASE at position 2 (a second
// physical copy of id-1, sharing id-1's own exact content and
// created_at per rule 2, placed after id-2's own entry) -- while
// dedup(baseline)'s own first-occurrence order ([id-1, id-2, id-3]) and
// the candidate's own list (an exact collapse, [id-1, id-2, id-3]) both
// stay perfectly monotone on their own terms. Without rule 2b this pair
// satisfies every other rule unchanged -- proving rule 2b is genuinely
// independent of rule 6, not shadowed by it.
func TestDuplicateCollapsePageCutShape_BaselineNonMonotoneRawOrderRefuses(t *testing.T) {
	baseItems := []string{
		pageCutItem("id-1", "a", 1),
		pageCutItem("id-2", "b", 2),
		pageCutItem("id-1", "a", 1), // id-1's second copy, out of raw physical order
		pageCutItem("id-3", "c", 3),
		pageCutItem("id-2", "b", 2), // id-2's second copy
	}
	candItems := []string{
		pageCutItem("id-1", "a", 1),
		pageCutItem("id-2", "b", 2),
		pageCutItem("id-3", "c", 3),
	}
	baseline := mustDecodeRESTSnapshot(t, dedupCollapseBody(baseItems))
	candidate := mustDecodeRESTSnapshot(t, dedupCollapseBody(candItems))

	shape := pageCutShape(5)
	plan := buildDuplicateCollapsePageCutPlan(shape, baseline.Data, candidate.Data)
	if plan.applies {
		t.Fatal("plan should refuse -- the baseline's own raw physical sequence is not weakly monotone DESC (id-1's second copy sits after id-2's entry)")
	}
}

// TestDuplicateCollapsePageCutShape_TailOnlyIDSubstitutionStillAdmits
// pins the same documented limit as
// TestDuplicateCollapsePageCutShape_TailOnlyFieldMutationStillAdmits, on
// IDField itself rather than an unrelated content field: the real
// captured candidate's own LAST row -- a candidate-only tail row -- has
// its own id replaced with a different, still-unique value no other
// element carries. Rule 3 checks only that IDField is UNIQUE within the
// candidate, never that it names a real, correct element; a
// wrong-but-unique id satisfies rule 3 exactly as a correct one does.
// The plan still applies, matching the type doc comment's own "What
// this shape CANNOT catch" paragraph: uniqueness and monotonicity are
// the only structural properties available for a row baseline never
// returned, never content correctness.
func TestDuplicateCollapsePageCutShape_TailOnlyIDSubstitutionStillAdmits(t *testing.T) {
	baseline, candidate, items := pageCutMutateCandidateItems(t)
	last := len(items) - 1
	row, ok := items[last].(map[string]any)
	if !ok {
		t.Fatalf("items[%d] is not an object: %#v", last, items[last])
	}
	if _, hasID := row[RESTDedupKeyField]; !hasID {
		t.Fatalf("items[%d] carries no %s -- fixture assumption broken", last, RESTDedupKeyField)
	}
	row[RESTDedupKeyField] = "id-THIS-SHAPE-CANNOT-VERIFY-IS-CORRECT"

	shape := pageCutShape(50)
	plan := buildDuplicateCollapsePageCutPlan(shape, baseline.Data, candidate.Data)
	if !plan.applies {
		t.Fatal("plan should still apply -- the substituted id is still unique within the candidate, and the row is candidate-only (no baseline counterpart to check its true id against)")
	}
}
