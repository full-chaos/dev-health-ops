package goapiproof

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
)

// This file exercises drilldownPRsParity's own WorkGraphEdgeDedupShape
// entry (restcorpus.go) through the SAME path go-api-rest-prove's Runner
// drives a live REST comparison through -- DecodeRESTSnapshot for each
// leg (a bare REST body carries no "data" envelope key, unlike
// DecodeSnapshot's GraphQL shape -- restadmit.go's own doc comment),
// InjectRESTDedupKeys on both (main.go's own call order, before Compare
// ever runs), then Compare -- against two real GET /api/v1/drilldown/prs
// response bodies captured from a production deployed-vs-deployed prove
// run, rather than the hand-authored literals workgraphedgedup_test.go
// uses.
//
// The captured pair carries every mechanism this corpus entry declares,
// AT ONCE, deliberately kept rather than trimmed to a cleaner case: the
// baseline (Python) repeats 20 PRs as duplicate rows (an unmerged
// ReplacingMergeTree physical version), every item's created_at/merged_at
// differs by the naive-vs-aware wire form the sibling entry declares, and
// ONE of the 20 duplicate groups (repo 7b9583ee.../PR 573) genuinely
// DISAGREES between its own two copies: one physical version has
// merged_at null, the other has a real timestamp -- the PR was merged
// between the two row versions being read, a live instance of the exact
// mechanism this citation names, not a pure duplicate. The other 19
// duplicate groups are content-identical.
//
// Because admission is evaluated per id, the 19 agreeing groups are
// admitted and PR 573's own fields -- the only ones a genuinely drifted
// duplicate group can ever produce -- report as ordinary, uncovered
// findings, each one's Detail naming PR 573's own dedup id.
const (
	drilldownPRsDedupBaselinePath  = "testdata/drilldownprs_dedup_baseline_aaab1cdc.json"
	drilldownPRsDedupCandidatePath = "testdata/drilldownprs_dedup_candidate_eebfa15c.json"
	// drilldownPRsDedupDriftedID is the one baseline id in the captured
	// pair whose own two physical copies disagree -- see this file's own
	// doc comment. InjectRESTDedupKeys joins repo_id and number with
	// restDedupKeySeparator (0x1f); built the same way here so the
	// literal matches exactly what the shape itself computes.
	drilldownPRsDedupDriftedID = "7b9583ee-4d24-2be7-4d09-34f815bebdd7" + restDedupKeySeparator + "573"
)

func drilldownPRsSnapshotFromFile(t *testing.T, path string) Snapshot {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture %s: %v", path, err)
	}
	snapshot, err := DecodeRESTSnapshot(body)
	if err != nil {
		t.Fatalf("decode REST fixture %s: %v", path, err)
	}
	snapshot.Data = InjectRESTDedupKeys(snapshot.Data, drilldownPRsDedup.ListPath, drilldownPRsDedup.KeyFields)
	return snapshot
}

// drilldownPRsWantMatched reads the tickets drilldownPRsParity's own
// declared entries carry, rather than a literal copy that could drift
// out of sync with restcorpus.go.
func drilldownPRsWantMatched() []string {
	want := make([]string, len(drilldownPRsParity.BaselineDefects))
	for i, d := range drilldownPRsParity.BaselineDefects {
		want[i] = d.Ticket
	}
	sort.Strings(want)
	return want
}

// TestDrilldownPRsParity_RealCapturedBodyAdmitsAgreeingGroupsAndCitesTheDriftedOneByID
// pins the per-id admission verdict against the real captured pair: the
// 19 content-identical duplicate groups are admitted (jsonValuesEqual's
// own instant-equality
// fix lets their created_at/merged_at agree across legs), and PR 573's
// own drifted group is excluded -- every remaining outside finding
// belongs to PR 573 alone, and each one's own Detail names its dedup id,
// so a reader of the report sees which id was not covered and why
// without cross-referencing the raw bodies. A test that merely asserted
// "zero differences" could not tell this fix apart from a bug that
// admits everything indiscriminately -- this asserts the SHAPE of the
// coverage, not just its size.
func TestDrilldownPRsParity_RealCapturedBodyAdmitsAgreeingGroupsAndCitesTheDriftedOneByID(t *testing.T) {
	baseline := drilldownPRsSnapshotFromFile(t, drilldownPRsDedupBaselinePath)
	candidate := drilldownPRsSnapshotFromFile(t, drilldownPRsDedupCandidatePath)

	result := Compare(baseline, candidate, drilldownPRsParity)

	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch -- a declared defect never converts one", result.TerminalState)
	}
	wantMatched := drilldownPRsWantMatched()
	if !equalStrings(result.BaselineDefectsMatched, wantMatched) {
		t.Fatalf("matched = %v, want %v -- the 19 agreeing groups and the naive-datetime entry must still be live: idle %v stale %v",
			result.BaselineDefectsMatched, wantMatched, result.IdleIntermittentBaselineDefects, result.StaleBaselineDefects)
	}
	// Exact and known for this captured pair: PR 573 has 4 fields (its
	// own dedup key, number, repo_id, title) that no OTHER declared entry
	// in drilldownPRsParity reaches, at both of its own two positions in
	// the baseline (20 and 21) -- 2*4 = 8. Its remaining fields
	// (created_at/merged_at) are ALSO excluded by the dedup shape, but
	// stay covered anyway: drilldownPRsParity's sibling, unshaped
	// naive-datetime entry cites those exact paths blanket-style and
	// does not care which id a difference under them belongs to -- an
	// id can be excluded by ONE citation and still covered by another
	// that reaches the same field through a different, unrelated
	// mechanism.
	const wantOutside = 8
	if result.DifferencesOutsideBaselineDefect != wantOutside {
		t.Fatalf("outside = %d, want %d -- findings %+v", result.DifferencesOutsideBaselineDefect, wantOutside, result.Findings)
	}
	// Every finding the dedup shape itself excluded must name PR 573's
	// own dedup id -- no OTHER id may ever be excluded in this fixture,
	// whether or not that finding also happens to be covered by the
	// sibling blanket entry.
	driftedIDQuoted := fmt.Sprintf("%q", drilldownPRsDedupDriftedID)
	for _, f := range result.Findings {
		if !strings.Contains(f.Detail, "not admitted by the declared duplicate-row shape") {
			continue
		}
		if !strings.Contains(f.Detail, driftedIDQuoted) {
			t.Errorf("dedup-shape-excluded finding %s cites a different id than PR 573's own: %s", f.Path, f.Detail)
		}
	}
}

// TestDrilldownPRsParity_RealCapturedBodyWithAGenuineRegressionStaysUncovered
// keeps the dedup shape's safety intent against the real captured pair:
// mutating one shared, non-duplicated PR's own title in the candidate
// leg is a real per-field regression hiding behind an id the two pages
// share. Admission is per id, so this excludes ONLY that one id -- the mutated finding
// stays outside and cites the mutated PR's own id, while the 19 agreeing
// duplicate groups and PR 573's own already-drifted group are judged
// exactly as they are in the unmutated fixture, so the ticket still
// reads as matched (something else really was admitted), never idle or
// stale.
func TestDrilldownPRsParity_RealCapturedBodyWithAGenuineRegressionStaysUncovered(t *testing.T) {
	baseline := drilldownPRsSnapshotFromFile(t, drilldownPRsDedupBaselinePath)
	candidate := drilldownPRsSnapshotFromFile(t, drilldownPRsDedupCandidatePath)

	items, ok := candidate.Data.(map[string]any)["items"].([]any)
	if !ok || len(items) == 0 {
		t.Fatalf("candidate fixture has no items list to mutate")
	}
	// items[4] (repo 7b9583ee.../574) is shared, non-duplicated on both
	// legs -- see this file's package doc comment.
	mutated, ok := items[4].(map[string]any)
	if !ok {
		t.Fatalf("items[4] is not an object: %#v", items[4])
	}
	mutatedID, _ := mutated[RESTDedupKeyField].(string)
	if mutatedID == "" {
		t.Fatalf("items[4] carries no %s to assert against", RESTDedupKeyField)
	}
	original, _ := mutated["title"].(string)
	mutated["title"] = original + " -- mutated for the regression test"

	result := Compare(baseline, candidate, drilldownPRsParity)

	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch", result.TerminalState)
	}
	wantMatched := drilldownPRsWantMatched()
	if !equalStrings(result.BaselineDefectsMatched, wantMatched) {
		t.Fatalf("matched = %v, want %v -- a regression on ONE id must not blind the citation to every other id it still explains: idle %v stale %v",
			result.BaselineDefectsMatched, wantMatched, result.IdleIntermittentBaselineDefects, result.StaleBaselineDefects)
	}
	foundMutation := false
	for _, f := range result.Findings {
		if f.Path != "$.data.items[4].title" {
			continue
		}
		foundMutation = true
		if strings.Contains(f.Detail, "not admitted by the declared duplicate-row shape") && !strings.Contains(f.Detail, fmt.Sprintf("%q", mutatedID)) {
			t.Errorf("mutated finding cites the wrong id: %s", f.Detail)
		}
	}
	if !foundMutation {
		t.Fatal("expected a finding at $.data.items[4].title -- the mutation did not produce one")
	}
}
