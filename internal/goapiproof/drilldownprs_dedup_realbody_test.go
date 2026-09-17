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
// ONE of the 20 duplicate groups (repo 7b9583ee.../PR 573) DISAGREES
// between its own two copies in merged_at alone: one physical version
// has merged_at null, the other a timestamp, and the candidate carries
// that timestamp. The other 19 duplicate groups are content-identical.
//
// Because admission is evaluated per id, the plain duplicate-row entry
// admits the 19 agreeing groups and refuses PR 573, naming its dedup id;
// the write-once merged_at entry admits PR 573 and nothing else.
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

// drilldownPRsPlainDedupRefusal and drilldownPRsWriteOnceRefusal are the
// Detail phrases refusalDetail writes for each dedup entry kind.
const (
	drilldownPRsPlainDedupRefusal = "not admitted by the declared duplicate-row shape"
	drilldownPRsWriteOnceRefusal  = "not admitted by the declared write-once merged_at/first_review_at/review_latency_hours rule"
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

// TestDrilldownPRsParity_RealCapturedBodyAdmitsAgreeingGroupsAndTheMergedAtGroup
// pins the per-id admission verdict against the real captured pair: the
// 19 content-identical duplicate groups are admitted by the plain entry,
// PR 573's null-then-populated merged_at group is admitted by the
// write-once entry alone, and nothing stays outside. Every finding the
// plain entry refused names PR 573's own dedup id, and no finding carries
// the write-once refusal -- this asserts the SHAPE of the coverage, not
// just its size.
func TestDrilldownPRsParity_RealCapturedBodyAdmitsAgreeingGroupsAndTheMergedAtGroup(t *testing.T) {
	baseline := drilldownPRsSnapshotFromFile(t, drilldownPRsDedupBaselinePath)
	candidate := drilldownPRsSnapshotFromFile(t, drilldownPRsDedupCandidatePath)

	result := Compare(baseline, candidate, drilldownPRsParity)

	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch -- a declared defect never converts one", result.TerminalState)
	}
	wantMatched := drilldownPRsWantMatched()
	if !equalStrings(result.BaselineDefectsMatched, wantMatched) {
		t.Fatalf("matched = %v, want %v: idle %v stale %v",
			result.BaselineDefectsMatched, wantMatched, result.IdleIntermittentBaselineDefects, result.StaleBaselineDefects)
	}
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	driftedIDQuoted := fmt.Sprintf("%q", drilldownPRsDedupDriftedID)
	sawPlainRefusal := false
	for _, f := range result.Findings {
		if strings.Contains(f.Detail, drilldownPRsWriteOnceRefusal) {
			t.Errorf("write-once entry refused a finding it should admit: %s: %s", f.Path, f.Detail)
		}
		if !strings.Contains(f.Detail, drilldownPRsPlainDedupRefusal) {
			continue
		}
		sawPlainRefusal = true
		if !strings.Contains(f.Detail, driftedIDQuoted) {
			t.Errorf("plain-entry refusal %s cites a different id than PR 573's own: %s", f.Path, f.Detail)
		}
	}
	if !sawPlainRefusal {
		t.Error("the plain duplicate-row entry refused nothing; PR 573's copies disagree and it must refuse them")
	}
}

// TestDrilldownPRsParity_RealCapturedBodyMergedAtGroupWithCandidateNullStaysUncovered
// sets PR 573's candidate merged_at to null in the real captured pair: the
// write-once entry must refuse the id, its findings stay outside, and each
// names PR 573's own dedup id under the write-once refusal.
func TestDrilldownPRsParity_RealCapturedBodyMergedAtGroupWithCandidateNullStaysUncovered(t *testing.T) {
	baseline := drilldownPRsSnapshotFromFile(t, drilldownPRsDedupBaselinePath)
	candidate := drilldownPRsSnapshotFromFile(t, drilldownPRsDedupCandidatePath)

	mutated := false
	for _, element := range candidate.Data.(map[string]any)["items"].([]any) {
		object := element.(map[string]any)
		if object[RESTDedupKeyField] == drilldownPRsDedupDriftedID {
			object["merged_at"] = nil
			mutated = true
		}
	}
	if !mutated {
		t.Fatal("candidate fixture carries no PR 573 row to mutate")
	}

	result := Compare(baseline, candidate, drilldownPRsParity)

	if result.DifferencesOutsideBaselineDefect == 0 {
		t.Fatalf("outside = 0, want > 0: a candidate null merged_at must not be admitted; findings %+v", result.Findings)
	}
	driftedIDQuoted := fmt.Sprintf("%q", drilldownPRsDedupDriftedID)
	sawWriteOnceRefusal := false
	for _, f := range result.Findings {
		if !strings.Contains(f.Detail, drilldownPRsWriteOnceRefusal) {
			continue
		}
		sawWriteOnceRefusal = true
		if !strings.Contains(f.Detail, driftedIDQuoted) {
			t.Errorf("write-once refusal %s cites a different id than PR 573's own: %s", f.Path, f.Detail)
		}
	}
	if !sawWriteOnceRefusal {
		t.Error("no finding carries the write-once refusal for PR 573")
	}
}

// TestDrilldownPRsParity_RealCapturedBodyWithAGenuineRegressionStaysUncovered
// keeps the dedup shape's safety intent against the real captured pair:
// mutating one shared, non-duplicated PR's own title in the candidate
// leg is a real per-field regression hiding behind an id the two pages
// share. Admission is per id, so this excludes ONLY that one id -- the mutated finding
// stays outside and cites the mutated PR's own id, while the 19 agreeing
// duplicate groups and PR 573's merged_at group are judged exactly as
// they are in the unmutated fixture, so every ticket still reads as
// matched (something else really was admitted), never idle or stale.
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
