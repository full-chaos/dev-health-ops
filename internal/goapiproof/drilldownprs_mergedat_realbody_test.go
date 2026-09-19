package goapiproof

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

// This file drives drilldownPRsParity through the same path
// go-api-rest-prove's Runner takes (DecodeRESTSnapshot, then
// InjectRESTDedupKeys on both legs, then Compare) against captured
// production GET and POST /api/v1/drilldown/prs response bodies. In
// every captured baseline, five pull requests appear more than once, and
// each one's copies differ ONLY in merged_at: null on some copies, one
// identical timestamp on the others. The candidate carries that
// timestamp. Both pages are bounded by the same limit, so the repeated
// rows push the baseline's tail rows off its page.
//
// The four request cases share bodies: default_window has its own
// baseline; default_filters and range_days_90 returned byte-identical
// baselines; those three returned one byte-identical candidate;
// explicit_scope_and_sort (limit 25) has its own pair.

// drilldownPRsMergedAtCase is one captured request case.
type drilldownPRsMergedAtCase struct {
	name      string
	baseline  string
	candidate string
	// limit is the request's own effective limit, as its corpus entry
	// binds it.
	limit int
}

// opts is the case's own Parity, as its corpus entry binds it.
func (c drilldownPRsMergedAtCase) opts() Options {
	return drilldownPRsParityWithLimit(c.limit)
}

var drilldownPRsMergedAtCases = []drilldownPRsMergedAtCase{
	{"GET default_window", "testdata/drilldownprs_mergedat_baseline_ada5c41c.json", "testdata/drilldownprs_mergedat_candidate_a086b357.json", drilldownPRsDefaultLimit},
	{"GET range_days_90", "testdata/drilldownprs_mergedat_baseline_a2fa3f34.json", "testdata/drilldownprs_mergedat_candidate_a086b357.json", drilldownPRsDefaultLimit},
	{"POST default_filters", "testdata/drilldownprs_mergedat_baseline_a2fa3f34.json", "testdata/drilldownprs_mergedat_candidate_a086b357.json", drilldownPRsDefaultLimit},
	{"POST explicit_scope_and_sort", "testdata/drilldownprs_mergedat_baseline_886028c5.json", "testdata/drilldownprs_mergedat_candidate_89ae9fa1.json", 25},
}

// drilldownPRsMergedAtIDs are the five (repo_id, number) identities whose
// baseline copies differ in merged_at alone, in every captured case.
var drilldownPRsMergedAtIDs = []string{
	"7b9583ee-4d24-2be7-4d09-34f815bebdd7" + restDedupKeySeparator + "576",
	"7b9583ee-4d24-2be7-4d09-34f815bebdd7" + restDedupKeySeparator + "578",
	"920f9442-07df-4217-4dc4-c5833c0b8268" + restDedupKeySeparator + "2628",
	"920f9442-07df-4217-4dc4-c5833c0b8268" + restDedupKeySeparator + "2629",
	"920f9442-07df-4217-4dc4-c5833c0b8268" + restDedupKeySeparator + "2630",
}

// drilldownPRsParityWithout returns drilldownPRsParity minus the entries
// whose shape keep rejects.
func drilldownPRsParityWithout(drop func(BaselineDefect) bool) Options {
	opts := drilldownPRsParity
	opts.BaselineDefects = nil
	for _, d := range drilldownPRsParity.BaselineDefects {
		if !drop(d) {
			opts.BaselineDefects = append(opts.BaselineDefects, d)
		}
	}
	return opts
}

// isWriteOnceEntry reports the copy-rule entry: the WorkGraphEdgeDedupShape
// entry that judges disagreeing copies by a declared field set.
func isWriteOnceEntry(d BaselineDefect) bool {
	return d.WorkGraphEdgeDedupShape != nil && d.WorkGraphEdgeDedupShape.copyRule() != nil
}

// drilldownPRsWriteOnceShape reads the write-once entry's shape from the
// corpus itself.
func drilldownPRsWriteOnceShape(t *testing.T) *WorkGraphEdgeDedupShape {
	t.Helper()
	var found *WorkGraphEdgeDedupShape
	for _, d := range drilldownPRsParity.BaselineDefects {
		if isWriteOnceEntry(d) {
			if found != nil {
				t.Fatal("drilldownPRsParity declares more than one write-once entry")
			}
			found = d.WorkGraphEdgeDedupShape
		}
	}
	if found == nil {
		t.Fatal("drilldownPRsParity declares no write-once entry")
	}
	wantFields := []string{"title", "author", "author_name", "merged_at", "first_review_at", "review_latency_hours"}
	if !equalStrings(found.RewrittenFields, wantFields) || len(found.WriteOnceFields) != 0 {
		t.Fatalf("rewritten fields = %q, write-once fields = %q, want rewritten %q and no write-once field", found.RewrittenFields, found.WriteOnceFields, wantFields)
	}
	return found
}

// TestDrilldownPRsParity_CapturedMergedAtCasesHaveNothingOutside pins the
// corpus verdict on every captured case: with the write-once entry the
// comparison leaves nothing outside and every drilldownPRsParity ticket
// is matched; without it, the same bodies leave findings outside, and
// every one of them belongs to one of the five merged_at ids -- so the
// write-once entry is what covers them, and it covers nothing else.
func TestDrilldownPRsParity_CapturedMergedAtCasesHaveNothingOutside(t *testing.T) {
	withoutWriteOnce := drilldownPRsParityWithout(isWriteOnceEntry)
	quotedIDs := make([]string, len(drilldownPRsMergedAtIDs))
	for i, id := range drilldownPRsMergedAtIDs {
		quotedIDs[i] = fmt.Sprintf("%q", id)
	}
	for _, c := range drilldownPRsMergedAtCases {
		t.Run(c.name, func(t *testing.T) {
			baseline := drilldownPRsSnapshotFromFile(t, c.baseline)
			candidate := drilldownPRsSnapshotFromFile(t, c.candidate)
			result := Compare(baseline, candidate, c.opts())
			if result.TerminalState != TerminalStateMismatch {
				t.Fatalf("terminal = %q, want mismatch", result.TerminalState)
			}
			if result.DifferencesOutsideBaselineDefect != 0 {
				t.Fatalf("outside = %d, want 0 -- findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
			}
			if want := drilldownPRsWantMatched(); !equalStrings(result.BaselineDefectsMatched, want) {
				t.Fatalf("matched = %v, want %v: idle %v stale %v live-unexplained %v", result.BaselineDefectsMatched, want,
					result.IdleIntermittentBaselineDefects, result.StaleBaselineDefects, result.LiveBaselineDefectsUnexplained)
			}
			if want := drilldownPRsWantIdleLengthTickets(); !equalStrings(result.IdleIntermittentBaselineDefects, want) {
				t.Fatalf("idle = %v, want %v -- both legs share one length here, so the length-collapse entry has nothing to admit", result.IdleIntermittentBaselineDefects, want)
			}
			for _, f := range result.Findings {
				if strings.Contains(f.Detail, drilldownPRsWriteOnceRefusal) {
					t.Errorf("write-once entry refused %s: %s", f.Path, f.Detail)
				}
			}

			// Fresh snapshots: Compare annotates findings, never data, but
			// each comparison gets its own decoded legs anyway.
			baseline = drilldownPRsSnapshotFromFile(t, c.baseline)
			candidate = drilldownPRsSnapshotFromFile(t, c.candidate)
			without := Compare(baseline, candidate, parityWithPageCutLimit(withoutWriteOnce, c.limit))
			if without.DifferencesOutsideBaselineDefect == 0 {
				t.Fatal("without the write-once entry nothing is outside; the captured case no longer exercises it")
			}
			outside := 0
			for _, f := range without.Findings {
				if !strings.Contains(f.Detail, drilldownPRsPlainDedupRefusal) {
					continue
				}
				named := false
				for _, q := range quotedIDs {
					if strings.Contains(f.Detail, q) {
						named = true
						break
					}
				}
				if !named {
					t.Errorf("plain-entry refusal %s names an id outside the five merged_at ids: %s", f.Path, f.Detail)
				}
				outside++
			}
			if outside < without.DifferencesOutsideBaselineDefect {
				t.Errorf("%d findings outside without the write-once entry, only %d of them refused by the plain entry", without.DifferencesOutsideBaselineDefect, outside)
			}
			t.Logf("outside without write-once entry = %d, with = %d", without.DifferencesOutsideBaselineDefect, result.DifferencesOutsideBaselineDefect)
		})
	}
}

// TestDrilldownPRsParity_CapturedMergedAtCasesAdmitExactlyTheFiveIDs pins
// the write-once plan on every captured case: it applies, and its
// admitted set is exactly the five merged_at ids -- no agreeing id and no
// other id.
func TestDrilldownPRsParity_CapturedMergedAtCasesAdmitExactlyTheFiveIDs(t *testing.T) {
	for _, c := range drilldownPRsMergedAtCases {
		t.Run(c.name, func(t *testing.T) {
			var shape *WorkGraphEdgeDedupShape
			for _, d := range c.opts().BaselineDefects {
				if isWriteOnceEntry(d) {
					shape = d.WorkGraphEdgeDedupShape
				}
			}
			baseline := drilldownPRsSnapshotFromFile(t, c.baseline)
			candidate := drilldownPRsSnapshotFromFile(t, c.candidate)
			plan := buildWorkGraphEdgeDedupPlan(shape, baseline.Data, candidate.Data)
			if !plan.applies {
				t.Fatal("write-once plan does not apply")
			}
			var admitted []string
			for id, ok := range plan.admittedIDs {
				if ok {
					admitted = append(admitted, id)
				}
			}
			sort.Strings(admitted)
			if !equalStrings(admitted, drilldownPRsMergedAtIDs) {
				t.Fatalf("admitted = %q, want %q", admitted, drilldownPRsMergedAtIDs)
			}
		})
	}
}

// TestDrilldownPRsParity_CapturedMergedAtCaseRefusesACandidateRegression
// mutates one merged_at id's candidate row in the captured bodies, once
// per refusal the write-once rule keeps: a candidate null, a candidate
// carrying a different timestamp, and a candidate differing in another
// field. Each leaves findings outside that name the mutated id.
func TestDrilldownPRsParity_CapturedMergedAtCaseRefusesACandidateRegression(t *testing.T) {
	target := drilldownPRsMergedAtIDs[0]
	mutations := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{"candidate merged_at different timestamp", func(o map[string]any) { o["merged_at"] = "2020-01-01T00:00:00Z" }},
		{"candidate title differs", func(o map[string]any) { o["title"] = "mutated title" }},
	}
	c := drilldownPRsMergedAtCases[0]
	for _, m := range mutations {
		t.Run(m.name, func(t *testing.T) {
			baseline := drilldownPRsSnapshotFromFile(t, c.baseline)
			candidate := drilldownPRsSnapshotFromFile(t, c.candidate)
			found := false
			for _, element := range candidate.Data.(map[string]any)["items"].([]any) {
				object := element.(map[string]any)
				if object[RESTDedupKeyField] == target {
					m.mutate(object)
					found = true
				}
			}
			if !found {
				t.Fatalf("candidate carries no row for %q", target)
			}
			result := Compare(baseline, candidate, drilldownPRsParity)
			if result.DifferencesOutsideBaselineDefect == 0 {
				t.Fatalf("outside = 0, want > 0 -- findings %+v", result.Findings)
			}
			quoted := fmt.Sprintf("%q", target)
			named := false
			for _, f := range result.Findings {
				if strings.Contains(f.Detail, drilldownPRsAccountingRefusal) {
					if !strings.Contains(f.Detail, drilldownPRsAccountingRefusal+quoted) {
						t.Errorf("accounting refusal %s names another row: %s", f.Path, f.Detail)
					}
					named = true
				}
			}
			if !named {
				t.Error("no finding carries the accounting refusal for the mutated id")
			}
		})
	}
}

// TestDrilldownPRsParity_CapturedMergedAtCandidateEqualToTheOlderCopyIsAdmitted
// pins the declared blind spot: a candidate row equal to the older
// physical copy (merged_at null) equals one whole baseline copy, and no
// writer of the table gives merged_at a direction, so it is admitted.
func TestDrilldownPRsParity_CapturedMergedAtCandidateEqualToTheOlderCopyIsAdmitted(t *testing.T) {
	c := drilldownPRsMergedAtCases[0]
	baseline := drilldownPRsSnapshotFromFile(t, c.baseline)
	candidate := drilldownPRsSnapshotFromFile(t, c.candidate)
	found := false
	for _, element := range candidate.Data.(map[string]any)["items"].([]any) {
		object := element.(map[string]any)
		if object[RESTDedupKeyField] == drilldownPRsMergedAtIDs[0] {
			object["merged_at"] = nil
			found = true
		}
	}
	if !found {
		t.Fatalf("candidate carries no row for %q", drilldownPRsMergedAtIDs[0])
	}
	result := Compare(baseline, candidate, drilldownPRsParity)
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}
