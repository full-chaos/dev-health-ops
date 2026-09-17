package goapiproof

import (
	"strings"
	"testing"
)

// The write-once merged_at guard this mode relies on is pinned upstream
// by TestGuardPullRequestMergedAtRegressionsRefusesNullOverPopulated
// (internal/providersync): a null merged_at arriving for a key whose
// stored value is populated is replaced by the stored value before the
// insert. These tests pin the comparator side only.

// writeOnceRow renders one synthetic drilldown/prs item. mergedAt is raw
// JSON ("null" or a quoted timestamp).
func writeOnceRow(number, title, mergedAt string) string {
	return `{"repo_id":"ABC-123","number":` + number + `,"title":"` + title + `","created_at":"2024-01-01T00:00:00Z","merged_at":` + mergedAt + `}`
}

func writeOnceBody(rows ...string) string {
	return `{"items":[` + strings.Join(rows, ",") + `]}`
}

const (
	writeOnceMerged      = `"2024-01-02T00:00:00Z"`
	writeOnceMergedNaive = `"2024-01-02T00:00:00"`
	writeOnceMergedOther = `"2024-01-03T00:00:00Z"`
)

// writeOnceID is InjectRESTDedupKeys' key for ABC-123 / number.
func writeOnceID(number string) string {
	return "ABC-123" + restDedupKeySeparator + number
}

// writeOnceOptions mirrors drilldownPRsParity's two dedup entries: a
// plain duplicate-row entry and a write-once entry over the given
// fields, without the datetime entry, so a difference on one of those
// fields is judged by the dedup entries alone.
func writeOnceOptions(fields ...string) Options {
	return Options{BaselineDefects: []BaselineDefect{
		{
			Ticket: "ABC-123", Reason: "test fixture",
			Paths: []string{"data.items"}, Intermittent: true, IntermittentReason: "test fixture",
			WorkGraphEdgeDedupShape: &WorkGraphEdgeDedupShape{EdgesListPath: "data.items", IDField: RESTDedupKeyField},
		},
		{
			Ticket: "ABC-456", Reason: "test fixture",
			Paths: []string{"data.items"}, Intermittent: true, IntermittentReason: "test fixture",
			WorkGraphEdgeDedupShape: &WorkGraphEdgeDedupShape{EdgesListPath: "data.items", IDField: RESTDedupKeyField, WriteOnceFields: fields},
		},
	}}
}

func writeOnceSnapshots(t *testing.T, baselineBody, candidateBody string) (Snapshot, Snapshot) {
	t.Helper()
	baseline := restSnapshotFromJSON(t, baselineBody)
	candidate := restSnapshotFromJSON(t, candidateBody)
	InjectRESTDedupKeys(baseline.Data, "items", []string{"repo_id", "number"})
	InjectRESTDedupKeys(candidate.Data, "items", []string{"repo_id", "number"})
	return baseline, candidate
}

// TestWriteOnceField_AdmitsNullThenPopulatedCopies covers the admitted
// case: copies of id 1 differ only in merged_at (null, then one
// timestamp), the candidate carries that timestamp, and the repeated rows
// shift later positions. Nothing is outside and both entries match.
func TestWriteOnceField_AdmitsNullThenPopulatedCopies(t *testing.T) {
	cases := []struct {
		name     string
		baseline string
	}{
		{"null then populated", writeOnceBody(writeOnceRow("1", "a", "null"), writeOnceRow("1", "a", writeOnceMerged), writeOnceRow("2", "b", "null"), writeOnceRow("3", "c", "null"))},
		{"populated then null", writeOnceBody(writeOnceRow("1", "a", writeOnceMerged), writeOnceRow("1", "a", "null"), writeOnceRow("2", "b", "null"), writeOnceRow("3", "c", "null"))},
		{"three copies, two null", writeOnceBody(writeOnceRow("1", "a", "null"), writeOnceRow("1", "a", writeOnceMerged), writeOnceRow("1", "a", "null"), writeOnceRow("2", "b", "null"))},
		{"naive baseline timestamp", writeOnceBody(writeOnceRow("1", "a", "null"), writeOnceRow("1", "a", writeOnceMergedNaive), writeOnceRow("2", "b", "null"), writeOnceRow("3", "c", "null"))},
	}
	candidate := writeOnceBody(writeOnceRow("1", "a", writeOnceMerged), writeOnceRow("2", "b", "null"), writeOnceRow("3", "c", "null"), writeOnceRow("4", "d", "null"))
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			baseline, cand := writeOnceSnapshots(t, c.baseline, candidate)
			result := Compare(baseline, cand, writeOnceOptions("merged_at"))
			if result.DifferencesOutsideBaselineDefect != 0 {
				t.Fatalf("outside = %d, want 0 -- findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
			}
			if !equalStrings(result.BaselineDefectsMatched, []string{"ABC-123", "ABC-456"}) {
				t.Fatalf("matched = %v, want both entries", result.BaselineDefectsMatched)
			}
		})
	}
}

// TestWriteOnceField_RefusesEveryNeighbouringCase keeps each refusal the
// write-once rule declares: id 1 is never admitted, findings stay
// outside, and the write-once refusal names id 1.
func TestWriteOnceField_RefusesEveryNeighbouringCase(t *testing.T) {
	tail := []string{writeOnceRow("2", "b", "null"), writeOnceRow("3", "c", "null")}
	baseRows := func(rows ...string) string { return writeOnceBody(append(rows, tail...)...) }
	candRows := func(first string) string {
		return writeOnceBody(first, writeOnceRow("2", "b", "null"), writeOnceRow("3", "c", "null"), writeOnceRow("4", "d", "null"))
	}
	nullThenMerged := baseRows(writeOnceRow("1", "a", "null"), writeOnceRow("1", "a", writeOnceMerged))
	mergedCandidate := candRows(writeOnceRow("1", "a", writeOnceMerged))

	cases := []struct {
		name      string
		field     string
		baseline  string
		candidate string
	}{
		{"two different populated values", "merged_at",
			baseRows(writeOnceRow("1", "a", "null"), writeOnceRow("1", "a", writeOnceMerged), writeOnceRow("1", "a", writeOnceMergedOther)),
			writeOnceBody(writeOnceRow("1", "a", writeOnceMerged), writeOnceRow("2", "b", "null"), writeOnceRow("3", "c", "null"), writeOnceRow("4", "d", "null"), writeOnceRow("5", "e", "null"))},
		{"two different populated values, no null", "merged_at",
			baseRows(writeOnceRow("1", "a", writeOnceMerged), writeOnceRow("1", "a", writeOnceMergedOther)), mergedCandidate},
		{"candidate carries null", "merged_at", nullThenMerged, candRows(writeOnceRow("1", "a", "null"))},
		{"candidate carries a different populated value", "merged_at", nullThenMerged, candRows(writeOnceRow("1", "a", writeOnceMergedOther))},
		{"candidate differs in another field", "merged_at", nullThenMerged, candRows(writeOnceRow("1", "changed", writeOnceMerged))},
		{"another leaf differs between the copies", "merged_at",
			baseRows(writeOnceRow("1", "a", "null"), writeOnceRow("1", "retitled", writeOnceMerged)), mergedCandidate},
		{"the null copy differs in another leaf", "merged_at",
			baseRows(writeOnceRow("1", "old title", "null"), writeOnceRow("1", "a", writeOnceMerged)), mergedCandidate},
		{"the null copy carries an extra field", "merged_at",
			baseRows(`{"repo_id":"ABC-123","number":1,"title":"a","created_at":"2024-01-01T00:00:00Z","merged_at":null,"extra":1}`, writeOnceRow("1", "a", writeOnceMerged)), mergedCandidate},
		{"id absent from the candidate", "merged_at", nullThenMerged,
			writeOnceBody(writeOnceRow("2", "b", "null"), writeOnceRow("3", "c", "null"), writeOnceRow("4", "d", "null"), writeOnceRow("5", "e", "null"))},
		{"a copy is missing the field", "merged_at",
			baseRows(`{"repo_id":"ABC-123","number":1,"title":"a","created_at":"2024-01-01T00:00:00Z"}`, writeOnceRow("1", "a", writeOnceMerged)), mergedCandidate},
		{"null versus populated in an undeclared field", "created_at",
			baseRows(writeOnceRow("1", "a", "null"), writeOnceRow("1", "a", writeOnceMerged)), mergedCandidate},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			baseline, candidate := writeOnceSnapshots(t, c.baseline, c.candidate)
			opts := writeOnceOptions(c.field)
			plan := buildWorkGraphEdgeDedupPlan(opts.BaselineDefects[1].WorkGraphEdgeDedupShape, baseline.Data, candidate.Data)
			if plan.admittedIDs[writeOnceID("1")] {
				t.Fatal("write-once plan admitted id 1")
			}
			result := Compare(baseline, candidate, opts)
			if result.DifferencesOutsideBaselineDefect == 0 {
				t.Fatalf("outside = 0, want > 0 -- findings %+v", result.Findings)
			}
			wantRefusal := "not admitted by the declared write-once " + c.field + " rule"
			named := false
			for _, f := range result.Findings {
				if strings.Contains(f.Detail, wantRefusal) && strings.Contains(f.Detail, `"`+strings.ReplaceAll(writeOnceID("1"), restDedupKeySeparator, `\x1f`)+`"`) {
					named = true
				}
			}
			if !named {
				t.Errorf("no finding names id 1 under the write-once refusal -- findings %+v", result.Findings)
			}
		})
	}
}

// TestWriteOnceField_LeavesAgreeingIDsToTheSiblingEntry pins the split:
// the write-once plan neither admits nor names an id whose copies agree,
// and on its own it leaves that id's shifted findings outside.
func TestWriteOnceField_LeavesAgreeingIDsToTheSiblingEntry(t *testing.T) {
	baseline, candidate := writeOnceSnapshots(t,
		writeOnceBody(writeOnceRow("1", "a", "null"), writeOnceRow("1", "a", "null"), writeOnceRow("2", "b", "null")),
		writeOnceBody(writeOnceRow("1", "a", "null"), writeOnceRow("2", "b", "null"), writeOnceRow("3", "c", "null")))
	opts := writeOnceOptions("merged_at")
	onlyWriteOnce := Options{BaselineDefects: opts.BaselineDefects[1:]}

	plan := buildWorkGraphEdgeDedupPlan(onlyWriteOnce.BaselineDefects[0].WorkGraphEdgeDedupShape, baseline.Data, candidate.Data)
	if !plan.applies {
		t.Fatal("plan does not apply; the fixture has a repeated and a shared id")
	}
	for id, ok := range plan.admittedIDs {
		if ok {
			t.Errorf("write-once plan admitted agreeing id %q", id)
		}
	}

	result := Compare(baseline, candidate, onlyWriteOnce)
	if result.DifferencesOutsideBaselineDefect == 0 {
		t.Fatal("outside = 0: the write-once entry alone covered agreeing duplicates")
	}
	if !equalStrings(result.LiveBaselineDefectsUnexplained, []string{"ABC-456"}) {
		t.Fatalf("live-unexplained = %v, want [ABC-456]", result.LiveBaselineDefectsUnexplained)
	}
	for _, f := range result.Findings {
		if strings.Contains(f.Detail, "write-once") {
			t.Errorf("write-once entry annotated an id it does not judge: %s", f.Detail)
		}
	}

	baseline, candidate = writeOnceSnapshots(t,
		writeOnceBody(writeOnceRow("1", "a", "null"), writeOnceRow("1", "a", "null"), writeOnceRow("2", "b", "null")),
		writeOnceBody(writeOnceRow("1", "a", "null"), writeOnceRow("2", "b", "null"), writeOnceRow("3", "c", "null")))
	both := Compare(baseline, candidate, opts)
	if both.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d with both entries, want 0", both.DifferencesOutsideBaselineDefect)
	}
	if !equalStrings(both.BaselineDefectsMatched, []string{"ABC-123"}) || !equalStrings(both.IdleIntermittentBaselineDefects, []string{"ABC-456"}) {
		t.Fatalf("matched = %v idle = %v, want plain entry matched and write-once entry idle", both.BaselineDefectsMatched, both.IdleIntermittentBaselineDefects)
	}
}

// TestWriteOnceField_CandidateRepeatedIDAdmitsNothing keeps rule 3 for
// the write-once mode: a repeated candidate id disables the plan.
func TestWriteOnceField_CandidateRepeatedIDAdmitsNothing(t *testing.T) {
	baseline, candidate := writeOnceSnapshots(t,
		writeOnceBody(writeOnceRow("1", "a", "null"), writeOnceRow("1", "a", writeOnceMerged), writeOnceRow("2", "b", "null")),
		writeOnceBody(writeOnceRow("1", "a", writeOnceMerged), writeOnceRow("1", "a", writeOnceMerged), writeOnceRow("2", "b", "null")))
	shape := writeOnceOptions("merged_at").BaselineDefects[1].WorkGraphEdgeDedupShape
	if plan := buildWorkGraphEdgeDedupPlan(shape, baseline.Data, candidate.Data); plan.applies {
		t.Fatalf("plan applies with a repeated candidate id: admitted %v", plan.admittedIDs)
	}
}

// TestWriteOnceRepresentative_PerFieldAgreementWithinTheSet pins the
// group rule directly, one field at a time: within the declared set, a
// field is fine whether every copy already agrees (including every copy
// null -- the still-open, still-unreviewed case) or some copies are null
// and the rest carry one identical value (by jsonValuesEqual, so a
// naive-vs-aware pair still counts as one value); two different non-null
// values for the same field refuses the whole group.
func TestWriteOnceRepresentative_PerFieldAgreementWithinTheSet(t *testing.T) {
	copyWith := func(mergedAt, firstReviewAt any) map[string]any {
		return map[string]any{"number": "1", "title": "a", "merged_at": mergedAt, "first_review_at": firstReviewAt}
	}
	const merged = "2024-01-02T00:00:00Z"
	const reviewed = "2024-01-01T12:00:00Z"
	cases := []struct {
		name  string
		group []map[string]any
		want  bool
	}{
		{"null then populated", []map[string]any{copyWith(nil, nil), copyWith(merged, nil)}, true},
		{"every copy already populated and agreeing", []map[string]any{copyWith(merged, nil), copyWith(merged, nil)}, true},
		{"every copy null -- still open, not a disagreement", []map[string]any{copyWith(nil, nil), copyWith(nil, nil)}, true},
		{"same instant, different wire form, still one value", []map[string]any{copyWith(nil, nil), copyWith(merged, nil), copyWith("2024-01-02T00:00:00", nil)}, true},
		{"two fields in the set transition together", []map[string]any{copyWith(nil, nil), copyWith(merged, reviewed)}, true},
		{"two different populated values for the same field", []map[string]any{copyWith(nil, nil), copyWith(merged, nil), copyWith("2024-01-03T00:00:00Z", nil)}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, ok := writeOnceRepresentative(c.group, []string{"merged_at", "first_review_at"})
			if ok != c.want {
				t.Fatalf("ok = %v, want %v", ok, c.want)
			}
		})
	}
	if _, ok := writeOnceRepresentative([]map[string]any{
		{"number": "1", "title": "a", "merged_at": nil},
		{"number": "1", "title": "retitled", "merged_at": merged},
	}, []string{"merged_at"}); ok {
		t.Fatal("a disagreement on a field outside the declared set must refuse the group")
	}
}
