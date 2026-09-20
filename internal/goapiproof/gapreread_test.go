package goapiproof

import (
	"strings"
	"testing"
)

func gapDoc(t *testing.T, raw string) Snapshot {
	t.Helper()
	s, err := DecodeRESTSnapshot([]byte(raw))
	if err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	return s
}

// gapDocs is the small alphabet: a in {1,2,3}, with and without an extra
// field, so value, presence and mixed differences all occur.
var gapDocs = []string{
	`{"a":1}`, `{"a":2}`, `{"a":3}`,
	`{"a":1,"x":1}`, `{"a":2,"x":1}`, `{"a":3,"x":1}`,
}

// TestClassifyGapReread_Enumeration runs every (B1, B3, C2) over the
// alphabet against one candidate C1 through the real Compare, and checks
// each cell against the rule stated independently: admitted iff the
// candidate is unchanged, the reference moved, and it now equals C1.
func TestClassifyGapReread_Enumeration(t *testing.T) {
	c1Raw := gapDocs[1]
	cells, admitted := 0, 0
	seen := map[GapRereadOutcome]bool{}
	for _, b1Raw := range gapDocs {
		first := Compare(gapDoc(t, b1Raw), gapDoc(t, c1Raw), Options{})
		if len(first.outsideFindings) == 0 {
			continue // b1 == c1: nothing to re-read
		}
		for _, b3Raw := range gapDocs {
			for _, c2Raw := range gapDocs {
				cells++
				got := ClassifyGapReread(first, gapDoc(t, b1Raw), gapDoc(t, c1Raw), gapDoc(t, b3Raw), gapDoc(t, c2Raw), Options{})
				var want GapRereadOutcome
				switch {
				case c2Raw != c1Raw:
					want = GapRereadCandidateUnstable
				case b3Raw == b1Raw:
					want = GapRereadBaselineUnchanged
				case b3Raw != c1Raw:
					want = GapRereadNoCleanMatch
				default:
					want = GapRereadAdmitted
					admitted++
				}
				seen[want] = true
				if got.Outcome != want {
					t.Fatalf("B1=%s C1=%s B3=%s C2=%s: outcome %s, want %s", b1Raw, c1Raw, b3Raw, c2Raw, got.Outcome, want)
				}
			}
		}
	}
	// 5 differing B1 x 1 admitted B3 (== C1) x 1 stable C2.
	if cells != 5*36 || admitted != 5 {
		t.Fatalf("cells=%d admitted=%d, want 180 and 5", cells, admitted)
	}
	for _, o := range []GapRereadOutcome{GapRereadAdmitted, GapRereadBaselineUnchanged, GapRereadNoCleanMatch, GapRereadCandidateUnstable} {
		if !seen[o] {
			t.Fatalf("outcome %s never produced by the enumeration", o)
		}
	}
}

// A stable difference (every read the same) is never admitted, whatever
// the candidate does.
func TestClassifyGapReread_StableDifferenceIsNeverAdmitted(t *testing.T) {
	for _, c := range gapDocs {
		for _, b := range gapDocs {
			if b == c {
				continue
			}
			first := Compare(gapDoc(t, b), gapDoc(t, c), Options{})
			got := ClassifyGapReread(first, gapDoc(t, b), gapDoc(t, c), gapDoc(t, b), gapDoc(t, c), Options{})
			if got.Outcome == GapRereadAdmitted {
				t.Fatalf("stable difference B=%s C=%s admitted", b, c)
			}
		}
	}
}

// Number spelling is not a move: B3 spelled 1.0 against B1 1 is unchanged.
func TestClassifyGapReread_NumberSpellingIsNotAMove(t *testing.T) {
	first := Compare(gapDoc(t, `{"a":1}`), gapDoc(t, `{"a":2}`), Options{})
	got := ClassifyGapReread(first, gapDoc(t, `{"a":1}`), gapDoc(t, `{"a":2}`), gapDoc(t, `{"a":1.0}`), gapDoc(t, `{"a":2.0}`), Options{})
	if got.Outcome != GapRereadBaselineUnchanged {
		t.Fatalf("outcome %s, want baseline_unchanged", got.Outcome)
	}
}

// TestGapRereadEligible_Table pins the eligibility table over every
// finding kind and every bracketed-re-read verdict.
func TestGapRereadEligible_Table(t *testing.T) {
	value := Compare(gapDoc(t, `{"a":1}`), gapDoc(t, `{"a":2}`), Options{})
	presence := Compare(gapDoc(t, `{"a":1}`), gapDoc(t, `{"a":1,"x":1}`), Options{})
	mixed := Compare(gapDoc(t, `{"a":1}`), gapDoc(t, `{"a":2,"x":1}`), Options{})
	clean := Compare(gapDoc(t, `{"a":1}`), gapDoc(t, `{"a":1}`), Options{})
	if !WriteSkewRereadNeeded(value) || WriteSkewRereadNeeded(presence) || WriteSkewRereadNeeded(mixed) {
		t.Fatal("fixture kinds are not value / presence / mixed")
	}
	structural := Result{StructuralRefusal: RefusalVacuousEmptyLegs}
	stands := func(unmoved bool) *WriteSkewDecision {
		return &WriteSkewDecision{Verdict: WriteSkewStands, ReferenceUnmoved: unmoved}
	}
	for _, cell := range []struct {
		name    string
		first   Result
		bracket *WriteSkewDecision
		want    bool
	}{
		{"value, no bracket run", value, nil, false},
		{"value, stands unmoved (R5)", value, stands(true), true},
		{"value, stands otherwise (R7)", value, stands(false), false},
		{"value, admitted", value, &WriteSkewDecision{Verdict: WriteSkewAdmitted}, false},
		{"value, refused", value, &WriteSkewDecision{Verdict: WriteSkewRefused}, false},
		{"presence, no bracket", presence, nil, true},
		{"mixed, no bracket", mixed, nil, true},
		{"match", clean, nil, false},
		{"structural refusal", structural, nil, false},
	} {
		if got := GapRereadEligible(cell.first, cell.bracket); got != cell.want {
			t.Errorf("%s: eligible=%v, want %v", cell.name, got, cell.want)
		}
	}
}

// TestGapRereadOutcomes_ListIsComplete pins the list to the constants.
func TestGapRereadOutcomes_ListIsComplete(t *testing.T) {
	want := map[GapRereadOutcome]bool{
		GapRereadAdmitted: true, GapRereadBaselineUnchanged: true, GapRereadNoCleanMatch: true, GapRereadCandidateUnstable: true,
		GapRereadReadFailed: true, GapRereadBudgetExhausted: true, GapRereadDeadlinePressure: true, GapRereadCancelled: true,
	}
	if len(GapRereadOutcomes) != len(want) {
		t.Fatalf("GapRereadOutcomes has %d entries, want %d", len(GapRereadOutcomes), len(want))
	}
	for _, o := range GapRereadOutcomes {
		if !want[o] {
			t.Fatalf("unexpected outcome %s", o)
		}
	}
}

// The delayed comparison's own refusals and Acceptance entries never
// admit: each is named in the detail.
func TestClassifyGapReread_DelayedComparisonRefusalsNeverAdmit(t *testing.T) {
	b1, c1 := gapDoc(t, `{"a":1}`), gapDoc(t, `{"a":2}`)
	first := Compare(b1, c1, Options{})
	sb1, sc1 := gapDoc(t, `{"l":[{"id":"y","v":1}]}`), gapDoc(t, `{"l":[{"id":"y","v":2}]}`)
	structural := ClassifyGapReread(Compare(sb1, sc1, Options{}), sb1, sc1, gapDoc(t, `{"l":[{"id":"x","v":2}]}`), sc1, Options{})
	if structural.Outcome != GapRereadNoCleanMatch || structural.Second.StructuralRefusal == "" || !strings.Contains(structural.Detail, "structurally") {
		t.Fatalf("structural: %+v", structural)
	}
	opts := Options{VolatileFields: map[string]string{"data.nothere": "matches nothing"}}
	first = Compare(b1, c1, opts)
	accepted := ClassifyGapReread(first, b1, c1, c1, c1, opts)
	if accepted.Outcome != GapRereadNoCleanMatch || !strings.Contains(accepted.Detail, "Acceptance") {
		t.Fatalf("acceptance: %+v", accepted)
	}
}
