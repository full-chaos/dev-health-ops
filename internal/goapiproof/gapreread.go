package goapiproof

import (
	"fmt"
	"time"
)

// The delayed re-read (CHAOS-6068): the bracketed re-read (writeskew.go)
// runs seconds after the candidate leg. The Python investment scope gate
// fails open for the 4-9 s gap between the materializer's
// work_unit_investments write and its membership marker (CHAOS-5975), so
// a case inside that gap has its second baseline read still inside it:
// second baseline == first baseline, the reference "did not move", and
// the difference is reported outside every declaration although the
// baseline's answer is only transiently wrong. A case whose outside
// findings are not all value leaves is never re-read at all.
//
// The delayed re-read waits out the gap (a bounded delay, well above the
// worst gap in the production records) and reads BOTH planes again. It
// admits a case only when the reference plane CHANGED and now agrees with
// the UNCHANGED candidate on the whole case:
//
//   - the candidate re-read (C2) equals the first candidate read (C1)
//     exactly: the candidate is stable, so no candidate defect that
//     varies can be reported as the baseline's;
//   - the delayed baseline read (B3) differs from the first baseline
//     read (B1): the reference moved. A stable difference gives B3 == B1
//     and stays outside;
//   - B3 compared with C1 under the case's declarations has nothing
//     outside and no Acceptance entry.
//
// Anything else leaves the case exactly as the first comparison and the
// bracketed re-read left it: the delayed stage can turn a "stands" into
// an admission and never anything into a refusal or a different verdict.

// GapRereadCitation is the citation a delayed-admitted case adds to its
// matched declarations.
const GapRereadCitation = "write-skew:delayed-reread"

// GapRereadOutcome names how the delayed stage ended for one case.
type GapRereadOutcome string

const (
	// GapRereadAdmitted: the reference moved and agrees with the stable
	// candidate on the whole case.
	GapRereadAdmitted GapRereadOutcome = "gap_admitted"
	// GapRereadBaselineUnchanged: B3 equals B1 -- a stable difference.
	GapRereadBaselineUnchanged GapRereadOutcome = "baseline_unchanged"
	// GapRereadNoCleanMatch: the reference moved but B3 still differs
	// from C1 outside the declarations (or is refused structurally, or
	// carries an Acceptance entry).
	GapRereadNoCleanMatch GapRereadOutcome = "baseline_moved_no_clean_match"
	// GapRereadCandidateUnstable: C2 differs from C1.
	GapRereadCandidateUnstable GapRereadOutcome = "candidate_unstable"
	// GapRereadReadFailed: a delayed read failed in transport or
	// admission (or was not JSON); the first verdict stands.
	GapRereadReadFailed GapRereadOutcome = "delayed_read_failed"
	// GapRereadBudgetExhausted: the run's delay budget had no room.
	GapRereadBudgetExhausted GapRereadOutcome = "delay_budget_exhausted"
	// GapRereadDeadlinePressure: the run deadline left no room for the
	// delay and two reads.
	GapRereadDeadlinePressure GapRereadOutcome = "deadline_pressure"
	// GapRereadCancelled: the run context ended during the delay.
	GapRereadCancelled GapRereadOutcome = "cancelled_during_delay"
)

// GapRereadOutcomes lists every outcome, in one place, for the tests that
// pin the decision table to the generated list.
var GapRereadOutcomes = []GapRereadOutcome{
	GapRereadAdmitted, GapRereadBaselineUnchanged, GapRereadNoCleanMatch, GapRereadCandidateUnstable,
	GapRereadReadFailed, GapRereadBudgetExhausted, GapRereadDeadlinePressure, GapRereadCancelled,
}

// Bounds for the delay flag: zero disables the stage, anything above the
// maximum is refused.
const (
	GapRereadDefaultDelay  = 15 * time.Second
	GapRereadMaxDelay      = 60 * time.Second
	GapRereadDefaultBudget = 4 * time.Minute
)

// GapRereadEligible reports whether a case can go to the delayed stage:
// its first comparison has a finding outside every declaration (never a
// structural refusal), and either no bracketed re-read applied (a
// presence, type or mixed difference) or the bracketed re-read found the
// reference unmoved at an outside leaf (WriteSkewDecision.ReferenceUnmoved).
// A bracketed re-read that refused, admitted, or stood for any other
// reason (R7) is final.
func GapRereadEligible(first Result, bracket *WriteSkewDecision) bool {
	if len(first.outsideFindings) == 0 {
		return false
	}
	if bracket == nil {
		return !WriteSkewRereadNeeded(first)
	}
	return bracket.Verdict == WriteSkewStands && bracket.ReferenceUnmoved
}

// GapRereadDecision is ClassifyGapReread's decision.
type GapRereadDecision struct {
	Outcome GapRereadOutcome
	Detail  string
	// Second is the comparison of B3 with C1, set when it was run; on
	// GapRereadAdmitted it is the clean comparison the case now stands on.
	Second Result
}

// ClassifyGapReread decides one eligible case from its first comparison,
// the first baseline (B1) and candidate (C1) reads, and the delayed
// candidate (C2) and baseline (B3) reads, in this precedence:
//
//  1. C2 differs from C1 -> candidate_unstable;
//  2. B3 equals B1 -> baseline_unchanged;
//  3. B3 compared with C1 is refused structurally, has an outside
//     difference or carries any Acceptance entry -> baseline_moved_no_clean_match;
//  4. otherwise -> gap_admitted.
func ClassifyGapReread(first Result, baseline1, candidate1, baseline3, candidate2 Snapshot, opts Options) GapRereadDecision {
	if len(first.outsideFindings) == 0 {
		return GapRereadDecision{Outcome: GapRereadBaselineUnchanged, Detail: "not eligible: nothing outside the declarations"}
	}
	if !decodedEqual(candidate2.Data, candidate1.Data) {
		return GapRereadDecision{Outcome: GapRereadCandidateUnstable, Detail: "the candidate's delayed read differs from its first read"}
	}
	if decodedEqual(baseline3.Data, baseline1.Data) {
		return GapRereadDecision{Outcome: GapRereadBaselineUnchanged, Detail: "the reference plane did not move over the delay: a stable difference"}
	}
	second := Compare(baseline3, candidate1, opts)
	if second.StructuralRefusal != "" {
		return GapRereadDecision{Outcome: GapRereadNoCleanMatch, Second: second,
			Detail: fmt.Sprintf("the delayed baseline read compared with the candidate is refused structurally (%s)", second.StructuralRefusal)}
	}
	if second.DifferencesOutsideBaselineDefect != 0 {
		return GapRereadDecision{Outcome: GapRereadNoCleanMatch, Second: second,
			Detail: fmt.Sprintf("the delayed baseline read still differs from the candidate (outside=%d)", second.DifferencesOutsideBaselineDefect)}
	}
	if len(second.Acceptance()) > 0 {
		return GapRereadDecision{Outcome: GapRereadNoCleanMatch, Second: second,
			Detail: "the delayed comparison carries an Acceptance entry; an admission never rests on it"}
	}
	return GapRereadDecision{Outcome: GapRereadAdmitted, Second: second}
}

// decodedEqual is exact equality of two decoded JSON values, numbers by
// exact numeric value (leafValuesEqual).
func decodedEqual(a, b any) bool {
	switch av := a.(type) {
	case map[string]any:
		bv, ok := b.(map[string]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for k, x := range av {
			y, present := bv[k]
			if !present || !decodedEqual(x, y) {
				return false
			}
		}
		return true
	case []any:
		bv, ok := b.([]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for i := range av {
			if !decodedEqual(av[i], bv[i]) {
				return false
			}
		}
		return true
	}
	return leafValuesEqual(a, b)
}
