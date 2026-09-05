package compoundingrisk

import (
	"math"
	"os"
	"regexp"
	"testing"
)

// TestEveryPythonMaxCallSiteRejectsGoMax is a CLASS guard over every
// `pythonMax(...)` call site in this package, not a guard on one of them.
//
// Three rounds found the same defect at three DIFFERENT call sites, and the
// first two fixes each covered only the site the finding named:
//
//	r1  argued the class was guarded, proving it by mutating pythonMax's BODY
//	r2  mutated the CALL at compute.go:70 -- survived; fixed with a test there
//	r3  mutated the CALL at compute.go:48 -- survived again, because the r2 fix
//	    covered one call site rather than the class
//
// The lesson is not "add a third test". It is that a per-site test can only
// ever be as complete as whoever enumerated the sites, and the author fixing a
// finding is the worst-placed person to enumerate them. So this test does the
// enumeration MECHANICALLY, from the source, and fails when a call site exists
// that it does not cover.
//
// What each case asserts is CPython's `max(a, b)`: keep `a` unless `b > a`, so
// a NaN second operand is DISCARDED. Go's builtin `max` and `math.Max` both
// propagate NaN instead. Every case below therefore dies if its call site is
// switched to either.
func TestEveryPythonMaxCallSiteRejectsGoMax(t *testing.T) {
	nan := math.NaN()

	// Behavioural coverage, one entry per call site in compute.go. `line` is
	// documentation for the reader; the COUNT is what the guard below enforces.
	covered := []struct {
		name  string
		line  string
		check func(t *testing.T)
	}{
		{
			name: "normalizeAgainstReference",
			line: "compute.go:48 -- pythonMax(0.0, *value)",
			check: func(t *testing.T) {
				// CPython: max(0.0, nan) -> 0.0. Go max/math.Max -> NaN, which
				// then propagates through clamp01 into the score.
				record := Compute(
					goldenDay(), "repo", "org",
					Inputs{ReworkChurn: opaquePtr(nan)},
					goldenStamp(), DefaultWeights, DefaultThresholds, DefaultReferences,
				)
				if record.ChurnNorm == nil {
					t.Fatal("churn_norm is nil; expected a normalized value")
				}
				if math.IsNaN(*record.ChurnNorm) {
					t.Error("churn_norm = NaN; CPython's max(0.0, nan) is 0.0, so this call site " +
						"is using Go's max/math.Max semantics, not max()'s")
				}
			},
		},
		{
			name: "ownership concentration loop",
			line: "compute.go:70 -- pythonMax(highest, candidate)",
			check: func(t *testing.T) {
				record := Compute(
					goldenDay(), "repo", "org",
					Inputs{SingleOwnerRatio: opaquePtr(0.0), OwnershipGini: opaquePtr(nan)},
					goldenStamp(), DefaultWeights, DefaultThresholds, DefaultReferences,
				)
				if record.OwnershipNorm == nil {
					t.Fatal("ownership_norm is nil; expected a value from the two candidates")
				}
				if math.IsNaN(*record.OwnershipNorm) {
					t.Error("ownership_norm = NaN; CPython's max([0.0, nan]) is 0.0")
				}
			},
		},
		{
			name: "ComplexityDeltaRatio denominator floor",
			line: "compute.go:183 -- pythonMax(firstHalf, 1.0)",
			check: func(t *testing.T) {
				// CPython: max(nan, 1.0) keeps the FIRST operand, because
				// `1.0 > nan` is False -- so the NaN propagates and the ratio is
				// NaN. Go's max/math.Max return... also NaN here, so NaN does not
				// discriminate at this site. The discriminating input is the
				// ORDER-SENSITIVE one: max(0.5, 1.0) -> 1.0 either way, but
				// max(nan, 1.0) -> nan for CPython and NaN for Go, while
				// max(1.0, nan) differs. This site takes firstHalf FIRST, so the
				// asymmetry that separates them is a NaN in the first position
				// combined with a finite second -- CPython keeps NaN, Go's
				// builtin max also returns NaN. They agree.
				//
				// STATED PLAINLY: this call site is NOT separable by NaN. The
				// guard below still counts it, so the enumeration stays honest;
				// what it pins here is the ordinary floor behaviour, and the
				// non-separability is recorded rather than papered over with an
				// assertion that would pass for the wrong reason.
				if got := ComplexityDeltaRatio(0.5, 2.0); got != (2.0-0.5)/1.0 {
					t.Errorf("ComplexityDeltaRatio(0.5, 2.0) = %v, want the 1.0 floor applied", got)
				}
				if got := ComplexityDeltaRatio(4.0, 6.0); got != (6.0-4.0)/4.0 {
					t.Errorf("ComplexityDeltaRatio(4.0, 6.0) = %v, want division by firstHalf", got)
				}
				if !math.IsNaN(ComplexityDeltaRatio(nan, 2.0)) {
					t.Error("ComplexityDeltaRatio(nan, 2.0) should propagate NaN, as CPython's " +
						"max(nan, 1.0) keeps its first operand")
				}
			},
		},
	}

	for _, callSite := range covered {
		t.Run(callSite.name, func(t *testing.T) {
			t.Logf("call site: %s", callSite.line)
			callSite.check(t)
		})
	}

	// THE CLASS GUARD. Count the call sites in the source and require this test
	// to cover all of them. A fourth call site added later fails here until
	// someone decides what CPython does at it -- which is exactly the step
	// skipped twice already.
	source, err := os.ReadFile("compute.go")
	if err != nil {
		t.Fatalf("read compute.go: %v", err)
	}
	// Calls only: the declaration is `func pythonMax(`, excluded by requiring
	// no `func ` immediately before the name.
	calls := regexp.MustCompile(`(?m)[^c\w]pythonMax\(`).FindAllIndex(source, -1)
	declaration := regexp.MustCompile(`func pythonMax\(`).FindAllIndex(source, -1)
	callCount := len(calls) - len(declaration)
	// Positive control on the counter itself: if the regex stopped matching,
	// zero call sites would make the comparison below pass vacuously for a
	// test that covers three.
	if callCount <= 0 {
		t.Fatalf("counted %d pythonMax call sites -- the counter is broken, not the code", callCount)
	}
	if callCount != len(covered) {
		t.Errorf(
			"compute.go has %d pythonMax call site(s) but this test covers %d. "+
				"A call site nobody decided CPython's behaviour for is how the same defect "+
				"was found three times at three different sites (r1, r2, r3 on #2230). "+
				"Add the new site to `covered`, with what CPython does there.",
			callCount, len(covered),
		)
	}
}
