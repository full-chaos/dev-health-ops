package units

import (
	"testing"
)

// Both coercions below were measured against THIS checkout's Python interpreter
// (2026-09-01), not reasoned about. They exist because codex round 2 on
// CHAOS-4441 PR1 constructed executed divergences for both, and because the
// consequence of either is the same silent one: a different confidence or a
// different cap re-partitions the split, changes work_unit_ids, and re-addresses
// rows across two tables written by two different jobs.
//
// The frozen golden cannot reach either class -- its confidences are all JSON
// numbers, and it pins one explicit integer cap.

// TestConfidenceFromValueNonStringBranches covers the NON-string arms of
// ConfidenceFromValue. The string arm is covered exhaustively by
// TestConfidenceFromStringMatchesPythonCorpus against a generated corpus --
// a hand-written string matrix failed twice on this function and has been
// replaced rather than extended.
func TestConfidenceFromValueNonStringBranches(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		input    any
		expected float64
	}{
		// Python checks isinstance(value, bool) BEFORE the numeric branch,
		// because bool subclasses int there. True is therefore 0.0, not 1.0 --
		// the one case where the "obvious" Go port silently inverts a meaning.
		{name: "bool true is zero not one", input: true, expected: 0},
		{name: "bool false", input: false, expected: 0},

		{name: "float64", input: 0.75, expected: 0.75},
		// The documented decode path: read the Float32 column, widen. 0.9 as a
		// float32 widened is NOT the double 0.9, and the split compares
		// confidences for equality against the component maximum.
		{name: "float32 widened", input: float32(0.9), expected: float64(float32(0.9))},
		{name: "int", input: 3, expected: 3},
		{name: "int32", input: int32(2), expected: 2},
		{name: "int64", input: int64(7), expected: 7},

		{name: "nil", input: nil, expected: 0},
		{name: "unexpected type", input: []string{"1"}, expected: 0},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if got := ConfidenceFromValue(testCase.input); got != testCase.expected {
				t.Errorf("ConfidenceFromValue(%v) = %v, python = %v", testCase.input, got, testCase.expected)
			}
		})
	}
}

// TestResolveMaxComponentNodesMatchesPythonInt pins the environment override
// against Python's `int()` (constants.py:35-44).
//
// A divergence here is the CHAOS-2779 hazard itself: the Go materializer and the
// Python membership projection read this variable in separate processes, and a
// different resolved value on each means different components, different
// work_unit_ids, and membership projected onto ids that do not exist.
func TestResolveMaxComponentNodesMatchesPythonInt(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		env      string
		expected int
		note     string
	}{
		// The divergence codex found and executed: Python honours PEP 515
		// digit separators, so "1_0" is 10. Atoi rejected it and silently
		// resolved the default -- 150 on Go, 10 on Python, same variable.
		{name: "underscore separated", env: "1_0", expected: 10},
		{name: "readable thousands", env: "1_000", expected: 1000},

		{name: "plain", env: "150", expected: 150},
		{name: "space padded", env: " 200 ", expected: 200},
		{name: "explicit plus", env: "+42", expected: 42},

		// Python parses these fine, but the caller's own >= 1 rule then rejects
		// them -- on BOTH planes, since constants.py applies the same rule.
		{name: "negative falls back", env: "-5", expected: DefaultMaxComponentNodes,
			note: "int() succeeds, the >= 1 guard rejects"},
		{name: "zero falls back", env: "0", expected: DefaultMaxComponentNodes,
			note: "int() succeeds, the >= 1 guard rejects"},

		// Python raises ValueError for each of these, so both planes fall back.
		{name: "empty", env: "", expected: DefaultMaxComponentNodes},
		{name: "whitespace only", env: "   ", expected: DefaultMaxComponentNodes},
		{name: "not a number", env: "abc", expected: DefaultMaxComponentNodes},
		{name: "float is not an int", env: "12.5", expected: DefaultMaxComponentNodes},
		{name: "doubled underscore", env: "1__0", expected: DefaultMaxComponentNodes},
		{name: "leading underscore", env: "_1", expected: DefaultMaxComponentNodes},
		{name: "trailing underscore", env: "1_", expected: DefaultMaxComponentNodes},
		{name: "sign only", env: "+", expected: DefaultMaxComponentNodes},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Setenv(MaxComponentNodesEnvVar, testCase.env)
			if got := ResolveMaxComponentNodes(nil); got != testCase.expected {
				t.Errorf("ResolveMaxComponentNodes() = %d, python-equivalent = %d (%s)",
					got, testCase.expected, testCase.note)
			}
		})
	}
}

// TestResolveMaxComponentNodesNonASCIIDigitsDivergeFromPython pins a KNOWN,
// ACCEPTED divergence so it cannot change silently.
//
// Python's int() accepts non-ASCII decimal digits -- int("١٢") == 12 -- and this
// implementation does not, so Go falls back to the default while Python would
// resolve 12. That is a real cross-plane divergence and it is documented rather
// than fixed: chasing Python's full numeric grammar in Go is unbounded and
// unverifiable, whereas asserting the RESOLVED cap is equal on both planes is
// bounded and provable, and catches this residual along with any other.
//
// If this test ever starts failing because Go began accepting the input, that is
// good news -- update it. If it fails because the fallback value changed, the
// operator-facing behaviour moved and the parity harness needs re-checking.
func TestResolveMaxComponentNodesNonASCIIDigitsDivergeFromPython(t *testing.T) {
	const arabicIndicTwelve = "١٢" // Python: int(...) == 12

	t.Setenv(MaxComponentNodesEnvVar, arabicIndicTwelve)
	got := ResolveMaxComponentNodes(nil)

	if got == 12 {
		t.Fatal(
			"Go now accepts non-ASCII decimal digits and agrees with Python; " +
				"this documented divergence is closed -- delete this test and update " +
				"the comment on parsePythonInt",
		)
	}
	if got != DefaultMaxComponentNodes {
		t.Errorf(
			"unparseable override should fall back to the default, got %d; "+
				"the documented divergence is that Python resolves 12 here and Go resolves %d",
			got, DefaultMaxComponentNodes,
		)
	}
}
