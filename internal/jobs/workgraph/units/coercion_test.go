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
		// Deliberately NOT 150: a value equal to DefaultMaxComponentNodes is
		// indistinguishable from the fallback, so it can prove nothing.
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

// TestResolveMaxComponentNodesAcceptsNonASCIIDigits replaces a test that
// pinned this as a KNOWN, ACCEPTED divergence.
//
// That test said: "If this test ever starts failing because Go began accepting
// the input, that is good news -- update it." It started failing. This is the
// update.
//
// PR1 declined the fix on the grounds that "chasing Python's full numeric
// grammar in Go is unbounded and unverifiable". The first half was right and
// the second was not: the grammar is bounded (int() accepts exactly
// str.isdecimal(), Unicode category Nd, 760 code points in Unicode 16) and it
// is verifiable, because the set can be DERIVED from the deployed interpreter
// instead of transcribed. What made it look unbounded was treating it as a
// question about Python-in-general rather than about one measurable predicate.
//
// The divergence mattered: INVESTMENT_MAX_COMPONENT_NODES written in full-width
// or Arabic-Indic digits made Python use the configured cap and Go fall back to
// the default, so the two planes split oversized components differently and
// minted different work_unit_ids.
func TestResolveMaxComponentNodesAcceptsNonASCIIDigits(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		env      string
		expected int
	}{
		{name: "arabic-indic", env: "\u0661\u0662", expected: 12},
		{name: "full width", env: "\uff12\uff10\uff10", expected: 200},
		{name: "devanagari", env: "\u0967\u0968\u0969", expected: 123},
		{name: "nko", env: "\u07c1\u07c2", expected: 12},
		{name: "mixed ascii and full width", env: "1\uff15", expected: 15},
		{name: "full width with ascii underscore", env: "\uff11_\uff10", expected: 10},

		// isdigit() but NOT isdecimal(): int() REJECTS these, so a port keyed
		// on the wrong predicate would accept values Python refuses. One letter
		// apart in the source, 128 code points apart in behaviour.
		{name: "superscript two is refused", env: "\u00b2", expected: DefaultMaxComponentNodes},
		{name: "circled one is refused", env: "\u2460", expected: DefaultMaxComponentNodes},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Setenv(MaxComponentNodesEnvVar, testCase.env)
			if got := ResolveMaxComponentNodes(nil); got != testCase.expected {
				t.Errorf("env %q resolved to %d, python resolves %d",
					testCase.env, got, testCase.expected)
			}
		})
	}
}

// TestNumericParsersRejectSeparatorsLikePythonNumerics pins the reason the
// numeric helpers must NOT adopt pythonStrip.
//
// Python uses TWO different whitespace definitions, and this is the whole
// subtlety of the fix above: str.strip() REMOVES 0x1c-0x1f, but int() and
// float() REJECT them outright. The scope guard that DOES need the wider rule
// lives at internal/jobs/investment.RequireOrganizationScope; this test exists
// here, beside the parser, so the two cannot be 'unified' without one of them
// failing.
//
//	" 150".strip() -> "150"        int(" 150")     -> 150
//	"\x1c150".strip() -> "150"     int("\x1c150")  -> ValueError
//
// Go's plain strings.TrimSpace happens to match the NUMERIC rule exactly, so
// parsePythonInt and confidenceFromString are correct as written. Applying
// pythonStrip to them "for consistency" would make Go accept a value Python
// raises on -- breaking the parsers while fixing nothing.
func TestNumericParsersRejectSeparatorsLikePythonNumerics(t *testing.T) {
	// 200, never 150. An earlier revision of this test used "150", whose
	// expected result IS DefaultMaxComponentNodes -- so a successful parse and
	// a fallback produced the same number and the assertion could not tell them
	// apart. It duly passed while pythonparity.Strip was wrongly applied to the
	// numeric parser, which is exactly the regression it exists to catch.
	//
	// Any test whose expected value equals the fallback proves nothing about
	// which path was taken.
	const distinctFromDefault = 200
	if DefaultMaxComponentNodes == distinctFromDefault {
		t.Fatalf("this test needs a value != DefaultMaxComponentNodes (%d) or it "+
			"cannot distinguish a parse from a fallback", DefaultMaxComponentNodes)
	}

	for _, separator := range []string{"\x1c", "\x1d", "\x1e", "\x1f"} {
		env := separator + "200"
		t.Setenv(MaxComponentNodesEnvVar, env)
		if got := ResolveMaxComponentNodes(nil); got != DefaultMaxComponentNodes {
			t.Errorf(
				"env %q resolved to %d, want the default (%d): Python's int() raises "+
					"ValueError on a leading separator, so both planes must fall back. "+
					"Resolving %d means the separator was STRIPPED -- pythonparity.Strip "+
					"was applied to the numeric parser; revert it, see IsSpace's doc",
				env, got, DefaultMaxComponentNodes, distinctFromDefault,
			)
		}
		// Trailing too: str.strip() removes from both ends, so a port using it
		// would be wrong at either.
		env = "200" + separator
		t.Setenv(MaxComponentNodesEnvVar, env)
		if got := ResolveMaxComponentNodes(nil); got != DefaultMaxComponentNodes {
			t.Errorf("env %q resolved to %d, want the default (%d)",
				env, got, DefaultMaxComponentNodes)
		}
	}

	// Ordinary whitespace IS accepted by int(), and must stay accepted -- again
	// with a value distinguishable from the fallback.
	t.Setenv(MaxComponentNodesEnvVar, " 200 ")
	if got := ResolveMaxComponentNodes(nil); got != distinctFromDefault {
		t.Errorf("ordinary whitespace must still be stripped for the numeric "+
			"parser, got %d want %d", got, distinctFromDefault)
	}
}
