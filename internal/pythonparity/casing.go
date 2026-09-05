package pythonparity

import (
	"sync"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

// Lower is CPython's `str.lower()`, and Upper is `str.upper()`.
//
// # Why not strings.ToLower / strings.ToUpper
//
// Go's stdlib applies SIMPLE, per-rune case mapping. CPython applies FULL
// case mapping, which differs in two ways that both change the answer:
//
//   - Multi-rune expansions. `'İ'.lower()` is "i" + U+0307 COMBINING DOT
//     ABOVE (two runes); `'ß'.upper()` is "SS" (two runes). strings.ToLower /
//     strings.ToUpper cannot lengthen a rune and leave both unchanged.
//
//   - CONTEXT-SENSITIVE final sigma, which is a single rune whose mapping
//     depends on its POSITION in the word:
//
//     'ΟΔΟΣ'  python .lower() -> 'οδος'    strings.ToLower -> 'οδοσ'
//
// `language.Und` is correct BY DESIGN, not by observed agreement: CPython's
// str.lower()/str.upper() are locale-independent by definition and never
// apply the Turkish or Lithuanian tailorings, so the untailored caser is the
// only admissible choice. Saying "it matched my test cases" would invite
// someone to swap in a tailored caser later.
//
// # Why a pool rather than a package-level Caser
//
// x/text documents "A Caser may be stateful and should therefore not be
// shared between goroutines" (cases.go:35-36) -- an exemption is granted to
// cases.Fold ALONE (:87), never to Lower/Upper. That is not merely defensive
// wording: Caser.String calls transform.String, whose first act is
// t.Reset(), mutating the transformer. Two goroutines sharing one Caser race
// on that state.
//
// A package-level shared Caser can therefore look correct indefinitely --
// a 64-goroutine race-detector probe over final-sigma inputs did NOT flag it
// -- and still be wrong; absence of a detected race is not proof of safety
// when the library states the contract outright. So this uses a sync.Pool:
// safe by construction under concurrency, and unlike per-call construction
// it stays cheap when called in a loop over rows, which is exactly how
// ai_impact's _safe_bucket and _is_test_path use it.
//
// Promoted here (CHAOS-4280) from two unexported copies that had drifted in
// exactly this respect -- internal/jobs/workgraph/edges/canonical.go shared a
// package-level Caser, internal/jobs/workgraph/units/telemetrylabels.go
// constructed one per call. Both now call this; the copies are deleted.
//
// # A MEASURED, BOUNDED DIVERGENCE -- READ THIS BEFORE RELYING ON Lower
//
// x/text is NOT a complete substitute for str.lower(). Its Final_Sigma
// lookahead is BOUNDED where CPython's is not, and the boundary is exactly 31
// case-ignorable runes (measured by lane-pathb-go via a codex round, carried
// here verbatim from telemetrylabels.go's doc comment rather than rediscovered):
//
//	("AΣ" + "." * n + "B").lower()      n <= 30      n >= 31
//	  CPython 3.14.7                          sigma        sigma      (medial)
//	  x/text cases.Lower(Und)                 sigma        FINAL      <- differs
//
// Final_Sigma says a capital sigma is final only when it is NOT followed by a
// cased letter, skipping case-ignorable characters in between. CPython sees the
// following "B" at any distance; x/text's scan gives up past 31 and concludes
// "no following cased letter".
//
// TestLowerAndUpperMatchLivePythonOnEveryMultiRuneMapping CANNOT see this: it
// enumerates single code points, and this divergence needs a 33+ rune input.
// TestFinalSigmaLookaheadBoundaryIsWhereWeMeasuredIt pins it directly.
//
// This is a SHARED helper, so the containment argument that made the divergence
// acceptable inside telemetrylabels (every allow-list entry and prefix there is
// ASCII, so both sigma spellings land in the same bucket) does NOT
// automatically transfer to a new caller. Before using Lower on a value where a
// medial-vs-final sigma could change the ANSWER -- an equality test against a
// non-ASCII literal, a persisted key, a hash input -- either prove the same
// containment for your call site or implement Final_Sigma directly. Callers
// whose comparands are all ASCII are contained by construction.
func Lower(value string) string {
	if value == "" {
		return ""
	}
	caser := lowerPool.Get().(*cases.Caser)
	defer lowerPool.Put(caser)
	return caser.String(value)
}

// Upper is CPython's `str.upper()`. See Lower's doc comment -- same full-case
// mapping, same locale-independence, same pooling rationale.
func Upper(value string) string {
	if value == "" {
		return ""
	}
	caser := upperPool.Get().(*cases.Caser)
	defer upperPool.Put(caser)
	return caser.String(value)
}

var lowerPool = sync.Pool{New: func() any {
	caser := cases.Lower(language.Und)
	return &caser
}}

var upperPool = sync.Pool{New: func() any {
	caser := cases.Upper(language.Und)
	return &caser
}}

// Fold is a CASE-INSENSITIVE COMPARISON KEY, not str.lower() -- do not use it
// anywhere Python's exact lowered BYTES are the contract (a persisted column,
// a hash input, anything compared against a value Python itself lowered and
// stored). It exists for the one case Lower's doc comment calls out as
// needing either a containment proof or a direct Final_Sigma
// implementation: a call site that lowercases a value ONLY to build an
// equality/prefix-match key, never to reproduce or persist Python's output.
//
// Case FOLDING (Unicode's CaseFolding.txt, what cases.Fold implements) has no
// position-dependent branching at all -- unlike lowering's Final_Sigma rule,
// every spelling of sigma (Σ, σ, ς) folds to the SAME value regardless of
// what follows. So Fold cannot exhibit Lower's measured 31-case-ignorable-
// rune Final_Sigma boundary (casing.go's own doc comment): there is no
// lookahead to bound in the first place.
//
// This trades one narrow divergence for a narrower one, not zero: Python's
// `.lower()` leaves an ALREADY-lowercase final sigma "ς" and an already-
// lowercase medial sigma "σ" as two DISTINCT strings (lowering a
// already-lowercase character is a no-op), so a Python comparison can tell
// them apart. Fold merges both to one value, so two inputs Python would
// treat as different could compare EQUAL here. Reaching this requires a
// LITERAL lowercase final-sigma character already present in the input
// (not one Python derived by lowering an uppercase Σ) -- narrower than the
// 31-rune bug it replaces, and worth stating rather than declaring zero risk.
//
// CHAOS-4280 (codex round chaos-4280-r1, finding 6): promoted here instead of
// living only in aiimpact/repoteams.go, so a second caller with the same
// "key-only, never persisted" shape does not reinvent this pool.
func Fold(value string) string {
	if value == "" {
		return ""
	}
	caser := foldPool.Get().(*cases.Caser)
	defer foldPool.Put(caser)
	return caser.String(value)
}

var foldPool = sync.Pool{New: func() any {
	caser := cases.Fold()
	return &caser
}}
