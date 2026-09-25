package pythonparity

import (
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

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
// # Final sigma: decided here, not by x/text
//
// x/text's own Final_Sigma lookahead is BOUNDED where CPython's is not: it
// gives up after 31 case-ignorable runes and concludes "no following cased
// letter" (measured by lane-pathb-go via a codex round):
//
//	("AΣ" + "." * n + "B").lower()      n <= 30      n >= 31
//	  CPython 3.14.7                          sigma        sigma      (medial)
//	  x/text cases.Lower(Und)                 sigma        FINAL
//
// So Lower does not let x/text map a capital sigma. It hands x/text only the
// sigma-free segments between them -- every other lowercase mapping is
// context-free, so a segment lowers exactly as it would inside the whole
// string -- and maps each U+03A3 itself with CPython's own rule
// (Objects/unicodeobject.c handle_capital_sigma): final (U+03C2) when a cased
// character precedes it and none follows, skipping case-ignorable characters
// in both directions at any distance; otherwise medial (U+03C3).
//
// Cased and Case_Ignorable come from Go's unicode tables, which may be a
// newer Unicode version than the running CPython's. A character assigned or
// re-categorised after CPython's version can be decided differently; the
// exhaustive oracle (TestSigmaPropertiesMatchLivePythonOnEveryCodePoint)
// names that set rather than hiding it.
func Lower(value string) string {
	if value == "" {
		return ""
	}
	caser := lowerPool.Get().(*cases.Caser)
	defer lowerPool.Put(caser)
	if !strings.ContainsRune(value, capitalSigma) {
		return caser.String(value)
	}
	var out strings.Builder
	out.Grow(len(value))
	start := 0
	for index, r := range value {
		if r != capitalSigma {
			continue
		}
		out.WriteString(caser.String(value[start:index]))
		if finalSigma(value, index) {
			out.WriteRune(finalSmallSigma)
		} else {
			out.WriteRune(smallSigma)
		}
		start = index + utf8.RuneLen(capitalSigma)
	}
	out.WriteString(caser.String(value[start:]))
	return out.String()
}

const (
	capitalSigma    = '\u03a3'
	smallSigma      = '\u03c3'
	finalSmallSigma = '\u03c2'
)

// finalSigma is CPython's handle_capital_sigma for the capital sigma at byte
// offset index: \p{cased} \p{case-ignorable}* U+03A3 !(\p{case-ignorable}*
// \p{cased}), with no bound on either scan.
func finalSigma(value string, index int) bool {
	before := value[:index]
	for before != "" {
		r, size := utf8.DecodeLastRuneInString(before)
		if caseIgnorable(r) {
			before = before[:len(before)-size]
			continue
		}
		if !cased(r) {
			return false
		}
		after := value[index+utf8.RuneLen(capitalSigma):]
		for after != "" {
			r, size := utf8.DecodeRuneInString(after)
			if caseIgnorable(r) {
				after = after[size:]
				continue
			}
			return !cased(r)
		}
		return true
	}
	return false
}

// cased is Unicode's Cased property: Lowercase (Ll + Other_Lowercase),
// Uppercase (Lu + Other_Uppercase) or Lt.
func cased(r rune) bool {
	return unicode.IsLower(r) || unicode.IsUpper(r) || unicode.IsTitle(r) ||
		unicode.Is(unicode.Other_Lowercase, r) || unicode.Is(unicode.Other_Uppercase, r)
}

// caseIgnorable is Unicode's Case_Ignorable property: Mn, Me, Cf, Lm, Sk, or
// Word_Break MidLetter, MidNumLet or Single_Quote (listed below; Go has no
// Word_Break tables).
func caseIgnorable(r rune) bool {
	if unicode.In(r, unicode.Mn, unicode.Me, unicode.Cf, unicode.Lm, unicode.Sk) {
		return true
	}
	switch r {
	case '\'', '.', ':', '\u00b7', '\u0387', '\u055f', '\u05f4', '\u2018', '\u2019',
		'\u2024', '\u2027', '\ufe13', '\ufe52', '\ufe55', '\uff07', '\uff0e', '\uff1a':
		return true
	}
	return false
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
