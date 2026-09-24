package pyunicodedata

import (
	"sort"
	"sync"
)

// NFC is unicodedata.normalize("NFC", text) for a Python str held as code
// points (a lone surrogate stays as it is, as in Python).
//
// It is the Unicode algorithm (UAX #15: canonical decomposition, canonical
// ordering, canonical composition) over the interpreter's own data, frozen
// in tables.go: the single-level canonical decompositions, the combining
// classes and the primary composites. golang.org/x/text's normalizer is
// not used: its tables follow a newer Unicode edition, it inserts U+034F
// into a run of more than 30 combining marks (the Stream-Safe format,
// which Python does not apply), and it composes a supplementary-plane base
// with a following mark as if it were the BMP code point sharing its low
// 16 bits (U+10057 U+0301 becomes U+1E82).
func NFC(text []rune) []rune {
	decomposed := make([]rune, 0, len(text))
	for _, r := range text {
		decomposed = appendDecomposition(decomposed, r)
	}
	canonicalOrder(decomposed)
	return compose(decomposed)
}

// Hangul syllable constants (Unicode §3.12).
const (
	hangulSBase  = 0xac00
	hangulLBase  = 0x1100
	hangulVBase  = 0x1161
	hangulTBase  = 0x11a7
	hangulLCount = 19
	hangulVCount = 21
	hangulTCount = 28
	hangulNCount = hangulVCount * hangulTCount
	hangulSCount = hangulLCount * hangulNCount
)

// appendDecomposition appends the full canonical decomposition of r.
func appendDecomposition(out []rune, r rune) []rune {
	if index := r - hangulSBase; index >= 0 && index < hangulSCount {
		out = append(out, hangulLBase+index/hangulNCount, hangulVBase+(index%hangulNCount)/hangulTCount)
		if t := index % hangulTCount; t != 0 {
			out = append(out, hangulTBase+t)
		}
		return out
	}
	mapping, ok := canonicalDecomposition(r)
	if !ok {
		return append(out, r)
	}
	for _, part := range mapping {
		out = appendDecomposition(out, part)
	}
	return out
}

func canonicalDecomposition(r rune) ([]rune, bool) {
	index := sort.Search(len(canonicalDecompositions), func(i int) bool { return canonicalDecompositions[i].r >= r })
	if index < len(canonicalDecompositions) && canonicalDecompositions[index].r == r {
		return canonicalDecompositions[index].mapping, true
	}
	return nil, false
}

// canonicalOrder sorts every run of non-starters by combining class,
// stably.
func canonicalOrder(text []rune) {
	for i := 0; i < len(text); {
		if Combining(text[i]) == 0 {
			i++
			continue
		}
		j := i
		for j < len(text) && Combining(text[j]) != 0 {
			j++
		}
		segment := text[i:j]
		sort.SliceStable(segment, func(a, b int) bool { return Combining(segment[a]) < Combining(segment[b]) })
		i = j
	}
}

// compose is canonical composition over decomposed, ordered text: a
// character joins the last starter when a primary composite exists and
// nothing kept between them blocks it (a starter, or a mark whose class is
// not lower).
func compose(text []rune) []rune {
	out := make([]rune, 0, len(text))
	starter := -1
	// lastClass is the class of the last character kept after the starter;
	// -1 means the starter is the last character kept.
	lastClass := -1
	for _, r := range text {
		class := Combining(r)
		// A starter (class 0) after a kept mark is blocked: lastClass is then
		// at least 1, never below 0.
		if starter >= 0 && (lastClass == -1 || lastClass < class) {
			if composite, ok := composePair(out[starter], r); ok {
				out[starter] = composite
				continue
			}
		}
		out = append(out, r)
		if class == 0 {
			starter = len(out) - 1
			lastClass = -1
		} else {
			lastClass = class
		}
	}
	return out
}

func composePair(first, second rune) (rune, bool) {
	if first >= hangulLBase && first < hangulLBase+hangulLCount && second >= hangulVBase && second < hangulVBase+hangulVCount {
		return hangulSBase + ((first-hangulLBase)*hangulVCount+(second-hangulVBase))*hangulTCount, true
	}
	if index := first - hangulSBase; index >= 0 && index < hangulSCount && index%hangulTCount == 0 &&
		second > hangulTBase && second < hangulTBase+hangulTCount {
		return first + (second - hangulTBase), true
	}
	composite, ok := compositionIndex()[[2]rune{first, second}]
	return composite, ok
}

var (
	compositionOnce sync.Once
	compositions    map[[2]rune]rune
)

func compositionIndex() map[[2]rune]rune {
	compositionOnce.Do(func() {
		compositions = make(map[[2]rune]rune, len(primaryComposites))
		for _, entry := range primaryComposites {
			compositions[[2]rune{entry[0], entry[1]}] = entry[2]
		}
	})
	return compositions
}

type decomposition struct {
	r       rune
	mapping []rune
}
