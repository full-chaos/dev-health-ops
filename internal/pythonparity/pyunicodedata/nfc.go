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
func NFC(text []rune) []rune { return normalize(text, current, false) }

// NFKC is unicodedata.normalize("NFKC", text) (Unicode 16.0.0): compatibility
// decomposition, canonical ordering, canonical composition.
func NFKC(text []rune) []rune { return normalize(text, current, true) }

// NFKC32 is unicodedata.ucd_3_2_0.normalize("NFKC", text), the form
// encodings.idna's nameprep uses: decomposition follows the Unicode 3.2.0
// data (a code point assigned later is not decomposed), ordering and
// composition follow the current data.
func NFKC32(text []rune) []rune { return normalize(text, unicode32, true) }

// normData is one Unicode edition's normalization data.
type normData struct {
	canonical  []decomposition
	compat     []decomposition
	composites [][3]rune
	combining  func(rune) int
	// inert reports a code point that does not decompose whatever the
	// tables say (nil: none); overrides maps a code point to its full
	// decomposition (nil: none).
	inert     func(rune) bool
	overrides []decomposition

	once         sync.Once
	compositions map[[2]rune]rune
}

var current = &normData{
	canonical:  canonicalDecompositions[:],
	compat:     compatDecompositions[:],
	composites: primaryComposites[:],
	combining:  Combining,
}

// unicode32 differs from current in the decompositions only: CPython's
// ucd_3_2_0 consults the 3.2 data when it decomposes (a code point assigned
// later does not decompose; five ideographs keep an older mapping) and the
// current data for combining classes and composition.
var unicode32 = &normData{
	canonical:  canonicalDecompositions[:],
	compat:     compatDecompositions[:],
	composites: primaryComposites[:],
	combining:  Combining,
	inert:      func(r rune) bool { return inRanges(unassigned32Ranges[:], r) },
	overrides:  decompositionOverrides32[:],
}

func normalize(text []rune, data *normData, compat bool) []rune {
	decomposed := make([]rune, 0, len(text))
	for _, r := range text {
		decomposed = data.appendDecomposition(decomposed, r, compat)
	}
	data.canonicalOrder(decomposed)
	return data.compose(decomposed)
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

// appendDecomposition appends the full decomposition of r: canonical, and
// with compat also the compatibility mappings.
func (d *normData) appendDecomposition(out []rune, r rune, compat bool) []rune {
	if index := r - hangulSBase; index >= 0 && index < hangulSCount {
		out = append(out, hangulLBase+index/hangulNCount, hangulVBase+(index%hangulNCount)/hangulTCount)
		if t := index % hangulTCount; t != 0 {
			out = append(out, hangulTBase+t)
		}
		return out
	}
	if d.inert != nil && d.inert(r) {
		return append(out, r)
	}
	if mapping, ok := lookupDecomposition(d.overrides, r); ok {
		return append(out, mapping...)
	}
	mapping, ok := lookupDecomposition(d.canonical, r)
	if !ok && compat {
		mapping, ok = lookupDecomposition(d.compat, r)
	}
	if !ok {
		return append(out, r)
	}
	for _, part := range mapping {
		out = d.appendDecomposition(out, part, compat)
	}
	return out
}

func lookupDecomposition(table []decomposition, r rune) ([]rune, bool) {
	index := sort.Search(len(table), func(i int) bool { return table[i].r >= r })
	if index < len(table) && table[index].r == r {
		return table[index].mapping, true
	}
	return nil, false
}

// canonicalOrder sorts every run of non-starters by combining class,
// stably.
func (d *normData) canonicalOrder(text []rune) {
	for i := 0; i < len(text); {
		if d.combining(text[i]) == 0 {
			i++
			continue
		}
		j := i
		for j < len(text) && d.combining(text[j]) != 0 {
			j++
		}
		segment := text[i:j]
		sort.SliceStable(segment, func(a, b int) bool { return d.combining(segment[a]) < d.combining(segment[b]) })
		i = j
	}
}

// compose is canonical composition over decomposed, ordered text: a
// character joins the last starter when a primary composite exists and
// nothing kept between them blocks it (a starter, or a mark whose class is
// not lower).
func (d *normData) compose(text []rune) []rune {
	out := make([]rune, 0, len(text))
	starter := -1
	// lastClass is the class of the last character kept after the starter;
	// -1 means the starter is the last character kept.
	lastClass := -1
	for _, r := range text {
		class := d.combining(r)
		// lastClass is -1 right after the starter (nothing between them,
		// never blocked: -1 is below every class) or the class, at least 1,
		// of the last mark kept after it (blocking a starter and any mark of
		// the same or a lower class).
		if starter >= 0 && lastClass < class {
			if composite, ok := d.composePair(out[starter], r); ok {
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

func (d *normData) composePair(first, second rune) (rune, bool) {
	if first >= hangulLBase && first < hangulLBase+hangulLCount && second >= hangulVBase && second < hangulVBase+hangulVCount {
		return hangulSBase + ((first-hangulLBase)*hangulVCount+(second-hangulVBase))*hangulTCount, true
	}
	if index := first - hangulSBase; index >= 0 && index < hangulSCount && index%hangulTCount == 0 &&
		second > hangulTBase && second < hangulTBase+hangulTCount {
		return first + (second - hangulTBase), true
	}
	d.once.Do(func() {
		d.compositions = make(map[[2]rune]rune, len(d.composites))
		for _, entry := range d.composites {
			d.compositions[[2]rune{entry[0], entry[1]}] = entry[2]
		}
	})
	composite, ok := d.compositions[[2]rune{first, second}]
	return composite, ok
}

type decomposition struct {
	r       rune
	mapping []rune
}
