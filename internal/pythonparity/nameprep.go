package pythonparity

import (
	"errors"
	"sort"
	"strings"

	"golang.org/x/net/idna"
	"golang.org/x/text/unicode/norm"
)

// ErrIDNA is the UnicodeError str.encode("idna") raises.
var ErrIDNA = errors.New("idna: invalid host")

func inRanges(table []runeRange, r rune) bool {
	index := sort.Search(len(table), func(i int) bool { return table[i].hi >= r })
	return index < len(table) && table[index].lo <= r
}

// nameprep is encodings.idna.nameprep (RFC 3491): the B.1 and B.2 mapping,
// NFKC, the prohibited tables, and the bidirectional rules.
func nameprep(label string) (string, error) {
	var mapped strings.Builder
	for _, r := range label {
		if inRanges(nameprepB1, r) {
			continue
		}
		if replacement, ok := nameprepB2[r]; ok {
			mapped.WriteString(replacement)
			continue
		}
		mapped.WriteRune(r)
	}
	normalized := nfkc32(mapped.String())
	runes := []rune(normalized)
	for _, r := range runes {
		if inRanges(nameprepProhibited, r) {
			return "", ErrIDNA
		}
	}
	randAL := make([]bool, len(runes))
	anyRandAL := false
	for index, r := range runes {
		randAL[index] = inRanges(nameprepD1, r)
		anyRandAL = anyRandAL || randAL[index]
	}
	if anyRandAL {
		for _, r := range runes {
			if inRanges(nameprepD2, r) {
				return "", ErrIDNA
			}
		}
		if !randAL[0] || !randAL[len(runes)-1] {
			return "", ErrIDNA
		}
	}
	return normalized, nil
}

// idnaDots is encodings.idna.dots: the four label separators.
var idnaDots = strings.NewReplacer("。", ".", "．", ".", "｡", ".")

// IDNAEncode is str.encode("idna") on CPython 3.14: an ASCII name passes
// through when no label is empty (the last excepted) and none is 64
// characters or more; any other name is split on the four IDNA dots and each
// label goes through ToASCII (ASCII fast path, nameprep, the "xn--" prefix
// check, punycode, a length under 64).
func IDNAEncode(host string) (string, error) {
	if host == "" {
		return "", nil
	}
	if isURLASCII(host) {
		labels := strings.Split(host, ".")
		for _, label := range labels[:len(labels)-1] {
			if label == "" {
				return "", ErrIDNA
			}
		}
		for _, label := range labels {
			if len(label) >= 64 {
				return "", ErrIDNA
			}
		}
		return host, nil
	}
	labels := strings.Split(idnaDots.Replace(host), ".")
	trailingDot := ""
	if labels[len(labels)-1] == "" {
		trailingDot = "."
		labels = labels[:len(labels)-1]
	}
	out := make([]string, 0, len(labels))
	for _, label := range labels {
		encoded, err := idnaToASCII(label)
		if err != nil {
			return "", err
		}
		out = append(out, encoded)
	}
	return strings.Join(out, ".") + trailingDot, nil
}

// idnaToASCII is encodings.idna.ToASCII for one label.
func idnaToASCII(label string) (string, error) {
	if isURLASCII(label) {
		if len(label) > 0 && len(label) < 64 {
			return label, nil
		}
		return "", ErrIDNA
	}
	prepared, err := nameprep(label)
	if err != nil {
		return "", err
	}
	if isURLASCII(prepared) {
		if len(prepared) > 0 && len(prepared) < 64 {
			return prepared, nil
		}
		return "", ErrIDNA
	}
	if strings.HasPrefix(strings.ToLower(prepared), "xn--") {
		return "", ErrIDNA
	}
	encoded, err := idna.Punycode.ToASCII(prepared)
	if err != nil || len(encoded) >= 64 {
		return "", ErrIDNA
	}
	return encoded, nil
}

// nfkc32 is unicodedata.ucd_3_2_0.normalize("NFKC", text). Go's tables are a
// later Unicode version; a code point Unicode 3.2 leaves unassigned is a
// starter that neither decomposes nor composes there, so the text is
// normalized in segments split at those code points.
func nfkc32(text string) string {
	var out, segment strings.Builder
	flush := func() {
		if segment.Len() > 0 {
			out.WriteString(norm.NFKC.String(segment.String()))
			segment.Reset()
		}
	}
	for _, r := range text {
		if replacement, ok := nfkc32Corrected[r]; ok {
			flush()
			out.WriteRune(replacement)
			continue
		}
		if inRanges(nameprepUnassigned32, r) {
			flush()
			out.WriteRune(r)
			continue
		}
		segment.WriteRune(r)
	}
	flush()
	return out.String()
}

// nfkc32Corrected are the five CJK compatibility ideographs whose
// decomposition Unicode changed after 3.2 (checked over every code point
// against unicodedata.ucd_3_2_0).
var nfkc32Corrected = map[rune]rune{0x2F868: 0x2136A, 0x2F874: 0x5F33, 0x2F91F: 0x43AB, 0x2F95F: 0x7AAE, 0x2F9BF: 0x4D57}
