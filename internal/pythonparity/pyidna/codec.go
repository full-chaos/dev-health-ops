package pyidna

import (
	"errors"
	"sort"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity/pyunicodedata"
)

// ErrCodec is the UnicodeError str.encode("idna") raises.
var ErrCodec = errors.New("idna: invalid host")

func inRuneRanges(table []runeRange, r rune) bool {
	index := sort.Search(len(table), func(i int) bool { return table[i].hi >= r })
	return index < len(table) && table[index].lo <= r
}

// nameprep is encodings.idna.nameprep (RFC 3491): the B.1 and B.2 mapping,
// NFKC, the prohibited tables, and the bidirectional rules.
func nameprep(label string) (string, error) {
	var mapped strings.Builder
	for _, r := range label {
		if inRuneRanges(nameprepB1, r) {
			continue
		}
		if replacement, ok := nameprepB2[r]; ok {
			mapped.WriteString(replacement)
			continue
		}
		mapped.WriteRune(r)
	}
	runes := pyunicodedata.NFKC32([]rune(mapped.String()))
	normalized := string(runes)
	for _, r := range runes {
		if inRuneRanges(nameprepProhibited, r) {
			return "", ErrCodec
		}
	}
	randAL := make([]bool, len(runes))
	anyRandAL := false
	for index, r := range runes {
		randAL[index] = inRuneRanges(nameprepD1, r)
		anyRandAL = anyRandAL || randAL[index]
	}
	if anyRandAL {
		for _, r := range runes {
			if inRuneRanges(nameprepD2, r) {
				return "", ErrCodec
			}
		}
		if !randAL[0] || !randAL[len(runes)-1] {
			return "", ErrCodec
		}
	}
	return normalized, nil
}

// idnaDots is encodings.idna.dots: the four label separators.
var idnaDots = strings.NewReplacer("。", ".", "．", ".", "｡", ".")

// CodecEncode is str.encode("idna") on CPython 3.14: an ASCII name passes
// through when no label is empty (the last excepted) and none is 64
// characters or more; any other name is split on the four IDNA dots and each
// label goes through ToASCII (ASCII fast path, nameprep, the "xn--" prefix
// check, punycode, a length under 64).
func CodecEncode(host string) (string, error) {
	if host == "" {
		return "", nil
	}
	if isASCIIText(host) {
		labels := strings.Split(host, ".")
		for _, label := range labels[:len(labels)-1] {
			if label == "" {
				return "", ErrCodec
			}
		}
		for _, label := range labels {
			if len(label) >= 64 {
				return "", ErrCodec
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
	if isASCIIText(label) {
		if len(label) > 0 && len(label) < 64 {
			return label, nil
		}
		return "", ErrCodec
	}
	prepared, err := nameprep(label)
	if err != nil {
		return "", err
	}
	if isASCIIText(prepared) {
		if len(prepared) > 0 && len(prepared) < 64 {
			return prepared, nil
		}
		return "", ErrCodec
	}
	if strings.HasPrefix(strings.ToLower(prepared), "xn--") {
		return "", ErrCodec
	}
	encoded := "xn--" + string(PunycodeEncode([]rune(prepared)))
	if len(encoded) >= 64 {
		return "", ErrCodec
	}
	return encoded, nil
}

func isASCIIText(text string) bool {
	for index := 0; index < len(text); index++ {
		if text[index] >= 0x80 {
			return false
		}
	}
	return true
}
