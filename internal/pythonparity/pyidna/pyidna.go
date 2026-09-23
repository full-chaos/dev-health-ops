// Package pyidna is the Python idna package (idna/core.py, the version
// pinned in uv.lock) for the calls email-validator makes: uts46_remap,
// alabel, encode and decode. A Python IDNAError is an *Error whose Message
// is str(exc), so a caller can put it into a user-facing message exactly
// as email-validator does.
//
// The idna data (UTS #46 mapping, IDNA 2008 code point classes, scripts,
// joining types) is frozen in tables.go from the pinned package, and the
// unicodedata questions come from pyunicodedata (the interpreter's own
// Unicode edition), so the two sources idna consults are both pinned.
// TestIDNATablesMatchLivePython and TestBehaviourMatchesLivePython keep them
// honest.
//
// Strings are code points ([]rune), since a Python str may hold a lone
// surrogate.
package pyidna

import (
	"fmt"
	"sort"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity/pyunicodedata"
)

// Kind is the IDNAError subclass Python raises.
type Kind int

const (
	// KindIDNA is IDNAError itself.
	KindIDNA Kind = iota
	// KindBidi is IDNABidiError.
	KindBidi
	// KindInvalidCodepoint is InvalidCodepoint.
	KindInvalidCodepoint
	// KindInvalidCodepointContext is InvalidCodepointContext.
	KindInvalidCodepointContext
)

// Error is a Python IDNAError; Message is str(exc).
type Error struct {
	Kind    Kind
	Message string
}

func (e *Error) Error() string { return e.Message }

func idnaError(kind Kind, format string, args ...any) *Error {
	return &Error{Kind: kind, Message: fmt.Sprintf(format, args...)}
}

const (
	viramaCombiningClass = 9
	maxInputLength       = 1024
)

var alabelPrefix = []byte("xn--")

func repr(text []rune) string { return pythonparity.StrReprRunes(text) }

func unot(r rune) string { return fmt.Sprintf("U+%04X", r) }

func isUnicodeDot(r rune) bool {
	return r == 0x2e || r == 0x3002 || r == 0xff0e || r == 0xff61
}

func joiningType(r rune) string {
	for _, entry := range joiningTypes {
		if inRanges(entry.ranges, r) {
			return entry.name
		}
	}
	return ""
}

// combiningClass is _combining_class: ok is false where Python raises
// ValueError("Unknown character in unicodedata").
func combiningClass(r rune) (int, bool) {
	value := pyunicodedata.Combining(r)
	if value == 0 && !pyunicodedata.HasName(r) {
		return 0, false
	}
	return value, true
}

func isScript(r rune, script string) bool {
	return inRanges(scripts[script], r)
}

func inRanges(ranges [][2]rune, r rune) bool {
	index := sort.Search(len(ranges), func(i int) bool { return ranges[i][1] > r })
	return index < len(ranges) && ranges[index][0] <= r
}

// checkBidi is check_bidi(label) with check_ltr=False.
func checkBidi(label []rune) *Error {
	if len(label) > maxInputLength {
		return idnaError(KindIDNA, "Label too long")
	}
	bidiLabel := false
	for index, r := range label {
		direction := pyunicodedata.Bidirectional(r)
		if direction == "" {
			return idnaError(KindBidi, "Unknown directionality in label %s at position %d", repr(label), index+1)
		}
		if direction == "R" || direction == "AL" || direction == "AN" {
			bidiLabel = true
		}
	}
	if !bidiLabel {
		return nil
	}
	var rtl bool
	switch direction := pyunicodedata.Bidirectional(label[0]); direction {
	case "R", "AL":
		rtl = true
	case "L":
		rtl = false
	default:
		return idnaError(KindBidi, "First codepoint in label %s must be directionality L, R or AL", repr(label))
	}
	validEnding := false
	numberType := ""
	for index, r := range label {
		direction := pyunicodedata.Bidirectional(r)
		if rtl {
			switch direction {
			case "R", "AL", "AN", "EN", "ES", "CS", "ET", "ON", "BN", "NSM":
			default:
				return idnaError(KindBidi, "Invalid direction for codepoint at position %d in a right-to-left label", index+1)
			}
			switch direction {
			case "R", "AL", "EN", "AN":
				validEnding = true
			case "NSM":
			default:
				validEnding = false
			}
			if direction == "AN" || direction == "EN" {
				if numberType == "" {
					numberType = direction
				} else if numberType != direction {
					return idnaError(KindBidi, "Can not mix numeral types in a right-to-left label")
				}
			}
		} else {
			switch direction {
			case "L", "EN", "ES", "CS", "ET", "ON", "BN", "NSM":
			default:
				return idnaError(KindBidi, "Invalid direction for codepoint at position %d in a left-to-right label", index+1)
			}
			switch direction {
			case "L", "EN":
				validEnding = true
			case "NSM":
			default:
				validEnding = false
			}
		}
	}
	if !validEnding {
		return idnaError(KindBidi, "Label ends with illegal codepoint directionality")
	}
	return nil
}

func equalRunes(a, b []rune) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// validContextJ is valid_contextj: ok is false where Python raises
// ValueError (an adjacent code point unknown to unicodedata).
func validContextJ(label []rune, pos int) (valid bool, ok bool) {
	switch label[pos] {
	case 0x200c:
		if pos > 0 {
			class, known := combiningClass(label[pos-1])
			if !known {
				return false, false
			}
			if class == viramaCombiningClass {
				return true, true
			}
		}
		found := false
		for i := pos - 1; i >= 0; i-- {
			jt := joiningType(label[i])
			if jt == "T" {
				continue
			}
			if jt == "L" || jt == "D" {
				found = true
			}
			break
		}
		if !found {
			return false, true
		}
		for i := pos + 1; i < len(label); i++ {
			jt := joiningType(label[i])
			if jt == "T" {
				continue
			}
			return jt == "R" || jt == "D", true
		}
		return false, true
	case 0x200d:
		if pos == 0 {
			return false, true
		}
		class, known := combiningClass(label[pos-1])
		if !known {
			return false, false
		}
		return class == viramaCombiningClass, true
	}
	return false, true
}

func validContextO(label []rune, pos int) bool {
	r := label[pos]
	switch {
	case r == 0x00b7:
		return pos > 0 && pos < len(label)-1 && label[pos-1] == 0x6c && label[pos+1] == 0x6c
	case r == 0x0375:
		if pos < len(label)-1 && len(label) > 1 {
			return isScript(label[pos+1], "Greek")
		}
		return false
	case r == 0x05f3 || r == 0x05f4:
		if pos > 0 {
			return isScript(label[pos-1], "Hebrew")
		}
		return false
	case r == 0x30fb:
		for _, c := range label {
			if c == 0x30fb {
				continue
			}
			if isScript(c, "Hiragana") || isScript(c, "Katakana") || isScript(c, "Han") {
				return true
			}
		}
		return false
	case r >= 0x660 && r <= 0x669:
		for _, c := range label {
			if c >= 0x6f0 && c <= 0x6f9 {
				return false
			}
		}
		return true
	case r >= 0x6f0 && r <= 0x6f9:
		for _, c := range label {
			if c >= 0x660 && c <= 0x669 {
				return false
			}
		}
		return true
	}
	return false
}

// checkLabel is check_label for a str label.
func checkLabel(label []rune) *Error {
	if len(label) > maxInputLength {
		return idnaError(KindIDNA, "Label too long")
	}
	if len(label) == 0 {
		return idnaError(KindIDNA, "Empty Label")
	}
	if len(label) > 254 {
		return idnaError(KindIDNA, "Label too long")
	}
	if !equalRunes(pyunicodedata.NFC(label), label) {
		return idnaError(KindIDNA, "Label must be in Normalization Form C")
	}
	// check_hyphen_ok
	if len(label) >= 4 && label[2] == '-' && label[3] == '-' {
		return idnaError(KindIDNA, "Label has disallowed hyphens in 3rd and 4th position")
	}
	if label[0] == '-' || label[len(label)-1] == '-' {
		return idnaError(KindIDNA, "Label must not start or end with a hyphen")
	}
	// check_initial_combiner
	if pyunicodedata.Category(label[0])[0] == 'M' {
		return idnaError(KindIDNA, "Label begins with an illegal combining character")
	}
	for pos, r := range label {
		switch {
		case inRanges(codepointClasses["PVALID"], r):
			continue
		case inRanges(codepointClasses["CONTEXTJ"], r):
			// check_label raises InvalidCodepointContext for an invalid
			// joiner INSIDE a "try: ... except ValueError", and IDNAError
			// is a ValueError (through UnicodeError): the handler catches
			// that raise too, so every refused joiner surfaces as this
			// message, the same as an unknown neighbouring code point.
			if valid, ok := validContextJ(label, pos); !ok || !valid {
				return idnaError(KindIDNA, "Unknown codepoint adjacent to joiner %s at position %d in %s", unot(r), pos+1, repr(label))
			}
		case inRanges(codepointClasses["CONTEXTO"], r):
			if !validContextO(label, pos) {
				return idnaError(KindInvalidCodepointContext, "Codepoint %s not allowed at position %d in %s", unot(r), pos+1, repr(label))
			}
		default:
			return idnaError(KindInvalidCodepoint, "Codepoint %s at position %d of %s not allowed", unot(r), pos+1, repr(label))
		}
	}
	return checkBidi(label)
}

func isASCII(text []rune) bool {
	for _, r := range text {
		if r >= 0x80 {
			return false
		}
	}
	return true
}

func runesToASCII(text []rune) []byte {
	out := make([]byte, len(text))
	for i, r := range text {
		out[i] = byte(r)
	}
	return out
}

func asciiToRunes(text []byte) []rune {
	out := make([]rune, len(text))
	for i, b := range text {
		out[i] = rune(b)
	}
	return out
}

// Alabel is alabel(label): the A-label as ASCII bytes.
func Alabel(label []rune) ([]byte, *Error) {
	if len(label) > maxInputLength {
		return nil, idnaError(KindIDNA, "Label too long")
	}
	if isASCII(label) {
		labelBytes := runesToASCII(label)
		if _, err := ulabelBytes(labelBytes); err != nil {
			return nil, err
		}
		if len(labelBytes) > 63 {
			return nil, idnaError(KindIDNA, "Label too long")
		}
		return labelBytes, nil
	}
	if err := checkLabel(label); err != nil {
		return nil, err
	}
	out := append(append([]byte{}, alabelPrefix...), punycodeEncode(label)...)
	if len(out) > 63 {
		return nil, idnaError(KindIDNA, "Label too long")
	}
	return out, nil
}

// Ulabel is ulabel(label) for a str label.
func Ulabel(label []rune) ([]rune, *Error) {
	if len(label) > maxInputLength {
		return nil, idnaError(KindIDNA, "Label too long")
	}
	if !isASCII(label) {
		if err := checkLabel(label); err != nil {
			return nil, err
		}
		return label, nil
	}
	return ulabelBytes(runesToASCII(label))
}

// ulabelBytes is ulabel() from the point where the label is ASCII bytes.
func ulabelBytes(labelBytes []byte) ([]rune, *Error) {
	if len(labelBytes) > maxInputLength {
		return nil, idnaError(KindIDNA, "Label too long")
	}
	lower := make([]byte, len(labelBytes))
	for i, b := range labelBytes {
		if b >= 'A' && b <= 'Z' {
			b += 'a' - 'A'
		}
		lower[i] = b
	}
	if len(lower) >= len(alabelPrefix) && string(lower[:len(alabelPrefix)]) == string(alabelPrefix) {
		rest := lower[len(alabelPrefix):]
		if len(rest) == 0 {
			return nil, idnaError(KindIDNA, "Malformed A-label, no Punycode eligible content found")
		}
		if rest[len(rest)-1] == '-' {
			return nil, idnaError(KindIDNA, "A-label must not end with a hyphen")
		}
		decoded, ok := punycodeDecode(rest)
		if !ok {
			return nil, idnaError(KindIDNA, "Invalid A-label")
		}
		if err := checkLabel(decoded); err != nil {
			return nil, err
		}
		return decoded, nil
	}
	label := asciiToRunes(lower)
	if err := checkLabel(label); err != nil {
		return nil, err
	}
	return label, nil
}

// UTS46Remap is uts46_remap(domain, std3_rules, transitional).
func UTS46Remap(domain []rune, std3Rules, transitional bool) ([]rune, *Error) {
	if len(domain) > maxInputLength {
		return nil, idnaError(KindIDNA, "Domain too long")
	}
	var out []rune
	for pos, r := range domain {
		index := sort.Search(len(uts46Starts), func(i int) bool { return uts46Starts[i] > r }) - 1
		status := uts46Statuses[index]
		replacement, hasReplacement := uts46Replacements[index], uts46HasReplacement[index]
		keep := status == 'V' || (status == 'D' && !transitional) || (status == '3' && !std3Rules && !hasReplacement)
		replace := hasReplacement && (status == 'M' || (status == '3' && !std3Rules) || (status == 'D' && transitional))
		switch {
		case keep:
			out = append(out, r)
		case replace:
			out = append(out, []rune(replacement)...)
		case status == 'I':
		default:
			return nil, idnaError(KindInvalidCodepoint, "Codepoint %s not allowed at position %d in %s", unot(r), pos+1, repr(domain))
		}
	}
	return pyunicodedata.NFC(out), nil
}

func splitLabels(text []rune) [][]rune {
	var labels [][]rune
	start := 0
	for i, r := range text {
		if isUnicodeDot(r) {
			labels = append(labels, text[start:i])
			start = i + 1
		}
	}
	return append(labels, text[start:])
}

// Encode is encode(s) with its defaults (strict=False, uts46=False).
func Encode(text []rune) ([]byte, *Error) {
	if len(text) > maxInputLength {
		return nil, idnaError(KindIDNA, "Domain too long")
	}
	if len(text) > 254 {
		return nil, idnaError(KindIDNA, "Domain too long")
	}
	labels := splitLabels(text)
	if len(labels) == 1 && len(labels[0]) == 0 {
		return nil, idnaError(KindIDNA, "Empty domain")
	}
	trailingDot := false
	if len(labels[len(labels)-1]) == 0 {
		labels = labels[:len(labels)-1]
		trailingDot = true
	}
	var result [][]byte
	for _, label := range labels {
		encoded, err := Alabel(label)
		if err != nil {
			return nil, err
		}
		if len(encoded) == 0 {
			return nil, idnaError(KindIDNA, "Empty label")
		}
		result = append(result, encoded)
	}
	if trailingDot {
		result = append(result, nil)
	}
	var out []byte
	for i, part := range result {
		if i > 0 {
			out = append(out, '.')
		}
		out = append(out, part...)
	}
	limit := 253
	if trailingDot {
		limit = 254
	}
	if len(out) > limit {
		return nil, idnaError(KindIDNA, "Domain too long")
	}
	return out, nil
}

// Decode is decode(s) with its defaults, for an ASCII str (the only input
// email-validator hands it).
func Decode(text []rune) ([]rune, *Error) {
	if !isASCII(text) {
		return nil, idnaError(KindIDNA, "Invalid ASCII in A-label")
	}
	if len(text) > maxInputLength {
		return nil, idnaError(KindIDNA, "Domain too long")
	}
	if len(text) > 254 {
		return nil, idnaError(KindIDNA, "Domain too long")
	}
	labels := splitLabels(text)
	if len(labels) == 1 && len(labels[0]) == 0 {
		return nil, idnaError(KindIDNA, "Empty domain")
	}
	trailingDot := false
	if len(labels[len(labels)-1]) == 0 {
		labels = labels[:len(labels)-1]
		trailingDot = true
	}
	var out []rune
	for i, label := range labels {
		decoded, err := Ulabel(label)
		if err != nil {
			return nil, err
		}
		if len(decoded) == 0 {
			return nil, idnaError(KindIDNA, "Empty label")
		}
		if i > 0 {
			out = append(out, '.')
		}
		out = append(out, decoded...)
	}
	if trailingDot {
		out = append(out, '.')
	}
	return out, nil
}
