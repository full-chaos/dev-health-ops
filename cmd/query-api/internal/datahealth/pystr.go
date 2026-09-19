package datahealth

import (
	"encoding/json"
	"io"
	"math"
	"strconv"
	"strings"
	"unicode"
)

// The Python resolver builds strings with str(), int() and str.lower()/split()
// over decoded JSON and stored text. These helpers reproduce those results so
// the two planes agree on every value a stored row can hold.

// orderedMap is a decoded JSON object that keeps its key order, as a Python
// dict does.
type orderedMap struct {
	keys []string
	vals []any
}

// decodeOrdered decodes one JSON value, keeping object key order and numbers
// as json.Number.
func decodeOrdered(text string) (any, bool) {
	decoder := json.NewDecoder(strings.NewReader(text))
	decoder.UseNumber()
	value, err := readValue(decoder)
	if err != nil {
		return nil, false
	}
	// json.loads rejects anything after the first value.
	if _, err := decoder.Token(); err != io.EOF {
		return nil, false
	}
	return value, true
}

func readValue(decoder *json.Decoder) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delim, isDelim := token.(json.Delim)
	if !isDelim {
		return token, nil
	}
	switch delim {
	case '{':
		object := &orderedMap{}
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			key, _ := keyToken.(string)
			value, err := readValue(decoder)
			if err != nil {
				return nil, err
			}
			object.keys = append(object.keys, key)
			object.vals = append(object.vals, value)
		}
		if _, err := decoder.Token(); err != nil && err != io.EOF {
			return nil, err
		}
		return object, nil
	case '[':
		list := []any{}
		for decoder.More() {
			value, err := readValue(decoder)
			if err != nil {
				return nil, err
			}
			list = append(list, value)
		}
		if _, err := decoder.Token(); err != nil && err != io.EOF {
			return nil, err
		}
		return list, nil
	}
	return nil, io.ErrUnexpectedEOF
}

// pyRepr is repr(value) for a decoded JSON value.
func pyRepr(value any) string {
	switch v := value.(type) {
	case nil:
		return "None"
	case bool:
		if v {
			return "True"
		}
		return "False"
	case string:
		return pyStringRepr(v)
	case json.Number:
		return pyNumberRepr(v)
	case []any:
		parts := make([]string, 0, len(v))
		for _, item := range v {
			parts = append(parts, pyRepr(item))
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case *orderedMap:
		parts := make([]string, 0, len(v.keys))
		for i, key := range v.keys {
			parts = append(parts, pyStringRepr(key)+": "+pyRepr(v.vals[i]))
		}
		return "{" + strings.Join(parts, ", ") + "}"
	}
	return ""
}

// pyStr is str(value): a string is itself, everything else its repr.
func pyStr(value any) string {
	if s, ok := value.(string); ok {
		return s
	}
	return pyRepr(value)
}

func pyStringRepr(s string) string {
	quote := byte('\'')
	if strings.Contains(s, "'") && !strings.Contains(s, "\"") {
		quote = '"'
	}
	var b strings.Builder
	b.WriteByte(quote)
	for _, r := range s {
		switch {
		case r == rune(quote) || r == '\\':
			b.WriteByte('\\')
			b.WriteRune(r)
		case r == '\n':
			b.WriteString(`\n`)
		case r == '\r':
			b.WriteString(`\r`)
		case r == '\t':
			b.WriteString(`\t`)
		case r == ' ' || unicode.IsPrint(r):
			b.WriteRune(r)
		case r < 0x100:
			b.WriteString(`\x` + hex(r, 2))
		case r < 0x10000:
			b.WriteString(`\u` + hex(r, 4))
		default:
			b.WriteString(`\U` + hex(r, 8))
		}
	}
	b.WriteByte(quote)
	return b.String()
}

func hex(r rune, width int) string {
	digits := strconv.FormatInt(int64(r), 16)
	return strings.Repeat("0", width-len(digits)) + digits
}

// pyNumberRepr is repr() of the int or float a JSON number decodes to.
func pyNumberRepr(n json.Number) string {
	text := n.String()
	if !strings.ContainsAny(text, ".eE") {
		if _, err := strconv.ParseInt(text, 10, 64); err == nil {
			return text
		}
		return text
	}
	f, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return text
	}
	return pyFloatRepr(f)
}

func pyFloatRepr(f float64) string {
	switch {
	case math.IsInf(f, 1):
		return "inf"
	case math.IsInf(f, -1):
		return "-inf"
	case math.IsNaN(f):
		return "nan"
	case f == 0:
		if math.Signbit(f) {
			return "-0.0"
		}
		return "0.0"
	}
	// Shortest round-trip digits and decimal exponent, then Python's layout:
	// fixed notation for -4 <= exp10 < 16, exponent form otherwise.
	e := strconv.FormatFloat(f, 'e', -1, 64)
	sign := ""
	if e[0] == '-' {
		sign = "-"
		e = e[1:]
	}
	mantissa, expText, _ := strings.Cut(e, "e")
	exp, _ := strconv.Atoi(expText)
	digits := strings.Replace(mantissa, ".", "", 1)
	if exp >= -4 && exp < 16 {
		switch {
		case exp >= len(digits)-1:
			return sign + digits + strings.Repeat("0", exp-(len(digits)-1)) + ".0"
		case exp >= 0:
			return sign + digits[:exp+1] + "." + digits[exp+1:]
		default:
			return sign + "0." + strings.Repeat("0", -exp-1) + digits
		}
	}
	out := digits[:1]
	if len(digits) > 1 {
		out += "." + digits[1:]
	}
	expSign := "+"
	if exp < 0 {
		expSign = "-"
		exp = -exp
	}
	expDigits := strconv.Itoa(exp)
	if len(expDigits) < 2 {
		expDigits = "0" + expDigits
	}
	return sign + out + "e" + expSign + expDigits
}

// pyLower is str.lower(): full Unicode lowercasing, where U+0130 becomes "i"
// plus a combining dot and a capital sigma at the end of a word becomes the
// final sigma.
func pyLower(s string) string {
	runes := []rune(s)
	var b strings.Builder
	for i, r := range runes {
		switch {
		case r == 0x130:
			b.WriteString("i̇")
		case r == 0x3a3:
			if finalSigma(runes, i) {
				b.WriteRune(0x3c2)
			} else {
				b.WriteRune(0x3c3)
			}
		default:
			b.WriteRune(unicode.ToLower(r))
		}
	}
	return b.String()
}

// finalSigma reports the Final_Sigma condition: a cased letter before (skipping
// case-ignorable characters) and none after.
func finalSigma(runes []rune, i int) bool {
	before := false
	for j := i - 1; j >= 0; j-- {
		if isCaseIgnorable(runes[j]) {
			continue
		}
		before = isCased(runes[j])
		break
	}
	if !before {
		return false
	}
	for j := i + 1; j < len(runes); j++ {
		if isCaseIgnorable(runes[j]) {
			continue
		}
		return !isCased(runes[j])
	}
	return true
}

func isCased(r rune) bool {
	return unicode.IsUpper(r) || unicode.IsLower(r) || unicode.IsTitle(r)
}

func isCaseIgnorable(r rune) bool {
	return unicode.In(r, unicode.Mn, unicode.Me, unicode.Cf, unicode.Lm, unicode.Sk) || r == '\'' || r == '.' || r == ':' || r == 0xb7 || r == 0x2019
}

// pySpace is str.isspace() for one rune.
func pySpace(r rune) bool {
	return unicode.IsSpace(r) || (r >= 0x1c && r <= 0x1f) || r == 0x85
}

// pyFields is str.split() with no separator.
func pyFields(s string) []string {
	return strings.FieldsFunc(s, pySpace)
}

// pyStrip is str.strip() with no arguments.
func pyStrip(s string) string {
	return strings.TrimFunc(s, pySpace)
}

// digitValue is the decimal value of a Unicode decimal digit; ok is false for
// any other rune. Decimal digits come in runs of ten starting at zero.
func digitValue(r rune) (int, bool) {
	if !unicode.IsDigit(r) {
		return 0, false
	}
	start := r
	for unicode.IsDigit(start - 1) {
		start--
	}
	return int(r-start) % 10, true
}
