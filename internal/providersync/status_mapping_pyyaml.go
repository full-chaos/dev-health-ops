package providersync

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"gopkg.in/yaml.v3"
)

// PyYAML-1.1 compatibility layer for the StatusMapping port.
//
// WHY THIS FILE EXISTS. The port originally assumed go-yaml v3 and PyYAML agree
// on what a bare scalar IS. They do not, and the disagreement is structural
// rather than incidental: go-yaml implements YAML **1.2**, PyYAML implements
// YAML **1.1**. `yes` is a bool to PyYAML and a string to go-yaml; `1e3` is a
// string to PyYAML and a float to go-yaml; `12:00:00` is the integer 43200 to
// PyYAML and a string to go-yaml. Because `_index_values` calls `str(raw)` on
// whatever the loader produced, every one of those differences lands directly in
// an index KEY -- silently, with both engines reporting a full, healthy map.
//
// So the mirroring has to happen at the RESOLVER, not at the formatter. Every
// rule below is transcribed from PyYAML's own implicit resolver and pinned by an
// executed oracle case; the ground truth was taken by running both engines over
// the same scalars, not by reading either implementation's documentation.
//
// SCOPE, stated rather than assumed: this mirrors PyYAML's IMPLICIT resolution
// of PLAIN scalars. A quoted or block scalar is a string in both engines (this
// is why every rule keys off node.Style). Explicitly-tagged scalars (`!!str
// 010`) are NOT mirrored -- no config in this family uses one, and PyYAML would
// honour the explicit tag while this layer would re-resolve the value. That is a
// known, written limitation, not an oversight.

// pythonFailure mirrors an exception the Python loader raises. The oracle pairs
// compare PHASE plus exception type, so an error that changes phase or type
// compares unequal instead of both engines merely "failing" and looking alike.
type pythonFailure struct {
	// Kind is the Python exception class name, e.g. "AttributeError".
	Kind string
	// Detail is Go-side context; it is NOT compared (Python's message wording
	// is not part of the contract), only Kind and phase are.
	Detail string
}

func (e *pythonFailure) Error() string {
	return fmt.Sprintf("mirrors Python %s: %s", e.Kind, e.Detail)
}

func attributeError(detail string) error {
	return &pythonFailure{Kind: "AttributeError", Detail: detail}
}

func typeError(detail string) error {
	return &pythonFailure{Kind: "TypeError", Detail: detail}
}

// pythonFailureKind returns the mirrored exception class for err, or "" when err
// is not a mirrored Python failure. The oracle rows carry this so a Go error
// that changes exception class compares unequal instead of both engines merely
// "failing" and looking alike.
func pythonFailureKind(err error) string {
	var failure *pythonFailure
	if errors.As(err, &failure) {
		return failure.Kind
	}
	return ""
}

// pyBoolIsTrue maps a PyYAML-resolved bool scalar to its Python value. PyYAML
// 1.1 accepts yes/on/true and their case variants; note that bare `y`/`n` are
// NOT booleans to PyYAML (executed), which is why only whole words appear here
// and in pyBoolPattern.
func pyBoolIsTrue(value string) bool {
	return strings.EqualFold(value, "yes") ||
		strings.EqualFold(value, "true") ||
		strings.EqualFold(value, "on")
}

// ---------------------------------------------------------------------------
// PyYAML 1.1 implicit resolver
// ---------------------------------------------------------------------------

// These patterns are PyYAML's resolver.py regexes, transcribed. Two details are
// easy to get wrong and are both load-bearing here:
//
//  1. the FLOAT pattern requires a literal dot, and its exponent requires an
//     explicit SIGN -- which is why `1e3` and `1.5e8` are STRINGS to PyYAML
//     while go-yaml floats both. Executed and pinned.
//  2. `0` alone is decimal, not octal, because the octal branch needs at least
//     one digit after the leading zero.
var (
	pyBoolPattern = regexp.MustCompile(
		`^(?:yes|Yes|YES|no|No|NO|true|True|TRUE|false|False|FALSE|on|On|ON|off|Off|OFF)$`)
	pyNullPattern      = regexp.MustCompile(`^(?:~|null|Null|NULL|)$`)
	pyIntBinary        = regexp.MustCompile(`^[-+]?0b[01_]+$`)
	pyIntOctal         = regexp.MustCompile(`^[-+]?0[0-7_]+$`)
	pyIntDecimal       = regexp.MustCompile(`^[-+]?(?:0|[1-9][0-9_]*)$`)
	pyIntHex           = regexp.MustCompile(`^[-+]?0x[0-9a-fA-F_]+$`)
	pyIntSexagesimal   = regexp.MustCompile(`^[-+]?[1-9][0-9_]*(?::[0-5]?[0-9])+$`)
	pyFloatFixed       = regexp.MustCompile(`^[-+]?(?:[0-9][0-9_]*)?\.[0-9_]*(?:[eE][-+][0-9]+)?$`)
	pyFloatSexagesimal = regexp.MustCompile(`^[-+]?[0-9][0-9_]*(?::[0-5]?[0-9])+\.[0-9_]*$`)
	pyFloatInf         = regexp.MustCompile(`^[-+]?\.(?:inf|Inf|INF)$`)
	pyFloatNaN         = regexp.MustCompile(`^\.(?:nan|NaN|NAN)$`)
	// PyYAML resolves these to datetime.date / datetime.datetime. See
	// pyScalarTimestamp and Decision Log D20.
	pyTimestampPattern = regexp.MustCompile(
		`^[0-9][0-9][0-9][0-9]-[0-9][0-9]?-[0-9][0-9]?(?:[Tt]|[ \t]+).*$|^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]$`)
)

type pyScalarKind int

const (
	pyScalarString pyScalarKind = iota
	pyScalarNull
	pyScalarBool
	pyScalarInt
	pyScalarFloat
	pyScalarTimestamp
)

// resolvePyScalar mirrors PyYAML's implicit resolution for one scalar node.
// A non-plain (quoted or block) scalar is always a string, in both engines.
func resolvePyScalar(node *yaml.Node) pyScalarKind {
	if node.Style != 0 {
		return pyScalarString
	}
	value := node.Value
	switch {
	case pyNullPattern.MatchString(value):
		return pyScalarNull
	case pyBoolPattern.MatchString(value):
		return pyScalarBool
	case pyIntBinary.MatchString(value), pyIntOctal.MatchString(value),
		pyIntDecimal.MatchString(value), pyIntHex.MatchString(value),
		pyIntSexagesimal.MatchString(value):
		return pyScalarInt
	case pyFloatInf.MatchString(value), pyFloatNaN.MatchString(value),
		pyFloatSexagesimal.MatchString(value), pyFloatFixed.MatchString(value):
		return pyScalarFloat
	case pyTimestampPattern.MatchString(value):
		return pyScalarTimestamp
	default:
		return pyScalarString
	}
}

func stripUnderscores(value string) string { return strings.ReplaceAll(value, "_", "") }

func splitSign(value string) (negative bool, rest string) {
	switch {
	case strings.HasPrefix(value, "-"):
		return true, value[1:]
	case strings.HasPrefix(value, "+"):
		return false, value[1:]
	}
	return false, value
}

// pyParseInt mirrors PyYAML's int construction, including the bases go-yaml
// disagrees about: a leading-zero scalar is OCTAL to PyYAML (`010` is 8), and
// `0o10` is NOT an int at all to PyYAML -- it is the string "0o10", which is why
// the octal branch below matches only the leading-zero spelling.
func pyParseInt(raw string) (int64, error) {
	negative, value := splitSign(stripUnderscores(raw))
	var parsed int64
	var err error
	switch {
	case strings.HasPrefix(value, "0b"):
		parsed, err = strconv.ParseInt(value[2:], 2, 64)
	case strings.HasPrefix(value, "0x"):
		parsed, err = strconv.ParseInt(value[2:], 16, 64)
	case strings.Contains(value, ":"):
		parsed, err = pyParseSexagesimalInt(value)
	case len(value) > 1 && strings.HasPrefix(value, "0"):
		parsed, err = strconv.ParseInt(value[1:], 8, 64)
	default:
		parsed, err = strconv.ParseInt(value, 10, 64)
	}
	if err != nil {
		return 0, fmt.Errorf("pyParseInt(%q): %w", raw, err)
	}
	if negative {
		parsed = -parsed
	}
	return parsed, nil
}

func pyParseSexagesimalInt(value string) (int64, error) {
	var total int64
	for _, part := range strings.Split(value, ":") {
		digits, err := strconv.ParseInt(part, 10, 64)
		if err != nil {
			return 0, err
		}
		total = total*60 + digits
	}
	return total, nil
}

func pyParseFloat(raw string) (float64, error) {
	value := stripUnderscores(raw)
	if pyFloatNaN.MatchString(value) {
		return math.NaN(), nil
	}
	if pyFloatInf.MatchString(value) {
		if strings.HasPrefix(value, "-") {
			return math.Inf(-1), nil
		}
		return math.Inf(1), nil
	}
	if pyFloatSexagesimal.MatchString(value) {
		negative, rest := splitSign(value)
		dot := strings.Index(rest, ".")
		whole, err := pyParseSexagesimalInt(rest[:dot])
		if err != nil {
			return 0, err
		}
		fraction := 0.0
		if dot+1 < len(rest) {
			fraction, err = strconv.ParseFloat("0"+rest[dot:], 64)
			if err != nil {
				return 0, err
			}
		}
		result := float64(whole) + fraction
		if negative {
			result = -result
		}
		return result, nil
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, fmt.Errorf("pyParseFloat(%q): %w", raw, err)
	}
	return parsed, nil
}

// pythonReprFloat mirrors CPython's float repr.
//
// Three things Go's %g gets wrong and every one of them reaches an index key:
// NaN/±Inf spell as "NaN"/"+Inf"/"-Inf" rather than Python's "nan"/"inf"/
// "-inf"; an integral float needs a trailing ".0"; and the fixed-vs-scientific
// switch happens at a DIFFERENT exponent. CPython uses scientific notation only
// when the decimal exponent is < -4 or >= 16 -- verified by execution
// (1e15 -> "1000000000000000.0", 1e16 -> "1e+16", 1.5e+8 -> "150000000.0",
// 0.0001 -> "0.0001", 1e-5 -> "1e-05"), which Go's %g does not reproduce.
func pythonReprFloat(value float64) string {
	switch {
	case math.IsNaN(value):
		return "nan"
	case math.IsInf(value, 1):
		return "inf"
	case math.IsInf(value, -1):
		return "-inf"
	}

	// Shortest round-trip digits, in a form whose exponent is explicit.
	scientific := strconv.FormatFloat(value, 'e', -1, 64)
	mantissa, exponentText, found := strings.Cut(scientific, "e")
	if !found {
		return scientific
	}
	exponent, err := strconv.Atoi(exponentText)
	if err != nil {
		return scientific
	}

	if exponent < -4 || exponent >= 16 {
		// Python always writes a sign and at least two exponent digits.
		sign := "+"
		if exponent < 0 {
			sign = "-"
			exponent = -exponent
		}
		return fmt.Sprintf("%se%s%02d", mantissa, sign, exponent)
	}

	fixed := strconv.FormatFloat(value, 'f', -1, 64)
	if !strings.Contains(fixed, ".") {
		fixed += ".0"
	}
	return fixed
}

// ---------------------------------------------------------------------------
// Python str()/repr() over resolved values
// ---------------------------------------------------------------------------

// pythonScalarStr renders one resolved scalar the way Python's str() would.
func pythonScalarStr(node *yaml.Node) (string, error) {
	switch resolvePyScalar(node) {
	case pyScalarString:
		return node.Value, nil
	case pyScalarNull:
		return "None", nil
	case pyScalarBool:
		if pyBoolIsTrue(node.Value) {
			return "True", nil
		}
		return "False", nil
	case pyScalarInt:
		parsed, err := pyParseInt(node.Value)
		if err != nil {
			return "", err
		}
		return strconv.FormatInt(parsed, 10), nil
	case pyScalarFloat:
		parsed, err := pyParseFloat(node.Value)
		if err != nil {
			return "", err
		}
		return pythonReprFloat(parsed), nil
	case pyScalarTimestamp:
		// DECLARED DIVERGENCE (Decision Log D20). PyYAML resolves this to a
		// datetime.date / datetime.datetime and str()s it; reproducing Python's
		// datetime repr (including its timezone rendering) is a second,
		// unversioned date formatter, so this port refuses LOUDLY instead of
		// guessing. The status/mapping/load pair carries a case that asserts the
		// divergence currently exists, declared through the harness's
		// excluded_fields mechanism -- a divergence recorded only in prose is a
		// claim nothing checks.
		return "", fmt.Errorf(
			"pythonScalarStr: %q resolves to a PyYAML timestamp at line %d; that is "+
				"declared divergence D20 and is deliberately NOT mirrored -- see the "+
				"timestamp case in the status/mapping/load pair", node.Value, node.Line)
	default:
		return "", fmt.Errorf("pythonScalarStr: unhandled scalar %q", node.Value)
	}
}

// pythonScalarRepr renders one resolved scalar the way Python's repr() would --
// identical to str() except that strings gain quotes.
func pythonScalarRepr(node *yaml.Node) (string, error) {
	if resolvePyScalar(node) == pyScalarString {
		return pythonReprString(node.Value), nil
	}
	return pythonScalarStr(node)
}

// pythonReprString mirrors CPython's string repr quoting: single quotes by
// default; double quotes when the value contains a single quote but no double
// quote; and the non-printable escapes CPython emits for control characters.
func pythonReprString(value string) string {
	hasSingle := strings.Contains(value, "'")
	hasDouble := strings.Contains(value, `"`)
	quote := byte('\'')
	if hasSingle && !hasDouble {
		quote = '"'
	}

	var builder strings.Builder
	builder.WriteByte(quote)
	for _, r := range value {
		switch r {
		case '\\':
			builder.WriteString(`\\`)
		case '\n':
			builder.WriteString(`\n`)
		case '\r':
			builder.WriteString(`\r`)
		case '\t':
			builder.WriteString(`\t`)
		case rune(quote):
			builder.WriteByte('\\')
			builder.WriteRune(r)
		default:
			if r < 0x20 || r == 0x7f {
				builder.WriteString(fmt.Sprintf(`\x%02x`, r))
				continue
			}
			builder.WriteRune(r)
		}
	}
	builder.WriteByte(quote)
	return builder.String()
}

// normKeyLower mirrors Python's str.lower(), which is FULL Unicode case mapping
// -- not Go's strings.ToLower, which is simple per-rune folding.
//
// The difference is observable on ordinary provider text: Python lowercases
// "İ" (U+0130) to "i" + U+0307 (two runes), while strings.ToLower produces a
// bare "i", so a label like "İSSUE" matches in Go and misses in Python. Final
// sigma differs the same way.
//
// language.Und is chosen DELIBERATELY and must not be "corrected" to Turkish
// because the motivating character is Turkish: cases.Lower(language.Turkish) is
// byte-identical to the broken strings.ToLower on İ, so that choice would
// reproduce the very defect this function exists to fix while looking like the
// fix. Python's str.lower() is locale-independent, and Und is the
// locale-independent caser.
//
// The Caser is constructed per call rather than cached: a cases.Caser carries
// internal state and is NOT safe for concurrent use.
func normKeyLower(value string) string {
	return cases.Lower(language.Und).String(value)
}
