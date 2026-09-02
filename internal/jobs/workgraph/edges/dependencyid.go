package edges

import (
	"errors"
	"strings"
	"unicode"
)

// PRReference is a dependency id that names a pull/merge request rather than an
// issue. Rows whose source OR target parses as one are excluded from the
// issue<->issue edge build and left to the mapping writer.
type PRReference struct {
	RepoSlug string
	PRNumber int
	Provider string
}

// ErrMalformedPRID marks an id that looks PR-shaped but whose number Python
// would fail to convert. See ParsePRDependencySource for why this is an error
// value rather than a silent skip.
var ErrMalformedPRID = errors.New("malformed pr id")

const (
	ProviderGitHub = "github"
	ProviderGitLab = "gitlab"
)

// ParsePRDependencySource is the Go twin of
// work_graph/builder.py::_parse_pr_dependency_source (:256-277).
//
// # WHY THIS IS NOT strconv.Atoi
//
// This function decides WHICH PIPELINE OWNS A ROW: a PR-shaped id is skipped by
// the issue<->issue build on the assumption the issue<->PR mapping writer claims
// it. A divergence here therefore does not produce a wrong edge — it produces a
// row that one pipeline skips and the other never sees, while each pipeline's
// own read == written + rejected still balances. That is the failure shape that
// hid a `uuid.Nil` divergence on a sibling port.
//
// Python guards with `number.isdigit()` and then calls `int(number)`. Those two
// do not accept the same characters, and `strconv.Atoi` matches neither.
// Executed against both runtimes:
//
//	input        python isdigit()+int()      strconv.Atoi
//	"5"          5                           5
//	"-5"         rejected (isdigit False)     ACCEPTED, -5
//	"+5"         rejected                     ACCEPTED, 5
//	"٥" U+0665   ACCEPTED, 5                  rejected
//	"５" U+FF15  ACCEPTED, 5                  rejected
//	"²" U+00B2   RAISES ValueError            rejected
//
// So Atoi is wrong in BOTH directions: it would claim rows Python leaves to the
// issue<->issue build, and abandon rows Python claims. This implements Python's
// accept-set deliberately instead — `unicode.IsDigit` is the Numeric_Type=Digit
// property `str.isdigit()` tests, and the decimal-value conversion is `int()`'s.
//
// # THE ONE DELIBERATE DIVERGENCE
//
// Python's `int()` rejects the superscripts that `isdigit()` accepts, and the
// conversion is unguarded, so `ghpr:owner/repo#²` raises a ValueError that
// nothing catches — aborting the entire org's build over one row (CHAOS-4811).
//
// Go does not reproduce the crash. It returns ErrMalformedPRID so the caller can
// reject that ONE row with a named, counted reason. This is the only place the
// port knowingly differs from Python, it is ruled and recorded in RISK-NOTES,
// and it is strictly safer: Python loses the whole build, Go loses one row and
// says which.
func ParsePRDependencySource(value string) (PRReference, error) {
	var body, separator, provider string
	switch {
	case strings.HasPrefix(value, "ghpr:"):
		body, separator, provider = strings.TrimPrefix(value, "ghpr:"), "#", ProviderGitHub
	case strings.HasPrefix(value, "gitlab:"):
		body, separator, provider = strings.TrimPrefix(value, "gitlab:"), "!", ProviderGitLab
	default:
		return PRReference{}, nil // not PR-shaped; the issue<->issue build keeps it
	}

	index := strings.LastIndex(body, separator)
	if index < 0 {
		return PRReference{}, nil
	}
	repoSlug, number := body[:index], body[index+len(separator):]
	if repoSlug == "" || !isPythonDigitString(number) {
		return PRReference{}, nil
	}

	parsed, ok := pythonIntFromDigits(number)
	if !ok {
		// isdigit() said yes, int() would say no. Python raises here; we reject
		// the row instead. Named, counted, and the only deliberate divergence.
		return PRReference{}, ErrMalformedPRID
	}
	if parsed <= 0 {
		return PRReference{}, nil
	}
	return PRReference{RepoSlug: repoSlug, PRNumber: parsed, Provider: provider}, nil
}

// numericTypeDigitNotDecimal is every rune for which Python's `str.isdigit()` is
// TRUE but `int()` RAISES — derived by scanning all 0x110000 code points through
// the live interpreter, not by hand.
//
// It is exactly 128 runes in 20 ranges, all category No: Python's `isdigit()` is
// Numeric_Type=Digit (760 Nd + these 128), while `int()` accepts only Nd. Go's
// `unicode.IsDigit` is Nd alone, so it silently disagrees with `isdigit()` on
// precisely this set — which is the set that crashes the Python builder
// (CHAOS-4811).
//
// TestNumericTypeDigitTableMatchesLivePython re-derives it, so it cannot rot.
var numericTypeDigitNotDecimal = &unicode.RangeTable{
	R16: []unicode.Range16{
		{Lo: 0x00B2, Hi: 0x00B3, Stride: 1},
		{Lo: 0x00B9, Hi: 0x00B9, Stride: 1},
		{Lo: 0x1369, Hi: 0x1371, Stride: 1},
		{Lo: 0x19DA, Hi: 0x19DA, Stride: 1},
		{Lo: 0x2070, Hi: 0x2070, Stride: 1},
		{Lo: 0x2074, Hi: 0x2079, Stride: 1},
		{Lo: 0x2080, Hi: 0x2089, Stride: 1},
		{Lo: 0x2460, Hi: 0x2468, Stride: 1},
		{Lo: 0x2474, Hi: 0x247C, Stride: 1},
		{Lo: 0x2488, Hi: 0x2490, Stride: 1},
		{Lo: 0x24EA, Hi: 0x24EA, Stride: 1},
		{Lo: 0x24F5, Hi: 0x24FD, Stride: 1},
		{Lo: 0x24FF, Hi: 0x24FF, Stride: 1},
		{Lo: 0x2776, Hi: 0x277E, Stride: 1},
		{Lo: 0x2780, Hi: 0x2788, Stride: 1},
		{Lo: 0x278A, Hi: 0x2792, Stride: 1},
	},
	R32: []unicode.Range32{
		{Lo: 0x10A40, Hi: 0x10A43, Stride: 1},
		{Lo: 0x10E60, Hi: 0x10E68, Stride: 1},
		{Lo: 0x11052, Hi: 0x1105A, Stride: 1},
		{Lo: 0x1F100, Hi: 0x1F10A, Stride: 1},
	},
}

// isPythonDigitString is `str.isdigit()`: non-empty, every rune Numeric_Type=Digit.
//
// NOT `unicode.IsDigit`, which is Nd only. That distinction is the whole point:
// the 128 runes in between are the ones Python accepts here and then crashes on
// at `int()`, and using Go's IsDigit would skip them silently instead of
// reporting a named, counted rejection.
//
// Accepts non-ASCII decimals ("٥", "５") and the No-digits ("²"); rejects a
// leading "+" or "-". None of that matches strconv.Atoi.
func isPythonDigitString(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if !unicode.Is(unicode.Nd, r) && !unicode.Is(numericTypeDigitNotDecimal, r) {
			return false
		}
	}
	return true
}

// pythonIntFromDigits is `int()` over a string `isdigit()` already accepted.
//
// `int()` takes Nd only, so any rune from numericTypeDigitNotDecimal makes it
// fail — which in Python is an uncaught ValueError. ok=false is that case.
func pythonIntFromDigits(value string) (int, bool) {
	total := 0
	for _, r := range value {
		digit := decimalValue(r)
		if digit < 0 {
			return 0, false
		}
		total = total*10 + digit
		if total > 1<<31 {
			// A PR number this large is not real; refuse rather than overflow.
			return 0, false
		}
	}
	return total, true
}

// decimalValue returns a rune's decimal value, or -1 if it has none.
//
// Go's stdlib exposes no "decimal digit value" helper, and r-'0' would be wrong
// for every non-ASCII digit Python accepts, so this finds the rune's own block
// zero within Nd.
func decimalValue(r rune) int {
	if r >= '0' && r <= '9' {
		return int(r - '0')
	}
	if !unicode.Is(unicode.Nd, r) {
		return -1
	}
	for zero := r; zero >= r-9; zero-- {
		if !unicode.Is(unicode.Nd, zero) {
			break
		}
		if !unicode.Is(unicode.Nd, zero-1) {
			return int(r - zero)
		}
	}
	return -1
}
