package edges

import (
	"errors"
	"math"
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
	// NumberExceedsInt64 marks a PR number Python parsed as a real positive
	// integer that Go cannot represent. PRNumber is 0 in that case and MUST NOT
	// be read; IsPR() stays correct. A consumer that needs the value has to
	// handle this explicitly rather than receive a silently truncated one.
	NumberExceedsInt64 bool
}

// IsPR reports whether this id belongs to the issue<->PR pipeline.
//
// Classification never reads PRNumber. A zero PRNumber means two different
// things -- "not PR-shaped" and "PR-shaped but larger than int64" -- and a
// caller comparing the number would silently hand the second case to the
// issue<->issue build, which does not own it.
func (reference PRReference) IsPR() bool {
	return reference.Provider != "" && (reference.PRNumber > 0 || reference.NumberExceedsInt64)
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

	parsed, positive, exceedsInt64, ok := pythonIntFromDigits(number)
	if !ok {
		// isdigit() said yes, int() would say no. Python raises here; we reject
		// the row instead. Named, counted, and the only deliberate divergence.
		return PRReference{}, ErrMalformedPRID
	}
	if !positive {
		// Python's `int(number) > 0` is false: an all-zero id is a SILENT skip,
		// not an error. Tested with thirty zeros, which no bounded conversion
		// would have distinguished from a large value.
		return PRReference{}, nil
	}
	return PRReference{
		RepoSlug: repoSlug, PRNumber: parsed, Provider: provider,
		NumberExceedsInt64: exceedsInt64,
	}, nil
}

// pythonIntMaxStrDigits is CPython's `sys.get_int_max_str_digits()` default.
//
// Above it, `int(<digit string>)` raises ValueError -- so in builder.py it is
// another uncaught crash on the same unguarded conversion (CHAOS-4811), and
// this port must bucket it as `malformed_pr_id` rather than claim the row for
// the issue<->PR pipeline.
//
// It is an interpreter SETTING, not a language constant: `sys.set_int_max_str_digits()`
// changes it, and it did not exist before 3.11. TestIntMaxStrDigitsMatchesLivePython
// reads it back from the deployed interpreter so a runtime that raised or lowered
// it cannot leave this port silently disagreeing.
const pythonIntMaxStrDigits = 4300

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
// pythonIntFromDigits replicates `int(value)` for the decision Python makes.
//
// # MAGNITUDE IS NOT A REJECTION REASON
//
// An earlier version of this function refused anything above 1<<31, with the
// comment "a PR number this large is not real". That was MY judgement standing
// in for the reference's behaviour, which is the one thing a port must never
// do. Python's ints are arbitrary-precision: there is no value `int()` refuses
// for being large, and the deployed reference parses a thirty-digit id happily
// (measured, codex round 1 P2).
//
// The cost was not a missed edge but a MISLABELLED one. `ghpr:o/r#3000000000`
// is an ordinary PR-shaped row that the issue<->PR pipeline owns; the bound
// bucketed it as `malformed_pr_id`, which is the counter reserved for the one
// ruled divergence -- a row that would have aborted the whole build under
// Python. Corrupting that counter with rows that are merely large would make
// the single most important signal in this port untrustworthy, and totals
// would still balance.
//
// `ok` is therefore false ONLY where Python's `int()` actually raises: a
// Numeric_Type=Digit rune that is not decimal. Every rune is examined even
// after the accumulator saturates, because `int()` scans the whole string and
// a trailing superscript still raises.
//
// `exceedsInt64` reports a value that is real and positive but not
// representable in Go. The CLASSIFICATION stays Python's; only the number is
// unavailable, and callers that need it must check this flag rather than read
// a silently truncated PRNumber.
func pythonIntFromDigits(value string) (number int, positive bool, exceedsInt64 bool, ok bool) {
	// CPython refuses str->int conversions above this many digits (PEP " +
	// 3.11's int_max_str_digits, a DoS guard). Round 1 removed a bound this
	// port invented; round 2 found that the reference has a bound of its OWN,
	// and that removing mine had made the port accept 4301 digits where Python
	// raises. Over-correcting is still diverging.
	//
	// The digit count is of the STRING, so leading zeros count: `int("0"*4301)`
	// raises exactly as `int("9"*4301)` does (measured, both directions).
	if len(value) > pythonIntMaxStrDigits {
		return 0, false, false, false
	}
	for _, r := range value {
		digit := decimalValue(r)
		if digit < 0 {
			return 0, false, false, false
		}
		if digit != 0 {
			positive = true
		}
		if exceedsInt64 {
			continue // keep validating the remaining runes; stop accumulating
		}
		if number > (math.MaxInt-digit)/10 {
			exceedsInt64 = true
			number = 0
			continue
		}
		number = number*10 + digit
	}
	return number, positive, exceedsInt64, true
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
