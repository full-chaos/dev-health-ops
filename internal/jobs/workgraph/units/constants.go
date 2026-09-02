package units

import (
	"errors"
	"math"

	"os"
	"strconv"
	"strings"
)

// DefaultMaxComponentNodes is the port of
// work_graph/investment/constants.py:26 INVESTMENT_MAX_COMPONENT_NODES.
//
// Connected components of work_graph_edges larger than this are
// deterministically split (see BuildComponents) rather than materialized as one
// giant unit: without the cap a single densely-linked hub (a changelog PR, say)
// percolates thousands of unrelated issues/PRs/commits into one component that
// dominates the Investment allocation chart (CHAOS-2775).
const DefaultMaxComponentNodes = 150

// MaxComponentNodesEnvVar is the environment override, matching Python's.
const MaxComponentNodesEnvVar = "INVESTMENT_MAX_COMPONENT_NODES"

// DefaultIntMaxStrDigits mirrors CPython's sys.get_int_max_str_digits().
//
// Since 3.11, int() REFUSES to parse a decimal string with more than this many
// DIGITS, raising ValueError -- a denial-of-service mitigation, since decimal
// conversion is quadratic. So a 4301-digit value is not a huge number to
// Python: it is a parse FAILURE, and resolve_max_component_nodes falls back to
// the default.
//
// That makes the boundary a three-way split, not two, which is what the
// saturation fix alone got wrong:
//
//	<= MaxInt digits ... parse exactly
//	> MaxInt, <= 4300 digits ... Python parses; Go saturates (behaviourally exact)
//	> 4300 digits ... Python RAISES; Go must REFUSE, not saturate
//
// The counting rule, measured rather than assumed:
//   - DIGITS only. A sign does not count, surrounding whitespace does not
//     count, and PEP 515 underscores do not count -- 4300 digits separated by
//     underscores is 8599 characters and parses fine.
//   - Leading zeros DO count. 4301 zeros raises, even though the value is 0.
//   - float() has NO such limit: float("1"*4301) succeeds. One more place
//     where Python's int and float string rules differ, alongside the
//     whitespace divergence documented on pythonparity.IsSpace.
//
// It is a RUNTIME setting (sys.set_int_max_str_digits), so the value is carried
// in the golden and read from the deployed interpreter by the rot guard rather
// than hard-coded as a fact about all Pythons.
const DefaultIntMaxStrDigits = 4300

// MembershipWeightThreshold is the port of constants.py:52
// MEMBERSHIP_WEIGHT_THRESHOLD (CHAOS-2430): the minimum category weight for a
// node to be recorded as a member of that theme/subcategory in
// work_unit_membership. The argmax category of each kind is always emitted even
// when below this threshold. Consumed by the membership projection (PR3/PR7),
// declared here so the two ported jobs cannot drift on it.
const MembershipWeightThreshold = 0.2

// ResolveMaxComponentNodes ports constants.py:29-44 resolve_max_component_nodes:
// explicit argument wins, then the environment, then the default; a
// non-positive or unparseable value falls back to the default rather than
// erroring.
//
// OPERATIONAL INVARIANT (constants.py:17-25, tracked as CHAOS-2779): work_unit_id
// hashes component MEMBERSHIP, so this cap must resolve IDENTICALLY for the
// materialize run and for the later membership projection -- they are separate
// process invocations that each re-resolve from the environment. A divergent
// value splits differently, computes different work_unit_ids, and silently
// skips those units' membership. Set INVESTMENT_MAX_COMPONENT_NODES identically
// on every worker/beat/CLI host, or nowhere.
//
// The Go port inherits that hazard verbatim AND WIDENS IT: with the Go
// materializer live and the Python projection still running, the two planes are
// now two languages reading the same variable out of two process environments.
// The cross-language golden asserts the resolved cap on both sides for exactly
// this reason, and porting the projection (CHAOS-4282) is what finally makes
// the two agree by construction.
//
// Pass a nil explicit value to fall through to the environment.
func ResolveMaxComponentNodes(explicit *int) int {
	if explicit != nil {
		if *explicit >= 1 {
			return *explicit
		}
		return DefaultMaxComponentNodes
	}
	raw, present := os.LookupEnv(MaxComponentNodesEnvVar)
	if !present {
		return DefaultMaxComponentNodes
	}
	parsed, ok := parsePythonInt(raw)
	if !ok || parsed < 1 {
		return DefaultMaxComponentNodes
	}
	return parsed
}

// parsePythonInt accepts what Python's int() accepts for the forms an operator
// can realistically put in an environment variable, and rejects what it rejects.
//
// This exists because a divergence here is EXACTLY the CHAOS-2779 hazard, not a
// cosmetic parser difference: this variable is read independently by the Go
// materializer and the Python membership projection, and if the two resolve it
// to different numbers they split components differently, compute different
// work_unit_ids, and silently write to non-existent unit ids on one plane.
// `INVESTMENT_MAX_COMPONENT_NODES=1_000` is a perfectly ordinary thing to write
// -- PEP 515 digit separators exist for readability and Python honours them --
// and bare strconv.Atoi rejects it, silently falling back to 150 on the Go side
// while Python resolves 1000. An earlier revision of this file dismissed that as
// "not a real configuration"; codex round 2 on CHAOS-4441 PR1 constructed it and
// showed the two planes producing different components. The dismissal was wrong.
//
// Accepted (measured against this checkout's interpreter): surrounding
// whitespace, a single leading + or -, and single underscores BETWEEN digits.
// Rejected, matching Python: leading, trailing or doubled underscores, empty or
// whitespace-only input, non-integers such as "12.5", and anything else.
//
// KNOWN RESIDUAL, now tested rather than asserted: Python's int() also accepts
// non-ASCII decimal digits (int("\u0661\u0662") == 12); this returns false and the caller
// falls back to the default. That remains a silent cross-plane divergence, and
// the honest mitigation is structural, not lexical -- the parity harness asserts
// the RESOLVED cap is equal on both planes (CHAOS-4441 plan, O1b item 4), which
// catches any residual regardless of which exotic literal caused it. Chasing
// Python's full numeric grammar in Go would be an unbounded and unverifiable
// commitment; asserting equality of the resolved value is bounded and provable.
// Pinned by TestResolveMaxComponentNodesMatchesPythonInt.
// decimalDigitValue returns the numeric value of a CPython-decimal rune, or -1.
//
// CPython's int() accepts any character where str.isdecimal() is true --
// Unicode category Nd, 760 code points -- not just ASCII '0'-'9'. So
// int("５６７") is 567 and int("١٢٣") is 123. The value is NOT "code point
// minus 0x30" outside ASCII; each run has its own zero.
//
// # WHY A GENERATED TABLE AND NOT unicode.IsDigit
//
// Two independent reasons, both found by measurement after simpler approaches
// failed:
//
//  1. VERSION SKEW. Go's unicode tables and CPython's are versioned separately.
//     Go 1.27 ships Unicode 17 and the reference interpreter reports 16, and
//     they already DISAGREE: U+11DE0-U+11DE9 are Nd to Go and unassigned to
//     Python. Using unicode.IsDigit would accept ten code points Python
//     refuses, today, on this toolchain.
//
//  2. ABUTTING RUNS. An earlier version derived the value by walking back to
//     the first non-digit. That is wrong wherever Nd runs touch:
//     U+1D7CE..U+1D7FF is FIFTY contiguous mathematical digits, five abutting
//     decades, so the walk-back crosses into the previous run and never
//     terminates correctly. It also had an off-by-one that returned every value
//     one too high -- parsePythonInt("١٢") gave 23 -- which still parses and
//     still looks like a number, so only a comparison against the interpreter's
//     own table caught it.
//
// pythonDecimalRuns is generated from the deployed interpreter by
// tests/fixtures/generate_python_decimal_digits_golden.py, which also verifies
// every run is exactly ten long and fails rather than emitting a table if that
// stops holding.
func decimalDigitValue(r rune) int {
	// ASCII fast path: the overwhelmingly common case, and the first table
	// entry, so the binary search below would find it anyway.
	if r >= '0' && r <= '9' {
		return int(r - '0')
	}

	low, high := 0, len(pythonDecimalRuns)-1
	for low <= high {
		middle := (low + high) / 2
		run := pythonDecimalRuns[middle]
		switch {
		case r < run[0]:
			high = middle - 1
		case r > run[1]:
			low = middle + 1
		default:
			return int(r - run[0])
		}
	}
	return -1
}

// parsePythonInt models CPython's int(str) for the values this port reads from
// configuration.
//
// Three Python behaviours that the obvious Go spelling gets wrong, each found
// by measurement and each pinned by a corpus:
//
//  1. ACCEPT SET. int() takes any str.isdecimal() character, not just ASCII.
//     Refusing a full-width or Arabic-Indic digit makes one plane use the
//     configured cap and the other fall back to its default.
//  2. DIGIT LIMIT. Above sys.get_int_max_str_digits() int() RAISES, so such a
//     value must be refused rather than saturated -- and the limit counts
//     CHARACTERS, not bytes. 4300 full-width digits is 4300 characters and
//     12900 bytes; counting bytes would refuse a value Python accepts.
//  3. RANGE. Below that limit but above MaxInt, Python parses a value Go
//     cannot hold. Saturating is exact for this caller (see below).
func parsePythonInt(raw string) (int, bool) {
	// strings.TrimSpace, NOT pythonparity.Strip, and the difference is the whole
	// point of TestNumericParsersRejectSeparatorsLikePythonNumerics: Python uses
	// TWO whitespace rules and this is the NUMERIC one. str.strip() removes
	// 0x1c-0x1f; int() and float() REJECT them:
	//
	//	" 150".strip() -> "150"      int(" 150")      -> 150
	//	int("\x1c150")                                -> ValueError
	//
	// Go's TrimSpace happens to match the numeric rule exactly. Adopting
	// pythonparity.Strip here "for consistency" would accept values Python
	// refuses -- I made precisely that mistake while adding non-ASCII digit
	// support, and the existing separator test did not catch it because its
	// expected value was the fallback (150), which is indistinguishable from a
	// successful parse of "150".
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return 0, false
	}

	runes := []rune(trimmed)
	sign := ""
	switch runes[0] {
	case '+':
		runes = runes[1:]
	case '-':
		sign, runes = "-", runes[1:]
	}
	if len(runes) == 0 {
		return 0, false
	}

	// Digits are normalised to ASCII as they are read, so the value can be
	// handed to Atoi while the COUNT stays a count of characters.
	var builder strings.Builder
	builder.Grow(len(runes))
	digitCount := 0
	previousWasDigit := false
	for index, r := range runes {
		if value := decimalDigitValue(r); value >= 0 {
			builder.WriteByte(byte('0' + value))
			digitCount++
			previousWasDigit = true
			continue
		}
		// PEP 515: an underscore must sit BETWEEN two digits. Underscores do
		// NOT count toward the digit limit -- 4300 digits separated by
		// underscores is 8599 characters and parses fine.
		if r == '_' {
			if !previousWasDigit || index == len(runes)-1 {
				return 0, false
			}
			previousWasDigit = false
			continue
		}
		return 0, false
	}
	if digitCount == 0 {
		return 0, false
	}

	// CPython refuses more than sys.get_int_max_str_digits() DIGITS (3.11+, a
	// DoS mitigation since decimal conversion is quadratic) and raises
	// ValueError. This is counted in CHARACTERS, which is why digitCount is
	// incremented per rune rather than taken from builder.Len() -- the two
	// differ for every non-ASCII digit.
	//
	// It must be REFUSED, not saturated: Python raises, so the caller falls
	// back to its default. Saturating would hand back MaxInt for a value
	// Python never accepted, the mirror image of the range defect below.
	if digitCount > DefaultIntMaxStrDigits {
		return 0, false
	}

	parsed, err := strconv.Atoi(sign + builder.String())
	if err != nil {
		// A RANGE error is NOT a malformed value, and collapsing the two is a
		// divergence: Python's int() is unbounded below the digit limit, so
		// forty 1s parses to 10^40 -- a cap so large that NO component is ever
		// split -- while treating ErrRange as malformed falls back to 150 and
		// splits aggressively. The planes then mint different work_unit_ids for
		// any org with a component above 150 nodes.
		//
		// Saturating is EXACT for this caller rather than approximate: a cap of
		// 10^40 and a cap of MaxInt split identically, because no component can
		// hold more than MaxInt nodes. Sign is preserved so the caller's `< 1`
		// check still rejects a huge NEGATIVE cap, as Python's does.
		//
		// The ErrRange test is unreachable-as-FALSE today -- the loop above has
		// rejected every non-digit, so Atoi can only fail with ErrRange, never
		// ErrSyntax, and a bare `err != nil` passes the whole suite (verified by
		// mutation). It is kept explicit because it states the intent and stops
		// being equivalent the moment the loop is loosened, at which point
		// saturating a malformed value would be far worse.
		if errors.Is(err, strconv.ErrRange) {
			if sign == "-" {
				return math.MinInt, true
			}
			return math.MaxInt, true
		}
		return 0, false
	}
	return parsed, true
}
