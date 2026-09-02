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
func parsePythonInt(raw string) (int, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return 0, false
	}

	sign := ""
	digits := trimmed
	switch digits[0] {
	case '+':
		digits = digits[1:]
	case '-':
		sign, digits = "-", digits[1:]
	}
	if digits == "" {
		return 0, false
	}

	// PEP 515: an underscore must sit BETWEEN two digits. Rejecting leading,
	// trailing and doubled underscores is what Python does, so a malformed
	// value falls back to the default on both planes rather than on neither.
	var builder strings.Builder
	builder.Grow(len(digits))
	previousWasDigit := false
	for index := 0; index < len(digits); index++ {
		character := digits[index]
		switch {
		case character >= '0' && character <= '9':
			builder.WriteByte(character)
			previousWasDigit = true
		case character == '_':
			if !previousWasDigit || index == len(digits)-1 {
				return 0, false
			}
			previousWasDigit = false
		default:
			return 0, false
		}
	}

	parsed, err := strconv.Atoi(sign + builder.String())
	if err != nil {
		// A RANGE error is NOT the same as a malformed value, and collapsing
		// the two is a real divergence (found by applying lane-4752-go's
		// magnitude axis).
		//
		// Python's int() is unbounded, so INVESTMENT_MAX_COMPONENT_NODES set to
		// forty 1s parses to 10^40 -- a cap so large that NO component is ever
		// split. Go's Atoi returns ErrRange, and treating that as "malformed"
		// falls back to the default of 150, which splits aggressively. The two
		// planes then mint completely different work_unit_ids for any org with
		// a component above 150 nodes, which is the CHAOS-2775 split this port
		// exists to reproduce.
		//
		// Saturating is EXACT here, not an approximation: a cap of 10^40 and a
		// cap of MaxInt produce identical splitting decisions, because no
		// component can contain more than MaxInt nodes. The unrepresentable
		// value and the saturated one are indistinguishable for every reachable
		// input.
		//
		// Sign is preserved so the caller's `< 1` check still rejects a huge
		// NEGATIVE cap, exactly as Python's does.
		//
		// The ErrRange test is currently unreachable-as-FALSE, and saying so is
		// more useful than implying the test covers it: the loop above has
		// already rejected every non-digit, so the string handed to Atoi is
		// sign-plus-digits and can only fail with ErrRange, never ErrSyntax.
		// Replacing this condition with a bare `err != nil` passes the whole
		// suite (verified by mutation). It is kept explicit anyway, because it
		// states the intent and because it stops being equivalent the moment
		// the digit filter above is loosened -- at which point saturating a
		// malformed value would be a silent, and much worse, defect.
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
