package units

import (
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
	// Python's int() strips surrounding whitespace before parsing; Atoi does
	// not, so " 150 " would resolve to 150 in Python and silently fall back to
	// the default here -- the precise cross-plane divergence this function must
	// not have. Trimming closes it.
	//
	// KNOWN RESIDUAL DIVERGENCE: Python's int() also accepts PEP 515 digit
	// separators ("1_0" -> 10), which Atoi rejects and this function would
	// therefore resolve to the default. Left unhandled deliberately -- writing
	// an underscore into a node-count environment variable is not a real
	// configuration, and emulating it would mean hand-rolling an int parser.
	// Recorded rather than silently accepted.
	parsed, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || parsed < 1 {
		return DefaultMaxComponentNodes
	}
	return parsed
}
