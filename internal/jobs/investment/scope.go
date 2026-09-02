// Package investment holds the native Go investment.materialize compute
// (CHAOS-4441). This file is the tenant-scope guard every entry point must
// call before reading anything.
package investment

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
)

// ErrOrganizationScopeRequired is returned when an entry point is asked to run
// without a tenant scope.
var ErrOrganizationScopeRequired = errors.New(
	"investment: a non-empty organization scope is required",
)

// RequireOrganizationScope is the Go equivalent of Python's
// materialize.py:1179-1188 gate, at the SAME LAYER: the entry point, before any
// fetch. It is NOT a new gate, and it is deliberately NOT in chquery.
//
// # WHY THE GUARD IS HERE AND NOT IN THE FETCHERS
//
// The fetchers are shape-identical to Python by design — chquery reproduces
// `if org_id:` verbatim, including the part where an empty org drops the filter
// (CHAOS-4804). Adding a refusal down there would make the two planes group
// differently, which is the exact failure this port exists to prevent. So the
// fetchers stay permissive and the entry point refuses, matching how Python is
// arranged.
//
// # WHAT AN EMPTY SCOPE ACTUALLY DOES, WHICH IS WHY THIS IS NOT DEFENSIVE NOISE
//
// With an empty org, fetch_work_graph_edges' WHERE clause becomes empty and the
// query reads every tenant's edges. The rows stay per-org because of the GROUP
// BY, but units.NodeKey is (type, id) with NO org — so a provider-scoped id
// present in two tenants becomes ONE node, the two graphs fuse into a single
// connected component, and the resulting work_unit_id hashes a node set drawn
// from multiple organisations. It is then written under whichever org the run
// was configured with.
//
// Nothing raises and nothing logs. The row is indistinguishable from a
// legitimate one afterwards. That is why the guard is loud and unconditional
// rather than a warning.
//
// # ONE DELIBERATE DIVERGENCE FROM PYTHON, IN THE SAFE DIRECTION
//
// Python's gate has two escape hatches: it permits an empty org for `mock` and
// `none` LLM providers, and for any run with `allow_unscoped`. THIS FUNCTION HAS
// NEITHER. That is a deliberate choice (team-lead ruling, 2026-09-01): no
// production path needs an unscoped investment run, and the escape hatches are
// what make the Python case reachable at all. If a prod path ever does need one,
// it should be an explicit argument on the caller — never an empty string that
// silently means "every tenant".
//
// The divergence is at the GUARD layer, not the compute layer, so it cannot
// change any work_unit_id the two planes agree on: a run this function refuses
// is a run Python would have executed unscoped, and there is no correct
// unscoped answer to disagree about.
// # FOUR RULES FOR IMPORTERS -- each is deliberate, none is obvious from the name
//
// This function is imported across lanes (lane-4752-go's workgraph.build entry
// point, and any future native investment path), so the choices below are
// stated rather than left to be inferred from the body. A copy that differs on
// any of them means the two planes disagree about what "scoped" means, which is
// the same class of defect the guard exists to prevent.
//
//  1. WHITESPACE-ONLY IS REFUSED, using Python's str.strip() rule -- which is
//     WIDER than strings.TrimSpace. See pythonStrip: Go's unicode.IsSpace omits
//     0x1c-0x1f, so a lone separator would otherwise be accepted as a scope
//     Python refuses.
//  2. THE ALL-ZERO UUID IS ACCEPTED. Python's rejection predicates run on the
//     rendered string, and "00000000-0000-0000-0000-000000000000" is non-empty
//     and therefore truthy. Excluding uuid.Nil here would reject rows Python
//     maps -- a false negative that keeps read == mapped + rejected balanced and
//     is invisible to every conservation check (CHAOS-4804).
//  3. EXISTENCE IS NOT CHECKED. A well-formed but unknown org passes. Whether
//     the org exists is the query's answer, not the guard's; refusing here would
//     make this a NEW gate rather than the ported one.
//  4. THERE IS NO allow_unscoped OR MOCK-PROVIDER ESCAPE HATCH, unlike Python's
//     materialize.py:1179-1188. That divergence is deliberate and sits at the
//     guard layer, so it cannot change any work_unit_id both planes produce: a
//     run this refuses is one Python would have executed UNSCOPED, and there is
//     no correct unscoped answer to disagree about. The absence is structural --
//     the function takes only the org string, so there is no argument to flip.
func RequireOrganizationScope(organizationID string) error {
	// pythonStrip, not strings.TrimSpace — see its doc comment. A whitespace-only
	// org is not a scope, and accepting one would reach the fetchers with a value
	// that matches no rows rather than every row: a different wrong answer, not a
	// right one.
	if pythonStrip(organizationID) == "" {
		return fmt.Errorf(
			"%w: refusing to run unscoped, which would read every tenant's "+
				"work_graph_edges and fuse them into shared components (CHAOS-4804)",
			ErrOrganizationScopeRequired,
		)
	}
	return nil
}

// pythonStrip removes exactly what Python's `str.strip()` removes.
//
// `strings.TrimSpace` is NOT equivalent, and the difference is small enough to
// look like pedantry until it is measured. Go's `unicode.IsSpace` is a strict
// SUBSET of Python's `str.isspace()`, missing exactly four code points:
//
//	0x1c FILE SEPARATOR, 0x1d GROUP SEPARATOR,
//	0x1e RECORD SEPARATOR, 0x1f UNIT SEPARATOR
//
// (Computed, not recalled: every code point in 0..0x10FFFF was compared between
// the two predicates; the delta is those four, in one direction only.)
//
// So `RequireOrganizationScope("\x1c")` accepted a scope Python rejects, which
// is a rejection-parity break: Python raises before any fetch, Go proceeds to a
// silent zero-row run. Found by codex round 2 on CHAOS-4441 PR2.
//
// # WHY THE NUMERIC HELPERS DELIBERATELY DO NOT USE THIS
//
// `parsePythonInt` (constants.go, PR1) and `confidenceFromString` (chquery) also
// mirror a Python whitespace rule, and they correctly use plain `TrimSpace` --
// because `int()` and `float()` do NOT accept the separator characters that
// `str.strip()` removes:
//
//	int("\x1c150")   -> ValueError        float("\x1c1.5") -> ValueError
//	" 150".strip()   -> "150"            int(" 150")     -> 150
//
// Python uses TWO different whitespace definitions and Go's `TrimSpace` happens
// to match the numeric one. Unifying these helpers "for consistency" would
// therefore BREAK the parsers while fixing nothing -- pinned by
// TestNumericParsersRejectSeparatorsLikePythonNumerics.
func pythonStrip(value string) string {
	return strings.TrimFunc(value, func(r rune) bool {
		// The four ASCII separators Python treats as whitespace and Go does not.
		if r >= 0x1c && r <= 0x1f {
			return true
		}
		return unicode.IsSpace(r)
	})
}
