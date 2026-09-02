// Package investment holds the native Go investment.materialize compute
// (CHAOS-4441). This file is the tenant-scope guard every entry point must
// call before reading anything.
package investment

import (
	"errors"
	"fmt"
	"strings"
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
func RequireOrganizationScope(organizationID string) error {
	// TrimSpace mirrors Python's `not (config.org_id or "").strip()` — a
	// whitespace-only org is not a scope, and accepting one would reach the
	// fetchers with a value that matches no rows rather than every row, which
	// is a different wrong answer, not a right one.
	if strings.TrimSpace(organizationID) == "" {
		return fmt.Errorf(
			"%w: refusing to run unscoped, which would read every tenant's "+
				"work_graph_edges and fuse them into shared components (CHAOS-4804)",
			ErrOrganizationScopeRequired,
		)
	}
	return nil
}
