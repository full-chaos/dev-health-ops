package analytics

// Package analytics: the read side of CHAOS-4441 plan.md section 5a's
// dedup obligation. work_unit_supersessions
// (085_work_unit_supersessions.sql) is an additive sidecar recording that
// a work_unit_id was retired by a later run that regrouped its nodes
// under a different id -- nothing in work_unit_investments/
// work_unit_repo_effort marks a row dead on its own.
//
// BINDING CONDITION (plan.md section 5a, verified against
// investment_membership_scope.py:26-68 at source): this exclusion applies
// INDEPENDENTLY of investmentMembershipScopeFilter()'s own scope_enabled
// gate, not folded into that gate's OR-condition. Folding it in would
// make the sidecar inherit the exact bypass window it exists to close --
// investmentMembershipScopeStateSource's own scope_enabled is 0
// ("unscoped_fallback"/"unscoped_no_marker") for part of EVERY
// materialize run and indefinitely whenever the membership projection
// lags or fails (CHAOS-4312), which is precisely when a superseded id
// would otherwise resurrect.

// supersededWorkUnitIDsFilter returns the WHERE-clause fragment
// LatestWorkUnitInvestmentsSource appends, unconditionally, alongside its
// own `WHERE org_id = {org_id:String}` and BEFORE
// investmentMembershipScopeFilter()'s conditional clause -- ordering does
// not change the boolean result (both are ANDed) but keeps the
// unconditional exclusion textually first, matching how a reader should
// read it: always-on, then scope-gated.
//
// Only LatestWorkUnitInvestmentsSource needs this. Every other reader in
// this package composes FROM that source (repoAllocationInvestmentSource,
// workUnitAuthorsSource, and the two direct call sites in flowmatrix.go)
// rather than reading work_unit_investments or work_unit_repo_effort
// directly, so filtering the one shared source is sufficient -- verified
// by grep: latestWorkUnitRepoEffortSource, the only sibling source reading
// a raw investment table, is itself only ever joined onto
// LatestWorkUnitInvestmentsSource (investment.go:333), never read alone.
func supersededWorkUnitIDsFilter() string {
	return `
              AND work_unit_id NOT IN (
                  SELECT superseded_work_unit_id
                  FROM work_unit_supersessions
                  WHERE org_id = {org_id:String}
              )`
}
