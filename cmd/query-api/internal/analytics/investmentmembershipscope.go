package analytics

// Package analytics: Go port of investment_membership_scope.py (157
// lines, e9ea257ff) and the shared helper it imports,
// resolvers/_membership_run_scope.py (e9ea257ff) -- the staleness/
// fallback state machine that gates LATEST_WORK_UNIT_INVESTMENTS_CTE's
// membership scoping, plus record_stale_investment_membership_scope, the
// telemetry hook CHAOS-4538 scope item 4 requires in THIS PR (root
// AGENTS.md: new logic ships with its telemetry in the same PR).
//
// RESTRUCTURING NOTE (§9 of the brief): every one of these Python
// fragments is a `WITH <name> AS (...)` CTE, chained into a comma
// list and referenced by name from later CTEs and from the outer
// query. The dev-health-go v0.4.0 ClickHouse client requires a literal
// SELECT as the first token and rejects a leading WITH
// (clickhouse/client.go:190) -- confirmed live in Wave 4 (4 of 6
// flowMatrix templates returned empty, silently, before that port was
// restructured; see flowmatrix.go's doc comments for the worked
// precedent this file follows). So every named CTE below becomes a
// bare Go function returning `(SELECT ...)` -- no WITH, no name -- and
// a caller that needs the same logical relation more than once (this
// file's own membershipScopedWorkUnitIDsSubquery reads
// latestCompleteMembershipRunSource AGAIN, and investment.go's
// latestWorkUnitInvestmentsSource embeds this file's whole scope-filter
// twice when it is itself referenced twice by workUnitAuthorsSource)
// calls the function again rather than sharing a materialized name.
// ClickHouse does not generally materialize a non-recursive WITH
// clause either (it is closer to a macro substitution than a temp
// table for this query shape), so this duplication is not a distinct
// performance regression versus what Python already asks ClickHouse to
// do -- it is the same query graph, spelled without names.

import (
	"fmt"

	"github.com/full-chaos/dev-health-go/clickhouse"
)

// bindingsForOrgAndTimeout is the common {org_id:String}/{timeout:UInt64}
// binding pair every membership-scope-only query in this file needs --
// factored out so FetchInvestmentMembershipScopeState (this package's
// investmentmembershiptelemetry.go) does not hand-roll it.
func bindingsForOrgAndTimeout(orgID string, timeoutSeconds int) []clickhouse.Binding {
	return []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "timeout", Value: timeoutSeconds},
	}
}

// legacyRunID mirrors _membership_run_scope.py's LEGACY_RUN_ID.
const legacyRunID = "__legacy__"

// latestCompleteMembershipRunSource ports
// investment_membership_scope.py:26-33's `latest_complete_membership_run`
// CTE body (the RICH, 3-column version investment_membership_scope.py
// defines for ITSELF -- distinct from and not to be confused with
// _membership_run_scope.py's own narrower LATEST_COMPLETE_RUN_SUBQUERY,
// which only projects latest_run_id and is used by OTHER readers like
// work_graph.py; investment_membership_scope.py never imports that
// narrower helper, it re-derives its own). No argMax nullability risk:
// run_id/completed_at are declared non-nullable on
// work_unit_membership_runs (migration 047_work_unit_membership_run_id.sql,
// `run_id String, completed_at DateTime64(3,'UTC')` -- not Nullable), so
// argMax(run_id, completed_at) has nothing to null-skip here.
func latestCompleteMembershipRunSource() string {
	return `(
        SELECT
            argMax(run_id, completed_at) AS latest_run_id,
            max(completed_at) AS latest_run_completed_at,
            count() AS marker_count
        FROM work_unit_membership_runs
        WHERE org_id = {org_id:String}
    )`
}

// latestInvestmentClockSource ports
// investment_membership_scope.py:34-38's `latest_investment_clock` CTE.
func latestInvestmentClockSource() string {
	return `(
        SELECT max(computed_at) AS latest_investment_computed_at
        FROM work_unit_investments
        WHERE org_id = {org_id:String}
    )`
}

// investmentMembershipScopeStateSource ports
// investment_membership_scope.py:39-68's `investment_membership_scope_state`
// CTE, inlined as a derived table exposing (scope_enabled, scope_mode,
// lag_seconds) -- the same three columns
// fetch_investment_membership_scope_state selects, so this one function
// serves both the WHERE-clause gate (investmentMembershipScopeEnabledExpr
// below, which reads only scope_enabled) and the telemetry query
// (FetchInvestmentMembershipScopeState, which reads scope_mode +
// lag_seconds) -- exactly mirroring how Python's single CTE feeds both
// INVESTMENT_MEMBERSHIP_SCOPE_FILTER and
// fetch_investment_membership_scope_state's own standalone SELECT.
func investmentMembershipScopeStateSource() string {
	return fmt.Sprintf(`(
        SELECT
            if(
                marker_count > 0
                AND latest_run_id != ''
                AND (
                    latest_investment_computed_at IS NULL
                    OR latest_investment_computed_at <= latest_run_completed_at
                ),
                1,
                0
            ) AS scope_enabled,
            multiIf(
                marker_count = 0 OR latest_run_id = '', 'unscoped_no_marker',
                latest_investment_computed_at IS NOT NULL
                AND latest_investment_computed_at > latest_run_completed_at,
                'unscoped_fallback',
                'scoped'
            ) AS scope_mode,
            toInt64(greatest(
                0,
                if(
                    latest_investment_computed_at IS NULL,
                    0,
                    dateDiff('second', latest_run_completed_at, latest_investment_computed_at)
                )
            )) AS lag_seconds
        FROM %s AS lcmr
        CROSS JOIN %s AS lic
    )`, latestCompleteMembershipRunSource(), latestInvestmentClockSource())
}

// legacyNodeMaxJoinSQL ports _membership_run_scope.py's LEGACY_NODE_MAX_JOIN
// verbatim (module-level constant, no line range in that file -- it is
// the entire LEGACY_NODE_MAX_JOIN assignment). Fixed aliases `m`/`lnm`,
// matching every caller's contract documented in that file's module
// doc comment.
func legacyNodeMaxJoinSQL() string {
	return `
            LEFT JOIN (
                SELECT
                    org_id,
                    node_type,
                    node_id,
                    max(computed_at) AS legacy_max_computed_at
                FROM work_unit_membership
                WHERE org_id = {org_id:String} AND run_id = ''
                GROUP BY org_id, node_type, node_id
            ) AS lnm
                ON lnm.org_id = m.org_id
                AND lnm.node_type = m.node_type
                AND lnm.node_id = m.node_id`
}

// runScopePredicateSQL ports _membership_run_scope.py's RUN_SCOPE_PREDICATE
// verbatim, substituting legacyRunID for its f-string interpolation.
func runScopePredicateSQL() string {
	return fmt.Sprintf(
		"(latest_run.latest_run_id != '%s' AND m.run_id = latest_run.latest_run_id) "+
			"OR (latest_run.latest_run_id = '%s' AND m.run_id = '' "+
			"AND m.computed_at = lnm.legacy_max_computed_at)",
		legacyRunID, legacyRunID,
	)
}

// membershipScopedWorkUnitIDsSource ports
// investment_membership_scope.py:71-81's
// `membership_scoped_work_unit_ids` CTE, inlined -- its own reference to
// `latest_complete_membership_run AS latest_run` becomes a fresh call to
// latestCompleteMembershipRunSource() rather than a name lookup.
func membershipScopedWorkUnitIDsSource() string {
	return fmt.Sprintf(`(
        SELECT DISTINCT m.work_unit_id AS work_unit_id
        FROM work_unit_membership AS m
        INNER JOIN %s AS latest_run ON 1 = 1
        %s
        WHERE m.org_id = {org_id:String}
          AND latest_run.latest_run_id != ''
          AND (%s)
    )`, latestCompleteMembershipRunSource(), legacyNodeMaxJoinSQL(), runScopePredicateSQL())
}

// investmentMembershipScopeFilter ports
// investment_membership_scope.py:88-95's INVESTMENT_MEMBERSHIP_SCOPE_FILTER
// -- the WHERE-clause fragment LATEST_WORK_UNIT_INVESTMENTS_CTE appends
// after its own `WHERE org_id = %(org_id)s`. Returned WITH its leading
// "AND (" so callers splice it directly after their own WHERE predicate,
// matching Python's exact indentation-independent semantics.
func investmentMembershipScopeFilter() string {
	return fmt.Sprintf(`
              AND (
                  (SELECT scope_enabled FROM %s) = 0
                  OR work_unit_id IN (
                      SELECT work_unit_id FROM %s
                  )
              )`, investmentMembershipScopeStateSource(), membershipScopedWorkUnitIDsSource())
}

// membershipScopeStateQuery is the standalone query
// FetchInvestmentMembershipScopeState runs -- the Go equivalent of
// investment_membership_scope.py:103-107's
// `WITH {STATE_CTES} SELECT scope_mode, lag_seconds FROM
// investment_membership_scope_state`, inlined to a bare SELECT FROM the
// derived table directly (no WITH, no intermediate name).
func membershipScopeStateQuery() string {
	return fmt.Sprintf(`
SELECT scope_mode, lag_seconds
FROM %s
SETTINGS max_execution_time = {timeout:UInt64}
`, investmentMembershipScopeStateSource())
}
