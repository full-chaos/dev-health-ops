package analytics

import "strings"

import "testing"

// TestInvestmentMembershipScopeStateSource_CrossJoinOperandsAreAliased pins
// a live-only defect found 2026-08-30 while executing PR #2022's dual-run
// harness against real ClickHouse (26.7.5): `investmentMembershipScopeStateSource()`'s
// `FROM %s CROSS JOIN %s` combined two unaliased derived tables --
// `latestCompleteMembershipRunSource()` and `latestInvestmentClockSource()`,
// neither of which self-aliases -- and ClickHouse 26.7's analyzer rejects
// that with Code 206 ALIAS_REQUIRED ("no alias for subquery or table
// function ... In scope SELECT ..."), unless
// `joined_subquery_requires_alias = 0` is set (it is not, here).
//
// This CTE was `WITH latest_complete_membership_run AS (...), latest_investment_clock
// AS (...) ... FROM latest_complete_membership_run CROSS JOIN latest_investment_clock`
// in Python (investment_membership_scope.py:39-68), where the CTE names
// serve as the join aliases for free. Porting away from `WITH` (the
// dev-health-go v0.4.0 client rejects a leading WITH, clickhouse/client.go:190
// -- this whole package's restructuring reason) dropped that implicit
// aliasing and nothing caught it: every existing test in this package uses
// a fake ClickHouse client that never executes real SQL, so a syntactically
// invalid-per-analyzer join compiled cleanly and passed every test.
// investmentMembershipScopeFilter() wires this function's SQL into the
// WHERE clause of latestWorkUnitInvestmentsSource() -- the foundation of
// nearly every investment-path query -- so every investment-path
// timeseries/breakdown/sankey/flowMatrix request was live-broken until
// this fix, regardless of the argMax/tuple-wrap correctness this package's
// other tests pin.
//
// Executed proof (not argued): the exact compiled SQL from
// CompileBreakdown(WORK_TYPE, COUNT, useInvestment=true) run via
// `docker exec dev-health-clickhouse-1 clickhouse-client` against a scratch
// database seeded through the real WorkUnitInvestmentRecord/
// ClickHouseMetricsSink.write_work_unit_investments path failed with Code
// 206 before this fix and returned `\N	1` (the CHAOS-4547 tuple-wrap's
// correct NULL, not a stale value) after it.
//
// This is a structural regression guard, not a substitute for that live
// proof -- it can only catch the class (a missing alias reappearing), not
// confirm the query is otherwise valid against a real engine.
func TestInvestmentMembershipScopeStateSource_CrossJoinOperandsAreAliased(t *testing.T) {
	sql := investmentMembershipScopeStateSource()
	if !strings.Contains(sql, "CROSS JOIN") {
		t.Fatalf("expected a CROSS JOIN in the compiled SQL, got: %s", sql)
	}
	// Split on "CROSS JOIN" and check each SIDE for its own alias
	// immediately following the closing paren of its derived table --
	// a wholesale "does the string contain AS lcmr somewhere" check
	// would pass even if the alias landed on the wrong side.
	parts := strings.SplitN(sql, "CROSS JOIN", 2)
	if len(parts) != 2 {
		t.Fatalf("expected exactly one CROSS JOIN, got: %s", sql)
	}
	left := strings.TrimSpace(parts[0])
	right := strings.TrimSpace(parts[1])
	if !strings.HasSuffix(strings.TrimRight(left, " \n"), ") AS lcmr") {
		t.Errorf("left side of CROSS JOIN is missing its alias (regression of the live ALIAS_REQUIRED fix) -- left tail: %q", tail(left, 20))
	}
	if !strings.HasPrefix(right, ") AS lic") && !strings.Contains(strings.SplitN(right, "\n", 2)[0], ") AS lic") {
		// latestInvestmentClockSource() is itself multi-line; the alias
		// lands right after ITS closing paren, which is the first line
		// of `right` here since the FROM %s / CROSS JOIN %s template has
		// no text between the placeholder and the newline.
		if !strings.Contains(right, ") AS lic") {
			t.Errorf("right side of CROSS JOIN is missing its alias (regression of the live ALIAS_REQUIRED fix) -- right head: %q", head(right, 40))
		}
	}
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}

func head(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
