package icfinalize

import (
	"strings"
	"testing"
	"time"
)

// The loader's parity surface is its SQL, and the differences that matter are
// single words. This pins each one against the CLICKHOUSE reference
// (loaders/clickhouse.py:1851) and, for the two that differ, against the
// SQLAlchemy implementation that must NOT be ported.
func TestRollingStatsSQLMatchesTheClickHouseReference(t *testing.T) {
	for _, required := range []string{
		"any(team_id)",            // NOT MAX(team_id) -- that is the SQLAlchemy one
		"median(cycle_p50_hours)", // NOT AVG(...)      -- that is the SQLAlchemy one
		"sum(loc_touched)",
		"sum(delivery_units)",
		"max(work_items_active)",
		"GROUP BY identity_id",
	} {
		if !strings.Contains(rollingStatsSQL, required) {
			t.Fatalf("rollingStatsSQL is missing %q", required)
		}
	}
	// The two SQLAlchemy spellings must be ABSENT. Asserting only the presence
	// of the right ones would still pass if both appeared.
	for _, forbidden := range []string{"MAX(team_id)", "AVG(cycle_p50_hours)", "avg(cycle_p50_hours)"} {
		if strings.Contains(rollingStatsSQL, forbidden) {
			t.Fatalf("rollingStatsSQL contains %q -- that is the SQLAlchemy loader "+
				"(loaders/sqlalchemy.py:389), which is DEAD on this path and "+
				"computes a different number", forbidden)
		}
	}
}

// The dedup form is not decoration: user_metrics_daily is append-only, so
// reading it raw would mix superseded generations into the 30-day sums.
func TestRollingStatsSQLDedupsOnTheCurrentNaturalKey(t *testing.T) {
	for _, required := range []string{
		"ORDER BY computed_at DESC",
		"LIMIT 1 BY org_id, repo_id, author_email, day",
	} {
		if !strings.Contains(rollingStatsSQL, required) {
			t.Fatalf("rollingStatsSQL is missing the dedup clause %q -- "+
				"append-only rows from earlier generations would be summed in", required)
		}
	}
	// FINAL is the RMT form and is wrong for this table: user_metrics_daily is
	// absent from RERUN_DEDUPED_DAILY_TABLES.
	if strings.Contains(rollingStatsSQL, "FINAL") {
		t.Fatal("rollingStatsSQL uses FINAL -- that is the ReplacingMergeTree form, " +
			"and user_metrics_daily is append-only, not an RMT")
	}
}

// Org scoping is a tenancy boundary, not a filter.
func TestRollingStatsSQLIsOrgScoped(t *testing.T) {
	if !strings.Contains(rollingStatsSQL, "org_id = {org_id:String}") {
		t.Fatal("rollingStatsSQL is not org-scoped -- one tenant's window would " +
			"aggregate another's rows")
	}
}

// The window is 30 days INCLUSIVE of as_of, mirroring
// `start = as_of - timedelta(days=29)`. An off-by-one here silently changes
// every rolling number.
func TestRollingWindowIsThirtyDaysInclusive(t *testing.T) {
	if rollingWindowDays != 29 {
		t.Fatalf("rollingWindowDays = %d, want 29 (as_of minus 29 = a 30-day "+
			"inclusive window)", rollingWindowDays)
	}
	asOf := time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC)
	start := asOf.AddDate(0, 0, -rollingWindowDays)
	if got := int(asOf.Sub(start).Hours()/24) + 1; got != 30 {
		t.Fatalf("window spans %d days, want 30", got)
	}
	if start.Format("2006-01-02") != "2026-08-06" {
		t.Fatalf("start = %s, want 2026-08-06", start.Format("2006-01-02"))
	}
}
