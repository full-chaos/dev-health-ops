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
		"median(cycle_p50_hours)", // NOT AVG(...)      -- that is the SQLAlchemy one
		"sum(loc_touched)",
		"sum(delivery_units)",
		"max(work_items_active)",
		"GROUP BY owner_team, identity_id",
	} {
		if !strings.Contains(rollingStatsSQL, required) {
			t.Fatalf("rollingStatsSQL is missing %q", required)
		}
	}
	// The two SQLAlchemy spellings must be ABSENT. Asserting only the presence
	// of the right ones would still pass if both appeared.
	// The team of a point is the owner of the repository of the row (ruling
	// D5985), never a team id stored on the row: no aggregate of team_id.
	for _, stored := range []string{"any(team_id)", "argMax(team_id", "u.team_id", "w.team_id", "w.team_name"} {
		if strings.Contains(rollingStatsSQL, stored) {
			t.Fatalf("rollingStatsSQL reads the stored team id (%q): the team of a point is the owner of its repository", stored)
		}
	}
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
	if strings.Contains(rollingStatsGitArmSQL, "FINAL") {
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

// The work-item counts of a point come from the work-item arm only
// (CHAOS-12535): the git arm takes the merged pull requests out of
// delivery_units (MergeICUserMetrics adds the completed items to it) and gives
// no WIP, so an item never counts on the owner of the person's repository too.
func TestTheGitArmCarriesNoWorkItemCount(t *testing.T) {
	for _, required := range []string{
		"toInt64(u.delivery_units) - toInt64(u.work_items_completed)",
		"toUInt64(0) AS work_items_active",
		"NOT has({synthesized_repo_ids:Array(String)}, toString(repo_id))",
	} {
		if !strings.Contains(rollingStatsGitArmSQL, required) {
			t.Errorf("the git arm is missing %q", required)
		}
	}
	for _, forbidden := range []string{"u.work_items_active", "work_item_user_metrics_daily"} {
		if strings.Contains(rollingStatsGitArmSQL, forbidden) {
			t.Errorf("the git arm reads %q", forbidden)
		}
	}
}

// The work-item arm reads work_item_user_metrics_daily as its other readers do
// (FINAL, the org view rows, the live rows), takes the team of each entry of
// the split, sums a day over the person's work scopes, and leaves no count
// behind: what the primary entries do not hold is unassigned.
func TestTheWorkItemArmReadsTheTeamSplit(t *testing.T) {
	for _, required := range []string{
		"FROM work_item_user_metrics_daily AS w FINAL",
		"(w.row_view IN (",
		"arrayZip(w.team_split_team_ids, w.team_split_is_primary, w.team_split_items_completed, w.team_split_wip_end_of_day)",
		"remainder -> tupleElement(remainder, 3) > 0 OR tupleElement(remainder, 4) > 0",
		"AND (tupleElement(split, 2) = 1 OR w.user_identity NOT IN ('', 'unknown', 'unassigned'))",
		"(c, p) -> p = 1, w.team_split_items_completed, w.team_split_is_primary",
		"(c, p) -> p = 1, w.team_split_wip_end_of_day, w.team_split_is_primary",
		"GROUP BY identity_id, owner_team, day",
		"AND w.org_id = {org_id:String}",
	} {
		if !strings.Contains(rollingStatsWorkItemArmSQL, required) {
			t.Errorf("the work-item arm is missing %q", required)
		}
	}
	if !strings.Contains(rollingStatsSQL, "toUInt32(max(work_items_active))") {
		t.Error("wip_max_30d is not the largest daily sum")
	}
}
