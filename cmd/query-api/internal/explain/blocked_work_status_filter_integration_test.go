//go:build integration

// This file is a regression pin for the ONE genuinely unexplained shape
// a deployed-vs-deployed comparison found in
// production for /explain's blocked_work metric: Go returned an empty
// response (value 0, no drivers, no contributors) for an org/window
// where the reference plane returned a nonzero headline and one driver.
// Every OTHER field-level difference that comparison found (tiny
// floating-point tail differences on a Nullable(Float64) column, and a
// sum-vs-avg ranking difference) is already a declared, ruled-correct
// divergence with its own citation. This one traces to the SAME already-
// ruled cause (the reference's own fetch_metric_value/
// fetch_metric_contributors/fetch_metric_driver_delta carry no status
// predicate at all, while this route's own blocked_work config applies
// one): when an org/window's ONLY matching rows for a driver's natural
// key are non-blocked-status, the reference still finds and sums them
// while this port correctly finds nothing -- a real answer of zero, not
// a bug, and the exact shape reproduced here against a live engine.
package explain

import (
	"context"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// TestBlockedWorkMetric_StatusFilterExcludesNonBlockedRows_LiveEngine
// seeds ONE row that legitimately belongs to the requested org/window but
// carries a non-blocked status, and confirms the route's own status
// predicate excludes it entirely from the headline, drivers, AND
// contributors -- an empty, zero-valued response, not an error and not a
// partial one.
func TestBlockedWorkMetric_StatusFilterExcludesNonBlockedRows_LiveEngine(t *testing.T) {
	ctx := context.Background()
	admin, client := newExplainTestClickHouse(ctx, t)

	const orgID = "org-blocked-work-excluded"
	day := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	computedAt := time.Date(2026, 9, 1, 6, 0, 0, 0, time.UTC)

	if err := admin.Exec(ctx, `
        INSERT INTO work_item_state_durations_daily
            (day, provider, work_scope_id, team_id, team_name, status, duration_hours, items_touched, computed_at, org_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `, day, "github", "proj-1", "team-x", "Team X", "in_progress", 50.0, uint32(1), computedAt, orgID); err != nil {
		t.Fatalf("seed non-blocked row: %v", err)
	}

	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	start := time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	compareStart := time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC)
	compareEnd := time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC)

	got, err := BuildExplainResponse(ctx, reader, orgID, Params{
		Metric:       "blocked_work",
		StartDay:     start,
		EndDay:       end,
		CompareStart: compareStart,
		CompareEnd:   compareEnd,
	})
	if err != nil {
		t.Fatalf("BuildExplainResponse: %v", err)
	}
	if got.Value != 0 {
		t.Errorf("value = %v, want 0 (the seeded row's status is not 'blocked')", got.Value)
	}
	if len(got.Drivers) != 0 {
		t.Errorf("drivers = %+v, want empty", got.Drivers)
	}
	if len(got.Contributors) != 0 {
		t.Errorf("contributors = %+v, want empty", got.Contributors)
	}

	// Confirm the row genuinely exists and matches every OTHER predicate
	// this route's own query applies (window, org) -- an unfiltered read
	// over the same rows finds it, which is what the reference plane's
	// own status-blind query does and why IT would report this org/
	// window as nonzero. The divergence is the status predicate, proven
	// present and doing exactly this, not an accidental empty result from
	// a broken window or org filter elsewhere in the query.
	rows, err := client.Query(ctx, `
SELECT toFloat64(sum(duration_hours)) AS value
FROM work_item_state_durations_daily
WHERE day >= {start_day:Date} AND day < {end_day:Date}
  AND org_id = {org_id:String}
`, []dhclickhouse.Binding{
		{Name: "start_day", Value: dateBindingValue(start)},
		{Name: "end_day", Value: dateBindingValue(end)},
		{Name: "org_id", Value: orgID},
	})
	if err != nil {
		t.Fatalf("unfiltered confirmation query: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("unfiltered confirmation query returned no row")
	}
	var unfiltered *float64
	if err := rows.Scan(&unfiltered); err != nil {
		t.Fatalf("scan unfiltered confirmation row: %v", err)
	}
	if unfiltered == nil || *unfiltered != 50.0 {
		t.Fatalf("unfiltered sum = %v, want 50 -- the seeded row should be visible to a status-blind read", unfiltered)
	}
}

// TestBlockedWorkMetric_StatusFilterIncludesBlockedRows_LiveEngine is this
// test's own positive-case sibling: a row that DOES carry status
// "blocked" is found, summed, and surfaced as both the headline and a
// driver -- the status predicate discriminates, it does not exclude
// everything.
func TestBlockedWorkMetric_StatusFilterIncludesBlockedRows_LiveEngine(t *testing.T) {
	ctx := context.Background()
	admin, client := newExplainTestClickHouse(ctx, t)

	const orgID = "org-blocked-work-included"
	day := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	computedAt := time.Date(2026, 9, 1, 6, 0, 0, 0, time.UTC)

	if err := admin.Exec(ctx, `
        INSERT INTO work_item_state_durations_daily
            (day, provider, work_scope_id, team_id, team_name, status, duration_hours, items_touched, computed_at, org_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `, day, "github", "proj-1", "team-x", "Team X", "blocked", 20.0, uint32(1), computedAt, orgID); err != nil {
		t.Fatalf("seed blocked row: %v", err)
	}

	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	start := time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	compareStart := time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC)
	compareEnd := time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC)

	got, err := BuildExplainResponse(ctx, reader, orgID, Params{
		Metric:       "blocked_work",
		StartDay:     start,
		EndDay:       end,
		CompareStart: compareStart,
		CompareEnd:   compareEnd,
	})
	if err != nil {
		t.Fatalf("BuildExplainResponse: %v", err)
	}
	if got.Value != 20.0 {
		t.Errorf("value = %v, want 20 (the seeded row's status IS 'blocked')", got.Value)
	}
	if len(got.Drivers) != 1 || got.Drivers[0].Value != 20.0 {
		t.Errorf("drivers = %+v, want one 20-value driver", got.Drivers)
	}
	if len(got.Contributors) != 1 || got.Contributors[0].Value != 20.0 {
		t.Errorf("contributors = %+v, want one 20-value contributor", got.Contributors)
	}
}
