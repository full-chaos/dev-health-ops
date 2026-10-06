//go:build integration

package operatingreview

import (
	"testing"
	"time"
)

// TestFetchInvestmentTakesTheNewestRowOfEachKey is the reader half of the
// append-only contract of investment_metrics_daily (CHAOS-8810): the table is
// plain MergeTree, every writer appends, and a reader takes the newest
// computed_at of each key.
//
// The rows are the ones the store holds for one day after a full sync, one
// hourly sync unit with one changed item, and the daily job:
//
//	key (repo, no team, quality, general)
//	  10:00  9 delivery units   the full sync
//	  11:00  1 delivery unit    the hourly unit: only the one item it fetched
//	  11:05  10 delivery units  the daily job, from stored rows
//	key (repo, team-x, quality, general)
//	  10:00  2 delivery units   the full sync
//	  11:05  0 delivery units   the daily job: the completion left this key
//
// The reader must return 10 for the area: the newest row of each key. A read
// with no dedupe returns 22.
func TestFetchInvestmentTakesTheNewestRowOfEachKey(t *testing.T) {
	ctx, admin, client := startOperatingReviewSchema(t)

	const org = "org-8810-investment"
	const repo = "11111111-1111-4111-8111-111111111111"
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	at := func(hour, minute int) time.Time {
		return day.Add(time.Duration(hour)*time.Hour + time.Duration(minute)*time.Minute)
	}
	insert := `INSERT INTO investment_metrics_daily
		(repo_id, day, team_id, investment_area, project_stream, delivery_units,
		 work_items_completed, prs_merged, churn_loc, cycle_p50_hours, computed_at, org_id)
		VALUES (?, ?, ?, 'quality', 'general', ?, ?, 0, 0, 0, ?, ?)`
	for _, row := range []struct {
		team       string
		units      uint32
		completed  uint32
		computedAt time.Time
	}{
		{"", 9, 3, at(10, 0)},
		{"", 1, 1, at(11, 0)},
		{"", 10, 4, at(11, 5)},
		{"team-x", 2, 2, at(10, 0)},
		{"team-x", 0, 0, at(11, 5)},
	} {
		if err := admin.Exec(ctx, insert, repo, day, row.team, row.units, row.completed, row.computedAt, org); err != nil {
			t.Fatalf("insert %+v: %v", row, err)
		}
	}

	rows, err := fetchInvestment(ctx, client, org, nil, day, day.AddDate(0, 0, 1))
	if err != nil {
		t.Fatalf("fetchInvestment: %v", err)
	}
	if len(rows) != 1 || rows[0].investmentArea != "quality" {
		t.Fatalf("fetchInvestment rows = %+v, want one row of the quality area", rows)
	}
	if rows[0].deliveryUnits != 10 {
		t.Fatalf("delivery units = %v, want 10: the newest row of each key (10 + 0). 22 is every row summed, 19 is the newest key only plus the stale full-sync rows",
			rows[0].deliveryUnits)
	}

	// With a team filter the key of that team alone: its newest row is zero.
	team := "team-x"
	rows, err = fetchInvestment(ctx, client, org, &team, day, day.AddDate(0, 0, 1))
	if err != nil {
		t.Fatalf("fetchInvestment team: %v", err)
	}
	if len(rows) != 1 || rows[0].deliveryUnits != 0 {
		t.Fatalf("fetchInvestment for team-x = %+v, want one row with 0 delivery units (the newest row of the key), not the stale 2", rows)
	}
}
