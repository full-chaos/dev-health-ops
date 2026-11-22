//go:build integration

package home

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// Four repositories (Code Churn) or four teams (Throughput) tied on the rank, on a real ClickHouse (CHAOS-14441): the
// summary and the caption name the same three drivers in the same order, the order of the id as a string. The
// repository ids sort one way as strings (a, b, c, d) and the other way as ClickHouse UUIDs (d, c, b, a). Both a move
// from a measured 0 (0 -> 5 each) and a percent move (10 -> 20 each).
func TestHomeTiedDriversHaveOneOrderOnClickHouse(t *testing.T) {
	ctx := context.Background()
	admin, client := newHomeTestClickHouse(ctx, t)
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	startDay, _, compareStart, _, err := TimeWindow(DefaultFilters(), now)
	if err != nil {
		t.Fatal(err)
	}
	at := startDay.Add(24 * time.Hour)
	for _, metric := range []string{"churn", "throughput"} {
		for _, zero := range []bool{true, false} {
			org := fmt.Sprintf("tie-order-%s-%t", metric, zero)
			prior, current := 10.0, 20.0
			if zero {
				prior, current = 0, 5
			}
			ids := tieDoubleRepos
			if metric == "throughput" {
				ids = tieDoubleTeams
			}
			for i, id := range ids {
				name := fmt.Sprintf("tie/%c", 'a'+i)
				if metric == "churn" {
					if err := admin.Exec(ctx, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, 'github', ?, ?, ?)`, id, name, org, at, at); err != nil {
						t.Fatal(err)
					}
					for d, v := range map[time.Time]float64{compareStart: prior, startDay: current} {
						if err := admin.Exec(ctx, `INSERT INTO repo_metrics_daily (org_id, repo_id, day, total_loc_touched, computed_at) VALUES (?, ?, ?, ?, ?)`, org, id, d, v, at); err != nil {
							t.Fatal(err)
						}
					}
					continue
				}
				if err := admin.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, repo_patterns, updated_at, org_id, provider, is_active)
VALUES (?, generateUUIDv4(), ?, [], ?, ?, 'github', 1)`, id, name, at, org); err != nil {
					t.Fatal(err)
				}
				ansWorkItemRow(ctx, t, admin, org, compareStart, "scope-0", id, uint32(prior), at)
				ansWorkItemRow(ctx, t, admin, org, startDay, "scope-0", id, uint32(current), at)
			}
			resp, err := BuildResponse(ctx, client, nil, org, DefaultFilters(), now)
			if err != nil {
				t.Fatal(err)
			}
			d := driverDoubleDelta(resp, metric)
			summary, caption := "", ""
			if len(resp.Summary) > 0 {
				summary = resp.Summary[0].Text
			}
			for _, s := range resp.Signals {
				if s.ID == "metric:"+metric && s.Driver != nil {
					caption = *s.Driver
				}
			}
			t.Logf("TIE-CH %s zero=%t fromZero=%t summary=%q caption=%q", metric, zero, d.FromZero(), summary, caption)
			if d.FromZero() != zero {
				t.Errorf("SEED %s zero=%t: %+v", metric, zero, d)
				continue
			}
			if !strings.HasSuffix(summary, " driven by tie/a, tie/b, tie/c.") {
				t.Errorf("TIE %s zero=%t: summary %q, want tie/a, tie/b, tie/c (the string order)", metric, zero, summary)
			}
			if want := d.Label + " appears to have moved most in tie/a, tie/b and tie/c."; caption != want {
				t.Errorf("TIE %s zero=%t: caption %q, want %q", metric, zero, caption, want)
			}
		}
	}
}
