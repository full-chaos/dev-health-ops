//go:build integration

package home

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// A move from a measured 0 in the no-team and team-set forms, two teams, on a real ClickHouse (CHAOS-14441). Main
// b737db18f3b2 named no caption, and a summary driver only for blocked_work ("Web", from its in_progress rise; log
// l14441-red-1.log 85e08322e742).
// Ops has 4 work scopes, Web has 1. Every prior row is a live measured 0 (items_started / items_touched keep it live).
//
//	metric          per-row current      value (own reducer)     own-measure order
//	cycle_time      Ops 48 h, Web 24 h   avg                     Ops, Web
//	throughput      Ops 1,    Web 3      sum: Ops 4, Web 3       Ops, Web (avg would give Web, Ops)
//	wip_saturation  Ops 0.5,  Web 0.25   avg                     Ops, Web
//	blocked_work    Ops 2,    Web 3      sum blocked: Ops 8, Web 3  Ops, Web; Web also has in_progress 1 -> 40 h
//
// NAMED (14441): the summary and the caption of the metric both name Ops and Web.
// MEASURE (CHAOS-14425): the names come in the order of the value's own reducer and status filter.
const (
	zeroPriorOps = "github:acme/ops"
	zeroPriorWeb = "github:acme/web"
)

func seedZeroPrior(ctx context.Context, t *testing.T, admin interface {
	Exec(context.Context, string, ...any) error
}, org, metric string, prior, current time.Time) {
	t.Helper()
	computedAt := current.Add(24 * time.Hour)
	perRow := map[string]map[string]float64{
		"cycle_time":     {zeroPriorOps: 48, zeroPriorWeb: 24},
		"throughput":     {zeroPriorOps: 1, zeroPriorWeb: 3},
		"wip_saturation": {zeroPriorOps: 0.5, zeroPriorWeb: 0.25},
		"blocked_work":   {zeroPriorOps: 2, zeroPriorWeb: 3},
	}[metric]
	scopes := map[string]int{zeroPriorOps: 4, zeroPriorWeb: 1}
	for _, team := range []string{zeroPriorOps, zeroPriorWeb} {
		for s := 0; s < scopes[team]; s++ {
			scope := fmt.Sprintf("scope-%d", s)
			for _, day := range []time.Time{prior, current} {
				v := 0.0
				if day.Equal(current) {
					v = perRow[team]
				}
				if metric == "blocked_work" {
					if err := admin.Exec(ctx, `INSERT INTO work_item_state_durations_daily
(day, provider, work_scope_id, team_id, team_name, status, duration_hours, items_touched, computed_at, org_id)
VALUES (?, 'github', ?, ?, '', 'blocked', ?, 1, ?, ?)`, day, scope, team, v, computedAt, org); err != nil {
						t.Fatal(err)
					}
					if team == zeroPriorWeb {
						other := 1.0
						if day.Equal(current) {
							other = 40
						}
						if err := admin.Exec(ctx, `INSERT INTO work_item_state_durations_daily
(day, provider, work_scope_id, team_id, team_name, status, duration_hours, items_touched, computed_at, org_id)
VALUES (?, 'github', ?, ?, '', 'in_progress', ?, 1, ?, ?)`, day, scope, team, other, computedAt, org); err != nil {
							t.Fatal(err)
						}
					}
					continue
				}
				var items, cycle, wip float64
				switch metric {
				case "cycle_time":
					cycle = v
				case "throughput":
					items = v
				case "wip_saturation":
					wip = v
				}
				if err := admin.Exec(ctx, `INSERT INTO work_item_metrics_daily
(org_id, day, provider, work_scope_id, team_id, team_name, items_started, items_completed,
 items_started_unassigned, items_completed_unassigned, wip_count_end_of_day, wip_unassigned_end_of_day,
 cycle_time_p50_hours, cycle_time_p90_hours, lead_time_p50_hours, lead_time_p90_hours,
 wip_age_p50_hours, wip_age_p90_hours, bug_completed_ratio, story_points_completed,
 new_bugs_count, new_items_count, defect_intro_rate, wip_congestion_ratio, predictability_score, computed_at)
VALUES (?, ?, 'github', ?, ?, '', 1, ?, 0, 0, 1, 0, ?, ?, 0, 0, 0, 0, 0, 0, 0, 0, 0, ?, 0, ?)`,
					org, day, scope, team, uint32(items), cycle, cycle, wip, computedAt); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	for team, name := range map[string]string{zeroPriorOps: "Ops", zeroPriorWeb: "Web"} {
		if err := admin.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, repo_patterns, updated_at, org_id, provider, is_active)
VALUES (?, generateUUIDv4(), ?, [], ?, ?, 'github', 1)`, team, name, computedAt, org); err != nil {
			t.Fatal(err)
		}
	}
}

func TestHomeZeroPriorDriverOnClickHouse(t *testing.T) {
	ctx := context.Background()
	admin, client := newHomeTestClickHouse(ctx, t)
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	for _, metric := range []string{"cycle_time", "throughput", "wip_saturation", "blocked_work"} {
		org := "zero-prior-14441-" + metric
		base := DefaultFilters()
		startDay, _, compareStart, _, err := TimeWindow(base, now)
		if err != nil {
			t.Fatal(err)
		}
		seedZeroPrior(ctx, t, admin, org, metric, compareStart, startDay)
		for _, form := range []struct {
			name string
			ids  []string
		}{{"no-team", nil}, {"team-set", []string{zeroPriorOps, zeroPriorWeb}}} {
			f := DefaultFilters()
			if form.ids != nil {
				f.Scope = ScopeFilter{Level: "team", IDs: form.ids}
			}
			resp, err := BuildResponse(ctx, client, nil, org, f, now)
			if err != nil {
				t.Fatal(err)
			}
			d := driverDoubleDelta(resp, metric)
			summary := ""
			if len(resp.Summary) > 0 {
				summary = resp.Summary[0].Text
			}
			caption := ""
			for _, s := range resp.Signals {
				if s.ID == "metric:"+metric && s.Driver != nil {
					caption = *s.Driver
				}
			}
			t.Logf("ZERO-PRIOR-CH %s %s fromZero=%t value=%g summary=%q caption=%q", metric, form.name, d.FromZero(), d.Value, summary, caption)
			if !d.FromZero() {
				t.Errorf("SEED %s %s: not a move from a measured 0: %+v", metric, form.name, d)
				continue
			}
			for _, name := range []string{"Ops", "Web"} {
				if !strings.Contains(summary, "driven by") || !strings.Contains(summary, name) {
					t.Errorf("NAMED %s %s: summary %q does not name %s", metric, form.name, summary, name)
				}
				if !strings.Contains(caption, name) {
					t.Errorf("NAMED %s %s: caption %q does not name %s", metric, form.name, caption, name)
				}
			}
			if !strings.HasSuffix(summary, " driven by Ops, Web.") {
				t.Errorf("MEASURE %s %s: summary %q, want the value's own order \"driven by Ops, Web.\"", metric, form.name, summary)
			}
			if want := d.Label + " appears to have moved most in Ops and Web."; caption != want {
				t.Errorf("MEASURE %s %s: caption %q, want %q", metric, form.name, caption, want)
			}
		}
	}
}
