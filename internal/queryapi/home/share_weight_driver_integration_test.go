//go:build integration

package home

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// Under a team scope, a repository SUM value is weighted by the team's share of each repository (scopeShareForMetric),
// and so is its driver read (CHAOS-14441), on a real ClickHouse. acme/a: Ops share 0.1 (Web 0.9), churn 0 -> 100
// (weighted 0 -> 10). acme/b: Ops share 1.0, churn 0 -> 20 (weighted 0 -> 20). Value for [Ops]: 0 -> 30 (weighted).
// Weighted order: acme/b, acme/a. Before the weight (head ad4c53b7, log l14441-share-repro-1.log 06170e9cdc5d) the driver
// named acme/a first (unweighted 100 > 20).
func TestHomeShareWeightedDriverOfATeamScopeOnClickHouse(t *testing.T) {
	ctx := context.Background()
	admin, client := newHomeTestClickHouse(ctx, t)
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	startDay, _, compareStart, _, err := TimeWindow(DefaultFilters(), now)
	if err != nil {
		t.Fatal(err)
	}
	const org = "share-weight-driver"
	at := startDay.Add(24 * time.Hour)
	ownedFrom := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for team, name := range map[string]string{"github:acme/ops": "Ops", "github:acme/web": "Web"} {
		if err := admin.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, repo_patterns, updated_at, org_id, provider, is_active)
VALUES (?, generateUUIDv4(), ?, [], ?, ?, 'github', 1)`, team, name, at, org); err != nil {
			t.Fatal(err)
		}
	}
	type owner struct {
		team    string
		primary uint8
		share   float64
	}
	for _, r := range []struct {
		name         string
		prior, after float64
		owners       []owner
	}{
		{"acme/a", 0, 100, []owner{{"github:acme/web", 1, 0.9}, {"github:acme/ops", 2, 0.1}}},
		{"acme/b", 0, 20, []owner{{"github:acme/ops", 1, 1.0}}},
	} {
		id := uuid.New()
		if err := admin.Exec(ctx, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, 'github', ?, ?, ?)`, id, r.name, org, at, at); err != nil {
			t.Fatal(err)
		}
		for day, v := range map[time.Time]float64{compareStart: r.prior, startDay: r.after} {
			if err := admin.Exec(ctx, `INSERT INTO repo_metrics_daily (org_id, repo_id, day, total_loc_touched, computed_at) VALUES (?, ?, ?, ?, ?)`, org, id, day, v, at); err != nil {
				t.Fatal(err)
			}
		}
		for _, o := range r.owners {
			if err := admin.Exec(ctx, `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at, share)
VALUES (?, 'github', ?, ?, ?, 'exact', 'inferred', ?, 0, 0, ?, NULL, ?, ?)`, org, o.team, id, r.name, o.primary, ownedFrom, ownedFrom, o.share); err != nil {
				t.Fatal(err)
			}
		}
	}
	f := DefaultFilters()
	f.Scope = ScopeFilter{Level: "team", IDs: []string{"github:acme/ops"}}
	resp, err := BuildResponse(ctx, client, nil, org, f, now)
	if err != nil {
		t.Fatal(err)
	}
	d := driverDoubleDelta(resp, "churn")
	caption := ""
	for _, s := range resp.Signals {
		if s.ID == "metric:churn" && s.Driver != nil {
			caption = *s.Driver
		}
	}
	summary := ""
	if len(resp.Summary) > 0 {
		summary = resp.Summary[0].Text
	}
	t.Logf("VALUE churn team=[Ops] fromZero=%t value=%g summary=%q", d.FromZero(), d.Value, summary)
	t.Logf("DRIVER churn team=[Ops] caption=%q", caption)
	if !d.FromZero() || d.Value != 30 {
		t.Errorf("SEED: want a move from a measured 0 to the weighted 30, got %+v", d)
	}
	if !strings.HasSuffix(summary, " driven by acme/b, acme/a.") {
		t.Errorf("SHARE: summary %q, want the weighted order acme/b, acme/a", summary)
	}
	if caption != "Code Churn appears to have moved most in acme/b and acme/a." {
		t.Errorf("SHARE: caption %q, want the weighted order acme/b and acme/a", caption)
	}
}
