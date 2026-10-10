//go:build integration

package home

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
)

// seedThemeUnit writes one work_unit_investments row: a unit ending at `to`, of the given effort
// (effort_metric churn_loc), split over the subcategories of `distribution`, with `prs` pull
// requests in its evidence.
func seedThemeUnit(t *testing.T, ctx context.Context, admin stdclickhouse.Conn, org, id, repo string, to time.Time, effort float64, distribution map[string]float64, status string, prs int) {
	t.Helper()
	themes := map[string]float64{}
	for subcategory, share := range distribution {
		for i := range subcategory {
			if subcategory[i] == '.' {
				themes[subcategory[:i]] += share
				break
			}
		}
	}
	prRefs := make([]string, 0, prs)
	for i := 0; i < prs; i++ {
		prRefs = append(prRefs, fmt.Sprintf("%s#pr%d", id, i))
	}
	evidence, _ := json.Marshal(map[string]any{"issues": []string{}, "prs": prRefs, "commits": []string{}, "edges": []string{}})
	if err := admin.Exec(ctx, `INSERT INTO work_unit_investments
(work_unit_id, from_ts, to_ts, repo_id, effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json, structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status, categorization_model_version, categorization_run_id, computed_at, org_id)
VALUES (?, ?, ?, ?, 'churn_loc', ?, ?, ?, ?, 0.8, 'high', ?, 'jev-test', 'run-1', ?, ?)`,
		id, to.Add(-48*time.Hour), to, repo, effort, themes, distribution, string(evidence), status, to.Add(time.Hour), org); err != nil {
		t.Fatalf("seed unit %s: %v", id, err)
	}
}

// The tile is the effort over the canonical themes of the units, dated by their END day, every
// categorization status counted as on the Investment page, the legacy table ignored.
func TestInvestmentByThemeIsTheCanonicalEffortOfTheUnits(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	admin, client := newHomeTestClickHouse(ctx, t)
	const org = "org-9195-by-theme"
	const repo = "11111111-1111-4111-8111-11111111aaaa"
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	synced := time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC)
	if err := admin.Exec(ctx, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, 'acme/a', 'github', ?, ?, ?)`, repo, org, synced, synced); err != nil {
		t.Fatal(err)
	}
	end := day.AddDate(0, 0, 3).Add(12 * time.Hour) // the window [07-27, 08-06)
	// ends in the window: 60% feature, 40% risk, effort 100, 2 PRs.
	seedThemeUnit(t, ctx, admin, org, "wu-1", repo, day.Add(10*time.Hour), 100, map[string]float64{"feature_delivery.roadmap": 0.6, "risk.security": 0.4}, "ok", 2)
	// ends in the window, insufficient evidence: counted as the Investment page counts it.
	seedThemeUnit(t, ctx, admin, org, "wu-2", repo, day.Add(11*time.Hour), 50, map[string]float64{"maintenance.debt": 1}, "insufficient_evidence", 1)
	// overlaps the window but ENDS after it: dated by its end day, so not in this window.
	seedThemeUnit(t, ctx, admin, org, "wu-3", repo, day.AddDate(0, 0, 10), 1000, map[string]float64{"quality.bugfix": 1}, "ok", 0)
	// ended before the window.
	seedThemeUnit(t, ctx, admin, org, "wu-4", repo, day.AddDate(0, 0, -30), 1000, map[string]float64{"quality.bugfix": 1}, "ok", 0)
	// The deprecated rule classifier's rows are ignored: a legacy key and a canonical one.
	if err := admin.Exec(ctx, `INSERT INTO investment_metrics_daily (repo_id, day, team_id, investment_area, project_stream, delivery_units, work_items_completed, prs_merged, churn_loc, cycle_p50_hours, computed_at, org_id)
VALUES (?, ?, '', 'product', 'general', 9, 9, 9, 9, 0, ?, ?), (?, ?, '', 'quality', 'general', 9, 9, 9, 9, 0, ?, ?)`, repo, day, synced, org, repo, day, synced, org); err != nil {
		t.Fatal(err)
	}

	f := Filters{Time: TimeFilter{RangeDays: 7, CompareDays: 7, EndDate: ptrTime(day.AddDate(0, 0, 3))}}
	resp, err := BuildResponse(ctx, client, nil, org, f, end)
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}
	got := map[string]ReworkThemeAllocation{}
	var order []string
	for _, row := range resp.ReworkThemeAllocation {
		got[row.Theme] = row
		order = append(order, row.Theme)
	}
	want := map[string]struct {
		effort, churn float64
		prs           int64
	}{
		"feature_delivery": {60, 60, 1}, // 100 x 0.6; 2 PRs x 0.6 = 1.2 -> 1
		"risk":             {40, 40, 1}, // 100 x 0.4; 2 PRs x 0.4 = 0.8 -> 1
		"maintenance":      {50, 50, 1},
	}
	if len(got) != len(want) {
		t.Fatalf("themes = %v, want exactly %d canonical themes (no legacy key, no unit outside the window): %+v", order, len(want), resp.ReworkThemeAllocation)
	}
	for theme, w := range want {
		g, ok := got[theme]
		// The shares are Float32 in the shared read (the Investment page's own narrowing).
		if !ok || math.Abs(g.Allocation-w.effort) > 1e-3 || float64(g.ChurnLOC) != w.churn || g.PRsMerged != w.prs {
			t.Errorf("%s = %+v, want effort %v churn %v prs %d", theme, g, w.effort, w.churn, w.prs)
		}
		if g.Label == "" {
			t.Errorf("%s has no label", theme)
		}
	}
	if order[0] != "feature_delivery" {
		t.Errorf("order = %v, want the largest effort first", order)
	}
	sum := 0.0
	for _, row := range resp.ReworkThemeAllocation {
		sum += row.AllocationPct
	}
	if sum < 99.999 || sum > 100.001 {
		t.Errorf("allocation_pct sums to %v, want 100", sum)
	}

	// A work-category filter narrows by theme (and by subcategory) like the Investment page's.
	f.Why = WhyFilter{WorkCategory: []string{"risk"}}
	filtered, err := BuildResponse(ctx, client, nil, org, f, end)
	if err != nil {
		t.Fatal(err)
	}
	if len(filtered.ReworkThemeAllocation) != 1 || filtered.ReworkThemeAllocation[0].Theme != "risk" {
		t.Errorf("work category risk = %+v, want the risk theme only", filtered.ReworkThemeAllocation)
	}
}
