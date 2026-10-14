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

// The tile is the COUNT over the canonical themes of the units (CHAOS-9622, D5999: a unit's effort_value, its
// changed lines, is not a weight), dated by their END day, every categorization status counted as on the
// Investment page, the legacy table ignored. The three units carry 100, 50 and 1000 lines and count 1 each.
func TestInvestmentByThemeIsTheCanonicalCountOfTheUnits(t *testing.T) {
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
		count, churn float64
		prs          int64
	}{
		"feature_delivery": {0.6, 60, 1}, // 1 unit x 0.6; 100 lines x 0.6 (the explicit churn_loc line); 2 PRs x 0.6 = 1.2 -> 1
		"risk":             {0.4, 40, 1}, // 1 unit x 0.4; 100 lines x 0.4; 2 PRs x 0.4 = 0.8 -> 1
		"maintenance":      {1, 50, 1},   // 1 unit; 50 lines
	}
	if len(got) != len(want) {
		t.Fatalf("themes = %v, want exactly %d canonical themes (no legacy key, no unit outside the window): %+v", order, len(want), resp.ReworkThemeAllocation)
	}
	for theme, w := range want {
		g, ok := got[theme]
		// The shares are Float32 in the shared read (the Investment page's own narrowing).
		if !ok || math.Abs(g.Allocation-w.count) > 1e-3 || float64(g.ChurnLOC) != w.churn || g.PRsMerged != w.prs {
			t.Errorf("%s = %+v, want count %v churn %v prs %d", theme, g, w.count, w.churn, w.prs)
		}
		if g.Label == "" {
			t.Errorf("%s has no label", theme)
		}
	}
	if order[0] != "maintenance" {
		t.Errorf("order = %v, want the largest count first (maintenance 1, feature_delivery 0.6, risk 0.4), not the largest effort (feature_delivery 100 lines)", order)
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

// CHAOS-9221: a repository scope reads the unit's repository ROWS (work_unit_repo_effort), not only its own
// repo_id column: a unit with a NULL own repo_id, or one that touches several repositories, is in the scope of
// each repository it touches. Before, 170 prod statements for such a repository returned no row at all.
func TestInvestmentByThemeRepositoryScopeReadsTheUnitsRepositoryRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	admin, client := newHomeTestClickHouse(ctx, t)
	const org = "org-9221-home"
	const repoA, repoB = "92210011-9221-4221-8221-922192219221", "92210012-9221-4221-8221-922192219221"
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	synced := time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC)
	for _, r := range [][2]string{{repoA, "acme/a"}, {repoB, "acme/b"}} {
		if err := admin.Exec(ctx, `INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, 'github', ?, ?, ?)`, r[0], r[1], org, synced, synced); err != nil {
			t.Fatal(err)
		}
	}
	end := day.Add(10 * time.Hour)
	// unit-own: own repo_id = A. unit-null: no own repo_id, repository rows A and B. unit-b: own repo_id = B only.
	seedThemeUnit(t, ctx, admin, org, "unit-own", repoA, end, 100, map[string]float64{"feature_delivery.roadmap": 1}, "ok", 0)
	seedThemeUnit(t, ctx, admin, org, "unit-b", repoB, end, 500, map[string]float64{"quality.bugfix": 1}, "ok", 0)
	evidence, _ := json.Marshal(map[string]any{"issues": []string{}, "prs": []string{}, "commits": []string{}, "edges": []string{}})
	if err := admin.Exec(ctx, `INSERT INTO work_unit_investments
(work_unit_id, from_ts, to_ts, repo_id, effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json, structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status, categorization_model_version, categorization_run_id, computed_at, org_id)
VALUES ('unit-null', ?, ?, NULL, 'churn_loc', 40, ?, ?, ?, 0.8, 'high', 'ok', 'jev-test', 'run-1', ?, ?)`,
		end.Add(-24*time.Hour), end, map[string]float64{"risk": 1}, map[string]float64{"risk.security": 1}, string(evidence), end.Add(time.Hour), org); err != nil {
		t.Fatal(err)
	}
	for _, repo := range []string{repoA, repoB} {
		if err := admin.Exec(ctx, `INSERT INTO work_unit_repo_effort (work_unit_id, repo_id, effort_metric, effort_value, allocation_weight, allocation_source, categorization_run_id, computed_at, org_id) VALUES ('unit-null', ?, 'churn_loc', 20, 0.5, 'pr_refs', 'run-1', ?, ?)`, repo, end.Add(time.Hour), org); err != nil {
			t.Fatal(err)
		}
	}
	tile := func(repo string) map[string]float64 {
		t.Helper()
		f := Filters{
			Time:  TimeFilter{RangeDays: 7, CompareDays: 7, EndDate: ptrTime(day.AddDate(0, 0, 3))},
			Scope: ScopeFilter{Level: "repo", IDs: []string{repo}},
		}
		if repo == "" {
			f.Scope = ScopeFilter{Level: "org"}
		}
		resp, err := BuildResponse(ctx, client, nil, org, f, day.AddDate(0, 0, 5))
		if err != nil {
			t.Fatalf("BuildResponse: %v", err)
		}
		out := map[string]float64{}
		for _, row := range resp.ReworkThemeAllocation {
			out[row.Theme] = row.Allocation
		}
		return out
	}
	a, b := tile(repoA), tile(repoB)
	if len(a) != 2 || math.Abs(a["feature_delivery"]-1) > 1e-3 || math.Abs(a["risk"]-0.5) > 1e-3 {
		t.Errorf("repository A tile = %v, want feature_delivery 1 (own column, no repository row: the whole unit) and risk 0.5 (the A row of the unit with no own repo_id)", a)
	}
	if len(b) != 2 || math.Abs(b["quality"]-1) > 1e-3 || math.Abs(b["risk"]-0.5) > 1e-3 {
		t.Errorf("repository B tile = %v, want quality 1 and risk 0.5 (the B row of the same unit)", b)
	}
	// The invariant: the repositories of a unit sum to the unit (1), never twice it; and no scope = the whole.
	if sum := a["risk"] + b["risk"]; math.Abs(sum-1) > 1e-3 {
		t.Errorf("risk over repositories A + B = %v, want the one unit", sum)
	}
	whole := tile("")
	if math.Abs(whole["risk"]-1) > 1e-3 {
		t.Errorf("unscoped risk = %v, want 1", whole["risk"])
	}
}
