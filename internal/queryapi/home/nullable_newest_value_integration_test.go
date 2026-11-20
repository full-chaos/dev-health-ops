//go:build integration

package home

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/explain"
)

// A Nullable value column (cycle_time_p50_hours, pr_first_review_p50_hours) is read from the newest row of each key,
// a NULL included, in every Home form: the answer is the same while an older version still sits in another part and
// after the parts merge, and it is explain's answer. A key whose newest row is a retraction row stays absent.

func nullableNewestDays() (prev, cur, now, older, newer time.Time) {
	prev, cur = time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC), time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
	return prev, cur, cur.AddDate(0, 0, 1).Add(12 * time.Hour), cur.Add(time.Hour), cur.Add(2 * time.Hour)
}

// nullableWorkItemRow is one org row of team on day; cycle nil stores NULL. wipCount 0 with cycle nil and wip 0 is a
// retraction row.
type nullableWorkItemRow struct {
	day      time.Time
	team     string
	cycle    any
	wip      float64
	wipCount uint32
	at       time.Time
}

func seedNullableWorkItems(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, org string, rows []nullableWorkItemRow) {
	t.Helper()
	// One INSERT per row: each version is its own part while merges are stopped.
	for _, row := range rows {
		if err := admin.Exec(ctx, `INSERT INTO work_item_metrics_daily
			(org_id, day, provider, work_scope_id, team_id, team_name, row_view, wip_count_end_of_day, wip_congestion_ratio, cycle_time_p50_hours, computed_at)
			VALUES (?, ?, 'jira', 'S', ?, ?, 'org', ?, ?, ?, ?)`, org, row.day, row.team, row.team, row.wipCount, row.wip, row.cycle, row.at); err != nil {
			t.Fatalf("insert %+v: %v", row, err)
		}
	}
}

// nullableServed is the served line of each metric (value, presence, prior, delta, spark), the summary sentence and the
// driver captions of one Home request.
func nullableServed(ctx context.Context, t *testing.T, client QueryClient, org string, metrics []string, teams ...string) (lines map[string]string, summary string, captions []string) {
	t.Helper()
	_, _, now, _, _ := nullableNewestDays()
	f := Filters{Time: TimeFilter{RangeDays: 1, CompareDays: 1, EndDate: &now}}
	if teams != nil {
		f.Scope = ScopeFilter{Level: "team", IDs: teams}
	}
	resp, err := BuildResponse(ctx, client, nil, org, f, now)
	if err != nil {
		t.Fatalf("%v BuildResponse: %v", teams, err)
	}
	lines = map[string]string{}
	for _, metric := range metrics {
		lines[metric] = metric + "=missing"
		for _, d := range resp.Deltas {
			if d.Metric != metric {
				continue
			}
			prior, pct := "nil", "nil"
			if d.PriorValue != nil {
				prior = fmt.Sprintf("%g", *d.PriorValue)
			}
			if d.DeltaPct != nil {
				pct = fmt.Sprintf("%.6g", *d.DeltaPct)
			}
			var spark []string
			for _, p := range d.Spark {
				spark = append(spark, fmt.Sprintf("%g", p.Value))
			}
			lines[metric] = fmt.Sprintf("%s=%g has=%v prior=%s pct=%s spark=[%s]", metric, d.Value, d.HasData, prior, pct, strings.Join(spark, " "))
		}
	}
	summary, captions = homeDriverText(resp)
	return lines, summary, captions
}

func nullableExplainValue(ctx context.Context, t *testing.T, client QueryClient, org, metric string, teams ...string) string {
	t.Helper()
	prev, cur, _, _, _ := nullableNewestDays()
	reader, err := explain.NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	params := explain.Params{Metric: metric, ScopeLevel: "org", StartDay: cur, EndDay: cur.AddDate(0, 0, 1), CompareStart: prev, CompareEnd: cur}
	if teams != nil {
		params.ScopeLevel, params.ScopeIDs = "team", teams
	}
	resp, err := explain.BuildExplainResponse(ctx, reader, org, params)
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%g", resp.Value)
}

func stopMerges(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, table string) {
	t.Helper()
	if err := admin.Exec(ctx, "SYSTEM STOP MERGES "+table); err != nil {
		t.Fatal(err)
	}
}

func mergeParts(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, table string) {
	t.Helper()
	if err := admin.Exec(ctx, "SYSTEM START MERGES "+table); err != nil {
		t.Fatal(err)
	}
	if err := admin.Exec(ctx, "OPTIMIZE TABLE "+table+" FINAL"); err != nil {
		t.Fatal(err)
	}
}

// Teams P and C: P prior 48 h, current 24 h; C prior 48 h, current an older row of 48 h and a newer live row with a
// NULL cycle (WIP, no completed item).
func nullableCycleSeed(withNewerNull bool) []nullableWorkItemRow {
	prev, cur, _, older, newer := nullableNewestDays()
	rows := []nullableWorkItemRow{
		{prev, "P", 48.0, 0, 1, older},
		{prev, "C", 48.0, 0, 1, older},
		{cur, "P", 24.0, 0, 1, older},
		{cur, "C", 48.0, 0, 1, older},
	}
	if withNewerNull {
		rows = append(rows, nullableWorkItemRow{cur, "C", nil, 0, 1, newer})
	}
	return rows
}

func checkCaptions(t *testing.T, what string, captions []string, want, notWant string) {
	t.Helper()
	if len(captions) == 0 {
		t.Errorf("%s: no driver caption; want one naming %s", what, want)
	}
	for _, caption := range captions {
		if !strings.Contains(caption, want) || strings.Contains(caption, notWant) {
			t.Errorf("%s: caption %q; want %s, never %s", what, caption, want, notWant)
		}
	}
}

// C1 (org form, team-set form), C2 (one team), C3 (explain): one answer in both merge states.
func TestHomeNullableCycleIsTheNewestRowInEveryMergeState(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := newHomeTestClickHouse(ctx, t)
	const org = "org-14424-home-cycle"
	stopMerges(ctx, t, admin, "work_item_metrics_daily")
	seedHomeTeamViewTeams(ctx, t, admin, org)
	seedNullableWorkItems(ctx, t, admin, org, nullableCycleSeed(true))
	// By hand: current day P 24 h, C has no cycle (its newest row is NULL): avg = 24 h = 1 d. Prior day avg(48, 48) h =
	// 2 d. -50 %. The move is P's; C has no current value, so it is no driver.
	const want = "cycle_time=1 has=true prior=2 pct=-50 spark=[1]"
	const wantSummary = "Cycle Time fell 50% driven by Platform."
	// One team C: no current value (no data), prior 2 d.
	const wantC = "cycle_time=0 has=false prior=2 pct=nil spark=[]"
	metrics := []string{"cycle_time"}
	for _, phase := range []string{"parts unmerged", "parts merged"} {
		if phase == "parts merged" {
			mergeParts(ctx, t, admin, "work_item_metrics_daily")
		}
		for _, form := range []struct {
			name  string
			teams []string
		}{{"no team", nil}, {"team set P C", []string{"P", "C"}}} {
			lines, summary, captions := nullableServed(ctx, t, client, org, metrics, form.teams...)
			wantHome(t, phase+": "+form.name, lines["cycle_time"], want)
			wantHome(t, phase+": "+form.name+" summary", summary, wantSummary)
			checkCaptions(t, phase+": "+form.name, captions, "Platform", "Core")
			wantHome(t, phase+": "+form.name+" explain", nullableExplainValue(ctx, t, client, org, "cycle_time", form.teams...), "1")
		}
		lines, summary, captions := nullableServed(ctx, t, client, org, metrics, "C")
		wantHome(t, phase+": one team C", lines["cycle_time"], wantC)
		if strings.Contains(summary, "Core") || len(captions) != 0 {
			t.Errorf("%s: one team C names a driver with no current value: summary %q captions %q", phase, summary, captions)
		}
		lines, _, _ = nullableServed(ctx, t, client, org, metrics, "P")
		wantHome(t, phase+": one team P", lines["cycle_time"], "cycle_time=1 has=true prior=2 pct=-50 spark=[1]")
	}
}

// C4: review latency reads pr_first_review_p50_hours, Nullable as cycle time is: the same rule.
func TestHomeNullableReviewLatencyIsTheNewestRowInEveryMergeState(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := newHomeTestClickHouse(ctx, t)
	const org = "org-14424-home-review"
	prev, cur, _, older, newer := nullableNewestDays()
	repoA, repoB := "0f8e2a40-0000-4000-8000-00000000000a", "0f8e2a40-0000-4000-8000-00000000000b"
	stopMerges(ctx, t, admin, "repo_metrics_daily")
	for _, row := range []struct {
		repo   string
		day    time.Time
		review any
		at     time.Time
	}{
		{repoA, prev, 4.0, older}, {repoB, prev, 4.0, older},
		{repoA, cur, 2.0, older}, {repoB, cur, 4.0, older},
		{repoB, cur, nil, newer}, // newer live row: no reviewed pull request that day
	} {
		if err := admin.Exec(ctx, `INSERT INTO repo_metrics_daily (org_id, repo_id, day, pr_first_review_p50_hours, computed_at)
			VALUES (?, toUUID(?), ?, ?, ?)`, org, row.repo, row.day, row.review, row.at); err != nil {
			t.Fatalf("insert %+v: %v", row, err)
		}
	}
	// By hand: current day repo A 2 h, repo B no review (newest row NULL): avg = 2 h. Prior avg(4, 4) = 4 h. -50 %.
	const want = "review_latency=2 has=true prior=4 pct=-50 spark=[2]"
	for _, phase := range []string{"parts unmerged", "parts merged"} {
		if phase == "parts merged" {
			mergeParts(ctx, t, admin, "repo_metrics_daily")
		}
		lines, _, _ := nullableServed(ctx, t, client, org, []string{"review_latency"})
		wantHome(t, phase+": no team", lines["review_latency"], want)
		wantHome(t, phase+": explain no team", nullableExplainValue(ctx, t, client, org, "review_latency"), "2")
	}
}

// C5: no NULL row: the answer of main (avg(24, 48) h = 1.5 d against 2 d), unchanged in both merge states.
func TestHomeNullableCycleControlWithoutANullRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := newHomeTestClickHouse(ctx, t)
	const org = "org-14424-home-control"
	stopMerges(ctx, t, admin, "work_item_metrics_daily")
	seedHomeTeamViewTeams(ctx, t, admin, org)
	seedNullableWorkItems(ctx, t, admin, org, nullableCycleSeed(false))
	const want = "cycle_time=1.5 has=true prior=2 pct=-25 spark=[1.5]"
	for _, phase := range []string{"parts unmerged", "parts merged"} {
		if phase == "parts merged" {
			mergeParts(ctx, t, admin, "work_item_metrics_daily")
		}
		for _, teams := range [][]string{nil, {"P", "C"}} {
			lines, summary, _ := nullableServed(ctx, t, client, org, []string{"cycle_time"}, teams...)
			wantHome(t, fmt.Sprintf("%s: %v", phase, teams), lines["cycle_time"], want)
			// Pinned from the base run: C's 0 % move (48 h against 48 h) is named too.
			wantHome(t, fmt.Sprintf("%s: %v summary", phase, teams), summary, "Cycle Time fell 25% driven by Core, Platform.")
		}
	}
}

// C6: the newest row of team C's current key is a retraction row (every measure 0, cycle NULL): the key is absent, so
// it adds no 0 to the WIP average, before and after the merge.
func TestHomeNullableRetractionRowKeepsTheKeyAbsent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := newHomeTestClickHouse(ctx, t)
	const org = "org-14424-home-retraction"
	prev, cur, _, older, newer := nullableNewestDays()
	stopMerges(ctx, t, admin, "work_item_metrics_daily")
	seedHomeTeamViewTeams(ctx, t, admin, org)
	seedNullableWorkItems(ctx, t, admin, org, []nullableWorkItemRow{
		{prev, "P", 48.0, 0.5, 1, older},
		{prev, "C", 48.0, 0.5, 1, older},
		{cur, "P", 24.0, 0.5, 1, older},
		{cur, "C", 48.0, 0.5, 1, older},
		{cur, "C", nil, 0, 0, newer},
	})
	// By hand: current day P only (C retracted): cycle 24 h = 1 d, WIP 0.5 = 50 %. Prior: cycle 2 d, WIP 50 %.
	want := map[string]string{
		"cycle_time":     "cycle_time=1 has=true prior=2 pct=-50 spark=[1]",
		"wip_saturation": "wip_saturation=50 has=true prior=50 pct=0 spark=[50]",
	}
	for _, phase := range []string{"parts unmerged", "parts merged"} {
		if phase == "parts merged" {
			mergeParts(ctx, t, admin, "work_item_metrics_daily")
		}
		for _, teams := range [][]string{nil, {"P", "C"}} {
			lines, _, _ := nullableServed(ctx, t, client, org, []string{"cycle_time", "wip_saturation"}, teams...)
			for metric, w := range want {
				wantHome(t, fmt.Sprintf("%s: %v %s", phase, teams, metric), lines[metric], w)
			}
		}
	}
}
