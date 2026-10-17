//go:build integration

package investmentexplain

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

// TestWorkUnitListWindowsPartitionTheWholePeriod pins CHAOS-9016 (D6022): the drill-down list of a window holds
// exactly the units the window's total counts, so the lists of adjacent windows hold each unit once.
func TestWorkUnitListWindowsPartitionTheWholePeriod(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := crossorg.Start(ctx, t)
	reader, err := NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	computed := time.Date(2026, 10, 2, 6, 0, 0, 0, time.UTC)
	day := func(m time.Month, d, h int) time.Time { return time.Date(2026, m, d, h, 0, 0, 0, time.UTC) }
	const org = "explain-window-partition-org"
	units := []struct {
		id       string
		from, to time.Time
	}{
		{"inside-a", day(9, 2, 0), day(9, 4, 0)},
		{"span-1", day(9, 10, 0), day(9, 20, 0)},
		{"span-2", day(9, 14, 12), day(9, 15, 12)},
		{"ends-on-boundary-start", day(9, 14, 0), day(9, 15, 0)},
		{"inside-b", day(9, 20, 0), day(9, 25, 0)},
	}
	for _, u := range units {
		crossorg.Exec(ctx, t, admin, `INSERT INTO work_unit_investments (work_unit_id, from_ts, to_ts, repo_id, provider, effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json, structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status, categorization_errors_json, categorization_model_version, categorization_input_hash, categorization_run_id, computed_at, org_id)
			VALUES (?, ?, ?, ?, 'jira', 'churn_loc', 100, map('maintenance', 1.0), map('maintenance.debt', 1.0), '{}', 0.7, 'moderate', 'ok', '{}', 'v1', 'hash', 'run-1', ?, ?)`,
			u.id, u.from, u.to, uuid.New(), computed, org)
	}
	windows := [][2]time.Time{{day(9, 1, 0), day(9, 15, 0)}, {day(9, 15, 0), day(10, 1, 0)}}
	whole := [2]time.Time{day(9, 1, 0), day(10, 1, 0)}
	ids := func(w [2]time.Time) []string {
		rows, err := reader.FetchWorkUnitInvestments(ctx, WorkUnitInvestmentsFilter{OrgID: org, StartTS: w[0], EndTS: w[1], Limit: 100})
		if err != nil {
			t.Fatalf("FetchWorkUnitInvestments %v: %v", w, err)
		}
		out := make([]string, 0, len(rows))
		for _, row := range rows {
			out = append(out, row.WorkUnitID)
		}
		return out
	}
	if got := ids(whole); len(got) != len(units) {
		t.Fatalf("whole period lists %v, want %d units", got, len(units))
	}
	seen := map[string]int{}
	for _, w := range windows {
		for _, id := range ids(w) {
			seen[id]++
		}
	}
	for _, u := range units {
		if seen[u.id] != 1 {
			t.Errorf("unit %s listed in %d windows, want exactly 1", u.id, seen[u.id])
		}
	}
}

// TestExplainMixWindowsPartitionTheWholePeriod pins CHAOS-9016 (D6022) on the breakdown the explanation reads: the
// mix of adjacent windows adds up to the mix of the whole period.
func TestExplainMixWindowsPartitionTheWholePeriod(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := crossorg.Start(ctx, t)
	reader, err := NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	computed := time.Date(2026, 10, 2, 6, 0, 0, 0, time.UTC)
	day := func(m time.Month, d, h int) time.Time { return time.Date(2026, m, d, h, 0, 0, 0, time.UTC) }
	const org = "explain-mix-window-partition-org"
	units := []struct {
		id       string
		from, to time.Time
	}{
		{"inside-a", day(9, 2, 0), day(9, 4, 0)},
		{"span-1", day(9, 10, 0), day(9, 20, 0)},
		{"span-2", day(9, 14, 12), day(9, 15, 12)},
		{"ends-on-boundary-start", day(9, 14, 0), day(9, 15, 0)},
		{"inside-b", day(9, 20, 0), day(9, 25, 0)},
	}
	for _, u := range units {
		crossorg.Exec(ctx, t, admin, `INSERT INTO work_unit_investments (work_unit_id, from_ts, to_ts, repo_id, provider, effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json, structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status, categorization_errors_json, categorization_model_version, categorization_input_hash, categorization_run_id, computed_at, org_id)
			VALUES (?, ?, ?, ?, 'jira', 'churn_loc', 100, map('maintenance', 1.0), map('maintenance.debt', 1.0), '{}', 0.7, 'moderate', 'ok', '{}', 'v1', 'hash', 'run-1', ?, ?)`,
			u.id, u.from, u.to, uuid.New(), computed, org)
	}
	windows := [][2]time.Time{{day(9, 1, 0), day(9, 15, 0)}, {day(9, 15, 0), day(10, 1, 0)}}
	whole := [2]time.Time{day(9, 1, 0), day(10, 1, 0)}
	total := func(w [2]time.Time) float64 {
		rows, err := reader.FetchInvestmentBreakdown(ctx, mixBreakdownFilters(ExplainInvestmentMixOptions{OrgID: org, StartTS: w[0], EndTS: w[1], ScopeLevel: "org"}))
		if err != nil {
			t.Fatalf("FetchInvestmentBreakdown %v: %v", w, err)
		}
		sum := 0.0
		for _, row := range rows {
			sum += row.Value
		}
		return sum
	}
	if got := total(whole); got != float64(len(units)) {
		t.Fatalf("whole period mix = %v, want %d", got, len(units))
	}
	if sum := total(windows[0]) + total(windows[1]); sum != float64(len(units)) {
		t.Errorf("windows add up to %v, whole period is %d: a unit counted in two windows", sum, len(units))
	}
}
