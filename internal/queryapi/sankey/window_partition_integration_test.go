//go:build integration

package sankey

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

// TestSankeyInvestmentFlowWindowsPartitionTheWholePeriod pins CHAOS-9016 (D6022) on the investment flow: a unit is
// dated by its end day, so the flows of adjacent windows add up to the whole period.
func TestSankeyInvestmentFlowWindowsPartitionTheWholePeriod(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := crossorg.Start(ctx, t)
	computed := time.Date(2026, 10, 2, 6, 0, 0, 0, time.UTC)
	day := func(m time.Month, d, h int) time.Time { return time.Date(2026, m, d, h, 0, 0, 0, time.UTC) }
	const org = "sankey-window-partition-org"
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
		items, err := fetchInvestmentFlowItems(ctx, client, w[0], w[1], "", nil, "", 100, org)
		if err != nil {
			t.Fatalf("fetchInvestmentFlowItems %v: %v", w, err)
		}
		sum := 0.0
		for _, item := range items {
			sum += item.Value
		}
		return sum
	}
	if got := total(whole); got != float64(len(units)) {
		t.Fatalf("whole period flow = %v, want %d", got, len(units))
	}
	if sum := total(windows[0]) + total(windows[1]); sum != float64(len(units)) {
		t.Errorf("windows add up to %v, whole period is %d: a unit counted in two windows", sum, len(units))
	}
}
