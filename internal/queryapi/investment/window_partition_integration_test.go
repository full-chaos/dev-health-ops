//go:build integration

package investment

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

// TestInvestmentWindowsPartitionTheWholePeriod pins CHAOS-9016 (D6022) against a real ClickHouse: each work unit is
// dated by its END day, so adjacent windows are additive: the sum of the breakdowns, the sunburst and the quality
// counts of [Sep 1, Sep 15) and [Sep 15, Oct 1) equals the same reads over [Sep 1, Oct 1). Two units span the
// Sep 15 boundary; each must count in exactly one window.
func TestInvestmentWindowsPartitionTheWholePeriod(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := crossorg.Start(ctx, t)
	const org = "investment-window-partition-org"
	computed := time.Date(2026, 10, 2, 6, 0, 0, 0, time.UTC)
	day := func(m time.Month, d, h int) time.Time { return time.Date(2026, m, d, h, 0, 0, 0, time.UTC) }
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
	reader, err := NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	windows := [][2]time.Time{{day(9, 1, 0), day(9, 15, 0)}, {day(9, 15, 0), day(10, 1, 0)}}
	whole := [2]time.Time{day(9, 1, 0), day(10, 1, 0)}

	read := func(w [2]time.Time) (breakdown, sunburst float64, quality int) {
		resp, err := BuildResponse(ctx, reader, org, Params{StartTS: w[0], EndTS: w[1], ScopeLevel: "org"})
		if err != nil {
			t.Fatalf("BuildResponse %v: %v", w, err)
		}
		breakdown, _ = resp.SubcategoryDistribution.Get("maintenance.debt")
		slices, err := BuildSunburstResponse(ctx, reader, org, SunburstParams{StartTS: w[0], EndTS: w[1], ScopeLevel: "org", Limit: 100})
		if err != nil {
			t.Fatalf("BuildSunburstResponse %v: %v", w, err)
		}
		for _, s := range slices {
			sunburst += s.Value
		}
		row, found, err := reader.FetchInvestmentQualityStats(ctx, QualityStatsFilters{OrgID: org, StartTS: w[0], EndTS: w[1]})
		if err != nil {
			t.Fatalf("FetchInvestmentQualityStats %v: %v", w, err)
		}
		if found {
			quality = row.Total
		}
		return breakdown, sunburst, quality
	}

	wholeB, wholeS, wholeQ := read(whole)
	if wholeB != float64(len(units)) || wholeS != float64(len(units)) || wholeQ != len(units) {
		t.Fatalf("whole period = (%v, %v, %d), want %d units in each read", wholeB, wholeS, wholeQ, len(units))
	}
	var sumB, sumS float64
	var sumQ int
	for _, w := range windows {
		b, s, q := read(w)
		sumB, sumS, sumQ = sumB+b, sumS+s, sumQ+q
	}
	if math.Abs(sumB-wholeB) > 1e-6 || math.Abs(sumS-wholeS) > 1e-6 || sumQ != wholeQ {
		t.Errorf("windows add up to (%v, %v, %d), whole period is (%v, %v, %d): a unit counted in two windows", sumB, sumS, sumQ, wholeB, wholeS, wholeQ)
	}
}
