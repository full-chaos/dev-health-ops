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

// TestRestInvestmentReadsTheIssueSizeAndSaysHowMuchIsSized pins CHAOS-9809 (D5999, D6027, D6031) against a real
// ClickHouse: REST /investment reads each unit's issue size (1 when unsized) by default, never its lines of code; the
// response carries the sizing coverage, and the "mostly unsized" sentence when fewer than half of the units carry a
// size. The sunburst carries no coverage, so it stays on COUNT (one per unit).
//
//   - mostly unsized org: four units (jira, gitlab, github, linear), one sized 5: total 5+1+1+1 = 8, coverage 1/4.
//   - half sized org: four units, two sized 5 and 2: total 5+2+1+1 = 9, coverage 2/4, no sentence (not below half).
func TestRestInvestmentReadsTheIssueSizeAndSaysHowMuchIsSized(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := crossorg.Start(ctx, t)
	computed := time.Date(2026, 9, 1, 6, 0, 0, 0, time.UTC)
	from := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC)
	providers := []string{"jira", "gitlab", "github", "linear"}

	for _, tc := range []struct {
		org       string
		sizes     []float64
		wantTotal float64
		wantShare float64
		wantNote  string
	}{
		{"rest-issue-size-mostly-unsized", []float64{5, 0, 0, 0}, 8, 0.25,
			"Mostly unsized: 1 of 4 work units have an issue size. The other 3 have no size, so each counts as 1 work unit."},
		{"rest-issue-size-half-sized", []float64{5, 2, 0, 0}, 9, 0.5, ""},
	} {
		t.Run(tc.org, func(t *testing.T) {
			for i, size := range tc.sizes {
				unit := tc.org + "-wu-" + providers[i]
				crossorg.Exec(ctx, t, admin, `INSERT INTO work_unit_investments (work_unit_id, from_ts, to_ts, repo_id, provider, effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json, structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status, categorization_errors_json, categorization_model_version, categorization_input_hash, categorization_run_id, computed_at, org_id)
					VALUES (?, ?, ?, ?, ?, 'churn_loc', 4000, map('maintenance', 1.0), map('maintenance.debt', 1.0), '{}', 0.7, 'moderate', 'ok', '{}', 'v1', 'hash', 'run-1', ?, ?)`,
					unit, from, to, uuid.New(), providers[i], computed, tc.org)
				if size > 0 {
					crossorg.Exec(ctx, t, admin, `INSERT INTO work_unit_issue_size (org_id, work_unit_id, issue_size, sized_issue_count, issue_count, categorization_run_id, computed_at) VALUES (?, ?, ?, 1, 1, 'run-1', ?)`,
						tc.org, unit, size, computed)
				}
			}
			reader, err := NewReader(client)
			if err != nil {
				t.Fatal(err)
			}
			got, err := BuildResponse(ctx, reader, tc.org, Params{StartTS: from.AddDate(0, 0, -1), EndTS: to.AddDate(0, 0, 1), ScopeLevel: "org"})
			if err != nil {
				t.Fatalf("BuildResponse: %v", err)
			}
			if got.Measure != "ISSUE_SIZE" || got.Denominator == nil || math.Abs(*got.Denominator-tc.wantTotal) > 1e-6 {
				t.Fatalf("measure %q denominator %v, want ISSUE_SIZE over %v (lines of code are never a weight)", got.Measure, got.Denominator, tc.wantTotal)
			}
			if value, _ := got.SubcategoryDistribution.Get("maintenance.debt"); math.Abs(value-tc.wantTotal) > 1e-6 {
				t.Errorf("maintenance.debt = %v, want %v", value, tc.wantTotal)
			}
			if got.SizingCoverage == nil || *got.SizingCoverage != tc.wantShare || got.UnsizedUnitCount == nil || *got.UnsizedUnitCount != 4-int(tc.wantShare*4) {
				t.Errorf("sizing = (%v, %v), want (%v, %d)", got.SizingCoverage, got.UnsizedUnitCount, tc.wantShare, 4-int(tc.wantShare*4))
			}
			if got.SizingNote != tc.wantNote {
				t.Errorf("sizing note = %q, want %q", got.SizingNote, tc.wantNote)
			}

			slices, err := BuildSunburstResponse(ctx, reader, tc.org, SunburstParams{StartTS: from.AddDate(0, 0, -1), EndTS: to.AddDate(0, 0, 1), ScopeLevel: "org", Limit: 100})
			if err != nil {
				t.Fatalf("BuildSunburstResponse: %v", err)
			}
			sunburst := 0.0
			for _, slice := range slices {
				sunburst += slice.Value
			}
			if math.Abs(sunburst-4) > 1e-6 {
				t.Errorf("sunburst total = %v, want the COUNT 4", sunburst)
			}
		})
	}
}
