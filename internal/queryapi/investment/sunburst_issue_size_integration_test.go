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

// TestSunburstIssueSizeIsOptInAndCarriesItsCoverage pins CHAOS-9873 (D6027, D6032) against a real ClickHouse: four
// units (jira, gitlab, github, linear), one sized 5. The default sunburst is the COUNT of the ported route (4, no
// sizing); the ISSUE_SIZE sunburst sums 5+1+1+1 = 8 and carries the coverage 1 of 4 with 3 unsized units.
func TestSunburstIssueSizeIsOptInAndCarriesItsCoverage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := crossorg.Start(ctx, t)
	const org = "sunburst-issue-size-org"
	computed := time.Date(2026, 9, 1, 6, 0, 0, 0, time.UTC)
	from := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC)
	for i, provider := range []string{"jira", "gitlab", "github", "linear"} {
		unit := org + "-wu-" + provider
		crossorg.Exec(ctx, t, admin, `INSERT INTO work_unit_investments (work_unit_id, from_ts, to_ts, repo_id, provider, effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json, structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status, categorization_errors_json, categorization_model_version, categorization_input_hash, categorization_run_id, computed_at, org_id)
			VALUES (?, ?, ?, ?, ?, 'churn_loc', 4000, map('maintenance', 1.0), map('maintenance.debt', 1.0), '{}', 0.7, 'moderate', 'ok', '{}', 'v1', 'hash', 'run-1', ?, ?)`,
			unit, from, to, uuid.New(), provider, computed, org)
		if i == 0 {
			crossorg.Exec(ctx, t, admin, `INSERT INTO work_unit_issue_size (org_id, work_unit_id, issue_size, sized_issue_count, issue_count, categorization_run_id, computed_at) VALUES (?, ?, 5, 1, 1, 'run-1', ?)`,
				org, unit, computed)
		}
	}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	params := SunburstParams{StartTS: from.AddDate(0, 0, -1), EndTS: to.AddDate(0, 0, 1), ScopeLevel: "org", Limit: 100}
	total := func(r SunburstResult) float64 {
		sum := 0.0
		for _, slice := range r.Slices {
			sum += slice.Value
		}
		return sum
	}

	plain, err := BuildSunburst(ctx, reader, org, params)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(total(plain)-4) > 1e-6 || plain.Sizing != nil {
		t.Errorf("default sunburst = %v with sizing %+v, want the COUNT 4 and no sizing", total(plain), plain.Sizing)
	}

	params.IssueSize = true
	sized, err := BuildSunburst(ctx, reader, org, params)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(total(sized)-8) > 1e-6 {
		t.Errorf("ISSUE_SIZE sunburst = %v, want 5+1+1+1 = 8 (lines of code are never a weight)", total(sized))
	}
	s := sized.Sizing
	if s == nil || s.Measure != "ISSUE_SIZE" || s.SizingCoverage == nil || *s.SizingCoverage != 0.25 ||
		s.UnsizedUnitCount == nil || *s.UnsizedUnitCount != 3 || s.WorkUnitCount == nil || *s.WorkUnitCount != 4 {
		t.Errorf("sizing = %+v, want ISSUE_SIZE 0.25 coverage, 3 unsized of 4", s)
	}
}
