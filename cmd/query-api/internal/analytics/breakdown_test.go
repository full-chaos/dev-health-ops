package analytics

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestCompileBreakdown_RejectsInvestment(t *testing.T) {
	req := BreakdownRequest{
		Dimension: DimensionRepo,
		Measure:   MeasureCount,
		StartDate: mustDate(t, "2026-01-01"),
		EndDate:   mustDate(t, "2026-01-31"),
		TopN:      10,
	}
	_, err := CompileBreakdown(req, "org-1", 30, true, nil)
	if err == nil {
		t.Fatal("expected rejection when useInvestment=true")
	}
}

func TestBreakdownRequestFromInput_TopNValidation(t *testing.T) {
	cases := []struct {
		name    string
		topN    int
		wantErr bool
	}{
		{"valid", 10, false},
		{"max boundary", 100, false},
		{"zero", 0, true},
		{"negative", -1, true},
		{"over limit", 101, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateTopN(tc.topN)
			if tc.wantErr && err == nil {
				t.Fatalf("validateTopN(%d): expected error, got nil", tc.topN)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("validateTopN(%d): unexpected error: %v", tc.topN, err)
			}
			if tc.wantErr {
				var ve *ValidationError
				if !errors.As(err, &ve) {
					t.Fatalf("expected *ValidationError, got %T", err)
				}
			}
		})
	}
}

func TestCompileBreakdown_NonInvestment_DefaultSourceAndLimit(t *testing.T) {
	req := BreakdownRequest{
		Dimension: DimensionTeam,
		Measure:   MeasureThroughput,
		StartDate: mustDate(t, "2026-01-01"),
		EndDate:   mustDate(t, "2026-01-31"),
		TopN:      25,
	}
	q, err := CompileBreakdown(req, "org-1", 30, false, nil)
	if err != nil {
		t.Fatalf("CompileBreakdown error = %v", err)
	}
	if !strings.Contains(q.sql, "team_id AS dimension_value") {
		t.Errorf("expected team_id dimension column, got: %s", q.sql)
	}
	if !strings.Contains(q.sql, "LIMIT {top_n:UInt32}") {
		t.Errorf("expected top_n limit placeholder, got: %s", q.sql)
	}
	bindings := bindingMap(q.bindings)
	if bindings["top_n"] != 25 {
		t.Errorf("top_n binding = %v, want 25", bindings["top_n"])
	}
}

func TestCompileBreakdown_TestopsCoverageMeasure(t *testing.T) {
	req := BreakdownRequest{
		Dimension: DimensionRepo,
		Measure:   MeasureCoverageLinePct,
		StartDate: mustDate(t, "2026-01-01"),
		EndDate:   mustDate(t, "2026-01-31"),
		TopN:      10,
	}
	q, err := CompileBreakdown(req, "org-1", 30, false, nil)
	if err != nil {
		t.Fatalf("CompileBreakdown error = %v", err)
	}
	if !strings.Contains(q.sql, "testops_coverage_metrics_daily") {
		t.Errorf("expected coverage source table, got: %s", q.sql)
	}
	if !strings.Contains(q.sql, "AVG(line_coverage_pct)") {
		t.Errorf("expected coverage measure expression, got: %s", q.sql)
	}
}

func TestCompileBreakdown_ReleaseImpactMeasureIsRawUndeduped(t *testing.T) {
	// CHAOS-4536: release_impact_daily has no dedup registration on the
	// Python side either -- this port must copy that gap faithfully, not
	// silently wrap it in a dedup subquery that would diverge from
	// Python's actual (undeduped) read.
	req := BreakdownRequest{
		Dimension: DimensionRepo,
		Measure:   MeasureFlagActivationRate,
		StartDate: mustDate(t, "2026-01-01"),
		EndDate:   mustDate(t, "2026-01-31"),
		TopN:      10,
	}
	q, err := CompileBreakdown(req, "org-1", 30, false, nil)
	if err != nil {
		t.Fatalf("CompileBreakdown error = %v", err)
	}
	if strings.Contains(q.sql, "LIMIT 1 BY") {
		t.Errorf("release_impact_daily must NOT be dedup-wrapped (CHAOS-4536, faithfully undeduped): %s", q.sql)
	}
	if !strings.Contains(q.sql, "FROM release_impact_daily") {
		t.Errorf("expected raw release_impact_daily FROM clause, got: %s", q.sql)
	}
}

func TestExecuteBreakdown_MapsRows(t *testing.T) {
	client := &fakeSingleClient{
		response: &fakeRowScanner{rows: [][]any{
			{"repo-a", 10.0},
			{"repo-b", 5.0},
		}},
	}
	q := compiledQuery{sql: "SELECT ..."}
	result, err := ExecuteBreakdown(context.Background(), client, q, "REPO", "COUNT")
	if err != nil {
		t.Fatalf("ExecuteBreakdown error = %v", err)
	}
	if result.Dimension != "REPO" || result.Measure != "COUNT" {
		t.Fatalf("unexpected result header: %+v", result)
	}
	if len(result.Items) != 2 {
		t.Fatalf("got %d items, want 2", len(result.Items))
	}
	if result.Items[0].Key != "repo-a" || result.Items[0].Value != 10.0 {
		t.Fatalf("unexpected items[0]: %+v", result.Items[0])
	}
	if result.Items[0].Label != nil {
		t.Fatalf("label resolution not yet ported -- expected nil label, got %v", *result.Items[0].Label)
	}
}

func TestExecuteBreakdown_QueryErrorPropagates(t *testing.T) {
	client := &fakeSingleClient{err: errors.New("boom")}
	q := compiledQuery{sql: "SELECT ..."}
	_, err := ExecuteBreakdown(context.Background(), client, q, "REPO", "COUNT")
	if err == nil {
		t.Fatal("expected error to propagate")
	}
}
