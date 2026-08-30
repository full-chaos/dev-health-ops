package analytics

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// TestCompileBreakdown_Investment_CompilesInlinedSource is CHAOS-4538's
// replacement for the retired TestCompileBreakdown_RejectsInvestment --
// investment path breakdown queries now compile, and this test pins the
// shape they must have: no leading WITH (dev-health-go v0.4.0 rejects any
// statement whose first token is not SELECT, clickhouse/client.go:190 --
// §9 of the brief), plus the CHAOS-4547 tuple-wrap fixes and
// membership-scope gate this port adds. Same investmentContextFor wiring
// as CompileTimeseries (breakdown.go's CompileBreakdown doc comment), so
// this mirrors TestCompileTimeseries_Investment_CompilesInlinedSource.
func TestCompileBreakdown_Investment_CompilesInlinedSource(t *testing.T) {
	req := BreakdownRequest{
		Dimension: DimensionRepo,
		Measure:   MeasureCount,
		StartDate: mustDate(t, "2026-01-01"),
		EndDate:   mustDate(t, "2026-01-31"),
		TopN:      10,
	}
	q, err := CompileBreakdown(req, "org-1", 30, true, nil)
	if err != nil {
		t.Fatalf("CompileBreakdown error = %v", err)
	}
	trimmed := strings.TrimSpace(q.sql)
	if !strings.HasPrefix(trimmed, "SELECT") {
		t.Fatalf("investment-path SQL must start with a literal SELECT (dev-health-go client rejects a leading WITH) -- got prefix: %q", trimmed[:min(40, len(trimmed))])
	}
	if strings.Contains(q.sql, "\nWITH ") || strings.HasPrefix(trimmed, "WITH") {
		t.Errorf("investment-path SQL must never contain a top-level WITH clause, got: %s", q.sql)
	}
	// CHAOS-4547 tuple-wrap fix: work_unit_type/work_unit_name/repo_id/
	// provider are Nullable per DDL and must be wrapped.
	for _, col := range []string{"work_unit_type", "work_unit_name", "repo_id", "provider"} {
		wrapped := "(argMax(tuple(" + col + "), computed_at)).1"
		if !strings.Contains(q.sql, wrapped) {
			t.Errorf("expected CHAOS-4547 tuple-wrap fix for %s, got: %s", col, q.sql)
		}
	}
	// Non-nullable columns stay plain argMax -- no unnecessary wrap.
	if !strings.Contains(q.sql, "argMax(effort_value, computed_at) AS effort_value") {
		t.Errorf("expected plain argMax for non-nullable effort_value, got: %s", q.sql)
	}
	if strings.Contains(q.sql, "tuple(effort_value)") {
		t.Errorf("effort_value is non-nullable Float64 -- tuple-wrapping it misrepresents the CHAOS-4547 audit, got: %s", q.sql)
	}
	// Membership-scope gate must be present (investmentmembershipscope.go).
	if !strings.Contains(q.sql, "scope_enabled") {
		t.Errorf("expected investment membership scope gate (scope_enabled), got: %s", q.sql)
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

// TestExecuteBreakdown_MidStreamFailureDiscardsPartialRows is the
// PARTIAL-ROW CLASS regression guard (BRIEF.md; found live in Lane B's
// fetchPeriodRows).
func TestExecuteBreakdown_MidStreamFailureDiscardsPartialRows(t *testing.T) {
	client := &fakeSingleClient{
		response: &fakeRowScanner{
			rows: [][]any{
				{"repo-a", 10.0},
				{"repo-b", 5.0},
			},
			err:      errors.New("mid-stream failure"),
			errAfter: 1,
		},
	}
	q := compiledQuery{sql: "SELECT ..."}
	result, err := ExecuteBreakdown(context.Background(), client, q, "REPO", "COUNT")
	if err == nil {
		t.Fatal("expected mid-stream failure to surface as an error")
	}
	if result.Items != nil {
		t.Fatalf("expected nil items on mid-stream failure, got %d partial items: %+v", len(result.Items), result.Items)
	}
}

// TestExecuteBreakdownRaw_MidStreamFailureDiscardsPartialRows tests
// executeBreakdownRaw DIRECTLY, not through ExecuteBreakdown --
// ExecuteBreakdown has its OWN unconditional discard on error ("return
// model.BreakdownResult{}, err"), which masks a regression in
// executeBreakdownRaw's own guard from any test that only goes through
// ExecuteBreakdown (empirically confirmed the same way as the
// queryNodes/queryEdges gap in flowmatrix_test.go).
func TestExecuteBreakdownRaw_MidStreamFailureDiscardsPartialRows(t *testing.T) {
	client := &fakeSingleClient{
		response: &fakeRowScanner{
			rows: [][]any{
				{"repo-a", 10.0},
				{"repo-b", 5.0},
			},
			err:      errors.New("mid-stream failure"),
			errAfter: 1,
		},
	}
	q := compiledQuery{sql: "SELECT ..."}
	rows, err := executeBreakdownRaw(context.Background(), client, q)
	if err == nil {
		t.Fatal("expected mid-stream failure to surface as an error")
	}
	if rows != nil {
		t.Fatalf("expected nil rows on mid-stream failure, got %d partial rows: %+v", len(rows), rows)
	}
}
