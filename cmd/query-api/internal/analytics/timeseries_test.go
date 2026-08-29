package analytics

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

// fakeSingleClient scripts one response for a single Query call --
// timeseries/breakdown each issue exactly one query, unlike flow-matrix's
// concurrent nodes+edges pair.
type fakeSingleClient struct {
	response  *fakeRowScanner
	err       error
	statement string
	bindings  []clickhouse.Binding
}

func (f *fakeSingleClient) Query(_ context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error) {
	f.statement = statement
	f.bindings = bindings
	if f.err != nil {
		return nil, f.err
	}
	return f.response, nil
}

func TestCompileTimeseries_RejectsInvestment(t *testing.T) {
	req := TimeseriesRequest{
		Dimension: DimensionRepo,
		Measure:   MeasureCount,
		Interval:  BucketIntervalDay,
		StartDate: mustDate(t, "2026-01-01"),
		EndDate:   mustDate(t, "2026-01-31"),
	}
	_, err := CompileTimeseries(req, "org-1", 30, true, nil)
	if err == nil {
		t.Fatal("expected rejection when useInvestment=true")
	}
	var ve *ValidationError
	if errors.As(err, &ve) {
		t.Fatalf("expected a plain not-yet-ported error, got a ValidationError: %v", err)
	}
}

func TestCompileTimeseries_NonInvestment_DefaultSource(t *testing.T) {
	req := TimeseriesRequest{
		Dimension: DimensionRepo,
		Measure:   MeasureCount,
		Interval:  BucketIntervalDay,
		StartDate: mustDate(t, "2026-01-01"),
		EndDate:   mustDate(t, "2026-01-31"),
	}
	q, err := CompileTimeseries(req, "org-1", 30, false, nil)
	if err != nil {
		t.Fatalf("CompileTimeseries error = %v", err)
	}
	if !strings.Contains(q.sql, "investment_metrics_daily") {
		t.Errorf("expected default source table in SQL, got: %s", q.sql)
	}
	if !strings.Contains(q.sql, "repo_id AS dimension_value") {
		t.Errorf("expected repo_id dimension column, got: %s", q.sql)
	}
	if !strings.Contains(q.sql, "SUM(work_items_completed) AS value") {
		t.Errorf("expected COUNT measure expression, got: %s", q.sql)
	}
	bindings := bindingMap(q.bindings)
	if bindings["org_id"] != "org-1" {
		t.Errorf("org_id binding = %v", bindings["org_id"])
	}
}

func TestCompileTimeseries_TestopsMeasureOverridesSource(t *testing.T) {
	req := TimeseriesRequest{
		Dimension: DimensionRepo,
		Measure:   MeasurePipelineDurationP95,
		Interval:  BucketIntervalDay,
		StartDate: mustDate(t, "2026-01-01"),
		EndDate:   mustDate(t, "2026-01-31"),
	}
	q, err := CompileTimeseries(req, "org-1", 30, false, nil)
	if err != nil {
		t.Fatalf("CompileTimeseries error = %v", err)
	}
	if !strings.Contains(q.sql, "testops_pipeline_metrics_daily") {
		t.Errorf("expected testops source table override, got: %s", q.sql)
	}
	if !strings.Contains(q.sql, "LIMIT 1 BY org_id, repo_id, day") {
		t.Errorf("expected append-only dedup wrapper, got: %s", q.sql)
	}
}

func TestCompileTimeseries_AuthorDimensionRejected(t *testing.T) {
	req := TimeseriesRequest{
		Dimension: DimensionAuthor,
		Measure:   MeasureCount,
		Interval:  BucketIntervalDay,
		StartDate: mustDate(t, "2026-01-01"),
		EndDate:   mustDate(t, "2026-01-31"),
	}
	_, err := CompileTimeseries(req, "org-1", 30, false, nil)
	if err == nil {
		t.Fatal("expected AUTHOR dimension to be rejected")
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T: %v", err, err)
	}
}

func TestCompileTimeseries_FiltersApplied(t *testing.T) {
	req := TimeseriesRequest{
		Dimension: DimensionRepo,
		Measure:   MeasureCount,
		Interval:  BucketIntervalDay,
		StartDate: mustDate(t, "2026-01-01"),
		EndDate:   mustDate(t, "2026-01-31"),
	}
	filters := &model.FilterInput{
		What: &model.WhatFilterInput{Repos: []string{"repo-a", "repo-b"}},
	}
	q, err := CompileTimeseries(req, "org-1", 30, false, filters)
	if err != nil {
		t.Fatalf("CompileTimeseries error = %v", err)
	}
	if !strings.Contains(q.sql, "AND repo_id IN {repo_filter_ids:Array(String)}") {
		t.Errorf("expected repo filter clause, got: %s", q.sql)
	}
	bindings := bindingMap(q.bindings)
	repos, ok := bindings["repo_filter_ids"].([]string)
	if !ok || len(repos) != 2 {
		t.Errorf("repo_filter_ids binding = %v", bindings["repo_filter_ids"])
	}
}

func TestExecuteTimeseries_GroupsByDimensionValue(t *testing.T) {
	client := &fakeSingleClient{
		response: &fakeRowScanner{rows: [][]any{
			{mustDate(t, "2026-01-01"), "repo-a", 1.0},
			{mustDate(t, "2026-01-02"), "repo-a", 2.0},
			{mustDate(t, "2026-01-01"), "repo-b", 5.0},
		}},
	}
	q := compiledQuery{sql: "SELECT ..."}
	results, err := ExecuteTimeseries(context.Background(), client, q, "REPO", "COUNT")
	if err != nil {
		t.Fatalf("ExecuteTimeseries error = %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("got %d results, want 2 (one per dimension_value)", len(results))
	}
	if results[0].DimensionValue != "repo-a" || len(results[0].Buckets) != 2 {
		t.Fatalf("unexpected results[0]: %+v", results[0])
	}
	if results[1].DimensionValue != "repo-b" || len(results[1].Buckets) != 1 {
		t.Fatalf("unexpected results[1]: %+v", results[1])
	}
}

// TestExecuteTimeseries_MidStreamFailureDiscardsPartialRows is the
// PARTIAL-ROW CLASS regression guard (BRIEF.md; found live in Lane B's
// fetchPeriodRows -- a scanner that Scan()s some rows successfully and
// THEN fails must not leave those rows feeding the caller).
func TestExecuteTimeseries_MidStreamFailureDiscardsPartialRows(t *testing.T) {
	client := &fakeSingleClient{
		response: &fakeRowScanner{
			rows: [][]any{
				{mustDate(t, "2026-01-01"), "repo-a", 1.0},
				{mustDate(t, "2026-01-02"), "repo-a", 2.0},
			},
			err:      errors.New("mid-stream failure"),
			errAfter: 1,
		},
	}
	q := compiledQuery{sql: "SELECT ..."}
	results, err := ExecuteTimeseries(context.Background(), client, q, "REPO", "COUNT")
	if err == nil {
		t.Fatal("expected mid-stream failure to surface as an error")
	}
	if results != nil {
		t.Fatalf("expected nil results on mid-stream failure, got %d partial results: %+v", len(results), results)
	}
}
