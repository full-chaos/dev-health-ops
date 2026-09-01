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
	calls     int
}

func (f *fakeSingleClient) Query(_ context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error) {
	f.statement = statement
	f.bindings = bindings
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	if f.response == nil {
		return nil, errors.New("fakeSingleClient: no response configured")
	}
	// Return a FRESH scanner each call, not the same stateful pointer --
	// fakeRowScanner's cursor advances during iteration and never resets,
	// so a caller that issues more than one query against this fake (e.g.
	// ResolveAnalyticsRepoFilters, which queries the repos table once per
	// filter field) would silently see an already-exhausted scanner on
	// its second call and read zero rows -- a fake-only artifact a real
	// ClickHouse connection would never produce (this exact bug was
	// caught by TestResolveAnalyticsRepoFilters_RewritesRepoScopeAndWhatRepos
	// failing under `go test -race ./...`, not assumed).
	return &fakeRowScanner{rows: f.response.rows, err: f.response.err, errAfter: f.response.errAfter}, nil
}

// TestCompileTimeseries_Investment_CompilesInlinedSource is CHAOS-4538's
// replacement for the retired TestCompileTimeseries_RejectsInvestment --
// investment path timeseries queries now compile, and this test pins the
// shape they must have: no leading WITH (dev-health-go v0.4.0 rejects
// any statement whose first token is not SELECT, clickhouse/client.go:190
// -- §9 of the brief; the whole reason this port had to be restructured
// away from Python's `WITH ... AS (...)` CTE chain), plus the specific
// CHAOS-4547 tuple-wrap fixes and membership-scope gate this port adds.
func TestCompileTimeseries_Investment_CompilesInlinedSource(t *testing.T) {
	req := TimeseriesRequest{
		Dimension: DimensionRepo,
		Measure:   MeasureCount,
		Interval:  BucketIntervalDay,
		StartDate: mustDate(t, "2026-01-01"),
		EndDate:   mustDate(t, "2026-01-31"),
	}
	q, err := CompileTimeseries(req, "org-1", 30, true, nil)
	if err != nil {
		t.Fatalf("CompileTimeseries error = %v", err)
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
	// membershipScopedWorkUnitIDsSource has no literal CTE name in the
	// inlined SQL text (it is a bare subquery), so assert on structural
	// markers instead: the scope_enabled scalar subquery and the
	// legacy-run predicate.
	if !strings.Contains(q.sql, "scope_enabled") {
		t.Errorf("expected investment membership scope gate (scope_enabled), got: %s", q.sql)
	}
	if !strings.Contains(q.sql, "__legacy__") {
		t.Errorf("expected legacy run-id predicate from the membership scope gate, got: %s", q.sql)
	}
	if !strings.Contains(q.sql, "ARRAY JOIN CAST(subcategory_distribution_json AS Array(Tuple(String, Float32))) AS subcategory_kv") {
		t.Errorf("expected the always-joined subcategory ARRAY JOIN, got: %s", q.sql)
	}
	if !strings.Contains(q.sql, "work_unit_investments.org_id = {org_id:String}") {
		t.Errorf("expected the work_unit_investments alias in the org_id predicate, got: %s", q.sql)
	}
	// REPO dimension triggers repo-allocation -- compiler.py's OWN inline
	// source (repoAllocationInvestmentSource), not
	// investment.py's REPO_ALLOCATED_WORK_UNIT_INVESTMENTS_SOURCE (see
	// that function's doc comment) -- distinguished by its
	// `wure.work_unit_id != ''` match flag, which the OTHER definition
	// does not use.
	if !strings.Contains(q.sql, "if(wure.work_unit_id != '', wure.repo_id, wui.repo_id) AS repo_id") {
		t.Errorf("expected compiler.py's repo-allocation source for a REPO-dimensioned investment query, got: %s", q.sql)
	}
	if !strings.Contains(q.sql, "LEFT JOIN repos AS r ON toString(r.id) = toString(repo_id)") {
		t.Errorf("expected the REPO-dimension repo join, got: %s", q.sql)
	}
	bindings := bindingMap(q.bindings)
	if bindings["org_id"] != "org-1" {
		t.Errorf("org_id binding = %v", bindings["org_id"])
	}
}

// TestCompileTimeseries_Investment_TeamDimension_UsesTeamVoteTupleWrap
// pins the CHAOS-4547 site-3 fix (buildUnitTeamSubquery's
// resolved_team argMax) reaches the compiled SQL for a TEAM-dimensioned
// investment timeseries query.
func TestCompileTimeseries_Investment_TeamDimension_UsesTeamVoteTupleWrap(t *testing.T) {
	req := TimeseriesRequest{
		Dimension: DimensionTeam,
		Measure:   MeasureCount,
		Interval:  BucketIntervalDay,
		StartDate: mustDate(t, "2026-01-01"),
		EndDate:   mustDate(t, "2026-01-31"),
	}
	q, err := CompileTimeseries(req, "org-1", 30, true, nil)
	if err != nil {
		t.Fatalf("CompileTimeseries error = %v", err)
	}
	if !strings.Contains(q.sql, "(argMax(tuple(resolved_team), (cnt, resolved_team_id))).1 AS team_label") {
		t.Errorf("expected CHAOS-4547 site-3 tuple-wrap fix on the team vote, got: %s", q.sql)
	}
	// resolved_team_id must NOT be wrapped -- its own ifNull falls back
	// to the literal '' (never NULL), so wrapping it would misrepresent
	// the audit (buildUnitTeamSubquery's doc comment).
	if strings.Contains(q.sql, "tuple(resolved_team_id)") {
		t.Errorf("resolved_team_id is never NULL -- tuple-wrapping it is an unneeded, unrepresentative change, got: %s", q.sql)
	}
	if !strings.Contains(q.sql, "ifNull(nullIf(ut.team_label, ''), 'unassigned')") {
		t.Errorf("expected the TEAM dimension column to reference ut.team_label, got: %s", q.sql)
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
	if !strings.Contains(q.sql, "toFloat64(SUM(work_items_completed)) AS value") {
		t.Errorf("expected COUNT measure expression wrapped in toFloat64 (uniform Float64 scan type), got: %s", q.sql)
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
			{mustTime("2026-01-01"), "repo-a", 1.0},
			{mustTime("2026-01-02"), "repo-a", 2.0},
			{mustTime("2026-01-01"), "repo-b", 5.0},
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

// TestExecuteTimeseries_AllNullBucketYieldsNilValue_NotZero is the
// CHAOS-4657 regression guard for ExecuteTimeseries's nullable scan --
// same shape and same both-directions requirement as CHAOS-4650's
// breakdown sibling (breakdown_test.go's
// TestExecuteBreakdown_AllNullGroupYieldsNilValue_NotZero). Proven in
// BOTH directions: a null-only assertion cannot catch a change that
// nils out EVERY value, which is the WORSE bug (a populated bucket
// silently losing its real value). One dimension_value (repo-null)
// whose measure column is SQL NULL for its one bucket must come back as
// model.TimeseriesBucket.Value == nil (JSON null on the wire, not the
// literal 0); another dimension_value (repo-real) in the SAME result
// set must still carry its real float64.
//
// RED-on-baseline: against ExecuteTimeseries's pre-fix `var value
// float64` (bare, non-pointer) destination, the fake's *float64 case
// mirrors the real clickhouse-go v2 driver's documented behaviour for a
// NULL Nullable(Float64) row (nullable.go's ScanRow: no case matches a
// bare *float64, so it returns nil WITHOUT writing to the destination)
// -- the zero-initialised 0.0 survives untouched, and this test's
// repo-null assertion (Value == nil) fails because the raw scanned
// value is 0.0, indistinguishable from a genuinely measured zero. This
// is the exact silent collapse CHAOS-4657 exists to remove.
func TestExecuteTimeseries_AllNullBucketYieldsNilValue_NotZero(t *testing.T) {
	client := &fakeSingleClient{
		response: &fakeRowScanner{rows: [][]any{
			{mustTime("2026-01-01"), "repo-null", nil},  // SQL NULL -- the all-NULL-bucket shape
			{mustTime("2026-01-01"), "repo-real", 42.5}, // populated -- the other direction
		}},
	}
	q := compiledQuery{sql: "SELECT ..."}
	results, err := ExecuteTimeseries(context.Background(), client, q, "REPO", "COVERAGE_LINE_PCT")
	if err != nil {
		t.Fatalf("ExecuteTimeseries error = %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("got %d results, want 2 (one per dimension_value)", len(results))
	}
	nullResult := results[0]
	realResult := results[1]
	if nullResult.DimensionValue != "repo-null" || len(nullResult.Buckets) != 1 || nullResult.Buckets[0].Value != nil {
		t.Fatalf("expected repo-null's bucket Value to be nil (SQL NULL scanned nullable, not silently 0.0), got %+v", nullResult)
	}
	if realResult.DimensionValue != "repo-real" || len(realResult.Buckets) != 1 || realResult.Buckets[0].Value == nil || *realResult.Buckets[0].Value != 42.5 {
		t.Fatalf("expected repo-real's bucket Value to be the real populated 42.5 -- a fix that nils out EVERY value would also pass a null-only test, got %+v", realResult)
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
				{mustTime("2026-01-01"), "repo-a", 1.0},
				{mustTime("2026-01-02"), "repo-a", 2.0},
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
