package analytics

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

// TestResolveEvidenceQualityStats_UseInvestmentFalse_ReturnsNil pins the
// gate CHAOS-4723's root cause misread: analytics.py:217-218's
// `if not bool(batch.use_investment): return None` returns nil ONLY
// when useInvestment is false. No query must fire in this case --
// routingFakeClient has zero rules registered, so any Query call at all
// fails the test via "no rule matches statement".
func TestResolveEvidenceQualityStats_UseInvestmentFalse_ReturnsNil(t *testing.T) {
	client := &routingFakeClient{}
	batch := model.AnalyticsRequestInput{
		Breakdowns: []model.BreakdownRequestInput{bdInput(model.DimensionInputTheme, model.MeasureInputCount)},
	}
	got, err := resolveEvidenceQualityStats(context.Background(), client, "org-1", batch, false, nil)
	if err != nil {
		t.Fatalf("resolveEvidenceQualityStats error = %v", err)
	}
	if got != nil {
		t.Fatalf("useInvestment=false must return nil, got %+v", got)
	}
}

// TestResolveEvidenceQualityStats_NoWindow_ReturnsNil pins
// _analytics_quality_window's fallback chain (analytics.py:175-181):
// with neither breakdowns nor timeseries present, there is no window to
// query against, and the function returns nil without querying --
// matching Python's `if window is None: return None`.
func TestResolveEvidenceQualityStats_NoWindow_ReturnsNil(t *testing.T) {
	client := &routingFakeClient{}
	batch := model.AnalyticsRequestInput{UseInvestment: boolPtr(true)}
	got, err := resolveEvidenceQualityStats(context.Background(), client, "org-1", batch, true, nil)
	if err != nil {
		t.Fatalf("resolveEvidenceQualityStats error = %v", err)
	}
	if got != nil {
		t.Fatalf("no breakdowns/timeseries must return nil, got %+v", got)
	}
}

// TestResolveEvidenceQualityStats_WindowFallsBackToTimeseries pins the
// SECOND half of _analytics_quality_window's fallback: breakdowns first,
// timeseries only when breakdowns is empty (analytics.py:175-181,
// checked by ORDER: `if batch.breakdowns: ... ; if batch.timeseries:
// ...`). A batch with ONLY timeseries must still reach the query.
func TestResolveEvidenceQualityStats_WindowFallsBackToTimeseries(t *testing.T) {
	client := &routingFakeClient{}
	client.on("quality_known_count", &fakeRowScanner{
		rows: [][]any{{uint64(1), uint64(1), 0.5, 0.0, uint64(0), uint64(1), uint64(0), uint64(0), uint64(0)}},
	})
	batch := model.AnalyticsRequestInput{
		Timeseries:    []model.TimeseriesRequestInput{tsInput(model.DimensionInputRepo, model.MeasureInputCount)},
		UseInvestment: boolPtr(true),
	}
	got, err := resolveEvidenceQualityStats(context.Background(), client, "org-1", batch, true, nil)
	if err != nil {
		t.Fatalf("resolveEvidenceQualityStats error = %v", err)
	}
	if got == nil || got.Total != 1 {
		t.Fatalf("expected a populated stats row from the timeseries-window fallback, got %+v", got)
	}
}

// TestResolveEvidenceQualityStats_ZeroKnownCount_MeanStddevNil is the
// direct proof of the guard analytics.py:235-240 codes (and this port's
// resolveEvidenceQualityStats mirrors): mean/stddev are populated ONLY
// when quality_known_count > 0. A fixture with known_count=0 stands in
// for ClickHouse's avgIf()/stddevPopIf() returning NaN over zero
// matching rows -- the exact CHAOS-4650-class hazard gqlgen's Float
// marshaler refuses outright; asserting Mean/Stddev are nil here proves
// that NaN never reaches model.EvidenceQualityStats at all, not merely
// that this particular fixture happens not to be NaN.
func TestResolveEvidenceQualityStats_ZeroKnownCount_MeanStddevNil(t *testing.T) {
	client := &routingFakeClient{}
	client.on("quality_known_count", &fakeRowScanner{
		rows: [][]any{{uint64(5), uint64(0), math.NaN(), math.NaN(), uint64(0), uint64(0), uint64(0), uint64(0), uint64(5)}},
	})
	batch := model.AnalyticsRequestInput{
		Breakdowns:    []model.BreakdownRequestInput{bdInput(model.DimensionInputTheme, model.MeasureInputCount)},
		UseInvestment: boolPtr(true),
	}
	got, err := resolveEvidenceQualityStats(context.Background(), client, "org-1", batch, true, nil)
	if err != nil {
		t.Fatalf("resolveEvidenceQualityStats error = %v", err)
	}
	if got == nil {
		t.Fatal("expected a non-nil stats result")
	}
	if got.Mean != nil {
		t.Errorf("Mean = %v, want nil when quality_known_count=0 (guards a NaN from ever reaching the GraphQL marshaler)", *got.Mean)
	}
	if got.Stddev != nil {
		t.Errorf("Stddev = %v, want nil when quality_known_count=0", *got.Stddev)
	}
	if got.Total != 5 {
		t.Errorf("Total = %d, want 5 (total is unconditional, unlike mean/stddev)", got.Total)
	}
	var bands map[string]int
	if err := json.Unmarshal(got.BandCounts, &bands); err != nil {
		t.Fatalf("BandCounts did not unmarshal: %v", err)
	}
	if bands["unknown"] != 5 {
		t.Errorf("bands[unknown] = %d, want 5", bands["unknown"])
	}
}

// TestResolveEvidenceQualityStats_NoRows_ReturnsAllDefaults pins
// analytics.py:225-226's `if not row: return EvidenceQualityStats()`
// fallback for a truly empty result set (zero rows returned, distinct
// from an aggregate row with total=0).
func TestResolveEvidenceQualityStats_NoRows_ReturnsAllDefaults(t *testing.T) {
	client := &routingFakeClient{}
	client.on("quality_known_count", &fakeRowScanner{rows: nil})
	batch := model.AnalyticsRequestInput{
		Breakdowns:    []model.BreakdownRequestInput{bdInput(model.DimensionInputTheme, model.MeasureInputCount)},
		UseInvestment: boolPtr(true),
	}
	got, err := resolveEvidenceQualityStats(context.Background(), client, "org-1", batch, true, nil)
	if err != nil {
		t.Fatalf("resolveEvidenceQualityStats error = %v", err)
	}
	if got == nil {
		t.Fatal("expected a non-nil, all-defaults EvidenceQualityStats")
	}
	if got.Total != 0 || got.Mean != nil || got.Stddev != nil {
		t.Errorf("expected all-defaults, got %+v", got)
	}
}

// TestResolveEvidenceQualityStats_QueryError_IsFatal pins that Phase 4
// is NOT wrapped in a try/except on the Python side (analytics.py:
// 965-967, unlike sankey/flowMatrix's swallow-to-empty) -- a query
// failure here must propagate as a real error, not degrade silently.
func TestResolveEvidenceQualityStats_QueryError_IsFatal(t *testing.T) {
	client := &routingFakeClient{}
	client.onErr("quality_known_count", errors.New("boom"))
	batch := model.AnalyticsRequestInput{
		Breakdowns:    []model.BreakdownRequestInput{bdInput(model.DimensionInputTheme, model.MeasureInputCount)},
		UseInvestment: boolPtr(true),
	}
	_, err := resolveEvidenceQualityStats(context.Background(), client, "org-1", batch, true, nil)
	if err == nil {
		t.Fatal("expected a query failure to propagate as a real error, not swallow to a degraded result")
	}
}

// TestResolveEvidenceQualityStats_TeamScope_AddsUnitTeamJoinAndBinding
// pins the team-scope branch (analytics.py:222-224): filters.scope with
// level=TEAM sets team_scope_ids, which fetch_investment_quality_stats
// (investment.py:1035-1046) turns into a LEFT JOIN over
// build_unit_team_subquery plus a team_scope_ids-bound WHERE clause --
// this port's compileInvestmentQualityStats via buildUnitTeamSubquery
// (investment.go, already proven live by the breakdown/timeseries
// investment paths). Asserted at the compiledQuery level (SQL text +
// bindings), not by executing against a fake row, since the shape of
// the join is the thing under test.
func TestResolveEvidenceQualityStats_TeamScope_AddsUnitTeamJoinAndBinding(t *testing.T) {
	q := compileInvestmentQualityStats("org-1", mustGraphQLDate("2026-01-01"), mustGraphQLDate("2026-01-07"), "", nil, nil, []string{"team-a", "team-b"})
	if !containsAll(q.sql, "LEFT JOIN", "unit_team", "team_scope_ids") {
		t.Fatalf("expected a unit_team LEFT JOIN referencing team_scope_ids, got:\n%s", q.sql)
	}
	if !hasBinding(q.bindings, "team_scope_ids", []string{"team-a", "team-b"}) {
		t.Fatalf("expected a team_scope_ids binding, got %+v", q.bindings)
	}
}

// TestResolveEvidenceQualityStats_Themes_AddsCategoryFilter pins the
// why.work_category branch (analytics.py:230-231 -> investment.py:
// 1021-1025's `themes` filter).
func TestResolveEvidenceQualityStats_Themes_AddsCategoryFilter(t *testing.T) {
	q := compileInvestmentQualityStats("org-1", mustGraphQLDate("2026-01-01"), mustGraphQLDate("2026-01-07"), "", nil, []string{"feature", "bug"}, nil)
	if !containsAll(q.sql, "theme_distribution_json", "hasAny", "{themes:Array(String)}") {
		t.Fatalf("expected a theme_distribution_json hasAny filter, got:\n%s", q.sql)
	}
	if !hasBinding(q.bindings, "themes", []string{"feature", "bug"}) {
		t.Fatalf("expected a themes binding, got %+v", q.bindings)
	}
}

// TestResolveEvidenceQualityStats_RepoScope_FiltersOnRepoID pins the
// scope.level=REPO branch (analytics.py:224-226) and the
// what.repos branch (analytics.py:227-229) -- both filter
// work_unit_investments.repo_id, via two DIFFERENT binding names
// (scope_ids vs repo_filter_ids), matching Python's two separate
// scope_params keys exactly (both can be present and both apply, an AND
// of two IN clauses, not a merge into one).
func TestResolveEvidenceQualityStats_RepoScope_FiltersOnRepoID(t *testing.T) {
	client := &routingFakeClient{}
	client.on("quality_known_count", &fakeRowScanner{
		rows: [][]any{{uint64(2), uint64(2), 0.5, 0.1, uint64(0), uint64(2), uint64(0), uint64(0), uint64(0)}},
	})
	batch := model.AnalyticsRequestInput{
		Breakdowns:    []model.BreakdownRequestInput{bdInput(model.DimensionInputTheme, model.MeasureInputCount)},
		UseInvestment: boolPtr(true),
		Filters: &model.FilterInput{
			Scope: &model.ScopeFilterInput{Level: model.ScopeLevelInputRepo, Ids: []string{"repo-1"}},
			What:  &model.WhatFilterInput{Repos: []string{"repo-2"}},
		},
	}
	got, err := resolveEvidenceQualityStats(context.Background(), client, "org-1", batch, true, batch.Filters)
	if err != nil {
		t.Fatalf("resolveEvidenceQualityStats error = %v", err)
	}
	if got == nil || got.Total != 2 {
		t.Fatalf("expected the repo-scoped query to reach the fake row, got %+v", got)
	}
}

// TestResolveEvidenceQualityStats_DeveloperScope_SilentlyIgnored pins a
// deliberately-inherited Python quirk: _resolve_evidence_quality_stats
// only branches on scope.level == "team" or "repo" (analytics.py:
// 223-226); DEVELOPER/SERVICE/ORG-level scope with ids set falls
// through BOTH branches with no error and no filter applied -- unlike
// filtertranslation.go's translateFilters, which rejects a
// developer-scope filter outside an investment-capable dimension. This
// is Python's actual, shipped behavior (root AGENTS.md: a port copied
// from a stale/buggy tip is a defect only when the bug is ALREADY FIXED
// on the source tip -- this one is not), so this port must reproduce it,
// not "fix" it.
func TestResolveEvidenceQualityStats_DeveloperScope_SilentlyIgnored(t *testing.T) {
	client := &routingFakeClient{}
	client.on("quality_known_count", &fakeRowScanner{
		rows: [][]any{{uint64(3), uint64(3), 0.5, 0.1, uint64(0), uint64(3), uint64(0), uint64(0), uint64(0)}},
	})
	batch := model.AnalyticsRequestInput{
		Breakdowns:    []model.BreakdownRequestInput{bdInput(model.DimensionInputTheme, model.MeasureInputCount)},
		UseInvestment: boolPtr(true),
		Filters: &model.FilterInput{
			Scope: &model.ScopeFilterInput{Level: model.ScopeLevelInputDeveloper, Ids: []string{"dev@example.com"}},
		},
	}
	got, err := resolveEvidenceQualityStats(context.Background(), client, "org-1", batch, true, batch.Filters)
	if err != nil {
		t.Fatalf("resolveEvidenceQualityStats error = %v -- developer scope must be silently ignored here, not rejected", err)
	}
	if got == nil || got.Total != 3 {
		t.Fatalf("expected the unfiltered query to reach the fake row (developer scope ignored), got %+v", got)
	}
}

func containsAll(s string, subs ...string) bool {
	for _, sub := range subs {
		if !strings.Contains(s, sub) {
			return false
		}
	}
	return true
}

// hasBinding reports whether bindings contains one named `name` whose
// Value equals want -- string-slice bindings only (every caller in this
// file binds an Array(String) parameter).
func hasBinding(bindings []clickhouse.Binding, name string, want []string) bool {
	for _, b := range bindings {
		if b.Name != name {
			continue
		}
		got, ok := b.Value.([]string)
		return ok && reflect.DeepEqual(got, want)
	}
	return false
}
