// Package analytics: this file is the top-level port of resolve_analytics
// (analytics.py:488-981), the batch `analytics` GraphQL root -- the
// operation this whole package exists for. See the package doc comment
// (validate.go) for the CHAOS-4506/CHAOS-4538 scope split this port
// observes: NON-INVESTMENT ONLY. Every operation that resolves
// useInvestment=true (or, per validate.go's dbColumn doc comment, a
// dimension the compiler cannot serve without it) is rejected loudly by
// the Compile* functions this orchestrator calls -- never silently
// answered with wrong data.
//
// FOUR PHASES, not one flat gather -- this is the fan-out shape the
// ticket asks to be preserved, not flattened (see the PR's design notes
// for the full reasoning, cited to analytics.py line numbers):
//
//	Phase 0 (repo-filter resolution, analytics.py:555-557): ONE sequential
//	  query. FATAL on error (no try/except on the Python side).
//	Phase 1 (timeseries+breakdowns, analytics.py:584-617): every request
//	  runs CONCURRENTLY (asyncio.gather(..., return_exceptions=True) --
//	  no cancellation on first failure), but the error that surfaces is
//	  NOT whichever fails first in wall-clock time -- Python scans
//	  timeseries results by INDEX, then breakdown results by INDEX, and
//	  raises the first Exception it meets. FATAL.
//	Phase 2 (sankey, analytics.py:622-931, if requested): sequential AFTER
//	  phase 1. compile_sankey (incl. validate_sankey_path/measure) is
//	  FATAL; only the ClickHouse execution step is caught and degrades to
//	  an empty SankeyResult.
//	Phase 3 (flowMatrix, analytics.py:935-963, if requested): sequential
//	  AFTER phase 2, same fatal-compile/swallow-execute split.
//	Phase 4 (evidence quality stats, analytics.py:965-981): entirely
//	  investment-path (CHAOS-4538/CHAOS-4723). PORTED as of CHAOS-4723 --
//	  gated on useInvestment via resolveEvidenceQualityStats
//	  (investmentquality.go), mirroring _resolve_evidence_quality_stats's
//	  own gate (analytics.py:217-218, `if not bool(batch.use_investment):
//	  return None`) EXACTLY: that gate returns None when use_investment is
//	  FALSE, i.e. it PASSES (populates real data) when useInvestment is
//	  true -- the web client's default. CHAOS-4723's root cause was this
//	  file's OWN prior doc comment misreading that gate as "always nil on
//	  this port's scope" and hardcoding both fields to nil unconditionally
//	  regardless of useInvestment; see investmentquality.go for the real
//	  port of fetch_investment_quality_stats (investment.py:1008-1079,
//	  `FROM latest_work_unit_investments`). NOT wrapped in a try/except on
//	  the Python side (analytics.py:965-967, unlike sankey/flowMatrix's
//	  swallow-to-empty) -- a query failure here is FATAL, matching this
//	  port's existing fatal-by-default error handling.
package analytics

import (
	"context"
	"fmt"
	"sync"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graphqljson"
)

// Resolve is the Go port of resolve_analytics (analytics.py:488-981).
//
// `SankeyResult.Coverage` IS NOW PORTED -- see sankeycoverage.go, which
// ports the Python coverage computation (analytics.py:658-907) including
// its investment/non-investment branching, its own filter construction
// and its degrade-to-nil try/except. It previously returned an
// unconditional nil, which reached users as "Not reported" on the
// Allocation coverage tiles while Python showed real values.
// `SankeyCoverage` is still nullable in the schema
// (`coverage: SankeyCoverage`, no `!`), so nil remains a valid
// honestly-degraded value for a genuine query failure -- it is no longer
// the only value this port can produce.
//
// Breakdown item LABEL RESOLUTION (the A7/A8 display-name framework) is
// likewise not yet ported -- see breakdown.go's ExecuteBreakdown doc
// comment.
func Resolve(ctx context.Context, client QueryClient, orgID string, batch model.AnalyticsRequestInput) (*model.AnalyticsResult, error) {
	if err := validateSubRequestCount(len(batch.Timeseries), len(batch.Breakdowns), batch.Sankey != nil, batch.FlowMatrix != nil); err != nil {
		return nil, err
	}

	for i, ts := range batch.Timeseries {
		if ts.DateRange == nil {
			return nil, newValidationError("dateRange", nil, "timeseries[%d].dateRange is required", i)
		}
		if err := validateDateRange(ts.DateRange.StartDate, ts.DateRange.EndDate); err != nil {
			return nil, err
		}
		interval, err := intervalFromInput(ts.Interval)
		if err != nil {
			return nil, err
		}
		if err := validateBuckets(ts.DateRange.StartDate, ts.DateRange.EndDate, interval); err != nil {
			return nil, err
		}
	}
	for i, bd := range batch.Breakdowns {
		if bd.DateRange == nil {
			return nil, newValidationError("dateRange", nil, "breakdowns[%d].dateRange is required", i)
		}
		if err := validateDateRange(bd.DateRange.StartDate, bd.DateRange.EndDate); err != nil {
			return nil, err
		}
		if err := validateTopN(bd.TopN); err != nil {
			return nil, err
		}
	}
	if batch.Sankey != nil {
		if batch.Sankey.DateRange == nil {
			return nil, newValidationError("dateRange", nil, "sankey.dateRange is required")
		}
		if err := validateDateRange(batch.Sankey.DateRange.StartDate, batch.Sankey.DateRange.EndDate); err != nil {
			return nil, err
		}
		if err := validateSankeyLimits(batch.Sankey.MaxNodes, batch.Sankey.MaxEdges); err != nil {
			return nil, err
		}
	}
	if batch.FlowMatrix != nil {
		if batch.FlowMatrix.DateRange == nil {
			return nil, newValidationError("dateRange", nil, "flowMatrix.dateRange is required")
		}
		if err := validateDateRange(batch.FlowMatrix.DateRange.StartDate, batch.FlowMatrix.DateRange.EndDate); err != nil {
			return nil, err
		}
		if err := validateSankeyLimits(batch.FlowMatrix.MaxNodes, batch.FlowMatrix.MaxEdges); err != nil {
			return nil, err
		}
	}

	// batch-level useInvestment resolution: timeseries/breakdown ALWAYS
	// coerce to a concrete bool (analytics.py:554 `bool(batch.use_investment)`,
	// None -> false) -- never left as None, which is exactly why their
	// dimension-based auto-route can never fire (validate.go's dbColumn
	// doc comment, CHAOS-4538).
	useInvestment := batch.UseInvestment != nil && *batch.UseInvestment

	// Phase 0: repo-filter resolution. FATAL on error.
	resolvedFilters, err := ResolveAnalyticsRepoFilters(ctx, client, orgID, batch.Filters)
	if err != nil {
		return nil, fmt.Errorf("analytics: repo filter resolution: %w", err)
	}

	// Phase 1: timeseries + breakdowns, concurrent, index-ordered first
	// error (NOT first-in-wall-clock-time -- see package doc comment).
	type tsOutcome struct {
		result []model.TimeseriesResult
		err    error
	}
	type bdOutcome struct {
		result model.BreakdownResult
		err    error
	}
	tsOutcomes := make([]tsOutcome, len(batch.Timeseries))
	bdOutcomes := make([]bdOutcome, len(batch.Breakdowns))

	var wg sync.WaitGroup
	wg.Add(len(batch.Timeseries) + len(batch.Breakdowns))
	for i, input := range batch.Timeseries {
		go func(i int, input model.TimeseriesRequestInput) {
			defer wg.Done()
			tsOutcomes[i].result, tsOutcomes[i].err = resolveOneTimeseries(ctx, client, orgID, input, useInvestment, resolvedFilters)
		}(i, input)
	}
	for i, input := range batch.Breakdowns {
		go func(i int, input model.BreakdownRequestInput) {
			defer wg.Done()
			bdOutcomes[i].result, bdOutcomes[i].err = resolveOneBreakdown(ctx, client, orgID, input, useInvestment, resolvedFilters)
		}(i, input)
	}
	wg.Wait()

	// Index-ordered first-error scan: ALL of timeseries before ANY of
	// breakdowns, regardless of which index actually failed or when
	// (analytics.py:597-617's two separate enumerate loops).
	for i, o := range tsOutcomes {
		if o.err != nil {
			return nil, fmt.Errorf("analytics: timeseries[%d]: %w", i, o.err)
		}
	}
	for i, o := range bdOutcomes {
		if o.err != nil {
			return nil, fmt.Errorf("analytics: breakdown[%d]: %w", i, o.err)
		}
	}

	var timeseriesResults []model.TimeseriesResult
	for _, o := range tsOutcomes {
		timeseriesResults = append(timeseriesResults, o.result...)
	}
	breakdownResults := make([]model.BreakdownResult, 0, len(bdOutcomes))
	for _, o := range bdOutcomes {
		breakdownResults = append(breakdownResults, o.result)
	}

	// Phase 2: sankey, sequential after phase 1. Compile is FATAL;
	// execute is swallowed to an empty result.
	var sankeyResult *model.SankeyResult
	if batch.Sankey != nil {
		result, err := resolveSankey(ctx, client, orgID, *batch.Sankey, batch.UseInvestment, resolvedFilters)
		if err != nil {
			return nil, fmt.Errorf("analytics: sankey: %w", err)
		}
		sankeyResult = result
	}

	// Phase 3: flowMatrix, sequential after phase 2 (NOT concurrent with
	// it). Same fatal-compile/swallow-execute split.
	var flowMatrixResult *model.FlowMatrixResult
	if batch.FlowMatrix != nil {
		result, err := resolveFlowMatrix(ctx, client, orgID, *batch.FlowMatrix, batch.UseInvestment, resolvedFilters)
		if err != nil {
			return nil, fmt.Errorf("analytics: flowMatrix: %w", err)
		}
		flowMatrixResult = result
	}

	// Phase 4: evidence quality stats -- entirely investment-path
	// (CHAOS-4538/CHAOS-4723), gated on useInvestment. FATAL on error,
	// no swallow (see this file's package doc comment).
	evidenceQualityStats, err := resolveEvidenceQualityStats(ctx, client, orgID, batch, useInvestment, resolvedFilters)
	if err != nil {
		return nil, fmt.Errorf("analytics: evidenceQualityStats: %w", err)
	}
	// analytics.py:970-973: evidence_quality_distribution is literally
	// evidence_quality_stats.band_counts, reused, never recomputed --
	// preserve that aliasing here (same JSON bytes on both fields) rather
	// than deriving it independently.
	var evidenceQualityDistribution graphqljson.JSON
	if evidenceQualityStats != nil {
		evidenceQualityDistribution = evidenceQualityStats.BandCounts
	}

	return &model.AnalyticsResult{
		Timeseries:                  timeseriesResults,
		Breakdowns:                  breakdownResults,
		Sankey:                      sankeyResult,
		FlowMatrix:                  flowMatrixResult,
		EvidenceQualityDistribution: evidenceQualityDistribution,
		EvidenceQualityStats:        evidenceQualityStats,
	}, nil
}

func resolveOneTimeseries(ctx context.Context, client QueryClient, orgID string, input model.TimeseriesRequestInput, useInvestment bool, filters *model.FilterInput) ([]model.TimeseriesResult, error) {
	req, err := TimeseriesRequestFromInput(input)
	if err != nil {
		return nil, err
	}
	q, err := CompileTimeseries(req, orgID, queryTimeoutSecs, useInvestment, filters)
	if err != nil {
		return nil, err
	}
	if useInvestment {
		// Ports _query_investment_dicts (investment.py:175-181): EVERY
		// investment query fires the stale-membership-scope telemetry
		// check immediately before its own real query, not at compile
		// time (a compile-only caller, e.g. a future dry-run/EXPLAIN
		// path, should not pay for or emit this signal). Swallowed
		// internally on its own fetch error -- see
		// RecordStaleInvestmentMembershipScope's doc comment.
		RecordStaleInvestmentMembershipScope(ctx, client, orgID, queryTimeoutSecs)
		// CHAOS-4759 transition guard: bounded-cooldown check, see
		// RecordArgMaxNullTransitionGuard's doc comment.
		RecordArgMaxNullTransitionGuard(ctx, client, orgID, queryTimeoutSecs)
	}
	return ExecuteTimeseries(ctx, client, q, string(input.Dimension), string(input.Measure))
}

func resolveOneBreakdown(ctx context.Context, client QueryClient, orgID string, input model.BreakdownRequestInput, useInvestment bool, filters *model.FilterInput) (model.BreakdownResult, error) {
	req, err := BreakdownRequestFromInput(input)
	if err != nil {
		return model.BreakdownResult{}, err
	}
	q, err := CompileBreakdown(req, orgID, queryTimeoutSecs, useInvestment, filters)
	if err != nil {
		return model.BreakdownResult{}, err
	}
	if useInvestment {
		// See resolveOneTimeseries's identical call for the reasoning.
		RecordStaleInvestmentMembershipScope(ctx, client, orgID, queryTimeoutSecs)
		RecordArgMaxNullTransitionGuard(ctx, client, orgID, queryTimeoutSecs)
	}
	return ExecuteBreakdown(ctx, client, q, string(input.Dimension), string(input.Measure))
}

// resolveSankey ports the batch.sankey branch of resolve_analytics
// (analytics.py:622-931), MINUS coverage (see Resolve's doc comment).
// compile_sankey's own validation is FATAL (returned as an error);
// ExecuteSankeyQueries's failure is caught here and degrades to an
// empty result, matching analytics.py:646-656 exactly.
// pathAutoRoutesToInvestment mirrors _get_context_params' auto-route
// (compiler.py:152-155): with force_investment None, any of THEME,
// SUBCATEGORY or WORK_TYPE in the dimension list selects the investment
// source. WORK_TYPE is in the set because investment_metrics_daily has
// no work_item_type column, so a non-investment WORK_TYPE query is
// structurally invalid -- Python treats it as an investment dimension
// rather than emitting broken SQL.
//
// Returning true here makes CompileSankey reject the request as
// not-yet-ported, which is correct: Python would have served it from the
// investment path this port does not implement (CHAOS-4538). Rejecting
// loudly beats answering with different semantics.
func pathAutoRoutesToInvestment(path []Dimension) bool {
	for _, d := range path {
		switch d {
		case DimensionTheme, DimensionSubcategory, DimensionWorkType:
			return true
		}
	}
	return false
}

func resolveSankey(ctx context.Context, client QueryClient, orgID string, input model.SankeyRequestInput, batchUseInvestment *bool, filters *model.FilterInput) (*model.SankeyResult, error) {
	req, err := SankeyRequestFromInput(input)
	if err != nil {
		return nil, err
	}
	// Sankey resolves useInvestment as a genuine THREE-state value, and
	// collapsing it early is a real defect rather than a simplification.
	//
	// analytics.py:634-636 passes `sk_req.use_investment if ... is not
	// None else batch.use_investment` through UNWRAPPED -- note it is
	// `batch.use_investment`, NOT `bool(batch.use_investment)`. When both
	// are unset the value stays None, and _get_context_params
	// (compiler.py:152-155) then AUTO-ROUTES to the investment path for
	// any of {THEME, SUBCATEGORY, WORK_TYPE}.
	//
	// The earlier version of this code took `batchUseInvestment bool`,
	// already collapsed by Resolve, so unset became false and the
	// auto-route could never fire: Go silently applied non-investment
	// semantics where Python routes to investment -- or emitted
	// structurally invalid WORK_TYPE SQL, since investment_metrics_daily
	// has no work_item_type column. The comment above it cited
	// analytics.py:554's `bool(batch.use_investment)`, which is true for
	// timeseries/breakdown and NOT for sankey; a timeseries fact
	// generalised to sankey is how the bug got written.
	effective := input.UseInvestment
	if effective == nil {
		effective = batchUseInvestment
	}
	var useInvestment bool
	if effective != nil {
		useInvestment = *effective
	} else {
		useInvestment = pathAutoRoutesToInvestment(req.Path)
	}

	nodesQuery, edgesQueries, err := CompileSankey(req, orgID, queryTimeoutSecs, useInvestment, filters)
	if err != nil {
		return nil, err
	}
	// DELIBERATELY NO RecordStaleInvestmentMembershipScope call here.
	// Verified by reading analytics.py directly (not assumed from the
	// "timeseries/coverage/sankey" wording in _execute_breakdown_query's
	// own comment at :457-459, which overstates sankey's coverage --
	// _execute_sankey_inner (analytics.py:275-331), the function BOTH
	// sankey and flowMatrix execution route through, contains NO call
	// to record_stale_investment_membership_scope at all; only
	// _execute_timeseries_query (:359-360) and _execute_breakdown_query
	// (:462-463) do. The ONE sankey-adjacent call site that exists
	// (analytics.py:867, inside the coverage computation) is inside
	// SankeyResult.Coverage -- and now that coverage IS ported, that call
	// site is ported WITH it, inside resolveSankeyCoverage
	// (sankeycoverage.go), gated on useInvestment exactly as Python gates
	// it. It stays out of THIS function: firing it here as well would
	// double-report for one request.
	nodes, edges, execErr := ExecuteSankeyQueries(ctx, client, []compiledQuery{nodesQuery}, edgesQueries)
	if execErr != nil {
		// Swallow: analytics.py:654-656 logs and degrades to empty.
		recordDegradation(ctx, "sankey", execErr)
		nodes, edges = nil, nil
	}

	// analytics.py:658-660 -- coverage is computed AFTER nodes/edges, and
	// unconditionally whenever a sankey was requested (`if batch.sankey is
	// not None`, which is exactly the condition under which this function
	// runs at all). It degrades to nil on any failure rather than
	// propagating an error; see sankeycoverage.go.
	// COVERAGE USES THE RAW THREE-STATE FLAG, NOT THE AUTO-ROUTED ONE.
	// This asymmetry is Python's, and reproducing it is the whole point:
	// nodes/edges go through compile_sankey -> _get_context_params, which
	// AUTO-ROUTES a nil use_investment to the investment path for any of
	// {THEME, SUBCATEGORY, WORK_TYPE}. Coverage does not. It reads
	// `request.use_investment` DIRECTLY (analytics.py:665-677:
	// `bool(request.use_investment)` for the columns, and
	// `if request.use_investment` for the table), and `bool(None)` is
	// False -- so an auto-routed sankey with the flag OMITTED computes its
	// nodes/edges from latest_work_unit_investments while computing its
	// coverage from investment_metrics_daily.
	//
	// Passing the auto-routed `useInvestment` here instead was a real
	// divergence found by review: for `TEAM -> THEME` with both
	// sankey.useInvestment and batch.useInvestment omitted, Python reads
	// the daily table and Go read latest_work_unit_investments, so a daily
	// row with no overlapping current work-unit row gave Python a real
	// coverage value and Go 0/0.
	//
	// Whether Python's asymmetry is itself desirable is NOT this port's
	// call (root AGENTS.md: a port copied from a buggy tip is a defect only
	// when the bug is already fixed on the source tip -- this one is not).
	// If it is ever changed, change it in Python first.
	coverageUseInvestment := effective != nil && *effective
	coverage := resolveSankeyCoverage(ctx, client, orgID, req, queryTimeoutSecs, coverageUseInvestment, filters)

	unit := model.SankeyValueUnitWorkUnits
	if req.Measure == MeasureChurnLOC {
		unit = model.SankeyValueUnitLoc
	}

	return &model.SankeyResult{
		Nodes:    nodes,
		Edges:    edges,
		Coverage: coverage,
		Unit:     unit,
	}, nil
}

// resolveFlowMatrix ports the batch.flow_matrix branch of
// resolve_analytics (analytics.py:935-963). Same fatal-compile/
// swallow-execute split as resolveSankey.
func resolveFlowMatrix(ctx context.Context, client QueryClient, orgID string, input model.FlowMatrixRequestInput, batchUseInvestment *bool, filters *model.FilterInput) (*model.FlowMatrixResult, error) {
	req, err := FlowMatrixRequestFromInput(input)
	if err != nil {
		return nil, err
	}
	// CHAOS-4538 codex round-1 P2 fix (2026-08-30): same three-state
	// resolution bug resolveSankey above already carries the scar tissue
	// for -- see that function's doc comment for the full incident. Before
	// this fix, req.UseInvestment was left as FlowMatrixRequestFromInput
	// set it: input.UseInvestment ONLY (the nested flowMatrix.useInvestment
	// field), so an explicit batch-level `useInvestment: false` with the
	// nested field unset could never reach compileFlowMatrixInvestmentDimension's
	// resolveUseInvestment call, and THEME/SUBCATEGORY silently kept
	// auto-routing to the investment source regardless of the batch flag.
	// Reproduced red on tip fccae28d5:
	// TestResolve_FlowMatrix_BatchUseInvestmentFalse_STILL_ROUTES_TO_INVESTMENT.
	// analytics.py:944-946 resolves this the same way sankey does:
	// `fm_req.use_investment if fm_req.use_investment is not None else
	// batch.use_investment`, UNWRAPPED (not bool()'d) so a genuine "both
	// unset" state still reaches resolveUseInvestment's own dimension
	// auto-route rather than being collapsed to false here.
	if req.UseInvestment == nil {
		req.UseInvestment = batchUseInvestment
	}

	nodesQuery, edgesQuery, err := CompileFlowMatrix(req, orgID, queryTimeoutSecs, filters)
	if err != nil {
		return nil, err
	}

	nodes, edges, execErr := ExecuteFlowMatrix(ctx, client, nodesQuery, edgesQuery)
	if execErr != nil {
		// Swallow: analytics.py:959-961 logs and degrades to empty.
		recordDegradation(ctx, "flowMatrix", execErr)
		nodes, edges = nil, nil
	}

	return &model.FlowMatrixResult{Nodes: nodes, Edges: edges}, nil
}
