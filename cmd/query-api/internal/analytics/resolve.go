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
//	Phase 4 (evidence quality stats, analytics.py:965-967): entirely
//	  investment-path (CHAOS-4538) -- always nil on this port's scope,
//	  confirmed by reading fetch_investment_quality_stats
//	  (investment.py:1008-1079, `FROM latest_work_unit_investments`) and
//	  _resolve_evidence_quality_stats's own gate (analytics.py:217-218,
//	  `if not bool(batch.use_investment): return None`).
package analytics

import (
	"context"
	"fmt"
	"sync"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

// Resolve is the Go port of resolve_analytics (analytics.py:488-981).
// NOTE: `SankeyResult.Coverage` is NOT YET PORTED -- always nil. The
// Python coverage computation (analytics.py:658-907) is a large,
// separately-complex sub-feature (~250 lines) with its own investment/
// non-investment branching, its own filter construction, and its own
// try/except; scoped out of this increment deliberately, flagged here
// rather than silently omitted. `SankeyCoverage` is nullable in the
// schema (`coverage: SankeyCoverage`, no `!`), so nil is a valid,
// honestly-degraded value, not a type violation -- but it IS a real gap
// versus Python for any caller requesting coverage, tracked as follow-up.
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
		result, err := resolveSankey(ctx, client, orgID, *batch.Sankey, useInvestment, resolvedFilters)
		if err != nil {
			return nil, fmt.Errorf("analytics: sankey: %w", err)
		}
		sankeyResult = result
	}

	// Phase 3: flowMatrix, sequential after phase 2 (NOT concurrent with
	// it). Same fatal-compile/swallow-execute split.
	var flowMatrixResult *model.FlowMatrixResult
	if batch.FlowMatrix != nil {
		result, err := resolveFlowMatrix(ctx, client, orgID, *batch.FlowMatrix, resolvedFilters)
		if err != nil {
			return nil, fmt.Errorf("analytics: flowMatrix: %w", err)
		}
		flowMatrixResult = result
	}

	// Phase 4: evidence quality stats -- entirely investment-path
	// (CHAOS-4538), always nil here. See this file's package doc comment.

	return &model.AnalyticsResult{
		Timeseries:                  timeseriesResults,
		Breakdowns:                  breakdownResults,
		Sankey:                      sankeyResult,
		FlowMatrix:                  flowMatrixResult,
		EvidenceQualityDistribution: nil,
		EvidenceQualityStats:        nil,
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
	return ExecuteBreakdown(ctx, client, q, string(input.Dimension), string(input.Measure))
}

// resolveSankey ports the batch.sankey branch of resolve_analytics
// (analytics.py:622-931), MINUS coverage (see Resolve's doc comment).
// compile_sankey's own validation is FATAL (returned as an error);
// ExecuteSankeyQueries's failure is caught here and degrades to an
// empty result, matching analytics.py:646-656 exactly.
func resolveSankey(ctx context.Context, client QueryClient, orgID string, input model.SankeyRequestInput, batchUseInvestment bool, filters *model.FilterInput) (*model.SankeyResult, error) {
	req, err := SankeyRequestFromInput(input)
	if err != nil {
		return nil, err
	}
	// Per-request useInvestment overrides the batch flag when set
	// (analytics.py: `sk_req.use_investment if sk_req.use_investment is
	// not None else batch.use_investment`) -- unlike timeseries/breakdown,
	// this preserves a genuine three-state resolution rather than
	// collapsing to a bool up front. This port's Compile* functions only
	// accept a bool, so `nil` (both unset) resolves to the batch flag,
	// matching Python's fallback.
	useInvestment := batchUseInvestment
	if input.UseInvestment != nil {
		useInvestment = *input.UseInvestment
	}

	nodesQuery, edgesQueries, err := CompileSankey(req, orgID, queryTimeoutSecs, useInvestment, filters)
	if err != nil {
		return nil, err
	}

	nodes, edges, execErr := ExecuteSankeyQueries(ctx, client, []compiledQuery{nodesQuery}, edgesQueries)
	if execErr != nil {
		// Swallow: analytics.py:654-656 logs and degrades to empty.
		recordDegradation(ctx, "sankey", execErr)
		nodes, edges = nil, nil
	}

	unit := model.SankeyValueUnitWorkUnits
	if req.Measure == MeasureChurnLOC {
		unit = model.SankeyValueUnitLoc
	}

	return &model.SankeyResult{
		Nodes:    nodes,
		Edges:    edges,
		Coverage: nil, // not yet ported, see Resolve's doc comment
		Unit:     unit,
	}, nil
}

// resolveFlowMatrix ports the batch.flow_matrix branch of
// resolve_analytics (analytics.py:935-963). Same fatal-compile/
// swallow-execute split as resolveSankey.
func resolveFlowMatrix(ctx context.Context, client QueryClient, orgID string, input model.FlowMatrixRequestInput, filters *model.FilterInput) (*model.FlowMatrixResult, error) {
	req, err := FlowMatrixRequestFromInput(input)
	if err != nil {
		return nil, err
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
