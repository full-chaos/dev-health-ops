package analytics

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graphqldate"
)

// primaryWorkItemTeamAttributionSource ports investment.py's
// PRIMARY_WORK_ITEM_TEAM_ATTRIBUTION_SOURCE (ops/src/dev_health_ops/api/
// queries/investment.py:271-285) verbatim -- the single source every
// Investment Sankey/coverage/flow-matrix TEAM join must read from
// (work_item_team_attributions FINAL, is_primary = 1, latest snapshot by
// computed_at per work_item_id). Self-contained (no other investment.py
// CTE dependency), which is why the TEAM/REPO/WORK_TYPE flow-matrix path
// -- unlike timeseries/breakdown/sankey's investment path -- does not
// need the much larger latest_work_unit_investments CTE family.
const primaryWorkItemTeamAttributionSource = `(
    SELECT
        work_item_id,
        team_id,
        team_name
    FROM work_item_team_attributions FINAL
    WHERE org_id = {org_id:String}
      AND is_primary = 1
      AND (work_item_id, computed_at) IN (
          SELECT work_item_id, max(computed_at)
          FROM work_item_team_attributions
          WHERE org_id = {org_id:String}
          GROUP BY work_item_id
      )
)`

// FlowMatrixRequest is the Go port of compiler.py's FlowMatrixRequest
// dataclass (compiler.py:119-129). CHAOS-5426: TEAM and REPO now honour
// UseInvestment exactly like AUTHOR/THEME/SUBCATEGORY do -- see
// CompileFlowMatrix's doc comment for the current routing shape. Only
// WORK_TYPE stays permanently on the fixed hand-written template path
// (compiler.py:495-518), regardless of UseInvestment.
type FlowMatrixRequest struct {
	Dimension     Dimension
	Measure       Measure
	StartDate     graphqldate.Date
	EndDate       graphqldate.Date
	MaxNodes      int
	MaxEdges      int
	UseInvestment *bool
}

// FlowMatrixRequestFromInput converts the GraphQL input to a
// FlowMatrixRequest, validating dimension/measure exactly like
// compile_flow_matrix's `validate_dimension`/`validate_measure` calls
// (compiler.py:475-476) -- both are re-validated even on the
// TEAM/REPO/WORK_TYPE fixed-template path, where `measure` ends up
// unused by the SQL itself (Python does the same: `measure =
// validate_measure(...)` runs unconditionally before the dimension
// branch, compiler.py:476).
func FlowMatrixRequestFromInput(input model.FlowMatrixRequestInput) (FlowMatrixRequest, error) {
	dim, err := dimensionFromInput(input.Dimension)
	if err != nil {
		return FlowMatrixRequest{}, err
	}
	measure, err := measureFromInput(input.Measure)
	if err != nil {
		return FlowMatrixRequest{}, err
	}
	if input.DateRange == nil {
		return FlowMatrixRequest{}, newValidationError("dateRange", nil, "flowMatrix.dateRange is required")
	}
	return FlowMatrixRequest{
		Dimension:     dim,
		Measure:       measure,
		StartDate:     input.DateRange.StartDate,
		EndDate:       input.DateRange.EndDate,
		MaxNodes:      input.MaxNodes,
		MaxEdges:      input.MaxEdges,
		UseInvestment: input.UseInvestment,
	}, nil
}

func dimensionFromInput(d model.DimensionInput) (Dimension, error) {
	switch d {
	case model.DimensionInputTeam:
		return DimensionTeam, nil
	case model.DimensionInputRepo:
		return DimensionRepo, nil
	case model.DimensionInputAuthor:
		return DimensionAuthor, nil
	case model.DimensionInputWorkType:
		return DimensionWorkType, nil
	case model.DimensionInputTheme:
		return DimensionTheme, nil
	case model.DimensionInputSubcategory:
		return DimensionSubcategory, nil
	}
	return "", newValidationError("dimension", string(d), "invalid dimension: %q", d)
}

func measureFromInput(m model.MeasureInput) (Measure, error) {
	switch m {
	case model.MeasureInputCount:
		return MeasureCount, nil
	case model.MeasureInputChurnLoc:
		return MeasureChurnLOC, nil
	case model.MeasureInputPrReworkRatio:
		return MeasurePRReworkRatio, nil
	case model.MeasureInputCycleTimeHours:
		return MeasureCycleTimeHours, nil
	case model.MeasureInputThroughput:
		return MeasureThroughput, nil
	case model.MeasureInputPipelineSuccessRate:
		return MeasurePipelineSuccessRate, nil
	case model.MeasureInputPipelineFailureRate:
		return MeasurePipelineFailureRate, nil
	case model.MeasureInputPipelineDurationP95:
		return MeasurePipelineDurationP95, nil
	case model.MeasureInputPipelineQueueTime:
		return MeasurePipelineQueueTime, nil
	case model.MeasureInputPipelineRerunRate:
		return MeasurePipelineRerunRate, nil
	case model.MeasureInputTestPassRate:
		return MeasureTestPassRate, nil
	case model.MeasureInputTestFailureRate:
		return MeasureTestFailureRate, nil
	case model.MeasureInputTestFlakeRate:
		return MeasureTestFlakeRate, nil
	case model.MeasureInputTestSuiteDurationP95:
		return MeasureTestSuiteDurationP95, nil
	case model.MeasureInputCoverageLinePct:
		return MeasureCoverageLinePct, nil
	case model.MeasureInputCoverageBranchPct:
		return MeasureCoverageBranchPct, nil
	case model.MeasureInputCoverageDeltaPct:
		return MeasureCoverageDeltaPct, nil
	case model.MeasureInputFlagFrictionDelta:
		return MeasureFlagFrictionDelta, nil
	case model.MeasureInputFlagErrorRateDelta:
		return MeasureFlagErrorRateDelta, nil
	case model.MeasureInputFlagCoverageRatio:
		return MeasureFlagCoverageRatio, nil
	case model.MeasureInputFlagActivationRate:
		return MeasureFlagActivationRate, nil
	}
	return "", newValidationError("measure", string(m), "invalid measure: %q", m)
}

// compiledQuery pairs one query's SQL with its bindings -- the Go
// equivalent of Python's `tuple[str, dict[str, Any]]` pair the compiler
// functions return.
type compiledQuery struct {
	sql      string
	bindings []clickhouse.Binding
}

// CompileFlowMatrix ports compile_flow_matrix (compiler.py:450-534,
// e9ea257ff) in full: WORK_TYPE's permanently-fixed template branch
// below, AND the AUTHOR/THEME/SUBCATEGORY "else" branch (compiler.py:
// 519-533, CHAOS-4538), which reuses the same sankey_nodes_template/
// sankey_edges_template shape sankey.go's CompileSankey builds, over
// investmentContextFor's investment machinery -- see
// compileFlowMatrixInvestmentDimension below.
//
// CHAOS-5426 (Go-only extension, see that ticket's expected-divergence
// note): TEAM and REPO now resolve UseInvestment exactly like AUTHOR/
// THEME/SUBCATEGORY -- resolveUseInvestment(their own one-element
// dimension list, req.UseInvestment) -- and route through
// compileFlowMatrixInvestmentTeamRepoDimension (NOT
// compileFlowMatrixInvestmentDimension -- see that function's doc
// comment for why they need a different edges shape) whenever it
// resolves true, reading LatestWorkUnitInvestmentsSource()/
// repoAllocationInvestmentSource() (via investmentContextFor) instead of
// the raw work_item_cycle_times/work_item_team_attributions tables the
// fixed templates below read. Neither TEAM nor REPO is in
// resolveUseInvestment's auto-route set ({THEME, SUBCATEGORY,
// WORK_TYPE}), so this only fires when the caller sends an explicit
// useInvestment=true (web's useChordFlow.ts always does) -- an
// unset/false UseInvestment for TEAM/REPO reaches the fixed-template
// branch below UNCHANGED, byte-identical to before this port. Python's
// compile_flow_matrix does NOT get this fix (chris's standing
// no-further-Python-GraphQL-work rule) -- Python's chord for TEAM/REPO
// keeps reading work_item_cycle_times regardless of use_investment; this
// is a deliberate, ticketed Go/Python divergence, not a parity bug.
//
// Deliberately mirrors Python's error-boundary shape: everything in this
// function (dimension/measure validation, plus the filtered-flow-matrix
// rejection below) is a FATAL error if it fails -- resolve_analytics
// calls compile_flow_matrix OUTSIDE its try/except (analytics.py:949-951,
// the try only wraps the execution call at 953-961). Only Execute's
// ClickHouse-query-and-scan step is meant to be caught by a caller that
// wants Python's swallow-to-empty-FlowMatrixResult behavior.
func CompileFlowMatrix(req FlowMatrixRequest, orgID string, timeoutSeconds int, filters *model.FilterInput) (nodes compiledQuery, edges compiledQuery, err error) {
	switch req.Dimension {
	case DimensionTeam, DimensionRepo:
		if resolveUseInvestment([]Dimension{req.Dimension}, req.UseInvestment) {
			// NOT compileFlowMatrixInvestmentDimension -- that function's
			// shared edges shape is source==target (self-pairs only), which
			// team-lead's codex-round-1 read (2026-09-07 09:50Z) correctly
			// rejected for TEAM/REPO: the web chord UI has self-links off
			// by default and no field to turn them on, so a self-pairs-only
			// result renders as "No flows match", the exact bug this ticket
			// exists to fix. See compileFlowMatrixInvestmentTeamRepoDimension's
			// doc comment for the bridged-self-join replacement.
			return compileFlowMatrixInvestmentTeamRepoDimension(req, orgID, timeoutSeconds, filters)
		}
	case DimensionWorkType:
		// WORK_TYPE never reads the investment source, regardless of
		// UseInvestment -- see flowMatrixUsesInvestmentSource's doc
		// comment.
	default:
		return compileFlowMatrixInvestmentDimension(req, orgID, timeoutSeconds, filters)
	}

	if hasActiveFilters(filters) {
		// Ports _reject_filtered_same_dimension_flow_matrix
		// (compiler.py:537-550) verbatim: CHAOS-2487, fail honestly
		// rather than silently return org-wide/unfiltered data for a
		// same-dimension flow matrix. Only reached for the fixed-template
		// path now (TEAM/REPO with useInvestment=true already returned
		// above, through compileFlowMatrixInvestmentTeamRepoDimension,
		// which supports filters via translateFilters exactly like
		// AUTHOR/THEME/SUBCATEGORY's compileFlowMatrixInvestmentDimension
		// does).
		return compiledQuery{}, compiledQuery{}, newValidationError(
			"filters", string(req.Dimension),
			"flowMatrix filters are not supported for same-dimension %s queries yet (CHAOS-2487); "+
				"remove filters or use theme/subcategory.", req.Dimension)
	}

	common := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "start_date", Value: dateBindingValue(req.StartDate.Time())},
		{Name: "end_date", Value: dateBindingValue(req.EndDate.Time())},
	}

	switch req.Dimension {
	case DimensionTeam:
		nodes = compiledQuery{
			sql:      fmt.Sprintf(flowMatrixTeamNodesTemplate, settingsMaxExecutionTime(timeoutSeconds)),
			bindings: append(append([]clickhouse.Binding{}, common...), clickhouse.Binding{Name: "limit_per_dim", Value: req.MaxNodes}),
		}
		edges = compiledQuery{
			sql:      fmt.Sprintf(flowMatrixTeamEdgesTemplate, settingsMaxExecutionTime(timeoutSeconds)),
			bindings: append(append([]clickhouse.Binding{}, common...), clickhouse.Binding{Name: "max_edges", Value: req.MaxEdges}),
		}
	case DimensionRepo:
		nodes = compiledQuery{
			sql:      fmt.Sprintf(flowMatrixRepoNodesTemplate, settingsMaxExecutionTime(timeoutSeconds)),
			bindings: append(append([]clickhouse.Binding{}, common...), clickhouse.Binding{Name: "limit_per_dim", Value: req.MaxNodes}),
		}
		edges = compiledQuery{
			sql:      fmt.Sprintf(flowMatrixRepoEdgesTemplate, settingsMaxExecutionTime(timeoutSeconds)),
			bindings: append(append([]clickhouse.Binding{}, common...), clickhouse.Binding{Name: "max_edges", Value: req.MaxEdges}),
		}
	case DimensionWorkType:
		nodes = compiledQuery{
			sql:      fmt.Sprintf(flowMatrixWorkTypeNodesTemplate, settingsMaxExecutionTime(timeoutSeconds)),
			bindings: append(append([]clickhouse.Binding{}, common...), clickhouse.Binding{Name: "limit_per_dim", Value: req.MaxNodes}),
		}
		edges = compiledQuery{
			sql:      fmt.Sprintf(flowMatrixWorkTypeEdgesTemplate, settingsMaxExecutionTime(timeoutSeconds)),
			bindings: append(append([]clickhouse.Binding{}, common...), clickhouse.Binding{Name: "max_edges", Value: req.MaxEdges}),
		}
	}
	return nodes, edges, nil
}

// flowMatrixUsesInvestmentSource reports whether CompileFlowMatrix would
// route req through the investment source (LatestWorkUnitInvestmentsSource /
// repoAllocationInvestmentSource, via either compileFlowMatrixInvestmentDimension
// for AUTHOR/THEME/SUBCATEGORY or compileFlowMatrixInvestmentTeamRepoDimension
// for TEAM/REPO) -- CHAOS-4759 codex round-2 P1 fix: resolveFlowMatrix
// (resolve.go) never sees the useInvestment value those functions resolve
// internally, so a caller that needs to know whether THIS request will
// touch the investment source (to gate RecordArgMaxNullTransitionGuard)
// has to make the same decision CompileFlowMatrix's own switch below
// makes. Kept here, deliberately duplicating the switch's case list
// rather than changing CompileFlowMatrix's signature to return the
// decision, so the two stay visually adjacent and a future dimension
// added to one switch is easy to spot missing from the other. CHAOS-5426:
// only WORK_TYPE now NEVER reads the investment source regardless of
// UseInvestment (CompileFlowMatrix's `case DimensionWorkType` branch
// skips the resolveUseInvestment check entirely); TEAM, REPO, and every
// other dimension resolve via resolveUseInvestment exactly as both
// investment-path functions below do.
func flowMatrixUsesInvestmentSource(req FlowMatrixRequest) bool {
	if req.Dimension == DimensionWorkType {
		return false
	}
	return resolveUseInvestment([]Dimension{req.Dimension}, req.UseInvestment)
}

// compileFlowMatrixInvestmentDimension ports compile_flow_matrix's
// AUTHOR/THEME/SUBCATEGORY "else" branch (compiler.py:519-533,
// e9ea257ff) -- CHAOS-4538's flow-matrix scope item. Python reuses
// sankey_nodes_template([dimension], ...) and
// sankey_edges_template(dimension, dimension, ...) (source==target: a
// same-dimension self-join, the flow-matrix "chord" shape) over
// _get_context_params's investment machinery.
//
// CHAOS-5426: TEAM and REPO do NOT route through this function, despite
// also being investment dimensions now -- this function's edges shape
// (source==target per row) produces ONLY self-pairs, which team-lead's
// codex-round-1 read (2026-09-07 09:50Z) correctly rejected for the chord
// UI (self-links off by default, no field to enable them). TEAM/REPO
// route through compileFlowMatrixInvestmentTeamRepoDimension below
// instead, which builds a genuine bridged self-join for cross-entity
// pairs. This function is therefore UNCHANGED by CHAOS-5426 -- still
// exactly AUTHOR/THEME/SUBCATEGORY, still exactly the same SQL text it
// produced before this ticket, on purpose (their same-row semantics may
// be intentional and are out of this ticket's scope, per team-lead's
// ruling).
//
// useInvestment here is resolveUseInvestment's per-request resolution
// (req.UseInvestment, i.e. FlowMatrixRequestInput's OWN useInvestment
// override -- compiler.py:478,480 passes `force_investment=
// request.use_investment`, NOT the batch flag; resolve.go's
// FlowMatrixRequestFromInput already captures this field). THEME and
// SUBCATEGORY auto-route to investment when unset (both are in
// resolveUseInvestment's investment_dims set); AUTHOR is not, but
// dbColumn rejects AUTHOR unconditionally regardless of useInvestment
// (validate.go's dbColumn doc comment: neither source table has a
// scalar per-row author identity column) -- so AUTHOR always fails here
// the same way it fails in CompileSankey, with no special-casing needed
// in this function.
func compileFlowMatrixInvestmentDimension(req FlowMatrixRequest, orgID string, timeoutSeconds int, filters *model.FilterInput) (nodes, edges compiledQuery, err error) {
	useInvestment := resolveUseInvestment([]Dimension{req.Dimension}, req.UseInvestment)

	dimCol, err := dbColumn(req.Dimension, useInvestment)
	if err != nil {
		return compiledQuery{}, compiledQuery{}, err
	}

	fc, err := translateFilters(filters, useInvestment, defaultFilterColumns())
	if err != nil {
		return compiledQuery{}, compiledQuery{}, err
	}

	var source, alias, dateFilter, extraClauses, measureExpr string
	if useInvestment {
		ictx := investmentContextFor([]Dimension{req.Dimension}, needsTeamJoin(filters), needsAuthorJoin(filters))
		source, alias, dateFilter, extraClauses = ictx.Source, ictx.Alias, ictx.DateFilter, ictx.ExtraClauses
		measureExpr, err = dbExpression(req.Measure, true, ictx.UseRepoAllocation)
	} else {
		source, alias, dateFilter = nonInvestmentSourceAndDateFilter(req.Measure)
		measureExpr, err = dbExpression(req.Measure, false, false)
	}
	if err != nil {
		return compiledQuery{}, compiledQuery{}, err
	}
	// Force a uniform Float64 result type -- see CompileTimeseries's doc
	// comment for the full reasoning; identical here.
	measureExpr = "toFloat64(" + measureExpr + ")"

	dimUpper := strings.ToUpper(string(req.Dimension))

	nodesSQL := fmt.Sprintf(`
SELECT
    '%s' AS dimension,
    toString(%s) AS node_id,
    %s AS value
FROM %s
%s
WHERE %s
  AND %s.org_id = {org_id:String}
%s
GROUP BY node_id
ORDER BY value DESC, node_id ASC
LIMIT {limit_per_dim:UInt32}
%s
`, dimUpper, dimCol, measureExpr, source, extraClauses, dateFilter, alias, fc.sql, settingsMaxExecutionTime(timeoutSeconds))
	nodesBindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "start_date", Value: dateBindingValue(req.StartDate.Time())},
		{Name: "end_date", Value: dateBindingValue(req.EndDate.Time())},
		{Name: "limit_per_dim", Value: req.MaxNodes},
	}
	nodesBindings = append(nodesBindings, fc.bindings...)

	// sankey_edges_template(dimension, dimension, ...): source==target,
	// a same-dimension self-join -- compiler.py:527-529.
	edgesSQL := fmt.Sprintf(`
SELECT
    '%s' AS source_dimension,
    '%s' AS target_dimension,
    toString(%s) AS source,
    toString(%s) AS target,
    %s AS value
FROM %s
%s
WHERE %s
  AND %s.org_id = {org_id:String}
%s
  AND %s IS NOT NULL
  AND %s IS NOT NULL
GROUP BY source, target
ORDER BY value DESC, source ASC, target ASC
LIMIT {max_edges:UInt32}
%s
`, dimUpper, dimUpper, dimCol, dimCol, measureExpr, source, extraClauses, dateFilter, alias, fc.sql, dimCol, dimCol, settingsMaxExecutionTime(timeoutSeconds))
	edgesBindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "start_date", Value: dateBindingValue(req.StartDate.Time())},
		{Name: "end_date", Value: dateBindingValue(req.EndDate.Time())},
		{Name: "max_edges", Value: req.MaxEdges},
	}
	edgesBindings = append(edgesBindings, fc.bindings...)

	return compiledQuery{sql: nodesSQL, bindings: nodesBindings}, compiledQuery{sql: edgesSQL, bindings: edgesBindings}, nil
}

// compileFlowMatrixInvestmentTeamRepoDimension builds TEAM and REPO's
// investment-mode flowMatrix query pair. CHAOS-5426, team-lead's codex-
// round-1 NOT GO (2026-09-07 09:50Z, verbatim reasoning): "the chord is
// 'Team exchange: pairs that frequently touch the same work'; the UI's
// 'Include self-links' is OFF by default, so a result of only self-pairs
// renders exactly what chris sees today: No flows match." Confirmed by
// reading model.FlowMatrixRequestInput (models_gen.go): there is no
// includeSelfLinks-shaped field anywhere on it, so this function
// unconditionally excludes self-pairs -- there is no flag to make them
// optional, matching the OLD fixed hand-written TEAM/REPO templates'
// own unconditional `a.team_id != b.team_id` / `a.repo_id != b.repo_id`
// exclusion (flowMatrixTeamEdgesTemplate / flowMatrixRepoEdgesTemplate
// below).
//
// Nodes are the same shape compileFlowMatrixInvestmentDimension builds
// for its own dimensions (one row per dimension value, GROUP BY
// node_id) -- duplicated here rather than shared, so an edit to either
// function's SQL text cannot silently perturb the other's.
//
// Edges are a genuine BRIDGED SELF-JOIN -- two aggregation passes, same
// shape as the old fixed templates' activity-select-then-self-join
// pattern, but reading the investment source instead of
// work_item_cycle_times:
//   - TEAM pairs: two DISTINCT teams (ut.team_label) with effort on the
//     SAME repo_id. repo_id is a native column on every investment row
//     regardless of dimension (LatestWorkUnitInvestmentsSource always
//     projects it) -- no extra join needed to bridge on it.
//   - REPO pairs: two DISTINCT repos (dbColumn's r.repo) touched by the
//     SAME team. investmentContextFor only adds the `ut` team join
//     automatically when TEAM is in the dimensions list or a filter
//     needs it -- neither is true for a bare REPO-dimension request, so
//     this function forces that join on for the edges half specifically.
//
// Both bridge columns exclude the empty/unassigned case (repo_id IS NOT
// NULL; ut.team_id != ”) -- matching the old templates' own
// `t.team_id IS NOT NULL AND t.team_id != ”` / not-null repo guards. An
// unassigned bridge would otherwise fan every work unit with no
// repo/team attribution out against every other one, which is an
// artefact of missing data, not a real shared-repo/shared-team
// relationship.
//
// measureExpr is computed once, from a nodes-only investmentContext
// (nodeCtx below) -- safe to reuse for the edges activity-select too,
// because UseRepoAllocation depends only on the ONE-element dimensions
// list ([]Dimension{req.Dimension}), which is identical between nodeCtx
// and the edges-only edgeCtx (edgeCtx only differs in whether the `ut`
// join is FORCED on, which does not change UseRepoAllocation).
func compileFlowMatrixInvestmentTeamRepoDimension(req FlowMatrixRequest, orgID string, timeoutSeconds int, filters *model.FilterInput) (nodes, edges compiledQuery, err error) {
	dimCol, err := dbColumn(req.Dimension, true)
	if err != nil {
		return compiledQuery{}, compiledQuery{}, err
	}

	fc, err := translateFilters(filters, true, defaultFilterColumns())
	if err != nil {
		return compiledQuery{}, compiledQuery{}, err
	}

	nodeCtx := investmentContextFor([]Dimension{req.Dimension}, needsTeamJoin(filters), needsAuthorJoin(filters))
	measureExpr, err := dbExpression(req.Measure, true, nodeCtx.UseRepoAllocation)
	if err != nil {
		return compiledQuery{}, compiledQuery{}, err
	}
	// Force a uniform Float64 result type -- see CompileTimeseries's doc
	// comment for the full reasoning; identical here.
	measureExpr = "toFloat64(" + measureExpr + ")"

	dimUpper := strings.ToUpper(string(req.Dimension))

	nodesSQL := fmt.Sprintf(`
SELECT
    '%s' AS dimension,
    toString(%s) AS node_id,
    %s AS value
FROM %s
%s
WHERE %s
  AND %s.org_id = {org_id:String}
%s
GROUP BY node_id
ORDER BY value DESC, node_id ASC
LIMIT {limit_per_dim:UInt32}
%s
`, dimUpper, dimCol, measureExpr, nodeCtx.Source, nodeCtx.ExtraClauses, nodeCtx.DateFilter, nodeCtx.Alias, fc.sql, settingsMaxExecutionTime(timeoutSeconds))
	nodesBindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "start_date", Value: dateBindingValue(req.StartDate.Time())},
		{Name: "end_date", Value: dateBindingValue(req.EndDate.Time())},
		{Name: "limit_per_dim", Value: req.MaxNodes},
	}
	nodesBindings = append(nodesBindings, fc.bindings...)

	edgeForceTeamJoin := req.Dimension == DimensionRepo || needsTeamJoin(filters)
	edgeCtx := investmentContextFor([]Dimension{req.Dimension}, edgeForceTeamJoin, needsAuthorJoin(filters))

	var bridgeCol, bridgeNotEmpty string
	switch req.Dimension {
	case DimensionTeam:
		bridgeCol = "repo_id"
		bridgeNotEmpty = "repo_id IS NOT NULL"
	case DimensionRepo:
		bridgeCol = "ut.team_id"
		bridgeNotEmpty = "ut.team_id != ''"
	default:
		return compiledQuery{}, compiledQuery{}, fmt.Errorf(
			"analytics: compileFlowMatrixInvestmentTeamRepoDimension: unsupported dimension %q", req.Dimension)
	}

	// activitySelect is embedded TWICE below (AS a, AS b) -- the named
	// ClickHouse params it references ({org_id:String} etc.) are matched
	// by name, not position, so edgesBindings below still only needs ONE
	// copy of each binding despite the text appearing twice. Same pattern
	// flowMatrixTeamActivitySelect/flowMatrixRepoEnrichedSelect already
	// use further down this file.
	activitySelect := fmt.Sprintf(`
    SELECT
        %s AS entity,
        %s AS bridge,
        %s AS value
    FROM %s
    %s
    WHERE %s
      AND %s.org_id = {org_id:String}
    %s
      AND %s
    GROUP BY entity, bridge
`, dimCol, bridgeCol, measureExpr, edgeCtx.Source, edgeCtx.ExtraClauses, edgeCtx.DateFilter, edgeCtx.Alias, fc.sql, bridgeNotEmpty)

	edgesSQL := fmt.Sprintf(`
SELECT
    '%s' AS source_dimension,
    '%s' AS target_dimension,
    toString(a.entity) AS source,
    toString(b.entity) AS target,
    toFloat64(SUM(a.value)) AS value
FROM (%s) AS a
INNER JOIN (%s) AS b ON a.bridge = b.bridge
WHERE a.entity != b.entity
GROUP BY source, target
ORDER BY value DESC, source ASC, target ASC
LIMIT {max_edges:UInt32}
%s
`, dimUpper, dimUpper, activitySelect, activitySelect, settingsMaxExecutionTime(timeoutSeconds))
	edgesBindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "start_date", Value: dateBindingValue(req.StartDate.Time())},
		{Name: "end_date", Value: dateBindingValue(req.EndDate.Time())},
		{Name: "max_edges", Value: req.MaxEdges},
	}
	edgesBindings = append(edgesBindings, fc.bindings...)

	return compiledQuery{sql: nodesSQL, bindings: nodesBindings}, compiledQuery{sql: edgesSQL, bindings: edgesBindings}, nil
}

// --- Query templates ---------------------------------------------------
//
// Hand-copied from sql/templates.py against feature-branch tip e91e0a0f7
// (CHAOS-4495's tie-break commit; see BRIEF.md's UNBLOCK block for why
// that tip, not main, is the correct source for this scope), citing
// file:line for each. `%(name)s` pyformat params become `{name:Type}`
// ClickHouse native params; `FINAL` placement is unchanged except at the
// three sites this port fixes -- called out individually below.

// flowMatrixTeamNodesTemplate ports flow_matrix_team_nodes_template
// (sql/templates.py:187-219) VERBATIM -- `wct FINAL` was already present
// on main/feature tip, not part of this port's CHAOS-4516 fix.
// flowMatrixTeamActivitySelect is the former `WITH team_activity AS (...)`
// CTE body, now a bare SELECT so it can be INLINED as a subquery.
//
// The pinned dev-health-go v0.4.0 client rejects any statement whose
// first token is not SELECT (clickhouse/client.go:190), so a
// WITH-leading query never reaches ClickHouse -- it returns
// ErrUnsafeStatement, which resolveFlowMatrix then SWALLOWS to an empty
// result. Shape copied from Lane A's workgraph/scope.go, which hit the
// identical guard live.
const flowMatrixTeamActivitySelect = `
    SELECT
        wct.work_item_id,
        wct.work_scope_id,
        wct.day,
        wct.org_id,
        t.team_id AS team_id
    FROM work_item_cycle_times AS wct FINAL
    INNER JOIN ` + primaryWorkItemTeamAttributionSource + ` AS t
      ON t.work_item_id = wct.work_item_id
    WHERE wct.day >= {start_date:Date} AND wct.day <= {end_date:Date}
      AND wct.org_id = {org_id:String}
      AND t.team_id IS NOT NULL
      AND t.team_id != ''
`

const flowMatrixTeamNodesTemplate = `
SELECT
    'TEAM' AS dimension,
    toString(team_id) AS node_id,
    toFloat64(uniqExact(work_item_id)) AS value
FROM (` + flowMatrixTeamActivitySelect + `) AS team_activity
GROUP BY node_id
ORDER BY value DESC, node_id ASC
LIMIT {limit_per_dim:UInt32}
%s
`

// flowMatrixTeamEdgesTemplate ports flow_matrix_team_edges_template
// (sql/templates.py:222-273) verbatim -- `wct FINAL` unchanged.
const flowMatrixTeamEdgesTemplate = `
SELECT
    'TEAM' AS source_dimension,
    'TEAM' AS target_dimension,
    toString(a.team_id) AS source,
    toString(b.team_id) AS target,
    toFloat64(uniqExact(a.work_item_id)) AS value
FROM (` + flowMatrixTeamActivitySelect + `) AS a
INNER JOIN (` + flowMatrixTeamActivitySelect + `) AS b
  ON a.work_scope_id = b.work_scope_id
  AND a.day = b.day
  AND a.org_id = b.org_id
WHERE a.team_id != b.team_id
GROUP BY source, target
ORDER BY value DESC, source ASC, target ASC
LIMIT {max_edges:UInt32}
%s
`

// flowMatrixRepoEnrichedSelect ports _FLOW_MATRIX_REPO_ENRICHED_CTE
// (sql/templates.py:276-293) -- `wct FINAL` unchanged.
//
// ALIAS HISTORY, recorded because the first fix here was itself
// incomplete and the same lesson applies to whoever edits this next:
//
// Round 1 (2026-08-29, CHAOS-4506 slot execution): BRIEF.md §4 had
// claimed this site "is what made `a.team_id` resolve here" purely from
// reading that the CTE projects `t.team_id` -- never executed against a
// live engine. Isolated live via `docker exec dev-health-clickhouse-1
// clickhouse-client` with a minimal two-row self-join repro: ClickHouse
// 26.7's analyzer rejects `a.team_id` with `UNKNOWN_IDENTIFIER: Maybe
// you meant: ['t.team_id']` when a select is duplicated into a self-join
// (`... AS a INNER JOIN (...) AS b`, this port's WITH-avoidance shape --
// see flowMatrixTeamActivitySelect's doc comment) and the referenced
// column is projected unaliased. Fixed `t.team_id AS team_id` and
// `wi.repo_id AS repo_id` only -- the two columns the live error named.
//
// Round 2 (2026-08-29, CHAOS-4519 fix on main, `3b4a1ea15`): that fix
// was NARROWER than the bug. `flowMatrixRepoEdgesTemplate` below
// qualifies FIVE columns across its self-join, not two -- `a.team_id`,
// `a.day`, `a.org_id`, `a.repo_id`, and `uniqExact(a.work_item_id)` --
// and `wct.day`/`wct.org_id`/`wct.work_item_id` were still unaliased.
// The dual-run's own test 2 caught it: Python's side (fixed on main with
// ALL FIVE columns aliased, run 2's red-on-baseline having gone red
// again on `a.org_id` after the team_id-only fix, per the ticket's own
// history) returned a real value; Go's side returned NO node at all --
// a SWALLOWED failure (resolve.go's degrade-to-empty), not the declared
// mismatch. Every projected column is now aliased, matching main's
// `_FLOW_MATRIX_REPO_ENRICHED_CTE` exactly. Same columns, same values,
// still zero semantic change -- purely what the analyzer needs to
// resolve ANY qualified reference across a self-joined duplicate, not
// just the ones a single failing query happened to name first.
const flowMatrixRepoEnrichedSelect = `
    SELECT
        wct.work_item_id AS work_item_id,
        t.team_id AS team_id,
        wct.day AS day,
        wct.org_id AS org_id,
        wi.repo_id AS repo_id,
        wi.type AS work_item_type
    FROM work_item_cycle_times AS wct FINAL
    INNER JOIN ` + primaryWorkItemTeamAttributionSource + ` AS t
      ON t.work_item_id = wct.work_item_id
    INNER JOIN work_items AS wi FINAL ON wct.work_item_id = wi.work_item_id
    WHERE wct.org_id = {org_id:String}
      AND wct.day >= {start_date:Date} AND wct.day <= {end_date:Date}
      AND wi.org_id = {org_id:String}
      AND t.team_id IS NOT NULL
      AND t.team_id != ''
`

// flowMatrixWorkTypeEnrichedSelect ports _FLOW_MATRIX_WORK_TYPE_ENRICHED_CTE
// (sql/templates.py:296-309).
//
// *** CHAOS-4516 FIX SITE 1 of 3 (sql/templates.py:304 on the Python
// side; feeds flow_matrix_work_type_edges_template). ***
// `work_item_cycle_times` is `ReplacingMergeTree(computed_at)`
// (BRIEF.md's verified exposure map); Python reads it here WITHOUT
// `FINAL` -- two pre-merge versions of one row tie on the entire ORDER
// BY the downstream templates apply, so a tie-break on the row's OWN
// keys cannot separate them (CHAOS-4515 class). The `INNER JOIN
// work_items AS wi FINAL` below is real but binds to `wi`, a DIFFERENT
// table -- confirmed by reading which alias FINAL attaches to, not by
// grepping for the word (BRIEF.md §3's documented trap).
//
// FIX (this port only -- chris ruled 06:52 PT 08-29 that no more work
// goes into the Python GraphQL layer, CHAOS-4516 routing reversed): add
// `FINAL` to `wct`. UNMEASURED as of this commit -- `argMax(...,
// computed_at) GROUP BY` is the documented alternative per CHAOS-4516's
// standing fix-shape ruling ("never add FINAL blindly... every fix
// carries a measured cost number"); `FINAL` is chosen here on
// consistency with the two sibling sites in this same file that already
// use `wct FINAL` (team_nodes/edges above, repo CTE above) plus this
// query's own GROUP BY + LIMIT shape (unlike Lane A's work_graph_edges,
// which had neither), but the actual argument and the row-count/part-
// count/max_threads/before-after-median number are NOT YET PRODUCED --
// see the PR's RISK-NOTES. Do not treat this as decided.
// flowMatrixWorkTypeEnrichedSelect: the same missing-alias analyzer bug
// found live in flowMatrixRepoEnrichedSelect above applies here too --
// flowMatrixWorkTypeEdgesTemplate's self-join references a.repo_id/
// b.repo_id, which this select fed unaliased. Fixed the same way, same
// verification method (see flowMatrixRepoEnrichedSelect's doc comment).
const flowMatrixWorkTypeEnrichedSelect = `
    SELECT
        wct.work_item_id AS work_item_id,
        wct.team_id AS team_id,
        wct.day AS day,
        wct.org_id AS org_id,
        wi.repo_id AS repo_id,
        wi.type AS work_item_type
    FROM work_item_cycle_times AS wct FINAL
    INNER JOIN work_items AS wi FINAL ON wct.work_item_id = wi.work_item_id
    WHERE wct.org_id = {org_id:String}
      AND wct.day >= {start_date:Date} AND wct.day <= {end_date:Date}
      AND wi.org_id = {org_id:String}
`

// flowMatrixRepoNodesTemplate ports flow_matrix_repo_nodes_template
// (sql/templates.py:312-335).
//
// *** CHAOS-4516 FIX SITE 2 of 3 (sql/templates.py:325 on the Python
// side). *** Same exposure/fix/UNMEASURED-cost shape as
// flowMatrixWorkTypeEnrichedSelect above -- see that doc comment. The
// `INNER JOIN work_items AS wi FINAL` here is likewise real but binds to
// `wi`, not `wct`.
const flowMatrixRepoNodesTemplate = `
SELECT
    'REPO' AS dimension,
    toString(wi.repo_id) AS node_id,
    toFloat64(uniqExact(wct.work_item_id)) AS value
FROM work_item_cycle_times AS wct FINAL
INNER JOIN work_items AS wi FINAL ON wct.work_item_id = wi.work_item_id
WHERE wct.day >= {start_date:Date} AND wct.day <= {end_date:Date}
  AND wct.org_id = {org_id:String}
  AND wi.org_id = {org_id:String}
  AND wi.repo_id IS NOT NULL
GROUP BY node_id
ORDER BY value DESC, node_id ASC
LIMIT {limit_per_dim:UInt32}
%s
`

// flowMatrixRepoEdgesTemplate ports flow_matrix_repo_edges_template
// (sql/templates.py:338-382) verbatim -- reads only through
// flowMatrixRepoEnrichedSelect, which already carries `wct FINAL`; not one
// of the 3 exposed sites.
const flowMatrixRepoEdgesTemplate = `
SELECT
    'REPO' AS source_dimension,
    'REPO' AS target_dimension,
    toString(a.repo_id) AS source,
    toString(b.repo_id) AS target,
    toFloat64(uniqExact(a.work_item_id)) AS value
FROM (` + flowMatrixRepoEnrichedSelect + `) AS a
INNER JOIN (` + flowMatrixRepoEnrichedSelect + `) AS b
  ON a.team_id = b.team_id
  AND a.day = b.day
  AND a.org_id = b.org_id
WHERE a.team_id IS NOT NULL AND a.team_id != ''
  AND b.team_id IS NOT NULL AND b.team_id != ''
  AND a.repo_id IS NOT NULL
  AND b.repo_id IS NOT NULL
  AND a.repo_id != b.repo_id
GROUP BY source, target
ORDER BY value DESC, source ASC, target ASC
LIMIT {max_edges:UInt32}
%s
`

// flowMatrixWorkTypeNodesTemplate ports flow_matrix_work_type_nodes_template
// (sql/templates.py:385-407).
//
// *** CHAOS-4516 FIX SITE 3 of 3 (sql/templates.py:397 on the Python
// side). *** Same shape as flowMatrixRepoNodesTemplate above.
const flowMatrixWorkTypeNodesTemplate = `
SELECT
    'WORK_TYPE' AS dimension,
    wi.type AS node_id,
    toFloat64(uniqExact(wct.work_item_id)) AS value
FROM work_item_cycle_times AS wct FINAL
INNER JOIN work_items AS wi FINAL ON wct.work_item_id = wi.work_item_id
WHERE wct.day >= {start_date:Date} AND wct.day <= {end_date:Date}
  AND wct.org_id = {org_id:String}
  AND wi.org_id = {org_id:String}
  AND wi.type IS NOT NULL AND wi.type != ''
GROUP BY node_id
ORDER BY value DESC, node_id ASC
LIMIT {limit_per_dim:UInt32}
%s
`

// flowMatrixWorkTypeEdgesTemplate ports flow_matrix_work_type_edges_template
// (sql/templates.py:410-446) -- the template body itself contains no
// direct read of work_item_cycle_times; it inherits the exposure from
// flowMatrixWorkTypeEnrichedSelect (fix site 1) it interpolates. BRIEF.md's
// correction: "fixing the template" without fixing the CTE would leave
// the CTE (and its OTHER consumers, if any existed) still exposed --
// there are none here, but the fix belongs at the CTE regardless.
const flowMatrixWorkTypeEdgesTemplate = `
SELECT
    'WORK_TYPE' AS source_dimension,
    'WORK_TYPE' AS target_dimension,
    a.work_item_type AS source,
    b.work_item_type AS target,
    toFloat64(uniqExact(a.work_item_id)) AS value
FROM (` + flowMatrixWorkTypeEnrichedSelect + `) AS a
INNER JOIN (` + flowMatrixWorkTypeEnrichedSelect + `) AS b
  ON a.repo_id = b.repo_id
  AND a.day = b.day
  AND a.org_id = b.org_id
WHERE a.repo_id IS NOT NULL
  AND b.repo_id IS NOT NULL
  AND a.work_item_type IS NOT NULL AND a.work_item_type != ''
  AND b.work_item_type IS NOT NULL AND b.work_item_type != ''
  AND a.work_item_type != b.work_item_type
GROUP BY source, target
ORDER BY value DESC, source ASC, target ASC
LIMIT {max_edges:UInt32}
%s
`

// QueryClient is the read-only ClickHouse query boundary this package
// needs -- same single-method shape every other query-api package
// declares independently (see e.g. cognitiveload.QueryClient's doc
// comment for why this is not shared through dev-health-go/readers).
// *clickhouse.Client satisfies this interface directly.
type QueryClient interface {
	Query(ctx context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error)
}

// ExecuteFlowMatrix runs the compiled nodes+edges query pair CONCURRENTLY
// -- the Go port of _execute_sankey_inner (analytics.py:275-331) for
// exactly this one-nodes-query/one-edges-query case (compile_flow_matrix
// always returns single-element query lists for TEAM/REPO/WORK_TYPE,
// compiler.py:534: `return [(nodes_sql, nodes_params)], [(edge_sql,
// edge_params)]` -- unlike compile_sankey, which can return multiple
// edges queries for a multi-hop path). Both queries run regardless of
// the other's outcome (goroutines + WaitGroup, not cancel-on-first-error)
// -- matching _execute_sankey_inner's own asyncio.gather(*queries) over
// each of nodes_queries/edges_queries, which likewise has no
// cancellation semantics for its (here, single-element) query list.
//
// This function returns a REAL error on failure -- it does NOT swallow
// to an empty result. resolve_analytics's Phase 3 (analytics.py:953-961)
// wraps ONLY this execution step in a try/except that degrades to empty
// nodes/edges; CompileFlowMatrix's validation errors above are NOT
// caught there (compile happens at analytics.py:949-951, outside the
// try). The future top-level analytics.Resolve orchestrator must
// reproduce that exact boundary: propagate CompileFlowMatrix's error,
// but catch ExecuteFlowMatrix's.
func ExecuteFlowMatrix(ctx context.Context, client QueryClient, nodesQuery, edgesQuery compiledQuery) ([]model.SankeyNode, []model.SankeyEdge, error) {
	var (
		wg       sync.WaitGroup
		nodes    []model.SankeyNode
		edges    []model.SankeyEdge
		nodesErr error
		edgesErr error
	)

	wg.Add(2)
	go func() {
		defer wg.Done()
		nodes, nodesErr = queryNodes(ctx, client, nodesQuery)
	}()
	go func() {
		defer wg.Done()
		edges, edgesErr = queryEdges(ctx, client, edgesQuery)
	}()
	wg.Wait()

	if nodesErr != nil {
		return nil, nil, fmt.Errorf("analytics: flowMatrix nodes: %w", nodesErr)
	}
	if edgesErr != nil {
		return nil, nil, fmt.Errorf("analytics: flowMatrix edges: %w", edgesErr)
	}
	return nodes, edges, nil
}

// queryNodes and queryEdges (below) both scan the measure column into a
// *float64, not a bare float64 -- CHAOS-4701, the SAME shape and SAME
// product ruling CHAOS-4650/CHAOS-4657 applied to breakdown.go's
// breakdownRow.Value / timeseries.go's ExecuteTimeseries; see
// breakdown.go's doc comment for the full mechanism writeup (verified
// against pinned clickhouse-go v2.47.0: Nullable.ScanRow only
// recognises **T for its NULL branch -- a bare *float64 destination
// never observes the NULL at all and silently keeps its
// zero-initialised 0.0).
//
// REACHABILITY, verified by reading the call graph, not assumed from
// CHAOS-4650/4657's precedent: these two functions are shared by TWO
// callers with DIFFERENT nullability exposure.
//
//  1. sankey.go's CompileSankey, for a request with useInvestment=false
//     (a genuine, client-reachable state -- SankeyRequest.UseInvestment
//     is a real three-state override, resolve.go's resolveSankey), routes
//     through dbExpression(measure, false, false), which is validate.go's
//     non-investment switch -- the same "AT RISK: Nullable(Float64)
//     (category 2)" measures (COVERAGE_LINE_PCT, PIPELINE_DURATION_P95,
//     PIPELINE_QUEUE_TIME, TEST_SUITE_DURATION_P95, FLAG_FRICTION_DELTA,
//     FLAG_ERROR_RATE_DELTA, FLAG_ACTIVATION_RATE, etc.) breakdown.go and
//     timeseries.go already proved reach SQL NULL for an all-NULL group.
//     `investmentFull` (query_route.go's registeredInvestmentFullDocument)
//     selects `sankey { nodes { value } edges { value } }`, so this path
//     is registered and reachable today.
//  2. flowmatrix.go's own compileFlowMatrixInvestmentDimension (the
//     AUTHOR/THEME/SUBCATEGORY branch of CompileFlowMatrix) calls the
//     IDENTICAL dbExpression(measure, false, false) when useInvestment
//     resolves false for that request -- reachable via the registered
//     `flowMatrix` document's own $batch.flowMatrix.dimension /
//     .useInvestment input variables, independent of what
//     web/src/lib/graphql/hooks/useChordFlow.ts's current TEAM/REPO/
//     WORK_TYPE-only caller happens to send; the document's persisted
//     text does not fix the dimension.
//
// The other flowMatrix path -- CompileFlowMatrix's TEAM/REPO/WORK_TYPE
// fixed hand-written templates (flowMatrixTeamNodesTemplate and its
// REPO/WORK_TYPE siblings) -- uses uniqExact(...), which is NEVER NULL
// (an empty group returns 0, not NULL), so that specific fixed shape
// does not itself manufacture the NULL. It shares this scan regardless:
// the fix must be safe for both nullable and non-nullable sources, which
// *float64 is (a non-NULL Float64 column scans into a non-nil pointer
// exactly as before).
//
// EXPECTED DIVERGENCE, class = product-decision (CHAOS-4650 ruling,
// applied here per the "Extend to class" ruling, CHAOS-4701 ticket
// comment, chris via team-lead, 2026-08-31): Python's sankey/flow-matrix
// builders (analytics.py) still collapse the same all-NULL case to 0.0
// -- deliberately left unchanged (root AGENTS.md GO-ONLY rule bars
// further behavior changes in the Python GraphQL layer; only the type
// CONTRACT widened, see src/dev_health_ops/api/graphql/models/outputs.py's
// SankeyNode/SankeyEdge doc comments). This is a two-field divergence,
// named precisely -- SankeyNode.Value and SankeyEdge.Value on this
// shared scan path -- and not a prefix that swallows
// BreakdownItem.Value/TimeseriesBucket.Value's own, separately-ledgered
// entries. Do not resolve it by reverting Go to Python's 0.0 collapse.
func queryNodes(ctx context.Context, client QueryClient, q compiledQuery) ([]model.SankeyNode, error) {
	rows, err := client.Query(ctx, q.sql, q.bindings)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	var out []model.SankeyNode
	for rows.Next() {
		var dimension, nodeID string
		// *float64, not float64 -- see this function's doc comment
		// (CHAOS-4701). Every value expression is still coerced to
		// Float64 in SQL (toFloat64) so ONE scan type serves both the
		// uniqExact-based flowMatrix templates and sankey's AVG/ratio
		// measures, which share this function; toFloat64 does NOT
		// collapse a Nullable(Float64) source, so the nullable
		// destination is required regardless of which caller compiled
		// this query. The native driver errors rather than converting
		// between UInt64 and Float64.
		var value *float64
		if scanErr := rows.Scan(&dimension, &nodeID, &value); scanErr != nil {
			return nil, fmt.Errorf("scan: %w", scanErr)
		}
		// Mirrors _execute_sankey_inner's node id shape exactly:
		// f"{dim}:{node_id}" (analytics.py:297), label = node_id
		// (analytics.py:298 -- the RAW node_id, not a resolved display
		// name; flow-matrix nodes are never passed through the
		// repo/team display-name resolver breakdown items use).
		out = append(out, model.SankeyNode{
			ID:        dimension + ":" + nodeID,
			Label:     nodeID,
			Dimension: dimension,
			Value:     value,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}
	return out, nil
}

func queryEdges(ctx context.Context, client QueryClient, q compiledQuery) ([]model.SankeyEdge, error) {
	rows, err := client.Query(ctx, q.sql, q.bindings)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	var out []model.SankeyEdge
	for rows.Next() {
		var sourceDim, targetDim, source, target string
		var value *float64 // *float64, not float64 -- see queryNodes's doc comment (CHAOS-4701)
		if scanErr := rows.Scan(&sourceDim, &targetDim, &source, &target, &value); scanErr != nil {
			return nil, fmt.Errorf("scan: %w", scanErr)
		}
		// Mirrors _execute_sankey_inner's edge shape exactly:
		// source=f"{source_dim}:{source}", target=f"{target_dim}:{target}"
		// (analytics.py:320-322) -- so a FlowMatrixResult edge's
		// source/target strings are directly comparable to its own
		// nodes' ids without a separate dimension field on the edge
		// (SankeyEdge has no dimension field at all -- schema.graphql).
		out = append(out, model.SankeyEdge{
			Source: sourceDim + ":" + source,
			Target: targetDim + ":" + target,
			Value:  value,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}
	return out, nil
}
