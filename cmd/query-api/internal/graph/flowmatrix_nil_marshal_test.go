package graph

// CHAOS-4506: executable disproof (or proof) of a codex P1 finding on
// the delta round at commit 0687d88f0 -- "flow-matrix violates its
// non-null-list contract for empty/degraded results, producing a
// GraphQL error instead of []."
//
// Two source reads disagreed before this file existed: codex argued
// FROM SOURCE that a nil Go slice fails FlowMatrixResult's
// `nodes: [SankeyNode!]!` / `edges: [SankeyEdge!]!` contract. The
// orchestrator ALSO argued from source, reading the array marshaler
// (marshalNSankeyNode2ᚕ...) alone, that `make(graphql.Array, len(nil))`
// is a valid empty array, not null. Neither read is authoritative on
// its own -- see AGENTS.md's "diagnoses need executed repro" -- so
// this file settles it by calling the REAL generated code gqlgen would
// call on a live request, not a synthetic re-derivation of it.
//
// THE PART BOTH SOURCE READS COULD HAVE MISSED, found by tracing the
// actual call chain rather than reading one function in isolation:
// the array marshaler is not the first place a nil slice could turn
// into `graphql.Null`. _FlowMatrixResult_nodes/_edges (generated.go)
// wrap `obj.Nodes`/`obj.Edges` in an `any` via
// `ec.ResolverMiddleware(ctx, func(rctx) (any, error) { return obj.Nodes, nil })`
// and then check `if resTmp == nil`. A NIL Go slice boxed into an
// `any` is a classic non-nil interface (typed nil) -- `resTmp == nil`
// is FALSE for it -- so that check does not fire and execution falls
// through to the array marshaler, which then produces `[]`. This test
// proves that chain end to end rather than asserting either half of
// it in isolation.
//
// This test is intentionally KEPT regardless of which way the result
// goes: if nil-slice-marshals-to-empty is load-bearing for this
// contract (it is -- it's exactly how both `resolveFlowMatrix` and
// `resolveSankey`'s swallow-to-empty paths reach the wire, resolve.go
// :308/:342), it deserves a pin, since a future gqlgen upgrade or a
// refactor of these generated functions could silently change it.

import (
	"bytes"
	"context"
	"testing"
	"time"

	gqlgen "github.com/99designs/gqlgen/graphql"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/analytics"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graphqldate"
)

// newTestExecutionContext builds the minimal *executionContext the
// generated marshal/field functions need to run outside a real HTTP
// request: an OperationContext whose ResolverMiddleware is a plain
// passthrough (the same shape gqlgen's own handler installs by
// default when no request-level middleware is configured), attached
// to ctx the same way graphql.WithOperationContext does on the real
// request path.
func newTestExecutionContext(t *testing.T) (*executionContext, context.Context) {
	t.Helper()
	opCtx := &gqlgen.OperationContext{
		ResolverMiddleware: func(ctx context.Context, next gqlgen.Resolver) (any, error) {
			return next(ctx)
		},
	}
	ec := &executionContext{OperationContext: opCtx}
	ctx := gqlgen.WithOperationContext(context.Background(), opCtx)
	return ec, ctx
}

func marshalToString(t *testing.T, m gqlgen.Marshaler) string {
	t.Helper()
	var buf bytes.Buffer
	m.MarshalGQL(&buf)
	return buf.String()
}

// TestFlowMatrixResult_NilNodesAndEdges_MarshalToEmptyArrays_NotError is
// the direct disproof/proof at the exact two field-resolver functions
// gqlgen generates for FlowMatrixResult.nodes/.edges -- the same code a
// real /query request executes for this operation.
func TestFlowMatrixResult_NilNodesAndEdges_MarshalToEmptyArrays_NotError(t *testing.T) {
	ec, ctx := newTestExecutionContext(t)

	obj := &model.FlowMatrixResult{Nodes: nil, Edges: nil}
	field := gqlgen.CollectedField{}

	nodesResult := ec._FlowMatrixResult_nodes(ctx, field, obj)
	if nodesResult == gqlgen.Null {
		t.Fatalf("nodes: got graphql.Null for a nil slice -- codex's P1 is CONFIRMED REAL: " +
			"a genuinely empty flowMatrix result (or the degrade-to-empty swallow at " +
			"resolve.go:342) would surface as a GraphQL error, not []")
	}
	if got := marshalToString(t, nodesResult); got != "[]" {
		t.Fatalf("nodes: expected the literal empty array \"[]\", got %q", got)
	}

	edgesResult := ec._FlowMatrixResult_edges(ctx, field, obj)
	if edgesResult == gqlgen.Null {
		t.Fatalf("edges: got graphql.Null for a nil slice -- codex's P1 is CONFIRMED REAL")
	}
	if got := marshalToString(t, edgesResult); got != "[]" {
		t.Fatalf("edges: expected the literal empty array \"[]\", got %q", got)
	}
}

// TestMarshalNSankeyNodeSlice_Nil_ProducesEmptyArray pins the specific
// function the orchestrator cited (generated.go's
// marshalNSankeyNode2ᚕ...) in isolation, since it is the one a future
// gqlgen regen is most likely to touch and the one whose behavior this
// whole question turns on: make(graphql.Array, len(nil)) is a valid,
// non-nil, zero-length Array, and ranging over it in MarshalGQL writes
// "[" immediately followed by "]" with no null-check short-circuit.
func TestMarshalNSankeyNodeSlice_Nil_ProducesEmptyArray(t *testing.T) {
	ec, ctx := newTestExecutionContext(t)

	var nilNodes []model.SankeyNode
	result := ec.marshalNSankeyNode2ᚕgithubᚗcomᚋfullᚑchaosᚋdevᚑhealthᚑopsᚋcmdᚋqueryᚑapiᚋinternalᚋgraphᚋmodelᚐSankeyNodeᚄ(ctx, nil, nilNodes)
	if result == gqlgen.Null {
		t.Fatalf("marshalNSankeyNode2ᚕ...: got graphql.Null for a nil slice input")
	}
	if got := marshalToString(t, result); got != "[]" {
		t.Fatalf("marshalNSankeyNode2ᚕ...: expected \"[]\", got %q", got)
	}

	var nilEdges []model.SankeyEdge
	edgeResult := ec.marshalNSankeyEdge2ᚕgithubᚗcomᚋfullᚑchaosᚋdevᚑhealthᚑopsᚋcmdᚋqueryᚑapiᚋinternalᚋgraphᚋmodelᚐSankeyEdgeᚄ(ctx, nil, nilEdges)
	if edgeResult == gqlgen.Null {
		t.Fatalf("marshalNSankeyEdge2ᚕ...: got graphql.Null for a nil slice input")
	}
	if got := marshalToString(t, edgeResult); got != "[]" {
		t.Fatalf("marshalNSankeyEdge2ᚕ...: expected \"[]\", got %q", got)
	}
}

// alwaysErrorsQueryClient satisfies analytics.QueryClient and fails
// every query -- the trigger for resolveFlowMatrix's degrade-to-empty
// swallow at resolve.go:342 (`nodes, edges = nil, nil`), so the object
// this test marshals is the REAL output of that real code path, not a
// hand-built stand-in for it.
type alwaysErrorsQueryClient struct{}

func (alwaysErrorsQueryClient) Query(ctx context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return nil, errTestForcedQueryFailure
}

var errTestForcedQueryFailure = &testForcedQueryError{}

type testForcedQueryError struct{}

func (*testForcedQueryError) Error() string { return "forced failure for CHAOS-4506 P1 disproof test" }

// TestResolveFlowMatrix_DegradePath_NilResult_MarshalsToEmptyArrays
// exercises analytics.Resolve's REAL degrade-to-empty swallow
// (resolve.go:342, "if it is reachable from a test" -- it is, via the
// exported orchestrator) and feeds its actual *model.FlowMatrixResult
// through the same generated field marshalers, so the object under
// test is the one the real degrade path produces, not a literal
// {Nodes: nil, Edges: nil} constructed by hand.
func TestResolveFlowMatrix_DegradePath_NilResult_MarshalsToEmptyArrays(t *testing.T) {
	batch := model.AnalyticsRequestInput{
		FlowMatrix: &model.FlowMatrixRequestInput{
			Dimension: model.DimensionInputTeam,
			Measure:   model.MeasureInputCount,
			DateRange: &model.DateRangeInput{
				StartDate: graphqldate.New(mustParseTestDate(t, "2026-08-01")),
				EndDate:   graphqldate.New(mustParseTestDate(t, "2026-08-31")),
			},
			MaxNodes: 50,
			MaxEdges: 200,
		},
	}

	result, err := analytics.Resolve(context.Background(), alwaysErrorsQueryClient{}, "org-chaos-4506-p1-disproof", batch)
	if err != nil {
		t.Fatalf("analytics.Resolve: unexpected top-level error (expected a swallowed degrade, not a fatal one): %v", err)
	}
	if result.FlowMatrix == nil {
		t.Fatalf("expected a non-nil FlowMatrixResult even on the degrade path (resolve.go:345 always returns one)")
	}
	if result.FlowMatrix.Nodes != nil || result.FlowMatrix.Edges != nil {
		t.Fatalf("expected the degrade path to produce nil Nodes/Edges (resolve.go:342) -- "+
			"got Nodes=%v Edges=%v; this test's premise depends on that shape",
			result.FlowMatrix.Nodes, result.FlowMatrix.Edges)
	}

	ec, ctx := newTestExecutionContext(t)
	field := gqlgen.CollectedField{}

	nodesResult := ec._FlowMatrixResult_nodes(ctx, field, result.FlowMatrix)
	if nodesResult == gqlgen.Null {
		t.Fatalf("degrade path: nodes marshaled to graphql.Null -- codex's P1 IS REAL for the swallow path")
	}
	if got := marshalToString(t, nodesResult); got != "[]" {
		t.Fatalf("degrade path: nodes expected \"[]\", got %q", got)
	}

	edgesResult := ec._FlowMatrixResult_edges(ctx, field, result.FlowMatrix)
	if edgesResult == gqlgen.Null {
		t.Fatalf("degrade path: edges marshaled to graphql.Null -- codex's P1 IS REAL for the swallow path")
	}
	if got := marshalToString(t, edgesResult); got != "[]" {
		t.Fatalf("degrade path: edges expected \"[]\", got %q", got)
	}
}

func mustParseTestDate(t *testing.T, s string) time.Time {
	t.Helper()
	parsed, err := time.Parse("2006-01-02", s)
	if err != nil {
		t.Fatalf("parse test date %q: %v", s, err)
	}
	return parsed
}
