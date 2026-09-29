package server

// CHAOS-7078: query-api's shared gqlgen server ran with library DEFAULTS
// (gqlhandler.NewDefaultServer's own doc comment: "Deprecated: This was and
// is just an example ... Not for prod") -- introspection on, Automatic
// Persisted Queries on, GET and websocket transports on, no depth or
// complexity limit anywhere. Before /graphql moves to Go (CHAOS-6263), this
// closes that gap with explicit options in newGraphQLServer (query_route.go),
// each with its own test in graphql_server_limits_test.go. gqlgen ships a
// complexity limiter (extension.ComplexityLimit) but no depth limiter --
// this file adds the missing half, matching that extension's own shape.

import (
	"context"

	"github.com/99designs/gqlgen/complexity"
	"github.com/99designs/gqlgen/graphql"
	"github.com/99designs/gqlgen/graphql/errcode"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// graphQLComplexityLimit and graphQLDepthLimit are sized from the real
// registered-document set (55 documents at measurement time, run through
// github.com/99designs/gqlgen/complexity.Calculate and this file's own
// selectionSetDepth via cmd/registrydump's real output -- never a
// hand-typed guess). The most expensive registered document measured was
// aiReviewLoad at complexity 76, depth 5 (operatingReview and
// dataHealthIdentity tie for deepest at 5). Set at roughly double that
// headroom: large enough that an ordinary future document does not need a
// limit bump on every schema change, small enough to still bound a
// pathological selection shape a REGISTERED document's own variables or
// fragments could still construct (registration fixes the document TEXT,
// which fixes complexity/depth for THAT text deterministically -- these
// limits exist for the day a registered document itself grows past what
// was measured here, not as a defense against an unregistered one, which
// operationForDocument's digest gate already refuses outright).
// TestEveryRegisteredDocumentStaysUnderTheConfiguredLimits pins the
// actual measured set against these numbers, so a future document that
// needs more fails loudly there, in CI, not silently at request time in
// production.
const (
	graphQLComplexityLimit = 150
	graphQLDepthLimit      = 10
)

// depthLimitExtensionName / errDepthLimitCode name this extension and its
// error code the same way extension.ComplexityLimit names its own
// ("ComplexityLimit" / "COMPLEXITY_LIMIT_EXCEEDED").
const (
	depthLimitExtensionName = "DepthLimit"
	errDepthLimitCode       = "DEPTH_LIMIT_EXCEEDED"
)

// depthLimit refuses an operation whose selection-set nesting exceeds Max.
// Mirrors extension.ComplexityLimit's own shape (graphql.HandlerExtension +
// graphql.OperationContextMutator) for the one cost dimension gqlgen does
// not bundle a limiter for.
type depthLimit struct {
	Max int
}

var (
	_ graphql.HandlerExtension        = depthLimit{}
	_ graphql.OperationContextMutator = depthLimit{}
)

func (depthLimit) ExtensionName() string { return depthLimitExtensionName }

func (depthLimit) Validate(graphql.ExecutableSchema) error { return nil }

func (d depthLimit) MutateOperationContext(_ context.Context, opCtx *graphql.OperationContext) *gqlerror.Error {
	op := opCtx.Doc.Operations.ForName(opCtx.OperationName)
	if op == nil {
		return nil
	}
	depth := selectionSetDepth(op.SelectionSet, 1)
	if depth > d.Max {
		err := gqlerror.Errorf("operation has depth %d, which exceeds the limit of %d", depth, d.Max)
		errcode.Set(err, errDepthLimitCode)
		return err
	}
	return nil
}

// selectionSetDepth walks a selection set (a query/mutation's field
// nesting), following fragment spreads and inline fragments -- both widen
// a document's real depth without adding a literal nested `{` at the
// caller's own top level, so a depth limiter that only counted brace
// nesting in the raw text could be defeated by moving the deep part into a
// fragment definition.
func selectionSetDepth(set ast.SelectionSet, current int) int {
	deepest := current
	for _, sel := range set {
		switch s := sel.(type) {
		case *ast.Field:
			if len(s.SelectionSet) > 0 {
				if d := selectionSetDepth(s.SelectionSet, current+1); d > deepest {
					deepest = d
				}
			}
		case *ast.FragmentSpread:
			if s.Definition != nil {
				if d := selectionSetDepth(s.Definition.SelectionSet, current); d > deepest {
					deepest = d
				}
			}
		case *ast.InlineFragment:
			if d := selectionSetDepth(s.SelectionSet, current); d > deepest {
				deepest = d
			}
		}
	}
	return deepest
}

// documentComplexity reports what complexity.Calculate computes for a
// parsed operation over es -- a named wrapper so
// graphql_server_limits_test.go's measurement and newGraphQLServer's own
// FixedComplexityLimit are provably reading the SAME function, never two
// copies of the same formula that could drift.
func documentComplexity(es graphql.ExecutableSchema, op *ast.OperationDefinition) int {
	return complexity.Calculate(es, op, nil)
}
