package graph

import (
	"context"
	"strings"

	"github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
)

// OperationOrgGuard is the Go form of the Python schema extension
// OrgIdAuthExtension (api/graphql/extensions.py) for every operation: it
// reads every orgId/org_id argument of the operation BEFORE any field runs,
// refuses one that is not a non-empty, unpadded string, refuses an operation
// that names more than one organization, and refuses one that names an
// organization other than the caller's. The refusal answers the whole
// operation (data null, no path, no location), exactly as Python's extension
// does, so a field that would have run under the caller's org never answers a
// request that named another one, and a mutation that would have written
// nothing never reaches its resolver.
//
// Scope, like Python's: only fields' own arguments named orgId/org_id are
// read. An organization id carried inside an input object (for example
// `input: { orgId }`) is not seen here, in Python or in Go.
//
// Named divergence: Python lets a verified, non-impersonating superuser name
// another organization. The identity query-api verifies carries no "verified"
// flag for a superuser, so the guard refuses that case where Python allows it:
// the safe direction.
type OperationOrgGuard struct{}

var _ interface {
	graphql.HandlerExtension
	graphql.OperationInterceptor
} = OperationOrgGuard{}

func (OperationOrgGuard) ExtensionName() string { return "OperationOrgGuard" }

func (OperationOrgGuard) Validate(graphql.ExecutableSchema) error { return nil }

func (OperationOrgGuard) InterceptOperation(ctx context.Context, next graphql.OperationHandler) graphql.ResponseHandler {
	operation := graphql.GetOperationContext(ctx)
	if operation == nil || operation.Operation == nil {
		return next(ctx)
	}
	if message := operationOrgViolation(ctx, operation); message != "" {
		return func(context.Context) *graphql.Response {
			return &graphql.Response{Errors: gqlerror.List{{Message: message}}}
		}
	}
	return next(ctx)
}

// operationOrgViolation is the refusal message for the operation, or "".
func operationOrgViolation(ctx context.Context, operation *graphql.OperationContext) string {
	var requested []string
	invalid := false
	visited := map[string]bool{}
	var collect func(set ast.SelectionSet)
	collect = func(set ast.SelectionSet) {
		for _, selection := range set {
			switch node := selection.(type) {
			case *ast.Field:
				for _, argument := range node.Arguments {
					if argument.Name != "orgId" && argument.Name != "org_id" {
						continue
					}
					value, ok := orgArgumentValue(argument.Value, operation.Variables)
					if !ok || value == "" || value != strings.TrimSpace(value) {
						invalid = true
						continue
					}
					requested = append(requested, value)
				}
				collect(node.SelectionSet)
			case *ast.InlineFragment:
				collect(node.SelectionSet)
			case *ast.FragmentSpread:
				if visited[node.Name] {
					continue
				}
				visited[node.Name] = true
				if fragment := operation.Doc.Fragments.ForName(node.Name); fragment != nil {
					collect(fragment.SelectionSet)
				}
			}
		}
	}
	collect(operation.Operation.SelectionSet)
	if invalid {
		return "A valid organization ID is required"
	}
	if len(requested) == 0 {
		return ""
	}
	first := requested[0]
	for _, other := range requested[1:] {
		if other != first {
			return "Only one organization may be queried per operation"
		}
	}
	claims, ok := authctx.FromContext(ctx)
	if !ok || claims.OrgID == "" {
		return "Authorization required"
	}
	if claims.OrgID != first {
		return "Access denied: cannot query org '" + first + "'"
	}
	return ""
}

// orgArgumentValue is the string an orgId argument carries: a string literal,
// or a variable holding a string. Anything else (null, a number, an absent
// variable) is not a string.
func orgArgumentValue(value *ast.Value, variables map[string]any) (string, bool) {
	if value == nil {
		return "", false
	}
	switch value.Kind {
	case ast.StringValue, ast.BlockValue:
		return value.Raw, true
	case ast.Variable:
		text, ok := variables[value.Raw].(string)
		return text, ok
	}
	return "", false
}
