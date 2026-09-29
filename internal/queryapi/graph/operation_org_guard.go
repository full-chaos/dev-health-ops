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
// A verified, non-impersonating superuser may name another org, as in Python
// (`(not is_superuser or is_impersonating) and requested != original` is the
// refusal there): the operation is then run against the named org, which the
// guard does by carrying it in the request's claims. Claims.IsSuperuser is
// copied from the verified envelope only, so it is Python's verified
// superuser. An impersonating superuser is refused like anyone else.
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
	message, rebindTo := operationOrgDecision(ctx, operation)
	if message != "" {
		return func(context.Context) *graphql.Response {
			return &graphql.Response{Errors: gqlerror.List{{Message: message}}}
		}
	}
	if rebindTo != "" {
		// A verified, non-impersonating superuser named another org: as in
		// Python, every field of this operation runs against that org.
		claims, _ := authctx.FromContext(ctx)
		claims.OrgID = rebindTo
		ctx = authctx.WithClaims(ctx, claims)
	}
	return next(ctx)
}

// operationOrgViolation is the refusal message for the operation, or "".
func operationOrgViolation(ctx context.Context, operation *graphql.OperationContext) string {
	message, _ := operationOrgDecision(ctx, operation)
	return message
}

// operationOrgDecision is the guard's whole decision: the refusal message ("" when
// the operation may run) and, when a verified, non-impersonating superuser named
// an org other than their own, that org (the operation then runs against it).
func operationOrgDecision(ctx context.Context, operation *graphql.OperationContext) (message, rebindTo string) {
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
		return "A valid organization ID is required", ""
	}
	if len(requested) == 0 {
		return "", ""
	}
	first := requested[0]
	for _, other := range requested[1:] {
		if other != first {
			return "Only one organization may be queried per operation", ""
		}
	}
	claims, ok := authctx.FromContext(ctx)
	if !ok || claims.OrgID == "" {
		return "Authorization required", ""
	}
	if claims.OrgID == first {
		return "", ""
	}
	if claims.IsSuperuser && !claims.ImpersonationActive {
		return "", first
	}
	return "Access denied: cannot query org '" + first + "'", ""
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
