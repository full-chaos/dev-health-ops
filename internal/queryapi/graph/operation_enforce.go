package graph

import (
	"context"
	"sync/atomic"

	"github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2/gqlerror"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
)

// EnforceRoot is one root field of an operation as the decision sees it: its key ("Query.name") and catalog action.
type EnforceRoot struct {
	Key    string
	Action string
}

// The codes of a refused operation's one error (extensions.code).
const (
	// CodeForbidden is a decision point's deny: HTTP 200 on /graphql and /query, the org guard's form.
	CodeForbidden = "FORBIDDEN"
	// CodeAuthzUnavailable is no verdict, or an obligation not carried out: HTTP 503, extensions.reason names why.
	CodeAuthzUnavailable = "SERVICE_UNAVAILABLE"
)

// EnforceRefusal refuses a whole operation. Forbidden is a decision point's deny; otherwise no verdict exists or an
// obligation is not carried out, and the operation is unavailable. Reason is the closed reason.
type EnforceRefusal struct {
	Forbidden bool
	Reason    string
}

// OperationEnforce decides the root fields of an operation before any resolver runs (CHAOS-10865). It sits after
// OperationOrgGuard, so the organization it decides for is the one the operation is served for (the caller's, or the
// one a verified superuser named). A refusal answers the whole operation with no data: a deny with code FORBIDDEN,
// the guard's form; anything else with code SERVICE_UNAVAILABLE and its reason, and the HTTP status 503 where the
// ingress carries a RefusalStatus. An admitted operation runs each root field with the scope decided for it.
type OperationEnforce struct {
	// Authorize decides the roots for the caller of claims; nil is off (the operation runs as without it).
	Authorize func(ctx context.Context, claims authctx.Claims, roots []EnforceRoot) (map[string]pepgate.Scope, *EnforceRefusal)
}

var _ interface {
	graphql.HandlerExtension
	graphql.OperationInterceptor
	graphql.RootFieldInterceptor
} = OperationEnforce{}

func (OperationEnforce) ExtensionName() string                   { return "OperationEnforce" }
func (OperationEnforce) Validate(graphql.ExecutableSchema) error { return nil }

type rootScopesKey struct{}

func (e OperationEnforce) InterceptOperation(ctx context.Context, next graphql.OperationHandler) graphql.ResponseHandler {
	operation := graphql.GetOperationContext(ctx)
	if e.Authorize == nil || operation == nil || operation.Operation == nil {
		return next(ctx)
	}
	var roots []EnforceRoot
	for _, field := range rootFields(operation) {
		key := rootKey(operation.Operation.Operation, field.Name)
		roots = append(roots, EnforceRoot{Key: key, Action: fieldActions[key]})
	}
	claims, _ := authctx.FromContext(ctx)
	scopes, refusal := e.Authorize(ctx, claims, roots)
	if refusal != nil {
		response := refusalResponse(ctx, refusal)
		return func(context.Context) *graphql.Response { return response }
	}
	return next(context.WithValue(ctx, rootScopesKey{}, scopes))
}

// InterceptRootField hands each root field its decided scope through the request's claims. The claims' OrgID keeps the
// ingress's spelling (main's answer for every spelling, product rule 1); the org helpers serve it only while it is the
// scope's organization as a UUID.
func (OperationEnforce) InterceptRootField(ctx context.Context, next graphql.RootResolver) graphql.Marshaler {
	scopes, _ := ctx.Value(rootScopesKey{}).(map[string]pepgate.Scope)
	field := graphql.GetRootFieldContext(ctx)
	operation := graphql.GetOperationContext(ctx)
	if field == nil || operation == nil || operation.Operation == nil {
		return next(ctx)
	}
	// gqlgen's RootFieldContext.Object is the field's name, not its type: the key takes the operation's type.
	if scope, ok := scopes[rootKey(operation.Operation.Operation, field.Field.Name)]; ok {
		if claims, ok := authctx.FromContext(ctx); ok {
			claims.Scope = scope
			ctx = authctx.WithClaims(ctx, claims)
		}
	}
	return next(ctx)
}

func refusalResponse(ctx context.Context, refusal *EnforceRefusal) *graphql.Response {
	status, _ := ctx.Value(refusalStatusKey{}).(*RefusalStatus)
	if refusal.Forbidden {
		if status != nil {
			status.forbidden.Store(true)
		}
		return &graphql.Response{Errors: gqlerror.List{{Message: "Not authorized", Extensions: map[string]any{"code": CodeForbidden}}}}
	}
	if status != nil {
		status.unavailable.Store(true)
	}
	return &graphql.Response{Errors: gqlerror.List{{Message: "Authorization unavailable",
		Extensions: map[string]any{"code": CodeAuthzUnavailable, "reason": refusal.Reason}}}}
}

// RefusalStatus is how an ingress learns that OperationEnforce refused an operation, so it can answer with its own
// status (503 for unavailable; the MCP listener also 403 for a deny).
type RefusalStatus struct {
	unavailable atomic.Bool
	forbidden   atomic.Bool
}

type refusalStatusKey struct{}

// WithRefusalStatus carries status to OperationEnforce.
func WithRefusalStatus(ctx context.Context, status *RefusalStatus) context.Context {
	return context.WithValue(ctx, refusalStatusKey{}, status)
}

// Unavailable reports whether the operation was refused as unavailable.
func (s *RefusalStatus) Unavailable() bool { return s != nil && s.unavailable.Load() }

// Forbidden reports whether the operation was refused by a decision point's deny.
func (s *RefusalStatus) Forbidden() bool { return s != nil && s.forbidden.Load() }
