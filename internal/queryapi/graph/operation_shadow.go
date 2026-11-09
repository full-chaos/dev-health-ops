package graph

import (
	"context"
	"strings"

	"github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2/ast"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
)

// shadowMaxObserves is the most Observe calls one operation makes; the rest count skipped with the reason cap.
const shadowMaxObserves = 7

// ShadowSink is what the interceptor needs of the process's shadow (*policy.Shadow): it tells it what the legacy
// check decided and counts what it will not ask. A nil sink is off.
type ShadowSink interface {
	Observe(ctx context.Context, observation policy.ShadowObservation)
	CountSkipped(ctx context.Context, reason string)
}

// ShadowSubject is the caller's bearer as the HTTP layer saw it after authentication: a value on the context, so the
// interceptor never reads a header. UserAccessToken is true for a token the PDP can resolve; SkipReason names the
// credential class that never asks it.
type ShadowSubject struct {
	Bearer          string
	UserAccessToken bool
	SkipReason      string
}

type shadowSubjectKey struct{}

// WithShadowSubject carries the caller's bearer to OperationShadow.
func WithShadowSubject(ctx context.Context, subject ShadowSubject) context.Context {
	return context.WithValue(ctx, shadowSubjectKey{}, subject)
}

// ShadowSubjectFrom is the subject WithShadowSubject carried, and whether there was one.
func ShadowSubjectFrom(ctx context.Context) (ShadowSubject, bool) {
	subject, ok := ctx.Value(shadowSubjectKey{}).(ShadowSubject)
	return subject, ok
}

// OperationShadow tells the shadow PDP, per root field of an operation, what the legacy check decided: the operation
// guard's verdict AND the field's own pre-resolver check. It runs just before OperationOrgGuard, changes no context
// and no response, and swallows every failure of its own: the operation runs exactly as it would without it.
type OperationShadow struct {
	// Sink is the process's shadow for this request; nil, or a nil result, is off.
	Sink func(ctx context.Context) ShadowSink
	// Enforced reports whether a user caller's root of action is decided by OperationEnforce; such a root has no
	// legacy answer to compare, so it is not observed. nil observes every root.
	Enforced func(ctx context.Context, action string) bool
}

var _ interface {
	graphql.HandlerExtension
	graphql.OperationInterceptor
} = OperationShadow{}

func (OperationShadow) ExtensionName() string                   { return "OperationShadow" }
func (OperationShadow) Validate(graphql.ExecutableSchema) error { return nil }

func (s OperationShadow) InterceptOperation(ctx context.Context, next graphql.OperationHandler) graphql.ResponseHandler {
	s.observe(ctx)
	return next(ctx)
}

// shadowRoot is one root field as the shadow sees it.
type shadowRoot struct {
	key, action, org string
	allowed          bool
}

func (s OperationShadow) observe(ctx context.Context) {
	defer func() { _ = recover() }()
	if s.Sink == nil {
		return
	}
	sink := s.Sink(ctx)
	if sink == nil {
		return
	}
	operation := graphql.GetOperationContext(ctx)
	if operation == nil || operation.Operation == nil {
		return
	}
	fields := rootFields(operation)
	if len(fields) == 0 {
		return
	}
	subject, _ := ShadowSubjectFrom(ctx)
	switch {
	case subject.Bearer == "":
		sink.CountSkipped(ctx, policy.ShadowReasonNoUserBearer)
		return
	case !subject.UserAccessToken:
		sink.CountSkipped(ctx, subject.SkipReason)
		return
	}
	claims, ok := authctx.FromContext(ctx)
	if !ok {
		return
	}
	if requested, invalid := collectOrgArguments(operation); invalid || !allEqual(requested) {
		sink.CountSkipped(ctx, policy.ShadowReasonOpRefused)
		return
	}
	message, rebindTo := operationOrgDecision(ctx, operation)
	if rebindTo != "" {
		claims.OrgID = rebindTo
	}
	prefix := operation.Operation.Operation
	var roots []shadowRoot
	for _, field := range fields {
		key := rootKey(prefix, field.Name)
		action, mapped := fieldActions[key]
		if !mapped {
			sink.CountSkipped(ctx, policy.ShadowReasonUnmapped)
			continue
		}
		if s.Enforced != nil && s.Enforced(ctx, action) {
			continue
		}
		named := fieldOrgArgument(field, operation.Variables)
		org := named
		if org == "" {
			org = claims.OrgID
		}
		allowed := message == ""
		if check, has := fieldPrechecks[key]; allowed && has {
			allowed = check(claims, named)
		}
		roots = append(roots, shadowRoot{key: key, action: action, org: org, allowed: allowed})
	}
	observeRoots(ctx, sink, policy.ShadowObservation{
		Bearer: subject.Bearer, UserAccessToken: true,
		Caller:        policy.ShadowCaller(claims.Holds(rule.ActionAdminOrgs), claims.Role),
		Impersonating: claims.ImpersonationActive || claims.Delegated,
	}, roots)
}

// observeRoots makes one Observe per distinct (action, resource org, verdict), the first field of each naming the row,
// at most shadowMaxObserves per operation; every other distinct one counts skipped with the reason cap.
func observeRoots(ctx context.Context, sink ShadowSink, base policy.ShadowObservation, roots []shadowRoot) {
	type pair struct {
		action, org string
		allowed     bool
	}
	seen := map[pair]bool{}
	observed := 0
	for _, root := range roots {
		p := pair{root.action, strings.ToLower(root.org), root.allowed}
		if seen[p] {
			continue
		}
		seen[p] = true
		if observed == shadowMaxObserves {
			sink.CountSkipped(ctx, policy.ShadowReasonCap)
			continue
		}
		observed++
		observation := base
		observation.Pattern, observation.OrgID, observation.LegacyAllowed = root.key, root.org, root.allowed
		sink.Observe(ctx, observation)
	}
}

func allEqual(values []string) bool {
	for _, v := range values[min(1, len(values)):] {
		if v != values[0] {
			return false
		}
	}
	return true
}

func rootKey(kind ast.Operation, name string) string {
	switch kind {
	case ast.Query:
		return "Query." + name
	case ast.Mutation:
		return "Mutation." + name
	}
	return "Subscription." + name
}

// rootFields are the operation's root fields (introspection ones left out), found through inline fragments and fragment
// spreads as the guard walks them.
func rootFields(operation *graphql.OperationContext) []*ast.Field {
	var out []*ast.Field
	visited := map[string]bool{}
	var walk func(set ast.SelectionSet)
	walk = func(set ast.SelectionSet) {
		for _, selection := range set {
			switch node := selection.(type) {
			case *ast.Field:
				if !strings.HasPrefix(node.Name, "__") {
					out = append(out, node)
				}
			case *ast.InlineFragment:
				walk(node.SelectionSet)
			case *ast.FragmentSpread:
				if visited[node.Name] {
					continue
				}
				visited[node.Name] = true
				if fragment := operation.Doc.Fragments.ForName(node.Name); fragment != nil {
					walk(fragment.SelectionSet)
				}
			}
		}
	}
	walk(operation.Operation.SelectionSet)
	return out
}

// fieldOrgArgument is the org the field's own orgId/org_id argument names, "" when it names none.
func fieldOrgArgument(field *ast.Field, variables map[string]any) string {
	for _, argument := range field.Arguments {
		if argument.Name == "orgId" || argument.Name == "org_id" {
			value, _ := orgArgumentValue(argument.Value, variables)
			return value
		}
	}
	return ""
}
