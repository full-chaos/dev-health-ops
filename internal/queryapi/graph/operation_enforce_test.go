package graph

import (
	"context"
	"testing"

	"github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2/ast"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
)

// Each root field runs with the scope decided for its key; a root with no decided scope keeps the request's.
func TestOperationEnforceHandsEachRootItsScope(t *testing.T) {
	scopes := map[string]pepgate.Scope{"Query.home": pepgate.Allowlist("22222222-2222-2222-2222-222222222222", []string{"acme/widgets"})}
	run := func(name string) authctx.Claims {
		ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "22222222-2222-2222-2222-222222222222"})
		ctx = context.WithValue(ctx, rootScopesKey{}, scopes)
		ctx = graphql.WithRootFieldContext(ctx, &graphql.RootFieldContext{Object: "Query", Field: graphql.CollectedField{Field: &ast.Field{Name: name}}})
		var seen authctx.Claims
		OperationEnforce{}.InterceptRootField(ctx, func(ctx context.Context) graphql.Marshaler {
			seen, _ = authctx.FromContext(ctx)
			return graphql.Null
		})
		return seen
	}
	if got := run("home"); got.Scope.Kind() != pepgate.KindAllowlist || len(got.Scope.RepoKeys()) != 1 {
		t.Fatalf("Query.home ran with scope kind %v keys %v, want the decided allowlist", got.Scope.Kind(), got.Scope.RepoKeys())
	}
	if got := run("pr"); got.Scope.Kind() != pepgate.KindNone {
		t.Fatalf("Query.pr ran with scope kind %v, want the request's (none)", got.Scope.Kind())
	}
}

// A refusal tells the ingress's RefusalStatus which kind it is, so /graphql and /query answer 503 for unavailable and
// the MCP listener 503 or 403; a deny keeps the guard's form.
func TestOperationEnforceRefusalSetsTheIngressStatus(t *testing.T) {
	for name, tc := range map[string]struct {
		refusal                EnforceRefusal
		code                   string
		unavailable, forbidden bool
	}{
		"deny":        {EnforceRefusal{Forbidden: true, Reason: "role_lacks_action"}, CodeForbidden, false, true},
		"unavailable": {EnforceRefusal{Reason: "pdp_unavailable"}, CodeAuthzUnavailable, true, false},
	} {
		t.Run(name, func(t *testing.T) {
			status := &RefusalStatus{}
			response := refusalResponse(WithRefusalStatus(context.Background(), status), &tc.refusal)
			if len(response.Errors) != 1 || response.Errors[0].Extensions["code"] != tc.code || response.Data != nil {
				t.Fatalf("response %+v, want one %s error and no data", response, tc.code)
			}
			if status.Unavailable() != tc.unavailable || status.Forbidden() != tc.forbidden {
				t.Fatalf("status unavailable=%t forbidden=%t, want %t %t", status.Unavailable(), status.Forbidden(), tc.unavailable, tc.forbidden)
			}
		})
	}
}
