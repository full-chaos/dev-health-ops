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
