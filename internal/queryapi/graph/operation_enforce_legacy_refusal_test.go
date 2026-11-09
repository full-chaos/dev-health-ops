package graph

import (
	"context"
	"testing"

	"github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2/ast"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
)

// CHAOS-13334: a root the decision refused (scope KindNone) runs its resolver only while its own legacy check refuses
// the caller, with the request's claims (the ones that check judged), so the resolver answers with its legacy refusal.
// While the check admits, or the root has none, the resolver never runs: the root answers FORBIDDEN, no data.
func TestOperationEnforceRefusedRootRunsOnlyItsLegacyRefusal(t *testing.T) {
	const org = "22222222-2222-2222-2222-222222222222"
	run := func(name, role string) (ran bool, seen authctx.Claims, errs int) {
		ctx := graphql.WithResponseContext(context.Background(), graphql.DefaultErrorPresenter, graphql.DefaultRecover)
		ctx = authctx.WithClaims(ctx, authctx.Claims{OrgID: org, Role: role, Scope: pepgate.Legacy(org)})
		ctx = context.WithValue(ctx, rootScopesKey{}, map[string]pepgate.Scope{"Query." + name: {}})
		ctx = graphql.WithOperationContext(ctx, &graphql.OperationContext{Operation: &ast.OperationDefinition{Operation: ast.Query}})
		ctx = graphql.WithRootFieldContext(ctx, &graphql.RootFieldContext{Object: name, Field: graphql.CollectedField{Field: &ast.Field{Name: name}}})
		OperationEnforce{}.InterceptRootField(ctx, func(ctx context.Context) graphql.Marshaler {
			ran = true
			seen, _ = authctx.FromContext(ctx)
			return graphql.Null
		})
		for _, err := range graphql.GetErrors(ctx) {
			if err.Extensions["code"] == CodeForbidden {
				errs++
			}
		}
		return ran, seen, errs
	}
	for _, role := range []string{"member", "viewer"} {
		ran, seen, errs := run("dataHealth", role)
		if !ran || errs != 0 || seen.Scope.Kind() != pepgate.KindLegacy {
			t.Fatalf("%s: ran=%t errors=%d scope=%v, want the resolver run with the request's scope (its legacy refusal)", role, ran, errs, seen.Scope.Kind())
		}
	}
	for name, role := range map[string]string{"dataHealth": "admin", "home": "member"} {
		if ran, _, errs := run(name, role); ran || errs != 1 {
			t.Fatalf("Query.%s as %s: ran=%t forbidden errors=%d, want no resolver and one FORBIDDEN error", name, role, ran, errs)
		}
	}
}
