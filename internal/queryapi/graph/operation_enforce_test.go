package graph

import (
	"context"
	goast "go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
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
		ctx = graphql.WithOperationContext(ctx, &graphql.OperationContext{Operation: &ast.OperationDefinition{Operation: ast.Query}})
		// As gqlgen builds it: Object is the field's name.
		ctx = graphql.WithRootFieldContext(ctx, &graphql.RootFieldContext{Object: name, Field: graphql.CollectedField{Field: &ast.Field{Name: name}}})
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

// G16: the organization a resolver reads in is the request claims' OrgID. Only the org guard's superuser rebind and
// the decision (the scope's org on an enforced root) write it into the claims a resolver runs with: a function that
// sets a claims OrgID and hands the claims to authctx.WithClaims. A new writer is a new way to serve an org nobody
// decided; listed both ways.
func TestOnlyTheGuardAndTheDecisionWriteTheClaimsOrg(t *testing.T) {
	want := map[string]bool{
		"operation_org_guard.go OperationOrgGuard.InterceptOperation": true,
	}
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	fset := token.NewFileSet()
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*goast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			site := name + " " + fn.Name.Name
			if fn.Recv != nil && len(fn.Recv.List) == 1 {
				if ident, ok := fn.Recv.List[0].Type.(*goast.Ident); ok {
					site = name + " " + ident.Name + "." + fn.Name.Name
				}
			}
			writes, hands := false, false
			goast.Inspect(fn.Body, func(n goast.Node) bool {
				switch node := n.(type) {
				case *goast.AssignStmt:
					for _, lhs := range node.Lhs {
						if sel, ok := lhs.(*goast.SelectorExpr); ok && sel.Sel.Name == "OrgID" {
							if base, ok := sel.X.(*goast.Ident); ok && base.Name == "claims" {
								writes = true
							}
						}
					}
				case *goast.CallExpr:
					if sel, ok := node.Fun.(*goast.SelectorExpr); ok && sel.Sel.Name == "WithClaims" {
						if pkg, ok := sel.X.(*goast.Ident); ok && pkg.Name == "authctx" {
							hands = true
						}
					}
				}
				return true
			})
			if writes && hands {
				got[site] = true
			}
		}
	}
	for site := range got {
		if !want[site] {
			t.Errorf("%s writes a claims OrgID; only the org guard's rebind and the decision may", site)
		}
	}
	for site := range want {
		if !got[site] {
			t.Errorf("listed writer %s writes no OrgID (stale list)", site)
		}
	}
}
