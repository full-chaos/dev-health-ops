package server

import (
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
)

// CHAOS-12659 Q18: the GraphQL ingresses (/query and its proof routes, run-operation, /graphql, the MCP /query) hand
// the operation layer claims whose Scope is the legacy path's scope of the org they act in. The enforce step of the
// operation replaces it while a cell is on; a zero Scope there would serve nothing.

// ingressSuccessForms is every accepted return of the GraphQL ingress authenticators, listed both ways: each form
// found must be listed for its function, and each listed form must be found. A form is "call:<f>": the claims are the
// result of <f>, either withLegacyScope or legacyClaims (the legacy scope of the claims' own org) or another listed
// authenticator. Anything else is "unlisted:<expression>".
var ingressSuccessForms = map[string][]string{
	"authenticateInternalRequest": {"call:authenticateEdgeCarrier", "call:withLegacyScope", "call:withLegacyScope"},
	"authenticateEdgeCarrier":     {"call:checkEdgeCarrier"},
	"checkEdgeCarrier":            {"call:withLegacyScope"},
	"authenticateGraphQLEdge":     {"call:authenticateEdgeTokenOnly"},
	"authenticateEdgeTokenOnly":   {"call:checkEdgeCarrier"},
	"mcpAuthenticate":             {"call:legacyClaims"},
}

// ingressAccepted reports whether a return statement of fn is an accepted caller: its last result says so.
func ingressAccepted(fn string, results []ast.Expr) bool {
	if len(results) == 1 {
		_, call := results[0].(*ast.CallExpr)
		return call
	}
	last := results[len(results)-1]
	switch fn {
	case "mcpAuthenticate":
		lit, ok := results[1].(*ast.BasicLit)
		return ok && lit.Value == "0"
	case "checkEdgeCarrier", "authenticateEdgeTokenOnly":
		id, ok := last.(*ast.Ident)
		return ok && id.Name == "edgeAccepted"
	}
	id, ok := last.(*ast.Ident)
	return ok && id.Name == "true"
}

func TestGraphQLIngressAuthenticatorsSetTheLegacyScope(t *testing.T) {
	fset := token.NewFileSet()
	src := func(e ast.Expr) string {
		var b strings.Builder
		if err := printer.Fprint(&b, fset, e); err != nil {
			t.Fatal(err)
		}
		return b.String()
	}
	found := map[string][]string{}
	for _, name := range []string{"internal_auth.go", "graphql_edge_route.go", "mcp_route.go"} {
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || ingressSuccessForms[fn.Name.Name] == nil {
				continue
			}
			name := fn.Name.Name
			found[name] = []string{}
			calledInto := map[string]string{}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				assign, ok := n.(*ast.AssignStmt)
				if !ok || len(assign.Rhs) != 1 || len(assign.Lhs) == 0 {
					return true
				}
				if call, ok := assign.Rhs[0].(*ast.CallExpr); ok {
					if callee, ok := call.Fun.(*ast.Ident); ok {
						calledInto[src(assign.Lhs[0])] = callee.Name
					}
				}
				return true
			})
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if _, nested := n.(*ast.FuncLit); nested {
					return false
				}
				ret, ok := n.(*ast.ReturnStmt)
				if !ok || len(ret.Results) == 0 || !ingressAccepted(name, ret.Results) {
					return true
				}
				first := ret.Results[0]
				form := "unlisted:" + src(first)
				switch e := first.(type) {
				case *ast.CallExpr:
					if callee, ok := e.Fun.(*ast.Ident); ok {
						form = "call:" + callee.Name
					}
				case *ast.Ident:
					if calledInto[e.Name] != "" {
						form = "call:" + calledInto[e.Name]
					}
				}
				found[name] = append(found[name], form)
				return true
			})
		}
	}
	for fn, want := range ingressSuccessForms {
		got, ok := found[fn]
		if !ok {
			t.Errorf("%s: not found in the server package", fn)
			continue
		}
		got = slices.Clone(got)
		slices.Sort(got)
		want = slices.Clone(want)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Errorf("%s: accepted returns %v, want %v (an unlisted form hands the operation a claims Scope that is not the legacy scope of its org)", fn, got, want)
		}
	}
}

// withLegacyScope and legacyClaims, the two forms above, scope claims to their own org and change nothing else.
func TestIngressScopeHelpersScopeTheClaimsOwnOrg(t *testing.T) {
	in := authctx.Claims{OrgID: "org-1", Role: "admin", IsSuperuser: true, ImpersonationActive: true, Delegated: true}
	want := in
	want.Scope = pepgate.Legacy("org-1")
	if got := withLegacyScope(in); got != want {
		t.Errorf("withLegacyScope = %+v, want %+v", got, want)
	}
	if got := legacyClaims("org-2"); got != (authctx.Claims{OrgID: "org-2", Scope: pepgate.Legacy("org-2")}) {
		t.Errorf("legacyClaims = %+v", got)
	}
	// A platform admin with no org keeps today's path (product rule 1): a Legacy scope that reads the empty org.
	got := withLegacyScope(authctx.Claims{})
	if org, ok := got.Scope.OrgID(); got.Scope.Kind() != pepgate.KindLegacy || !ok || org != "" {
		t.Errorf("no org: scope kind %d org %q ok %v, want the legacy scope of the empty org", got.Scope.Kind(), org, ok)
	}
}

// Each carrier of /query, and the MCP carrier, reaches the resolvers with the legacy scope of the org it acts in.
func TestGraphQLIngressCarriersServeTheLegacyScope(t *testing.T) {
	verifier, priv := iaVerifier(t)
	handler, seen := iaDispatch(t, verifier)
	envelope := iaEnvelope(t, priv, principal.Claims{OrgID: "org-env", Role: "member"})
	if code := iaPost(t, handler, func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+envelope) }); code != http.StatusOK {
		t.Fatalf("envelope carrier: %d", code)
	}
	if code := iaPost(t, handler, iaHeaders("org-hdr", "member", false, false)); code != http.StatusOK {
		t.Fatalf("header carrier: %d", code)
	}

	store := ecMemberStore()
	edgeHandler, edgeSeen := iaDispatchWithEdge(t, nil, ecEdgeAuth(t, store), store)
	edge := ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), role: "member", tokenVersion: 5})
	if rec := ecPost(edgeHandler, edge); rec.Code != http.StatusOK {
		t.Fatalf("edge carrier: %d %s", rec.Code, rec.Body.String())
	}

	request := httptest.NewRequest(http.MethodPost, "/query", nil)
	iaHeaders("org-mcp", "viewer", false, false)(request)
	mcp, status, reason := mcpAuthenticate(request)
	if status != 0 || reason != "" {
		t.Fatalf("MCP carrier: %d %q", status, reason)
	}

	if len(*seen) != 2 || len(*edgeSeen) != 1 {
		t.Fatalf("resolver runs %d + %d, want 2 + 1", len(*seen), len(*edgeSeen))
	}
	for _, cell := range []struct {
		name  string
		scope pepgate.Scope
		org   string
	}{
		{"envelope", (*seen)[0].Scope, "org-env"},
		{"headers", (*seen)[1].Scope, "org-hdr"},
		{"edge", (*edgeSeen)[0].Scope, ecOrg.String()},
		{"mcp", mcp.Scope, "org-mcp"},
	} {
		org, ok := cell.scope.OrgID()
		if cell.scope.Kind() != pepgate.KindLegacy || !ok || org != cell.org {
			t.Errorf("%s: scope kind %d org %q, want the legacy scope of %s", cell.name, cell.scope.Kind(), org, cell.org)
		}
	}
}

// /graphql's identity (the scope-selected org, after its membership check) reaches the resolvers with the legacy scope
// of that org, which differs from the token's own here.
func TestGraphQLEdgeServesTheLegacyScopeOfTheSelectedOrg(t *testing.T) {
	store := newCountingIdentityStore(&fakeEdgeStore{
		states:  map[uuid.UUID]policy.UserState{ecUser: {IsActive: true, TokenVersion: 5}},
		found:   map[uuid.UUID]bool{ecUser: true},
		members: map[[2]uuid.UUID]bool{{ecUser, ecOrg}: true, {ecUser, irOtherOrg}: true},
	})
	handler, seen := edgeClaimsHarness(t, store)
	recorder := irPost(t, handler, "/graphql", irToken(t, false), irOtherOrg.String())
	if recorder.Code != http.StatusOK || len(*seen) != 1 {
		t.Fatalf("%d ran=%v, want 200 and one run", recorder.Code, *seen)
	}
	got := (*seen)[0]
	if org, ok := got.Scope.OrgID(); got.Scope.Kind() != pepgate.KindLegacy || !ok || org != irOtherOrg.String() || got.OrgID != irOtherOrg.String() {
		t.Fatalf("claims %+v, want org and legacy scope %s", got, irOtherOrg)
	}
}
