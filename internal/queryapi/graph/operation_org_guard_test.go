package graph

import (
	"context"
	"testing"

	"github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/parser"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
)

func guardOperation(t *testing.T, document string, variables map[string]any) *graphql.OperationContext {
	t.Helper()
	doc, err := parser.ParseQuery(&ast.Source{Input: document})
	if err != nil {
		t.Fatal(err)
	}
	return &graphql.OperationContext{Doc: doc, Operation: doc.Operations[0], Variables: variables}
}

// The guard answers what OrgIdAuthExtension answers, before any field runs,
// for a mutation and for a query alike.
func TestOperationOrgViolationOverItsInputDomain(t *testing.T) {
	t.Parallel()
	const own = "org-own"
	byVariable := `mutation M($orgId: String!, $x: String) { a: doIt(orgId: $orgId) b: doIt(org_id: $x) }`
	single := `mutation M($orgId: String) { doIt(orgId: $orgId) }`
	literal := `mutation M { doIt(orgId: "org-own") }`
	fragment := `mutation M($orgId: String) { ...F } fragment F on Mutation { doIt(orgId: $orgId) }`
	nested := `mutation M($orgId: String) { doIt(orgId: $orgId) { inner(orgId: "other") } }`
	noOrg := `mutation M { doIt }`
	querySingle := `query Q($orgId: String) { doIt(orgId: $orgId) }`
	queryNested := `query Q($orgId: String) { doIt(orgId: $orgId) { inner(orgId: "other") } }`
	queryTwo := `query Q($orgId: String!, $x: String) { a: doIt(orgId: $orgId) b: doIt(org_id: $x) }`
	claims := authctx.Claims{OrgID: own}
	superuser := authctx.Claims{OrgID: own, IsSuperuser: true}
	impersonating := authctx.Claims{OrgID: own, IsSuperuser: true, ImpersonationActive: true}
	for _, tc := range []struct {
		name      string
		document  string
		variables map[string]any
		claims    *authctx.Claims
		want      string
	}{
		{"own org by variable", single, map[string]any{"orgId": own}, &claims, ""},
		{"own org literal", literal, nil, &claims, ""},
		{"own org through a fragment", fragment, map[string]any{"orgId": own}, &claims, ""},
		{"no orgId argument", noOrg, nil, &claims, ""},
		{"another org", single, map[string]any{"orgId": "org-other"}, &claims, "Access denied: cannot query org 'org-other'"},
		{"variable absent", single, map[string]any{}, &claims, "A valid organization ID is required"},
		{"variable null", single, map[string]any{"orgId": nil}, &claims, "A valid organization ID is required"},
		{"variable a number", single, map[string]any{"orgId": 7}, &claims, "A valid organization ID is required"},
		{"empty", single, map[string]any{"orgId": ""}, &claims, "A valid organization ID is required"},
		{"padded left", single, map[string]any{"orgId": " " + own}, &claims, "A valid organization ID is required"},
		{"padded right", single, map[string]any{"orgId": own + "\n"}, &claims, "A valid organization ID is required"},
		{"two different orgs", byVariable, map[string]any{"orgId": own, "x": "org-other"}, &claims, "Only one organization may be queried per operation"},
		{"the same org twice", byVariable, map[string]any{"orgId": own, "x": own}, &claims, ""},
		{"nested field argument", nested, map[string]any{"orgId": own}, &claims, "Only one organization may be queried per operation"},
		{"query: own org", querySingle, map[string]any{"orgId": own}, &claims, ""},
		{"query: another org", querySingle, map[string]any{"orgId": "org-other"}, &claims, "Access denied: cannot query org 'org-other'"},
		{"query: empty", querySingle, map[string]any{"orgId": ""}, &claims, "A valid organization ID is required"},
		{"query: padded", querySingle, map[string]any{"orgId": " " + own}, &claims, "A valid organization ID is required"},
		{"query: variable absent", querySingle, map[string]any{}, &claims, "A valid organization ID is required"},
		{"query: two different orgs", queryTwo, map[string]any{"orgId": own, "x": "org-other"}, &claims, "Only one organization may be queried per operation"},
		{"query: nested field argument", queryNested, map[string]any{"orgId": own}, &claims, "Only one organization may be queried per operation"},
		{"query: no identity", querySingle, map[string]any{"orgId": own}, nil, "Authorization required"},
		{"superuser naming another org", single, map[string]any{"orgId": "org-other"}, &superuser, ""},
		{"superuser with an empty orgId", single, map[string]any{"orgId": ""}, &superuser, "A valid organization ID is required"},
		{"superuser naming two orgs", byVariable, map[string]any{"orgId": own, "x": "org-other"}, &superuser, "Only one organization may be queried per operation"},
		{"impersonating superuser naming another org", single, map[string]any{"orgId": "org-other"}, &impersonating, "Access denied: cannot query org 'org-other'"},
		{"no identity", single, map[string]any{"orgId": own}, nil, "Authorization required"},
		{"identity without an org", single, map[string]any{"orgId": own}, &authctx.Claims{}, "Authorization required"},
	} {
		ctx := context.Background()
		if tc.claims != nil {
			ctx = authctx.WithClaims(ctx, *tc.claims)
		}
		got := operationOrgViolation(ctx, guardOperation(t, tc.document, tc.variables))
		if got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
}

// Every field of the executable schema that takes an orgId (or org_id)
// argument is covered by the guard: named with another org it is refused
// before it runs, named with the caller's own org it is not. The list is read
// from the schema, so a field added later is covered or fails here.
func TestOperationOrgGuardCoversEveryOrgIDField(t *testing.T) {
	t.Parallel()
	const own = "org-own"
	claims := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: own})
	covered := 0
	for _, root := range []*ast.Definition{parsedSchema.Query, parsedSchema.Mutation} {
		if root == nil {
			continue
		}
		keyword := "query"
		if root == parsedSchema.Mutation {
			keyword = "mutation"
		}
		for _, field := range root.Fields {
			for _, argument := range field.Arguments {
				if argument.Name != "orgId" && argument.Name != "org_id" {
					continue
				}
				covered++
				document := keyword + " Q($o: String) { " + field.Name + "(" + argument.Name + ": $o) }"
				if got := operationOrgViolation(claims, guardOperation(t, document, map[string]any{"o": "org-other"})); got != "Access denied: cannot query org 'org-other'" {
					t.Errorf("%s.%s(%s): another org: got %q", root.Name, field.Name, argument.Name, got)
				}
				if got := operationOrgViolation(claims, guardOperation(t, document, map[string]any{"o": own})); got != "" {
					t.Errorf("%s.%s(%s): own org: got %q", root.Name, field.Name, argument.Name, got)
				}
			}
		}
	}
	if covered < 30 {
		t.Fatalf("only %d org-scoped fields found in the schema; the sweep is not reading it", covered)
	}
}

// A verified, non-impersonating superuser naming another org runs the whole
// operation against that org (Python rebinds the context org); anyone else runs
// against their own.
func TestOperationOrgGuardRunsTheOperationAgainstTheNamedOrgForASuperuser(t *testing.T) {
	t.Parallel()
	operation := guardOperation(t, `query Q($orgId: String) { doIt(orgId: $orgId) }`, map[string]any{"orgId": "org-other"})
	run := func(claims authctx.Claims) (org string, ran bool) {
		ctx := graphql.WithOperationContext(authctx.WithClaims(context.Background(), claims), operation)
		handler := OperationOrgGuard{}.InterceptOperation(ctx, func(inner context.Context) graphql.ResponseHandler {
			ran = true
			seen, _ := authctx.FromContext(inner)
			org = seen.OrgID
			return func(context.Context) *graphql.Response { return &graphql.Response{} }
		})
		handler(ctx)
		return org, ran
	}
	if org, ran := run(authctx.Claims{OrgID: "org-own", IsSuperuser: true}); !ran || org != "org-other" {
		t.Errorf("superuser: ran=%v org=%q, want the operation to run as org-other", ran, org)
	}
	if _, ran := run(authctx.Claims{OrgID: "org-own"}); ran {
		t.Error("an ordinary caller naming another org must not run the operation")
	}
	own := guardOperation(t, `query Q($orgId: String) { doIt(orgId: $orgId) }`, map[string]any{"orgId": "org-own"})
	ctx := graphql.WithOperationContext(authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-own", IsSuperuser: true}), own)
	var seenOrg string
	OperationOrgGuard{}.InterceptOperation(ctx, func(inner context.Context) graphql.ResponseHandler {
		seen, _ := authctx.FromContext(inner)
		seenOrg = seen.OrgID
		return func(context.Context) *graphql.Response { return &graphql.Response{} }
	})(ctx)
	if seenOrg != "org-own" {
		t.Errorf("superuser naming their own org: ran as %q, want org-own", seenOrg)
	}
}

// CHAOS-7710 (owner decision 2026-10-04): an org-less verified superuser has
// no org data access. Naming an org without an org in the claims is refused,
// and the operation never runs. The recorded Python answer rebinds instead;
// the difference is declared in docs/go-migration-matrix.md.
func TestOperationOrgGuardRefusesAnOrglessSuperuserNamingAnOrg(t *testing.T) {
	t.Parallel()
	operation := guardOperation(t, `query Q($orgId: String) { doIt(orgId: $orgId) }`, map[string]any{"orgId": "org-other"})
	ctx := graphql.WithOperationContext(authctx.WithClaims(context.Background(), authctx.Claims{IsSuperuser: true}), operation)
	ran := false
	response := OperationOrgGuard{}.InterceptOperation(ctx, func(context.Context) graphql.ResponseHandler {
		ran = true
		return func(context.Context) *graphql.Response { return &graphql.Response{} }
	})(ctx)
	if ran {
		t.Fatal("an org-less superuser naming an org must not run the operation")
	}
	if response == nil || len(response.Errors) != 1 || response.Errors[0].Message != "Authorization required" {
		t.Fatalf("got %+v, want one error %q", response, "Authorization required")
	}
}

// An impersonation session carries the target org id, so the operation naming
// that org runs, as that org.
func TestOperationOrgGuardServesAnImpersonationSessionNamingItsOrg(t *testing.T) {
	t.Parallel()
	operation := guardOperation(t, `query Q($orgId: String) { doIt(orgId: $orgId) }`, map[string]any{"orgId": "org-target"})
	claims := authctx.Claims{OrgID: "org-target", IsSuperuser: true, ImpersonationActive: true}
	ctx := graphql.WithOperationContext(authctx.WithClaims(context.Background(), claims), operation)
	var seenOrg string
	ran := false
	response := OperationOrgGuard{}.InterceptOperation(ctx, func(inner context.Context) graphql.ResponseHandler {
		ran = true
		seen, _ := authctx.FromContext(inner)
		seenOrg = seen.OrgID
		return func(context.Context) *graphql.Response { return &graphql.Response{} }
	})(ctx)
	if !ran || seenOrg != "org-target" {
		t.Fatalf("ran=%v org=%q, want the operation to run as org-target", ran, seenOrg)
	}
	if response == nil || len(response.Errors) != 0 {
		t.Fatalf("got %+v, want no errors", response)
	}
}
