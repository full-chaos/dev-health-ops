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
