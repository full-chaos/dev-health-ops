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

// The guard answers what OrgIdAuthExtension answers, before any field runs.
func TestMutationOrgViolationOverItsInputDomain(t *testing.T) {
	t.Parallel()
	const own = "org-own"
	byVariable := `mutation M($orgId: String!, $x: String) { a: doIt(orgId: $orgId) b: doIt(org_id: $x) }`
	single := `mutation M($orgId: String) { doIt(orgId: $orgId) }`
	literal := `mutation M { doIt(orgId: "org-own") }`
	fragment := `mutation M($orgId: String) { ...F } fragment F on Mutation { doIt(orgId: $orgId) }`
	nested := `mutation M($orgId: String) { doIt(orgId: $orgId) { inner(orgId: "other") } }`
	noOrg := `mutation M { doIt }`
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
		{"no identity", single, map[string]any{"orgId": own}, nil, "Authorization required"},
		{"identity without an org", single, map[string]any{"orgId": own}, &authctx.Claims{}, "Authorization required"},
	} {
		ctx := context.Background()
		if tc.claims != nil {
			ctx = authctx.WithClaims(ctx, *tc.claims)
		}
		got := mutationOrgViolation(ctx, guardOperation(t, tc.document, tc.variables))
		if got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
}
