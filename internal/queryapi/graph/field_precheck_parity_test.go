package graph

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2/gqlerror"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// guardSpec is a pre-resolver check a root resolver calls: an exported function of a package (by import path suffix),
// or a function of this package (importSuffix empty).
type guardSpec struct{ importSuffix, name string }

var resolverGuards = []guardSpec{
	{"internal/queryapi/datahealth", "RequireOperator"},
	{"internal/queryapi/producttelemetry", "RequirePlatformAdmin"},
	{"", "requireMutationOrg"},
}

// scanGuardCalls maps "Query.name" / "Mutation.name" to the guards its resolver method calls, read from the Go
// source: the import path of a selector's package decides, never the name it is imported under.
func scanGuardCalls(t *testing.T, sources map[string]string) map[string][]string {
	t.Helper()
	out := map[string][]string{}
	fset := token.NewFileSet()
	for name, src := range sources {
		file, err := parser.ParseFile(fset, name, src, 0)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		importOf := map[string]string{}
		for _, spec := range file.Imports {
			path, _ := strconv.Unquote(spec.Path.Value)
			local := filepath.Base(path)
			if spec.Name != nil {
				local = spec.Name.Name
			}
			importOf[local] = path
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || len(fn.Recv.List) != 1 || fn.Body == nil {
				continue
			}
			star, ok := fn.Recv.List[0].Type.(*ast.StarExpr)
			if !ok {
				continue
			}
			recv, ok := star.X.(*ast.Ident)
			if !ok || (recv.Name != "queryResolver" && recv.Name != "mutationResolver") {
				continue
			}
			kind := strings.TrimSuffix(recv.Name, "Resolver")
			key := strings.ToUpper(kind[:1]) + kind[1:] + "." + strings.ToLower(fn.Name.Name[:1]) + fn.Name.Name[1:]
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				for _, g := range resolverGuards {
					switch fun := call.Fun.(type) {
					case *ast.Ident:
						if g.importSuffix == "" && fun.Name == g.name {
							out[key] = append(out[key], g.name)
						}
					case *ast.SelectorExpr:
						pkg, ok := fun.X.(*ast.Ident)
						if ok && g.importSuffix != "" && fun.Sel.Name == g.name && strings.HasSuffix(importOf[pkg.Name], g.importSuffix) {
							out[key] = append(out[key], g.name)
						}
					}
				}
				return true
			})
		}
	}
	return out
}

// parityProblems is every disagreement between the guard calls the resolvers make and the table's keys.
func parityProblems(called map[string][]string, tableKeys []string) []string {
	var problems []string
	for key := range called {
		if !slices.Contains(tableKeys, key) {
			problems = append(problems, key+": a resolver calls a pre-check the table does not list")
		}
	}
	for _, key := range tableKeys {
		if len(called[key]) == 0 {
			problems = append(problems, key+": the table lists a pre-check its resolver never calls")
		}
	}
	slices.Sort(problems)
	return problems
}

func graphSources(t *testing.T) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") || name == "generated.go" {
			continue
		}
		raw, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		out[name] = string(raw)
	}
	return out
}

func tableKeys() []string {
	var keys []string
	for key := range fieldPrechecks {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

// C3 (P2): every root resolver that calls a pre-resolver check has a table row, and every row is called.
func TestOperationShadow_ParityTableCompleteness(t *testing.T) {
	if problems := parityProblems(scanGuardCalls(t, graphSources(t)), tableKeys()); len(problems) != 0 {
		t.Fatalf("the pre-check table and the resolvers disagree: %v", problems)
	}
	if len(fieldPrechecks) != 7 {
		t.Fatalf("%d pre-check rows, want 7 (dataHealth, the platform dashboard, five mutations)", len(fieldPrechecks))
	}
}

// Q8: the scan must catch a known defect of each kind: a resolver that calls a guard under a renamed import, and a
// table row whose resolver skips the guard. No allowlist file stands in for the scan.
func TestOperationShadow_ParityScanCatchesPlantedDefects(t *testing.T) {
	const renamed = `package graph
import (
	dh "github.com/full-chaos/dev-health-ops/internal/queryapi/datahealth"
)
func (r *queryResolver) DataHealth(ctx context.Context, team string) (*model.DataHealth, error) {
	err := dh.RequireOperator(dh.Principal{})
	return nil, err
}
`
	const skipped = `package graph
func (r *queryResolver) ProductTelemetryPlatformDashboard(ctx context.Context) error {
	return nil
}
`
	called := scanGuardCalls(t, map[string]string{"renamed.go": renamed, "skipped.go": skipped})
	if !slices.Equal(called["Query.dataHealth"], []string{"RequireOperator"}) {
		t.Fatalf("the scan missed a guard called under a renamed import: %v", called)
	}
	problems := parityProblems(called, []string{"Query.productTelemetryPlatformDashboard"})
	want := []string{
		"Query.dataHealth: a resolver calls a pre-check the table does not list",
		"Query.productTelemetryPlatformDashboard: the table lists a pre-check its resolver never calls",
	}
	if !slices.Equal(problems, want) {
		t.Fatalf("problems %v, want %v", problems, want)
	}
}

var parityClaims = map[string]authctx.Claims{
	"member":                     {OrgID: shOwn},
	"admin role":                 {OrgID: shOwn, Role: "admin"},
	"superuser":                  {OrgID: shOwn, IsSuperuser: true},
	"impersonating superuser":    {OrgID: shOwn, IsSuperuser: true, ImpersonationActive: true},
	"delegated member":           {OrgID: shOwn, Delegated: true},
	"orgless telemetry holder":   {Platform: orglessSet(rule.ActionAdminTelemetry)},
	"orgless without the action": {Platform: orglessSet(rule.ActionAdminStats)},
}

// resolverPassesItsPrecheck runs the REAL resolver of key under ctx and tells whether its own pre-check let it by.
// A stub-less resolver that passes the check fails later (a nil dependency): that is "passed".
func resolverPassesItsPrecheck(ctx context.Context, key, org string) (passed bool) {
	refusal := map[string]bool{"A valid organization ID is required": true, "Authorization required": true}
	defer func() {
		if recover() != nil {
			passed = true
		}
	}()
	var err error
	q, m := &queryResolver{&Resolver{}}, &mutationResolver{&Resolver{}}
	switch key {
	case "Query.dataHealth":
		_, err = q.DataHealth(ctx, "t")
	case "Query.productTelemetryPlatformDashboard":
		_, err = q.ProductTelemetryPlatformDashboard(ctx, model.ProductTelemetryDashboardInput{})
	case "Mutation.createSavedReport":
		_, err = m.CreateSavedReport(ctx, org, model.CreateSavedReportInput{})
	case "Mutation.updateSavedReport":
		_, err = m.UpdateSavedReport(ctx, org, "r", model.UpdateSavedReportInput{})
	case "Mutation.deleteSavedReport":
		_, err = m.DeleteSavedReport(ctx, org, "r")
	case "Mutation.cloneSavedReport":
		_, err = m.CloneSavedReport(ctx, org, model.CloneSavedReportInput{})
	case "Mutation.triggerReport":
		_, err = m.TriggerReport(ctx, org, "r")
	}
	var gqlErr *gqlerror.Error
	if err == nil || !asGQLError(err, &gqlErr) {
		return true
	}
	if strings.HasPrefix(key, "Mutation.") {
		return !(refusal[gqlErr.Message] || strings.HasPrefix(gqlErr.Message, "Access denied"))
	}
	return gqlErr.Extensions["code"] != "AUTHORIZATION_ERROR" || gqlErr.Message == "org_id is required for all analytics queries"
}

func asGQLError(err error, target **gqlerror.Error) bool {
	e, ok := err.(*gqlerror.Error)
	if ok {
		*target = e
	}
	return ok
}

// C2 (P1): for each caller and each pre-checked root, the verdict the shadow is told equals "the guard lets the op
// run AND the real resolver's own check lets it by".
func TestOperationShadow_ParityGuardVsTable(t *testing.T) {
	for who, claims := range parityClaims {
		for _, key := range tableKeys() {
			for _, org := range []string{shOwn, shOther, ""} {
				document, variables := rootDoc(key, org)
				claims := claims
				sink := &fakeSink{}
				run := runShadow(t, document, variables, &claims, &userSubject, sink)
				if len(sink.observed) != 1 {
					t.Errorf("%s %s org %q: observed %d, skipped %v", who, key, org, len(sink.observed), sink.skipped)
					continue
				}
				guard := OperationOrgGuard{}
				operation := guardOperation(t, document, variables)
				ctx := graphql.WithOperationContext(authctx.WithClaims(context.Background(), claims), operation)
				ranInner := false
				var inner context.Context
				guard.InterceptOperation(ctx, func(c context.Context) graphql.ResponseHandler {
					ranInner, inner = true, c
					return func(context.Context) *graphql.Response { return &graphql.Response{} }
				})(ctx)
				want := ranInner && resolverPassesItsPrecheck(inner, key, org)
				if got := sink.observed[0].LegacyAllowed; got != want {
					t.Errorf("%s %s org %q: told %v, the guard and the real resolver say %v", who, key, org, got, want)
				}
				_ = run
			}
		}
	}
}

// P3: every root the guard admits for an org-less caller has a table verdict that agrees with the guard's admission.
func TestOperationShadow_ParityOrglessTableVsGuard(t *testing.T) {
	for name, action := range orglessRootActions {
		key := "Query." + name
		check, ok := fieldPrechecks[key]
		if !ok {
			t.Errorf("%s admits an org-less caller but has no table row", key)
			continue
		}
		operation := guardOperation(t, `query Q { `+name+` }`, nil)
		for _, tc := range []struct {
			claims authctx.Claims
			want   bool
		}{
			{authctx.Claims{Platform: orglessSet(action)}, true},
			{authctx.Claims{Platform: orglessSet(rule.ActionAdminStats)}, false},
			{authctx.Claims{Platform: orglessSet(action), ImpersonationActive: true}, false},
		} {
			if got := orglessOperationAllowed(tc.claims, operation); got != tc.want {
				t.Errorf("%s: the guard admits %v, want %v", key, got, tc.want)
			}
			if got := check(tc.claims, ""); got != tc.want {
				t.Errorf("%s: the table says %v, want %v", key, got, tc.want)
			}
		}
	}
	_ = policy.ShadowReasonCap
}
