package server

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/pep"
	"github.com/google/uuid"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/workloadinput"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/internalidentity"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp/pdptest"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

// CHAOS-13074: the GraphQL shadow. One OperationShadow, built by one function, sits before the org guard on every
// GraphQL server; the process's one shadow holds the REST rows and the 52 root rows.

func TestGraphQLShadowRowsAreTheFieldActionTable(t *testing.T) {
	rows := graphQLShadowRows()
	table := graph.FieldActionTable()
	if len(rows) != 52 || len(rows) != len(table) {
		t.Fatalf("%d rows for %d table roots, want 52", len(rows), len(table))
	}
	rest, err := pdp.RESTShadowRows(authv1.QueryAPIEndpointProfiles)
	if err != nil {
		t.Fatal(err)
	}
	for key, action := range table {
		row, ok := rows[key]
		if !ok || row.Action != action {
			t.Errorf("%s: row %+v, want action %s", key, row, action)
		}
		if _, clash := rest[key]; clash {
			t.Errorf("%s is also a REST row", key)
		}
	}
	for _, key := range []string{"Query.productTelemetryPlatformDashboard"} {
		if row := rows[key]; row.ResourceType != "platform" || !row.HighRisk {
			t.Errorf("%s: row %+v, want a high-risk platform row", key, row)
		}
	}
	if row := rows["Mutation.createSavedReport"]; !row.HighRisk {
		t.Errorf("a reports.write row %+v is not high-risk", row)
	}
}

func TestGatedPlaneShadowHoldsTheGraphQLRows(t *testing.T) {
	srv := pdptest.New(t)
	env := map[string]string{pdp.EnvMode: "shadow", pdp.EnvAuthProxyURL: srv.URL, workloadinput.EnvIssuer: pdptest.Issuer,
		workloadinput.EnvSubjectFile: pdptest.BootstrapFile(t)}
	_, gate := gatedPlaneHandler(knobs(env), http.NewServeMux())
	shadow := gate.Shadow()
	if shadow == nil {
		t.Fatal("the plane's gate has no shadow")
	}
	before := shadowCounters(t)
	shadow.Observe(context.Background(), policy.ShadowObservation{Pattern: "Query.home", Bearer: "aaaa.bbbb.cccc-dddd-eeee-ffff", OrgID: "org-1",
		Caller: policy.ShadowCallerMember, UserAccessToken: true, LegacyAllowed: true})
	gate.Wait()
	// The row is known to the shadow, so it asks; the workload exchange of this fixture has no token endpoint, so the ask has no verdict.
	if got := shadowDelta(before, shadowCounters(t)); len(got) != 1 || got[shadowResultsMetric+"|reason=none,result=error"] != 1 {
		t.Fatalf("5.12 counters %v, want one result=error for a known row", got)
	}
}

// C10: one constructor builds the interceptor for both servers; both servers install it through the one guard slot,
// shadow first, then the org guard, and neither names the org guard itself.
func TestGraphQLShadowOneHarnessOnBothServers(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	literals, enforceLiterals := 0, 0
	fset := token.NewFileSet()
	calls := map[string][]string{}
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(fset, name, raw, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.CompositeLit:
				if sel, ok := node.Type.(*ast.SelectorExpr); ok && sel.Sel.Name == "OperationShadow" {
					literals++
				}
				if sel, ok := node.Type.(*ast.SelectorExpr); ok && sel.Sel.Name == "OperationEnforce" {
					enforceLiterals++
				}
			case *ast.FuncDecl:
				switch node.Name.Name {
				case "newGraphQLServerWithLimits", "newMCPGraphQLServer", "useGraphQLGuards":
				default:
					return true
				}
				ast.Inspect(node.Body, func(m ast.Node) bool {
					call, ok := m.(*ast.CallExpr)
					if !ok {
						return true
					}
					switch fun := call.Fun.(type) {
					case *ast.Ident:
						if fun.Name == "graphQLShadow" || fun.Name == "useGraphQLGuards" || fun.Name == "graphQLEnforce" {
							calls[node.Name.Name] = append(calls[node.Name.Name], fun.Name)
						}
					case *ast.SelectorExpr:
						if len(call.Args) == 1 && fun.Sel.Name == "Use" {
							if lit, ok := call.Args[0].(*ast.CompositeLit); ok {
								if sel, ok := lit.Type.(*ast.SelectorExpr); ok && sel.Sel.Name == "OperationOrgGuard" {
									calls[node.Name.Name] = append(calls[node.Name.Name], "guard")
								}
							}
						}
					}
					return true
				})
			}
			return true
		})
	}
	if literals != 1 {
		t.Errorf("%d graph.OperationShadow literals in the server package, want the one in graphQLShadow", literals)
	}
	if enforceLiterals != 1 {
		t.Errorf("%d graph.OperationEnforce literals in the server package, want the one in graphQLEnforce", enforceLiterals)
	}
	// CHAOS-10865: the decision runs after the org guard, so it decides for the organization the guard admitted.
	if got := calls["useGraphQLGuards"]; !slices.Equal(got, []string{"graphQLShadow", "guard", "graphQLEnforce"}) {
		t.Errorf("useGraphQLGuards: calls %v, want the shadow, the org guard, then the decision", got)
	}
	for _, fn := range []string{"newGraphQLServerWithLimits", "newMCPGraphQLServer"} {
		if got := calls[fn]; !slices.Equal(got, []string{"useGraphQLGuards"}) {
			t.Errorf("%s: calls %v, want only useGraphQLGuards", fn, got)
		}
	}
}

// The plane hands the MCP listener a handler that carries the process's gate.
func TestGraphQLShadowMCPPlaneIsGated(t *testing.T) {
	raw, err := os.ReadFile("pdp_gate.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "withGate(mcpMux, gate)") {
		t.Fatal("pdp_gate.go (gatedListenerHandlers) does not wrap the MCP listener's handler with the process's gate")
	}
}

// Q3: an MCP op has no user bearer, so the one harness counts it skipped with its own reason.
func TestGraphQLShadowMCPOpCountsNoUserBearer(t *testing.T) {
	srv := pdptest.New(t)
	gate := pdp.New(pdp.Config{Mode: pdp.ModeShadow, Decider: pepOf(t, srv.URL), ExtraShadowRows: graphQLShadowRows()})
	es := graph.NewExecutableSchema(graph.Config{Resolvers: &graph.Resolver{}})
	gql := newMCPGraphQLServer(es, mcpDefaultLimits())
	before := shadowCounters(t)
	body := `{"query":"query Q { sourceHealth(orgId: \"org-1\") { provider } }"}`
	req := httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	ctx := pdp.WithGate(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"}), gate)
	gql.ServeHTTP(httptest.NewRecorder(), req.WithContext(ctx))
	want := map[string]int64{shadowResultsMetric + "|reason=no_user_bearer,result=skipped": 1}
	if got := settle(t, before, want); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("5.12 counters %v, want %v", got, want)
	}
	if n := len(srv.Requests()); n != 0 {
		t.Fatalf("the PDP was asked %d times for an op with no user bearer", n)
	}
}

// The MCP listener's plane carries the process's gate on each request, as the public plane's handler does.
func TestMCPListenerHandlerCarriesTheGate(t *testing.T) {
	gate := pdp.New(pdp.Config{Mode: pdp.ModeOff})
	var seen *pdp.Gate
	next := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { seen = pdp.FromContext(r.Context()) })
	withGate(next, gate).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/query", nil))
	if seen != gate {
		t.Fatalf("handler saw gate %p, want %p", seen, gate)
	}
}

type subjectProbe struct {
	subjects []graph.ShadowSubject
	found    []bool
}

func (p *subjectProbe) handler() *routeswitch.Mux {
	mux := routeswitch.NewMux(routeswitch.StaticSwitch{"probe": true})
	mux.Register("probe", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		subject, ok := graph.ShadowSubjectFrom(r.Context())
		p.subjects, p.found = append(p.subjects, subject), append(p.found, ok)
		w.WriteHeader(http.StatusOK)
	}))
	return mux
}

// C13: /graphql puts the caller's bearer and its class on the context after authentication; /query does the same for
// its bearer (CHAOS-12659 G6). The routed edge token is a user access token (the HS256 class is retired), so its
// subject is the user token class with no skip reason; the shadow only observes (graph.OperationShadow.InterceptOperation).
// The retired envelope never reaches the operation: /query refuses it before any run.
func TestGraphQLShadowSubjectReachesTheOperation(t *testing.T) {
	store := &fakeEdgeStore{
		states:  map[uuid.UUID]policy.UserState{ecUser: {IsActive: true, IsSuperuser: true, TokenVersion: 5}},
		found:   map[uuid.UUID]bool{ecUser: true},
		members: map[[2]uuid.UUID]bool{{ecUser, ecOrg}: true},
		roles:   map[[2]uuid.UUID]string{{ecUser, ecOrg}: "owner"},
	}
	verifier, priv := iaVerifier(t)
	probe := &subjectProbe{}
	mux := probe.handler()
	auth := ecEdgeAuth(t, store)
	byDigest := map[string]string{digestHex(iaDocument): "probe"}
	edge := internalidentity.Public(onPattern("/graphql", withRouteProfile(graphQLEdgeChain(newDocumentDispatchHandler(os.Getenv, mux, byDigest, verifier, auth, store, "", nil),
		graphQLEdgeDeps{auth: auth, maxBytes: defaultGraphQLMaxQueryBytes}))))
	token := ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), tokenVersion: 5})
	request := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{"query": `+jsonQuote(t, iaDocument)+`}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	edge.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK || len(probe.subjects) != 1 || !probe.found[0] {
		t.Fatalf("/graphql: %d, subjects %+v found %v", recorder.Code, probe.subjects, probe.found)
	}
	if got := probe.subjects[0]; got.Bearer != token || !got.UserAccessToken || got.SkipReason != policy.ShadowReasonNone {
		t.Fatalf("/graphql subject: class fields %+v, want the edge token as a user access token with no skip reason", got)
	}

	probe = &subjectProbe{}
	mux = probe.handler()
	query := newDocumentDispatchHandler(os.Getenv, mux, byDigest, verifier, nil, nil, "", nil)
	envelope := iaEnvelope(t, priv, principal.Claims{OrgID: "org-1", Role: "admin"})
	request = httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(`{"query": `+jsonQuote(t, iaDocument)+`}`))
	request.Header.Set("Authorization", "Bearer "+envelope)
	recorder = httptest.NewRecorder()
	query.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusUnauthorized || len(probe.subjects) != 0 {
		t.Fatalf("/query: %d, subjects %+v, want the envelope refused with no run", recorder.Code, probe.subjects)
	}
}

func TestGraphQLShadowSubjectOfAUserAccessToken(t *testing.T) {
	m := newAccessMinter(t)
	user := m.mint(ecUser, nil, nil)
	if got := graphQLShadowSubject(user); got.Bearer != user || !got.UserAccessToken || got.SkipReason != policy.ShadowReasonNone {
		t.Fatalf("user token subject %+v", got)
	}
	if got := graphQLShadowSubject("not-a-jwt"); got.UserAccessToken || got.SkipReason != policy.ShadowReasonEnvelope {
		t.Fatalf("other token subject %+v", got)
	}
}

// C14: a panic inside the shadow never changes the GraphQL answer (its bytes) nor the org guard's verdict; the shadow
// recovers, counts the failure, and the request has no 500. The same ops run on /graphql and on the MCP server.
func TestGraphQLShadowPanicNeverChangesTheAnswerOrTheGuard(t *testing.T) {
	m := newAccessMinter(t)
	subject := graphQLShadowSubject(m.mint(ecUser, nil, nil))
	panicking := funcDecider{func(context.Context, pep.Request) pep.Decision { panic("pdp client bug") }}
	gate := pdp.New(pdp.Config{Mode: pdp.ModeShadow, Decider: panicking, ExtraShadowRows: graphQLShadowRows(), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	ops := map[string]string{
		"guard admits":  `query Q { sourceHealth(orgId: "org-1") { provider } }`,
		"guard refuses": `query Q { sourceHealth(orgId: "org-2") { provider } }`,
	}
	servers := map[string]func() http.Handler{
		"graphql": func() http.Handler {
			return newGraphQLServerWithLimits(&graph.Resolver{}, 1000, 10)
		},
		"mcp": func() http.Handler {
			return newMCPGraphQLServer(graph.NewExecutableSchema(graph.Config{Resolvers: &graph.Resolver{}}), mcpDefaultLimits())
		},
	}
	serve := func(h http.Handler, query string, g *pdp.Gate) (int, string) {
		req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{"query":`+jsonQuote(t, query)+`}`))
		req.Header.Set("Content-Type", "application/json")
		ctx := authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "org-1"})
		ctx = graph.WithShadowSubject(ctx, subject)
		if g != nil {
			ctx = pdp.WithGate(ctx, g)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req.WithContext(ctx))
		return rec.Code, rec.Body.String()
	}
	for name, build := range servers {
		for opName, query := range ops {
			t.Run(name+"/"+opName, func(t *testing.T) {
				baseCode, baseBody := serve(build(), query, nil)
				before := shadowCounters(t)
				code, body := serve(build(), query, gate)
				gate.Wait()
				if code >= 500 || code != baseCode || body != baseBody {
					t.Fatalf("with a panicking shadow: %d %q, want %d %q", code, body, baseCode, baseBody)
				}
				got := shadowDelta(before, shadowCounters(t))
				if name == "graphql" && got[shadowResultsMetric+"|reason=none,result=error"] != 1 {
					t.Fatalf("5.12 counters %v, want the failure counted once as result=error", got)
				}
			})
		}
	}
}
