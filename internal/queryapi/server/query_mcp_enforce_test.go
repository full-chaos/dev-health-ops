package server

import (
	"context"
	"crypto/ed25519"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/99designs/gqlgen/graphql"

	"github.com/full-chaos/dev-health-ops/internal/auth/workloadinput"
	"github.com/full-chaos/dev-health-ops/internal/authzenforce"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp/pdptest"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

// CHAOS-12659 (6.11c): the /query family (/query, /query/run-operation) and the MCP listener's /query reach the one
// GraphQL decision (CHAOS-13160) through their real ingresses, as Build wires them: the internal and MCP listeners
// carry the process's gate, /query hands the operation its bearer, and every ingress sets the legacy scope. "cell on"
// = QUERY_API_AUTHZ_ENFORCE (user callers, F9a) or QUERY_API_HEADERS_AUTHZ_ENFORCE (the header carrier, F9b) lists
// the namespace; the PDP is pdptest (allow with organization-wide obligations unless told otherwise).

// qmPlane is the /query family and the MCP listener as Build wires them: mountQueryRouteSets over the real dispatch
// handler, run-operation and MCP handler, then gatedListenerHandlers with the gate built from env.
type qmPlane struct {
	internal, mcp http.Handler
	ch            *teamScopeReachClient
	mcpCH         *countingMCPClient
	roots         *atomic.Int64
	user          string
	envelopeKey   ed25519.PrivateKey
	gate          *pdp.Gate
}

func newQMPlane(t *testing.T, env map[string]string, documents ...string) *qmPlane {
	t.Helper()
	quietStdLog(t)
	store := newQueryAPIUserStoreFrom(newAccessStore())
	m := newAccessMinter(t)
	auth := ecEdgeAuth(t, store)
	if err := routeAuthenticator(auth, testAccess(m.keys, nil, accessTestNow).opts); err != nil {
		t.Fatal(err)
	}
	verifier, envelopeKey := iaVerifier(t)
	getenv := getenvFunc(func(string) string { return "" })

	client := &teamScopeReachClient{}
	server := newGraphQLServerWithLimits(&graph.Resolver{ClickHouse: client}, 1000, 10)
	roots := &atomic.Int64{}
	server.AroundRootFields(func(ctx context.Context, next graphql.RootResolver) graphql.Marshaler {
		roots.Add(1)
		return next(ctx)
	})
	routeMux := routeswitch.NewMux(routeswitch.StaticSwitch{"gql": true})
	routeMux.Register("gql", server)
	byDigest := map[string]string{}
	for _, document := range documents {
		byDigest[digestHex(document)] = "gql"
	}
	query := newDocumentDispatchHandler(getenv, routeMux, byDigest, verifier, auth, store, "", nil, newClassRowGate(allMCPRootsEnabled()))
	mcpCH := &countingMCPClient{}
	stub := func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) }
	mux, internalMux, mcpMux := http.NewServeMux(), http.NewServeMux(), http.NewServeMux()
	mountQueryRouteSets(getenv, mux, internalMux, mcpMux, queryRouteHandlers{
		Query:        query,
		Registry:     stub,
		BuildInfo:    stub,
		RunOperation: markClassGated(query),
		MCP:          newMCPHandlerWithLimits(mcpCH, nil, allMCPRootsEnabled(), getenv, mcpDefaultLimits()),
	}, graphQLEdgeDeps{auth: auth, maxBytes: defaultGraphQLMaxQueryBytes})
	handler, internal, mcp, gate := gatedListenerHandlers(knobs(env), mux, internalMux, mcpMux)
	plane := &Plane{Handler: handler, InternalHandler: internal, MCPHandler: mcp}
	_, internalListener := Listeners("127.0.0.1:0", "127.0.0.1:0", plane, nil, nil)
	mcpListener := MCPListener("127.0.0.1:0", plane, nil)
	if internalListener == nil || mcpListener == nil {
		t.Fatal("expected the internal and MCP listeners")
	}
	return &qmPlane{internal: internalListener.server.Handler, mcp: mcpListener.server.Handler, ch: client, mcpCH: mcpCH, roots: roots,
		user: m.mint(ecUser, nil, nil), envelopeKey: envelopeKey, gate: gate}
}

// qmEnv is the process env of a gate over srv in mode, with the user (F9a) and header (F9b) cell lists.
func qmEnv(t *testing.T, srv *pdptest.Server, mode, userCells, headerCells string) map[string]string {
	t.Helper()
	env := map[string]string{}
	if mode != "" {
		env[pdp.EnvMode] = mode
		env[pdp.EnvAuthProxyURL] = srv.URL
		env[workloadinput.EnvIssuer] = pdptest.Issuer
		env[workloadinput.EnvBootstrapFile] = pdptest.BootstrapFile(t)
	}
	if userCells != "" {
		env[authzenforce.QueryAPI.Env()] = userCells
	}
	if headerCells != "" {
		env[authzenforce.QueryAPIHeaders.Env()] = headerCells
	}
	return env
}

// qmCarrier is how a request states its identity.
type qmCarrier func(p *qmPlane, r *http.Request)

func qmBearer(token string) qmCarrier {
	return func(_ *qmPlane, r *http.Request) { r.Header.Set("Authorization", "Bearer "+token) }
}

var (
	qmUser    qmCarrier = func(p *qmPlane, r *http.Request) { r.Header.Set("Authorization", "Bearer "+p.user) }
	qmHeaders qmCarrier = func(_ *qmPlane, r *http.Request) {
		for key, values := range mcpHeaders(ecOrg.String(), "member", "false", "false") {
			r.Header[key] = values
		}
	}
)

// queryRun sends document to path on the internal listener and reports what it answered and read.
func (p *qmPlane) queryRun(t *testing.T, path, document string, carrier qmCarrier) enforceRun {
	t.Helper()
	p.ch.mu.Lock()
	p.ch.statements = nil
	p.ch.mu.Unlock()
	p.roots.Store(0)
	request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"query": `+jsonQuote(t, document)+`}`))
	request.Header.Set("Content-Type", "application/json")
	carrier(p, request)
	recorder := httptest.NewRecorder()
	p.internal.ServeHTTP(recorder, request)
	p.gate.Wait()
	p.ch.mu.Lock()
	defer p.ch.mu.Unlock()
	return enforceRun{code: recorder.Code, body: recorder.Body.String(), statements: len(p.ch.statements), roots: int(p.roots.Load())}
}

// mcpRun sends the MCP hotspots document (a metrics.read root) for ecOrg on the MCP listener.
func (p *qmPlane) mcpRun(t *testing.T) enforceRun {
	t.Helper()
	before := p.mcpCH.calls.Load()
	recorder := mcpDo(p.mcp, http.MethodPost, mcpHeaders(ecOrg.String(), "member", "false", "false"), mcpBody(t, mcpHotspotsQuery, mcpHotspotsVariables(ecOrg.String())))
	p.gate.Wait()
	return enforceRun{code: recorder.Code, body: recorder.Body.String(), statements: int(p.mcpCH.calls.Load() - before)}
}

// asks is the PDP's decision requests (the workload exchange is not one).
func asks(srv *pdptest.Server) []pdptest.Request { return srv.Requests() }

// E1 (Q1): every cell off and the gate off: no ask, and every carrier of every ingress is served.
func TestQueryFamilyCellsOffAskNothing(t *testing.T) {
	srv := pdptest.New(t)
	p := newQMPlane(t, qmEnv(t, srv, "", "", ""), enforceDocAnalytics)
	for name, run := range map[string]enforceRun{
		"/query user":          p.queryRun(t, "/query", enforceDocAnalytics, qmUser),
		"/query headers":       p.queryRun(t, "/query", enforceDocAnalytics, qmHeaders),
		"/query/run-operation": p.queryRun(t, runOperationPath, enforceDocAnalytics, qmHeaders),
		"MCP /query (headers)": p.mcpRun(t),
	} {
		if run.code != http.StatusOK || strings.Contains(run.body, `"errors"`) || run.statements == 0 {
			t.Errorf("%s: %d %s with %d statements, want served", name, run.code, run.body, run.statements)
		}
	}
	if n := len(asks(srv)); n != 0 {
		t.Fatalf("the PDP was asked %d times with every cell off", n)
	}
}

// E2 (Q2, G1 + G6): a user access token on /query is decided: one ask, for the token, the root's action and the
// served organization; the allow is served.
func TestQueryUserTokenIsDecided(t *testing.T) {
	srv := pdptest.New(t)
	p := newQMPlane(t, qmEnv(t, srv, pdp.ModeEnforce, "analytics", ""), enforceDocAnalytics)
	p.queryRun(t, "/query", enforceDocAnalytics, qmUser).served(t)
	got := asks(srv)
	if len(got) != 1 || got[0].Action != "analytics.read" || got[0].OrganizationID != ecOrg.String() || got[0].PrincipalReference != p.user {
		t.Fatalf("asks %+v, want one analytics.read ask for the user token in %s", got, ecOrg)
	}
}

// E3 (Q9): a deny on /query is FORBIDDEN and no verdict is a 503; neither reads anything.
func TestQueryUserTokenDenyAndNoVerdict(t *testing.T) {
	srv := pdptest.New(t)
	srv.Deny("role_lacks_action")
	p := newQMPlane(t, qmEnv(t, srv, pdp.ModeEnforce, "analytics", ""), enforceDocAnalytics)
	p.queryRun(t, "/query", enforceDocAnalytics, qmUser).forbidden(t)

	down := pdptest.New(t)
	down.Fail(http.StatusInternalServerError)
	q := newQMPlane(t, qmEnv(t, down, pdp.ModeEnforce, "analytics", ""), enforceDocAnalytics)
	run := q.queryRun(t, "/query", enforceDocAnalytics, qmUser)
	if run.code != http.StatusServiceUnavailable || !strings.Contains(run.body, `"code":"SERVICE_UNAVAILABLE"`) || run.statements != 0 || run.roots != 0 {
		t.Fatalf("no verdict: %d %s, %d statements, %d roots; want 503 SERVICE_UNAVAILABLE and nothing read", run.code, run.body, run.statements, run.roots)
	}
}

// E4 (Q10a, D6721 F9b off): the user cells on and the header cells off: the header carrier on /query, run-operation
// and MCP stays on today's path, with no ask.
func TestHeaderCarrierFollowsItsOwnCellsOff(t *testing.T) {
	srv := pdptest.New(t)
	p := newQMPlane(t, qmEnv(t, srv, pdp.ModeEnforce, "analytics,metrics", ""), enforceDocAnalytics)
	for name, run := range map[string]enforceRun{
		"/query headers":       p.queryRun(t, "/query", enforceDocAnalytics, qmHeaders),
		"/query/run-operation": p.queryRun(t, runOperationPath, enforceDocAnalytics, qmHeaders),
		"MCP /query":           p.mcpRun(t),
	} {
		if run.code != http.StatusOK || strings.Contains(run.body, `"errors"`) || run.statements == 0 {
			t.Errorf("%s: %d %s with %d statements, want served on today's path", name, run.code, run.body, run.statements)
		}
	}
	if n := len(asks(srv)); n != 0 {
		t.Fatalf("the PDP was asked %d times for the header carrier", n)
	}
}

// E5 (Q10b, Q17 behaviour): a header cell on: the header carrier has no principal the PDP can judge, so /query,
// run-operation (an internal listener route) and MCP refuse it with a 503, read nothing and ask nothing.
func TestHeaderCarrierWithItsCellOnIsRefused(t *testing.T) {
	srv := pdptest.New(t)
	p := newQMPlane(t, qmEnv(t, srv, pdp.ModeEnforce, "", "analytics,metrics"), enforceDocAnalytics)
	p.queryRun(t, "/query", enforceDocAnalytics, qmHeaders).code503(t, string(pdp.ReasonNoPrincipal))
	p.queryRun(t, runOperationPath, enforceDocAnalytics, qmHeaders).code503(t, string(pdp.ReasonNoPrincipal))
	mcp := p.mcpRun(t)
	if mcp.code != http.StatusServiceUnavailable || mcp.statements != 0 || !strings.Contains(mcp.body, string(pdp.ReasonNoPrincipal)) {
		t.Errorf("MCP: %d %s with %d statements, want 503 %s and nothing read", mcp.code, mcp.body, mcp.statements, pdp.ReasonNoPrincipal)
	}
	if n := len(asks(srv)); n != 0 {
		t.Fatalf("the PDP was asked %d times for a caller with no principal", n)
	}
}

// E6 (Q11): /query has no exempt class: the HS256 edge token and the envelope, the classes the exempt list held, are
// retired. With the cell on, the envelope is refused (401) before any read, and it asks nothing.
func TestQueryRetiredClassesAreNotAsked(t *testing.T) {
	srv := pdptest.New(t)
	env := qmEnv(t, srv, pdp.ModeEnforce, "analytics", "")
	p := newQMPlane(t, env, enforceDocAnalytics)
	envelope := iaEnvelope(t, p.envelopeKey, principal.Claims{OrgID: ecOrg.String(), Role: "member"})
	if got := p.queryRun(t, "/query", enforceDocAnalytics, qmBearer(envelope)); got.code != http.StatusUnauthorized || got.statements != 0 || got.roots != 0 {
		t.Fatalf("envelope (a retired class): %d %q with %d statements, want 401 and nothing read", got.code, got.body, got.statements)
	}
	if n := len(asks(srv)); n != 0 {
		t.Fatalf("the PDP was asked %d times for a retired class", n)
	}
}

// E7 (Q15, G5): an operation with roots of two actions asks once per action, for the same organization.
func TestQueryOperationAsksOncePerAction(t *testing.T) {
	srv := pdptest.New(t)
	p := newQMPlane(t, qmEnv(t, srv, pdp.ModeEnforce, "analytics,metrics", ""), enforceDocTwo)
	p.queryRun(t, "/query", enforceDocTwo, qmUser).served(t)
	actions := map[string]int{}
	for _, ask := range asks(srv) {
		if ask.OrganizationID != ecOrg.String() {
			t.Errorf("ask for %q, want %s", ask.OrganizationID, ecOrg)
		}
		actions[ask.Action]++
	}
	if len(actions) != 2 || actions["analytics.read"] != 1 || actions["metrics.read"] != 1 {
		t.Fatalf("asks by action %v, want analytics.read and metrics.read once each", actions)
	}
}

// E8 (Q16): with only the metrics cell on (shadow mode), an analytics-only operation on /query is not enforced: it is
// served on today's path.
func TestQueryRootOfAnOffCellIsTodaysPath(t *testing.T) {
	srv := pdptest.New(t)
	p := newQMPlane(t, qmEnv(t, srv, pdp.ModeShadow, "metrics", ""), enforceDocAnalytics)
	p.queryRun(t, "/query", enforceDocAnalytics, qmUser).served(t)
}
