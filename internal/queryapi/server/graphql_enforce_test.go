package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/99designs/gqlgen/graphql"
	"github.com/full-chaos/dev-health-go/pep"
	"github.com/google/uuid"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
	"github.com/full-chaos/dev-health-ops/internal/auth/workloadinput"
	"github.com/full-chaos/dev-health-ops/internal/authzenforce"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp/pdptest"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

// CHAOS-10865 (6.11b PR-1): a GraphQL operation is decided per root field through the 5.17 cell of the root's
// action namespace. Cells G1-G15 and S-a..S-d of PLAN-10865 (+ addendum 2). "cell on" = the namespace is listed.

var (
	enforceDocAnalytics = `query Q { securityOverview(orgId: "` + ecOrg.String() + `") { __typename } }`
	enforceDocMetrics   = `query Q { busFactor(orgId: "` + ecOrg.String() + `") { __typename } }`
	enforceDocTwo       = `query Q { securityOverview(orgId: "` + ecOrg.String() + `") { __typename } busFactor(orgId: "` + ecOrg.String() + `") { __typename } }`
	enforceDocOtherOrg  = `query Q { securityOverview(orgId: "` + servedOrgB.String() + `") { __typename } }`
)

// enforceRun is what one operation answered and read.
type enforceRun struct {
	code       int
	body       string
	statements int
	// roots is how many root fields ran (a resolver was entered).
	roots int
}

func (r enforceRun) code503(t *testing.T, reason string) {
	t.Helper()
	if r.code != http.StatusServiceUnavailable || !strings.Contains(r.body, `"code":"SERVICE_UNAVAILABLE"`) ||
		!strings.Contains(r.body, `"reason":"`+reason+`"`) || !strings.Contains(r.body, `"data":null`) || r.statements != 0 || r.roots != 0 {
		t.Fatalf("got %d %s with %d statements and %d root fields run, want 503 SERVICE_UNAVAILABLE reason %s, data null, 0 statements, 0 root fields", r.code, r.body, r.statements, r.roots, reason)
	}
}

func (r enforceRun) forbidden(t *testing.T) {
	t.Helper()
	if r.code != http.StatusOK || !strings.Contains(r.body, `"code":"FORBIDDEN"`) || !strings.Contains(r.body, `"data":null`) || r.statements != 0 || r.roots != 0 {
		t.Fatalf("got %d %s with %d statements and %d root fields run, want 200 FORBIDDEN, data null, 0 statements, 0 root fields", r.code, r.body, r.statements, r.roots)
	}
}

func (r enforceRun) served(t *testing.T) {
	t.Helper()
	if r.code != http.StatusOK || strings.Contains(r.body, `"errors"`) || r.statements == 0 {
		t.Fatalf("got %d %s with %d statements, want a served answer", r.code, r.body, r.statements)
	}
}

// enforceCaller is how the request reaches the operation: a user access token (the production edge for an at+jwt
// bearer), the HS256 edge token as it is, or a superuser's edge token with a user subject.
type enforceCaller struct {
	user      bool
	superuser bool
	// impersonate is a go-api impersonation row of the caller (a superuser) for ecTarget in servedOrgB: /graphql then
	// runs as the target, with the admin's own token.
	impersonate bool
	// orgHeader is the X-Org-Id the request names; memberOfB makes the caller a member of servedOrgB.
	orgHeader string
	memberOfB bool
}

var (
	callerUser  = enforceCaller{user: true}
	callerHS256 = enforceCaller{}
)

// runGraphQLEnforce sends one document through the real /graphql pipeline (edge chain, org scope, dispatch handler)
// into the real GraphQL server over a recording ClickHouse client, with gate on the request. For a user caller the
// subject is a user access token, as the edge sets it for an at+jwt bearer (the test's edge token is hs256).
func runGraphQLEnforce(t *testing.T, gate *pdp.Gate, caller enforceCaller, document string) enforceRun {
	t.Helper()
	quietStdLog(t)
	store := &fakeEdgeStore{
		states:  map[uuid.UUID]policy.UserState{ecUser: {IsActive: true, IsSuperuser: caller.superuser || caller.impersonate, TokenVersion: 5}},
		found:   map[uuid.UUID]bool{ecUser: true},
		members: map[[2]uuid.UUID]bool{{ecUser, ecOrg}: true},
		roles:   map[[2]uuid.UUID]string{{ecUser, ecOrg}: "admin"},
	}
	if caller.memberOfB {
		store.members[[2]uuid.UUID{ecUser, servedOrgB}] = true
		store.roles[[2]uuid.UUID{ecUser, servedOrgB}] = "admin"
	}
	if caller.impersonate {
		store.sessions = map[uuid.UUID]*policy.Impersonation{ecUser: {AdminUserID: ecUser, TargetUserID: ecTarget, TargetOrgID: servedOrgB, TargetRole: "viewer"}}
	}
	client := &teamScopeReachClient{}
	server := newGraphQLServerWithLimits(&graph.Resolver{ClickHouse: client}, 1000, 10)
	var roots atomic.Int64
	server.AroundRootFields(func(ctx context.Context, next graphql.RootResolver) graphql.Marshaler {
		roots.Add(1)
		return next(ctx)
	})
	subject := graphQLShadowSubject(newAccessMinter(t).mint(ecUser, nil, nil))
	mux := routeswitch.NewMux(routeswitch.StaticSwitch{"gql": true})
	mux.Register("gql", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := pdp.WithGate(r.Context(), gate)
		if caller.user {
			ctx = graph.WithShadowSubject(ctx, subject)
		}
		server.ServeHTTP(w, r.WithContext(ctx))
	}))
	verifier, _ := iaVerifier(t)
	auth := ecEdgeAuth(t, store)
	chain := graphQLEdgeChain(newDocumentDispatchHandler(os.Getenv, mux, map[string]string{digestHex(document): "gql"}, verifier, auth, store, "", nil),
		graphQLEdgeDeps{auth: auth, maxBytes: defaultGraphQLMaxQueryBytes})
	token := ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), role: "admin", isSuperuser: caller.superuser || caller.impersonate, tokenVersion: 5})
	request := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{"query": `+jsonQuote(t, document)+`}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+token)
	if caller.orgHeader != "" {
		request.Header.Set("X-Org-Id", caller.orgHeader)
	}
	recorder := httptest.NewRecorder()
	chain.ServeHTTP(recorder, request)
	gate.Wait()
	client.mu.Lock()
	defer client.mu.Unlock()
	return enforceRun{code: recorder.Code, body: recorder.Body.String(), statements: len(client.statements), roots: int(roots.Load())}
}

// runMCPEnforce sends one document through the MCP listener's GraphQL server, the header carrier: the org comes from
// the X-DH-Internal-* headers, no bearer exists, and the ingress set today's scope (CHAOS-13164's ingress fact).
func runMCPEnforce(t *testing.T, gate *pdp.Gate, document string) enforceRun {
	t.Helper()
	client := &teamScopeReachClient{}
	gql := newMCPGraphQLServer(graph.NewExecutableSchema(graph.Config{Resolvers: &graph.Resolver{ClickHouse: client}}), mcpDefaultLimits())
	req := httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(`{"query":`+jsonQuote(t, document)+`}`))
	req.Header.Set("Content-Type", "application/json")
	claims := authctx.Claims{OrgID: ecOrg.String(), Role: "admin", Scope: pepgate.Legacy(ecOrg.String())}
	ctx := pdp.WithGate(authctx.WithClaims(req.Context(), claims), gate)
	rec := httptest.NewRecorder()
	gql.ServeHTTP(rec, req.WithContext(ctx))
	gate.Wait()
	client.mu.Lock()
	defer client.mu.Unlock()
	return enforceRun{code: rec.Code, body: rec.Body.String(), statements: len(client.statements)}
}

// enforceGate is a gate over decider with the QueryAPI cells cellList, its counters on its own reader.
func enforceGate(t *testing.T, mode, cellList string, decider pep.Decider) (*pdp.Gate, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	return pdp.New(pdp.Config{Mode: mode, Decider: decider, Switch: authzenforce.Parse(authzenforce.QueryAPI, cellList),
		ExtraShadowRows: graphQLShadowRows(), Meter: provider.Meter("test"), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}), reader
}

// enforceGateFromEnv is the process's gate as FromSettings builds it from env, over the fake PDP srv.
func enforceGateFromEnv(t *testing.T, srv *pdptest.Server, env map[string]string) *pdp.Gate {
	t.Helper()
	all := map[string]string{pdp.EnvMode: pdp.ModeEnforce, pdp.EnvAuthProxyURL: srv.URL, workloadinput.EnvIssuer: pdptest.Issuer,
		workloadinput.EnvBootstrapFile: pdptest.BootstrapFile(t)}
	for k, v := range env {
		all[k] = v
	}
	return pdp.FromSettingsWithRows(knobs(all), slog.New(slog.NewTextHandler(io.Discard, nil)), graphQLShadowRows())
}

// countingDecider counts its decisions and answers through next.
type countingDecider struct {
	next  pep.Decider
	mu    sync.Mutex
	asked []pep.Request
}

func (d *countingDecider) Decide(ctx context.Context, req pep.Request) pep.Decision {
	d.mu.Lock()
	d.asked = append(d.asked, req)
	d.mu.Unlock()
	return d.next.Decide(ctx, req)
}

func (d *countingDecider) DecideBatch(context.Context, []pep.Request) []pep.Decision {
	panic("DecideBatch is not used by the gate")
}

func (d *countingDecider) requests() []pep.Request {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]pep.Request(nil), d.asked...)
}

// G12: every cell off (and the gate off): no PDP call, and the answer and its reads are today's, byte for byte.
func TestGraphQLEnforceCellOffIsTodaysPath(t *testing.T) {
	legacy := runGraphQLEnforce(t, nil, callerUser, enforceDocTwo)
	srv := pdptest.New(t)
	srv.Deny("role_lacks_action")
	for name, gate := range map[string]*pdp.Gate{
		"mode off":        pdp.New(pdp.Config{Mode: pdp.ModeOff}),
		"shadow, no cell": func() *pdp.Gate { g, _ := enforceGate(t, pdp.ModeShadow, "", pepOf(t, srv.URL)); return g }(),
		"shadow, other namespaces on": func() *pdp.Gate {
			g, _ := enforceGate(t, pdp.ModeShadow, "git,work_items", pepOf(t, srv.URL))
			return g
		}(),
		"env: headers cell names these": enforceGateFromEnv(t, srv, map[string]string{pdp.EnvMode: pdp.ModeShadow, "QUERY_API_HEADERS_AUTHZ_ENFORCE": "analytics,metrics"}),
	} {
		t.Run(name, func(t *testing.T) {
			before := len(srv.Requests())
			got := runGraphQLEnforce(t, gate, callerUser, enforceDocTwo)
			if got != legacy {
				t.Fatalf("differs from today's path:\n got %+v\nwant %+v", got, legacy)
			}
			if gate.Mode() == pdp.ModeShadow {
				return // the shadow asks in the background; it never changes the answer
			}
			if n := len(srv.Requests()) - before; n != 0 {
				t.Fatalf("asked the PDP %d times with every cell off", n)
			}
		})
	}
}

// G1: cell on, an organization-wide allow for the asked org: served, one ask per distinct action, the principal is
// the user bearer and the resource the asked org.
func TestGraphQLEnforceAllowServes(t *testing.T) {
	legacy := runGraphQLEnforce(t, nil, callerUser, enforceDocTwo)
	decider := &countingDecider{next: pepOf(t, pdptest.New(t).URL)}
	gate, _ := enforceGate(t, pdp.ModeEnforce, "analytics,metrics", decider)
	got := runGraphQLEnforce(t, gate, callerUser, enforceDocTwo)
	if got != legacy {
		t.Fatalf("an allow changed the answer:\n got %+v\nwant %+v", got, legacy)
	}
	asked := decider.requests()
	if len(asked) != 2 {
		t.Fatalf("asked %d times, want 2 (one per distinct action)", len(asked))
	}
	actions := map[string]bool{}
	for _, req := range asked {
		actions[req.Action] = true
		if req.OrganizationID != ecOrg.String() || req.ResourceKey != ecOrg.String() || req.PrincipalReference == "" {
			t.Fatalf("asked %+v, want the user bearer about %s", req, ecOrg)
		}
	}
	if !actions["analytics.read"] || !actions["metrics.read"] {
		t.Fatalf("asked actions %v, want analytics.read and metrics.read", actions)
	}
}

// G2 + G5: cell on, a PDP deny (no membership included): FORBIDDEN in the guard's form, no read.
func TestGraphQLEnforceDenyIsForbidden(t *testing.T) {
	for _, reason := range []string{"role_lacks_action", "no_membership"} {
		srv := pdptest.New(t)
		srv.Deny(reason)
		gate, _ := enforceGate(t, pdp.ModeEnforce, "analytics", pepOf(t, srv.URL))
		runGraphQLEnforce(t, gate, callerUser, enforceDocAnalytics).forbidden(t)
	}
}

// G3: an orgId of another org (no superuser): the org guard refuses as today, and the PDP is never asked.
func TestGraphQLEnforceOtherOrgIsTheGuardsRefusal(t *testing.T) {
	legacy := runGraphQLEnforce(t, nil, callerUser, enforceDocOtherOrg)
	decider := &countingDecider{next: pepOf(t, pdptest.New(t).URL)}
	gate, _ := enforceGate(t, pdp.ModeEnforce, "analytics", decider)
	got := runGraphQLEnforce(t, gate, callerUser, enforceDocOtherOrg)
	if got != legacy || len(decider.requests()) != 0 {
		t.Fatalf("got %+v with %d asks, want the guard's refusal %+v and no ask", got, len(decider.requests()), legacy)
	}
}

// G4: an allow whose tenant_scope is another org, absent, or not a UUID is not carried out: 503, no read.
func TestGraphQLEnforceTenantMismatchIs503(t *testing.T) {
	for name, obligations := range map[string]map[string]any{
		"other org": {"tenant_scope": servedOrgB.String(), "repository_scope": "organization"},
		"absent":    {"repository_scope": "organization"},
		"not uuid":  {"tenant_scope": "acme", "repository_scope": "organization"},
	} {
		t.Run(name, func(t *testing.T) {
			srv := pdptest.New(t)
			srv.Obligations(obligations)
			gate, _ := enforceGate(t, pdp.ModeEnforce, "analytics", pepOf(t, srv.URL))
			runGraphQLEnforce(t, gate, callerUser, enforceDocAnalytics).code503(t, string(pdp.ReasonTenantMismatch))
		})
	}
}

// G6, machinery (not a product rule): the PEP asks the PDP about the organization the org guard admitted (here a
// superuser's named org) and obeys the answer; this cell's PDP denies. A platform admin is allowed without membership by
// PDP policy (ticket 13209, product rule 1); no enforce flip before it.
func TestGraphQLEnforcePEPAsksAboutTheNamedOrgAndObeys(t *testing.T) {
	srv := pdptest.New(t)
	srv.Deny("no_membership")
	decider := &countingDecider{next: pepOf(t, srv.URL)}
	gate, _ := enforceGate(t, pdp.ModeEnforce, "analytics", decider)
	runGraphQLEnforce(t, gate, enforceCaller{user: true, superuser: true}, enforceDocOtherOrg).forbidden(t)
	if asked := decider.requests(); len(asked) != 1 || asked[0].OrganizationID != servedOrgB.String() {
		t.Fatalf("asked %+v, want one ask about the named org %s", asked, servedOrgB)
	}
}

// G9: no verdict fails closed with a 503 and no read: the PDP down, no client with a cell on, a misconfigured gate.
func TestGraphQLEnforceNoVerdictIs503(t *testing.T) {
	down := pdptest.New(t)
	down.Fail(http.StatusServiceUnavailable)
	downGate, _ := enforceGate(t, pdp.ModeEnforce, "analytics", pepOf(t, down.URL))
	runGraphQLEnforce(t, downGate, callerUser, enforceDocAnalytics).code503(t, string(pep.ReasonUnavailable))
	noClient, _ := enforceGate(t, pdp.ModeOff, "analytics", nil)
	runGraphQLEnforce(t, noClient, callerUser, enforceDocAnalytics).code503(t, string(pdp.ReasonNoDecisionPoint))
	bad, _ := enforceGate(t, "banana", "analytics", nil)
	runGraphQLEnforce(t, bad, callerUser, enforceDocAnalytics).code503(t, string(pdp.ReasonNoDecisionPoint))
}

// G10, machinery (not a product rule): an allowlist on a root the registry does not list (the registry is empty in this
// change) is not carried out, [] included; an allowlist holding "*" is the whole organization. Whether the PDP ever sends
// an allowlist for these reads is PDP policy (ticket 13207, product rule 2: org-wide reads); no enforce flip before it.
func TestGraphQLEnforceAllowlistIsRefusedUntilARootCarriesIt(t *testing.T) {
	for name, keys := range map[string][]string{"one": {"acme/widgets"}, "empty": {}} {
		t.Run(name, func(t *testing.T) {
			srv := pdptest.New(t)
			srv.Obligations(map[string]any{"tenant_scope": ecOrg.String(), "repository_allowlist": keys})
			gate, _ := enforceGate(t, pdp.ModeEnforce, "analytics", pepOf(t, srv.URL))
			runGraphQLEnforce(t, gate, callerUser, enforceDocAnalytics).code503(t, string(pdp.ReasonNotCarriedOut))
		})
	}
	srv := pdptest.New(t)
	srv.Obligations(map[string]any{"tenant_scope": ecOrg.String(), "repository_allowlist": []string{"*"}})
	gate, _ := enforceGate(t, pdp.ModeEnforce, "analytics", pepOf(t, srv.URL))
	runGraphQLEnforce(t, gate, callerUser, enforceDocAnalytics).served(t)
}

// G11: redaction, step_up, and a minimum_assurance above what query-api carries out are refused, never served.
func TestGraphQLEnforceUncarriedObligationsAre503(t *testing.T) {
	for name, extra := range map[string]map[string]any{
		"redaction": {"redaction": []string{"personal_identifiers"}},
		"step_up":   {"step_up": true},
		"aal2":      {"minimum_assurance": "aal2"},
	} {
		t.Run(name, func(t *testing.T) {
			obligations := map[string]any{"tenant_scope": ecOrg.String(), "repository_scope": "organization"}
			for k, v := range extra {
				obligations[k] = v
			}
			srv := pdptest.New(t)
			srv.Obligations(obligations)
			gate, _ := enforceGate(t, pdp.ModeEnforce, "analytics", pepOf(t, srv.URL))
			runGraphQLEnforce(t, gate, callerUser, enforceDocAnalytics).code503(t, string(pdp.ReasonNotCarriedOut))
		})
	}
}

// G14: the HS256 edge token stays on today's path with every cell on and a PDP that denies: no ask, one exempt count
// per distinct action.
func TestGraphQLEnforceHS256IsExempt(t *testing.T) {
	legacy := runGraphQLEnforce(t, nil, callerHS256, enforceDocTwo)
	srv := pdptest.New(t)
	srv.Deny("role_lacks_action")
	decider := &countingDecider{next: pepOf(t, srv.URL)}
	gate, reader := enforceGate(t, pdp.ModeEnforce, strings.Join(authzenforce.Namespaces(), ","), decider)
	got := runGraphQLEnforce(t, gate, callerHS256, enforceDocTwo)
	if got != legacy || len(decider.requests()) != 0 {
		t.Fatalf("got %+v with %d asks, want today's %+v and no ask", got, len(decider.requests()), legacy)
	}
	exempt := (&gatedREST{reader: reader}).counter(t, "dev_health_query_api_pdp_exempt_total")
	if exempt["action=analytics.read,class="+pdp.ClassHS256] != 1 || exempt["action=metrics.read,class="+pdp.ClassHS256] != 1 || len(exempt) != 2 {
		t.Fatalf("exempt counts %v, want one per action for %s", exempt, pdp.ClassHS256)
	}
}

// G15: two roots of two actions, one denied: the whole operation is refused, no partial data, no read.
func TestGraphQLEnforceOneDeniedRootRefusesTheOperation(t *testing.T) {
	allow, deny := pdptest.New(t), pdptest.New(t)
	deny.Deny("role_lacks_action")
	byAction := funcDecider{func(ctx context.Context, req pep.Request) pep.Decision {
		if req.Action == "metrics.read" {
			return pepOf(t, deny.URL).Decide(ctx, req)
		}
		return pepOf(t, allow.URL).Decide(ctx, req)
	}}
	gate, _ := enforceGate(t, pdp.ModeEnforce, "analytics,metrics", byAction)
	runGraphQLEnforce(t, gate, callerUser, enforceDocTwo).forbidden(t)
}

// An operation whose ingress set no scope (pepgate KindNone) is refused while a cell of one of its roots is on, in
// either cell list, whatever its caller; with every such cell off it runs as today.
func TestGraphQLEnforceUnscopedOperationIsRefused(t *testing.T) {
	serve := func(cells string) (enforceRun, int) {
		decider := &countingDecider{next: pepOf(t, pdptest.New(t).URL)}
		gate, _ := enforceGate(t, pdp.ModeShadow, cells, decider)
		client := &teamScopeReachClient{}
		server := newGraphQLServerWithLimits(&graph.Resolver{ClickHouse: client}, 1000, 10)
		req := httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(`{"query":`+jsonQuote(t, enforceDocAnalytics)+`}`))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		ctx := graph.WithShadowSubject(req.Context(), graphQLShadowSubject(newAccessMinter(t).mint(ecUser, nil, nil)))
		server.ServeHTTP(rec, req.WithContext(pdp.WithGate(authctx.WithClaims(ctx, authctx.Claims{OrgID: ecOrg.String()}), gate)))
		client.mu.Lock()
		defer client.mu.Unlock()
		return enforceRun{code: rec.Code, body: rec.Body.String(), statements: len(client.statements)}, len(decider.requests())
	}
	got, asks := serve("analytics")
	var body struct {
		Data   json.RawMessage `json:"data"`
		Errors []struct {
			Extensions map[string]any `json:"extensions"`
		} `json:"errors"`
	}
	if err := json.Unmarshal([]byte(got.body), &body); err != nil || len(body.Errors) != 1 || string(body.Data) != "null" ||
		body.Errors[0].Extensions["reason"] != string(pdp.ReasonNoScope) || got.statements != 0 || asks != 0 {
		t.Fatalf("got %s with %d statements and %d asks, want one %s refusal, data null, no read, no ask", got.body, got.statements, asks, pdp.ReasonNoScope)
	}
	if got, _ := serve("git"); strings.Contains(got.body, `"errors"`) || got.statements == 0 {
		t.Fatalf("cells of other namespaces only: got %+v, want today's answer", got)
	}
}

// S-a..S-d (D6721, the F9a/F9b split): the user caller follows QUERY_API_AUTHZ_ENFORCE, the header carrier (MCP)
// follows QUERY_API_HEADERS_AUTHZ_ENFORCE. A header carrier with its cell on has no principal to ask about: refused
// authz_enforce_no_principal, no read, no ask. Shadow mode, so a cell that is off is today's path.
func TestGraphQLEnforceCallerClassPicksTheCell(t *testing.T) {
	for name, tc := range map[string]struct {
		user, headers  string
		userEnforced   bool
		headersRefused bool
	}{
		"S-a headers off":                 {user: "", headers: "", userEnforced: false, headersRefused: false},
		"S-b headers on":                  {user: "", headers: "analytics", userEnforced: false, headersRefused: true},
		"S-c user on, headers off":        {user: "analytics", headers: "", userEnforced: true, headersRefused: false},
		"S-d user off, headers on":        {user: "git", headers: "analytics", userEnforced: false, headersRefused: true},
		"both on":                         {user: "analytics", headers: "analytics", userEnforced: true, headersRefused: true},
		"headers lists another namespace": {user: "", headers: "git", userEnforced: false, headersRefused: false},
	} {
		t.Run(name, func(t *testing.T) {
			srv := pdptest.New(t)
			srv.Deny("role_lacks_action")
			env := map[string]string{pdp.EnvMode: pdp.ModeShadow, "QUERY_API_AUTHZ_ENFORCE": tc.user, "QUERY_API_HEADERS_AUTHZ_ENFORCE": tc.headers}
			user := runGraphQLEnforce(t, enforceGateFromEnv(t, srv, env), callerUser, enforceDocAnalytics)
			if tc.userEnforced {
				user.forbidden(t)
			} else {
				user.served(t)
			}
			asks := len(srv.Requests())
			mcp := runMCPEnforce(t, enforceGateFromEnv(t, srv, env), enforceDocAnalytics)
			if tc.headersRefused {
				if !strings.Contains(mcp.body, `"reason":"`+string(pdp.ReasonNoPrincipal)+`"`) || !strings.Contains(mcp.body, `"data":null`) || mcp.statements != 0 {
					t.Fatalf("header carrier: %d %s with %d statements, want the no-principal refusal and no read", mcp.code, mcp.body, mcp.statements)
				}
			} else if strings.Contains(mcp.body, `"errors"`) || mcp.statements == 0 {
				t.Fatalf("header carrier: %d %s with %d statements, want today's answer", mcp.code, mcp.body, mcp.statements)
			}
			if n := len(srv.Requests()) - asks; n != 0 {
				t.Fatalf("a header carrier asked the PDP %d times", n)
			}
		})
	}
}

// acp ruling (1) on the REST rows, kept for the roots: QUERY_API_PDP_MODE=enforce never acts as shadow, so a user
// caller's root whose cell is off is refused (503 authz_enforce_cell_off) and asks nothing. The header carrier is
// not judged by the mode: with its own cell off it stays on today's path (D6721: Ask Dev keeps working at F9a).
func TestGraphQLEnforceModeEnforceWithTheCellOff(t *testing.T) {
	srv := pdptest.New(t)
	env := map[string]string{"QUERY_API_AUTHZ_ENFORCE": "git"}
	runGraphQLEnforce(t, enforceGateFromEnv(t, srv, env), callerUser, enforceDocAnalytics).code503(t, string(pdp.ReasonEnforceCellOff))
	mcp := runMCPEnforce(t, enforceGateFromEnv(t, srv, env), enforceDocAnalytics)
	if strings.Contains(mcp.body, `"errors"`) || mcp.statements == 0 || len(srv.Requests()) != 0 {
		t.Fatalf("header carrier: %d %s with %d statements and %d asks, want today's answer and no ask", mcp.code, mcp.body, mcp.statements, len(srv.Requests()))
	}
}

// A root that is enforced is not also shadowed: shadow mode with the cell on asks once, the decision's ask.
func TestGraphQLEnforceEnforcedRootIsNotShadowed(t *testing.T) {
	decider := &countingDecider{next: pepOf(t, pdptest.New(t).URL)}
	gate, _ := enforceGate(t, pdp.ModeShadow, "analytics", decider)
	runGraphQLEnforce(t, gate, callerUser, enforceDocAnalytics).served(t)
	if n := len(decider.requests()); n != 1 {
		t.Fatalf("asked %d times, want 1 (the decision, no shadow ask)", n)
	}
}

// Product rule 1 (D6773): a root whose action is not organization-scoped (the platform dashboard) is never decided
// as an organization: with every cell on, the admin namespace included, it answers as main does (RequirePlatformAdmin
// decides), asks nothing, for an org member and for an org-less platform caller whose ingress set no scope.
func TestGraphQLEnforcePlatformRootAnswersAsMain(t *testing.T) {
	doc := `query Q { productTelemetryPlatformDashboard(input: {startDate: "2026-01-01", endDate: "2026-01-02"}) { __typename } }`
	every := strings.Join(authzenforce.Namespaces(), ",")
	member := authctx.Claims{OrgID: ecOrg.String(), Role: "admin", Scope: pepgate.Legacy(ecOrg.String())}
	orgless := authctx.Claims{Platform: func() *rule.Platform { p := rule.PlatformOf(rule.ActionAdminTelemetry); return &p }()}
	subject := graphQLShadowSubject(newAccessMinter(t).mint(ecUser, nil, nil))
	for name, claims := range map[string]authctx.Claims{"org member": member, "org-less platform caller": orgless} {
		t.Run(name, func(t *testing.T) {
			legacy, _, _ := runRootsAs(t, nil, nil, claims, subject, doc)
			for mode, m := range map[string]string{"enforce": pdp.ModeEnforce, "shadow": pdp.ModeShadow} {
				decider := &countingDecider{next: pepOf(t, pdptest.New(t).URL)}
				gate, _ := enforceGate(t, m, every, decider)
				got, _, _ := runRootsAs(t, gate, nil, claims, subject, doc)
				if got != legacy {
					t.Fatalf("%s mode, every cell on: %+v, want main's answer %+v", mode, got, legacy)
				}
				for _, req := range decider.requests() {
					if req.Action == rule.ActionAdminTelemetry {
						t.Fatalf("%s mode: the platform action was asked about an organization: %+v", mode, req)
					}
				}
			}
		})
	}
}

// S4 on the MCP listener: a refused header-carrier operation answers HTTP 503 with the refusal body, and no read.
func TestGraphQLEnforceMCPListenerAnswers503(t *testing.T) {
	srv := pdptest.New(t)
	gate := enforceGateFromEnv(t, srv, map[string]string{pdp.EnvMode: pdp.ModeShadow, "QUERY_API_HEADERS_AUTHZ_ENFORCE": "metrics"})
	ch := &countingMCPClient{}
	getenv := getenvFunc(func(string) string { return "" })
	mcpMux := http.NewServeMux()
	mountQueryRouteSets(getenv, http.NewServeMux(), http.NewServeMux(), mcpMux, queryRouteHandlers{
		Query:     http.NotFound,
		Registry:  http.NotFound,
		BuildInfo: http.NotFound,
		MCP:       newMCPHandlerWithLimits(ch, nil, allMCPRootsEnabled(), getenv, mcpDefaultLimits()),
	}, graphQLEdgeDeps{auth: ecEdgeAuth(t, &fakeEdgeStore{}), maxBytes: defaultGraphQLMaxQueryBytes})
	listener := MCPListener("127.0.0.1:0", &Plane{Handler: http.NewServeMux(), InternalHandler: http.NewServeMux(), MCPHandler: withGate(mcpMux, gate)}, nil)
	rec := mcpDo(listener.server.Handler, http.MethodPost, validMCPHeaders(), mcpBody(t, mcpHotspotsQuery, mcpHotspotsVariables(mcpTestOrg)))
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), `"code":"`+graph.CodeAuthzUnavailable+`"`) || ch.calls.Load() != 0 || len(srv.Requests()) != 0 {
		t.Fatalf("got %d %s with %d ClickHouse calls and %d asks, want 503 %s, no read, no ask", rec.Code, rec.Body.String(), ch.calls.Load(), len(srv.Requests()), graph.CodeAuthzUnavailable)
	}
}

// runRootsAs runs one document on the /graphql server with claims and subject as an ingress set them (no edge), with
// gate on the request, and returns what it answered, the asks and every bound org_id value of its reads.
func runRootsAs(t *testing.T, gate *pdp.Gate, decider *countingDecider, claims authctx.Claims, subject graph.ShadowSubject, document string) (enforceRun, int, []string) {
	t.Helper()
	client := &teamScopeReachClient{}
	server := newGraphQLServerWithLimits(&graph.Resolver{ClickHouse: client}, 1000, 10)
	var roots atomic.Int64
	server.AroundRootFields(func(ctx context.Context, next graphql.RootResolver) graphql.Marshaler {
		roots.Add(1)
		return next(ctx)
	})
	req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{"query":`+jsonQuote(t, document)+`}`))
	req.Header.Set("Content-Type", "application/json")
	ctx := graph.WithShadowSubject(authctx.WithClaims(req.Context(), claims), subject)
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req.WithContext(pdp.WithGate(ctx, gate)))
	gate.Wait()
	client.mu.Lock()
	defer client.mu.Unlock()
	var orgs []string
	for _, bindings := range client.bindings {
		for _, binding := range bindings {
			if binding.Name == "org_id" {
				orgs = append(orgs, fmt.Sprint(binding.Value))
			}
		}
	}
	asks := 0
	if decider != nil {
		asks = len(decider.requests())
	}
	return enforceRun{code: rec.Code, body: rec.Body.String(), statements: len(client.statements), roots: int(roots.Load())}, asks, orgs
}

// Known flaw F1 (13211 lists the only accepted impersonation narrowing): a legacy impersonation session (go-api's
// row, the admin's own token) answers as main does with every cell on: served as the target, never decided as the
// actor in the target's organization (no ask); in shadow mode the shadow still sees it, as on main.
func TestGraphQLEnforceLegacyImpersonationAnswersAsMain(t *testing.T) {
	impersonating := enforceCaller{user: true, impersonate: true}
	doc := `query Q { securityOverview(orgId: "` + servedOrgB.String() + `") { __typename } }`
	legacy := runGraphQLEnforce(t, nil, impersonating, doc)
	if legacy.code != http.StatusOK || strings.Contains(legacy.body, `"errors"`) || legacy.statements == 0 || legacy.roots != 1 {
		t.Fatalf("main's impersonated answer %+v, want served as the target org, its one root field run", legacy)
	}
	srv := pdptest.New(t)
	srv.Deny("no_membership")
	decider := &countingDecider{next: pepOf(t, srv.URL)}
	on, _ := enforceGate(t, pdp.ModeEnforce, strings.Join(authzenforce.Namespaces(), ","), decider)
	if got := runGraphQLEnforce(t, on, impersonating, doc); got != legacy {
		t.Fatalf("enforce, every cell on: %+v, want main's answer %+v", got, legacy)
	}
	if n := len(decider.requests()); n != 0 {
		t.Fatalf("asked the PDP %d times for a legacy impersonation session", n)
	}
	shadowDecider := &countingDecider{next: pepOf(t, srv.URL)}
	shadow, _ := enforceGate(t, pdp.ModeShadow, "analytics", shadowDecider)
	before := shadowCounters(t)
	if got := runGraphQLEnforce(t, shadow, impersonating, doc); got != legacy {
		t.Fatalf("shadow, cell on: %+v, want main's answer %+v", got, legacy)
	}
	// The shadow sees the session as it does on main: counted, never asked (the 5.12 core's impersonation rule).
	want := map[string]int64{shadowResultsMetric + "|reason=none,result=impersonation_refused": 1}
	if got := settle(t, before, want); fmt.Sprint(got) != fmt.Sprint(want) || len(shadowDecider.requests()) != 0 {
		t.Fatalf("5.12 counters %v with %d asks, want %v and no ask", got, len(shadowDecider.requests()), want)
	}
}

// G7 (D6714): a delegated bearer is judged by the PDP for the target org it acts in (asked once, about that org):
// an allow serves it, a decision for another org refuses it.
func TestGraphQLEnforceDelegatedBearerIsJudgedForTheTargetOrg(t *testing.T) {
	target := servedOrgB.String()
	claims := authctx.Claims{OrgID: target, Role: "viewer", Delegated: true, ImpersonationActive: true, Scope: pepgate.Legacy(target)}
	subject := graphQLShadowSubject(newAccessMinter(t).mint(ecUser, nil, nil))
	doc := `query Q { securityOverview(orgId: "` + target + `") { __typename } }`
	decider := &countingDecider{next: pepOf(t, pdptest.New(t).URL)}
	gate, _ := enforceGate(t, pdp.ModeEnforce, "analytics", decider)
	got, asks, _ := runRootsAs(t, gate, decider, claims, subject, doc)
	got.served(t)
	if req := decider.requests(); asks != 1 || req[0].OrganizationID != target {
		t.Fatalf("asked %+v, want one ask about the target org %s", req, target)
	}
	other := pdptest.New(t)
	other.Obligations(map[string]any{"tenant_scope": ecOrg.String(), "repository_scope": "organization"})
	gate, _ = enforceGate(t, pdp.ModeEnforce, "analytics", pepOf(t, other.URL))
	got, _, _ = runRootsAs(t, gate, nil, claims, subject, doc)
	if !strings.Contains(got.body, `"reason":"`+string(pdp.ReasonTenantMismatch)+`"`) || got.statements != 0 {
		t.Fatalf("cross-org decision: %+v, want the tenant_scope_mismatch refusal and no read", got)
	}
}

// G13, after the envelope class was retired: every ingress refuses the envelope, so it never reaches an operation. A
// subject with the envelope's skip reason that did reach one names no class: with every cell on it is refused
// (authz_enforce_no_principal) before any read, asks nothing and counts nothing on the exempt counter.
func TestGraphQLEnforceEnvelopeIsNotAnExemptClass(t *testing.T) {
	claims := authctx.Claims{OrgID: ecOrg.String(), Role: "admin", Scope: pepgate.Legacy(ecOrg.String())}
	envelope := graph.ShadowSubject{Bearer: "envelope.jws.value", SkipReason: policy.ShadowReasonEnvelope}
	decider := &countingDecider{next: pepOf(t, pdptest.New(t).URL)}
	gate, reader := enforceGate(t, pdp.ModeEnforce, strings.Join(authzenforce.Namespaces(), ","), decider)
	got, asks, _ := runRootsAs(t, gate, decider, claims, envelope, enforceDocTwo)
	if !strings.Contains(got.body, `"reason":"`+string(pdp.ReasonNoPrincipal)+`"`) || got.statements != 0 || asks != 0 {
		t.Fatalf("got %+v with %d asks, want the authz_enforce_no_principal refusal, no read and no ask", got, asks)
	}
	if exempt := (&gatedREST{reader: reader}).counter(t, "dev_health_query_api_pdp_exempt_total"); len(exempt) != 0 {
		t.Fatalf("exempt counts %v, want none", exempt)
	}
}

// Q11 + product rule 1: an enforced root reads in the organization the PDP decided, as the ingress spelled it: every
// root (one with an orgId argument, one that takes none, one that reads the claims' org itself) answers and reads as
// main does with the cells off, for any spelling of the org's UUID; the scope's org is the same UUID.
func TestGraphQLEnforceReadsTheScopeOrganization(t *testing.T) {
	upper := strings.ToUpper("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	claims := authctx.Claims{OrgID: upper, Role: "admin", Scope: pepgate.Legacy(upper)}
	subject := graphQLShadowSubject(newAccessMinter(t).mint(ecUser, nil, nil))
	for name, doc := range map[string]string{
		"orgId argument":       `query Q { securityOverview(orgId: "` + upper + `") { __typename } }`,
		"no org argument":      `query Q { improveOpportunities(limit: 5, windowDays: 30) { __typename } }`,
		"reads the claims org": `query Q { hotspots(input: {orgId: "` + upper + `", sinceUtc: "2026-01-01T00:00:00Z", untilUtc: "2026-01-15T00:00:00Z"}) { __typename } }`,
	} {
		t.Run(name, func(t *testing.T) {
			legacy, _, legacyOrgs := runRootsAs(t, nil, nil, claims, subject, doc)
			if legacy.statements == 0 || len(legacyOrgs) == 0 {
				t.Fatalf("main's run read nothing: %+v", legacy)
			}
			decider := &countingDecider{next: pepOf(t, pdptest.New(t).URL)}
			gate, _ := enforceGate(t, pdp.ModeEnforce, "analytics,metrics", decider)
			got, asks, orgs := runRootsAs(t, gate, decider, claims, subject, doc)
			if got != legacy || !reflect.DeepEqual(orgs, legacyOrgs) || asks != 1 {
				t.Fatalf("enforced: %+v bound %v with %d asks, want main's %+v bound %v and one ask", got, orgs, asks, legacy, legacyOrgs)
			}
		})
	}
}

// Product rule 1 (D6773): a platform admin with no organization (only a superuser passes the verifier with none)
// answers as main does with every cell on and the PDP is never asked, as on the REST rows.
func TestGraphQLEnforceSuperuserWithNoOrgAnswersAsMain(t *testing.T) {
	claims := authctx.Claims{IsSuperuser: true, Scope: pepgate.Legacy("")}
	subject := graphQLShadowSubject(newAccessMinter(t).mint(ecUser, nil, nil))
	for name, doc := range map[string]string{
		"no org argument": `query Q { improveOpportunities(limit: 5, windowDays: 30) { __typename } }`,
		"orgId argument":  `query Q { securityOverview(orgId: "` + ecOrg.String() + `") { __typename } }`,
	} {
		t.Run(name, func(t *testing.T) {
			legacy, _, legacyOrgs := runRootsAs(t, nil, nil, claims, subject, doc)
			decider := &countingDecider{next: pepOf(t, pdptest.New(t).URL)}
			gate, _ := enforceGate(t, pdp.ModeEnforce, strings.Join(authzenforce.Namespaces(), ","), decider)
			got, asks, orgs := runRootsAs(t, gate, decider, claims, subject, doc)
			if got != legacy || !reflect.DeepEqual(orgs, legacyOrgs) || asks != 0 {
				t.Fatalf("enforced: %+v bound %v with %d asks, want main's %+v bound %v and no ask", got, orgs, asks, legacy, legacyOrgs)
			}
		})
	}
}

// Q11-a: X-Org-Id B (a member of B) with an orgId argument A is two organizations: the org guard refuses, and the PDP
// is never asked.
func TestGraphQLEnforceHeaderOrgAndArgumentOrgDifferIsTheGuardsRefusal(t *testing.T) {
	caller := enforceCaller{user: true, orgHeader: servedOrgB.String(), memberOfB: true}
	legacy := runGraphQLEnforce(t, nil, caller, enforceDocAnalytics)
	decider := &countingDecider{next: pepOf(t, pdptest.New(t).URL)}
	gate, _ := enforceGate(t, pdp.ModeEnforce, "analytics", decider)
	got := runGraphQLEnforce(t, gate, caller, enforceDocAnalytics)
	if got != legacy || got.statements != 0 || !strings.Contains(got.body, `"errors"`) || len(decider.requests()) != 0 {
		t.Fatalf("got %+v with %d asks, want the guard's refusal %+v, no read, no ask", got, len(decider.requests()), legacy)
	}
}

// The guard lets a verified superuser name another org in any spelling of its UUID; with every cell on and a PDP allow
// the operation answers as main does (the org ids are compared as the ingress spelled them, the decision as UUIDs).
func TestGraphQLEnforceSuperuserNamingAnOrgInAnotherSpellingAnswersAsMain(t *testing.T) {
	upper := strings.ToUpper("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
	superuser := enforceCaller{user: true, superuser: true}
	for name, doc := range map[string]string{
		"orgId argument": `query Q { securityOverview(orgId: "` + upper + `") { __typename } }`,
		"two roots":      `query Q { securityOverview(orgId: "` + upper + `") { __typename } busFactor(orgId: "` + upper + `") { __typename } }`,
	} {
		t.Run(name, func(t *testing.T) {
			legacy := runGraphQLEnforce(t, nil, superuser, doc)
			if legacy.code != http.StatusOK || strings.Contains(legacy.body, `"errors"`) {
				t.Fatalf("main's answer %+v, want served", legacy)
			}
			decider := &countingDecider{next: pepOf(t, pdptest.New(t).URL)}
			gate, _ := enforceGate(t, pdp.ModeEnforce, "analytics,metrics", decider)
			if got := runGraphQLEnforce(t, gate, superuser, doc); got != legacy {
				t.Fatalf("every cell on, PDP allow: %+v, want main's answer %+v", got, legacy)
			}
			if len(decider.requests()) == 0 {
				t.Fatal("the PDP was not asked")
			}
		})
	}
}
