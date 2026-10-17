package server

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/internalidentity"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

// The endpoint-profile rows state what each served route accepts. This file
// proves the rows against BEHAVIOUR: the real listener handlers (Build's
// route-set mounting, the identity middleware and the compat shim) answer
// every HTTP method and every credential carrier, and the rows must equal what
// came back: a (method, route) that is not 404 or 405 has a row, and the
// credential classes of a row are exactly the carriers the stack authenticates.

const (
	classEnvelope  = "effective_principal_envelope"
	classEdgeToken = "ops_access_token_hs256"
)

var probeMethods = []string{
	http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut,
	http.MethodPatch, http.MethodDelete, http.MethodOptions,
}

// behaviourStubbed are routes the harness mounts with a stub handler because
// the real one needs a database: the probe cannot speak for them, so their
// rows are checked for presence and listener only.
var behaviourStubbed = map[string]string{
	"/query/proof":         "mounted with a stub: the real handler is the /query pipeline over a database",
	"/query/proof-write":   "mounted with a stub: the real handler is the /query pipeline over a database",
	"/query/run-operation": "mounted with a stub: the real handler is the /query pipeline over a database",
}

type behaviourStack struct {
	handlers map[string]http.Handler
	envelope string
	edge     string
}

func newBehaviourStack(t *testing.T) *behaviourStack {
	t.Helper()
	getenv := getenvFunc(func(name string) string {
		switch name {
		case deploymentEnvEnv:
			return "development"
		case proofRouteEnabledEnv:
			return "true"
		}
		return ""
	})
	verifier, priv := iaVerifier(t)
	store := &fakeEdgeStore{
		states:  map[uuid.UUID]policy.UserState{ecUser: {IsActive: true, TokenVersion: 5}},
		found:   map[uuid.UUID]bool{ecUser: true},
		members: map[[2]uuid.UUID]bool{{ecUser, ecOrg}: true},
	}
	auth := ecEdgeAuth(t, store)
	mux := routeswitch.NewMux(routeswitch.StaticSwitch{"probe": true})
	mux.Register("probe", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = authctx.FromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))
	query := newDocumentDispatchHandler(getenv, mux, map[string]string{digestHex(iaDocument): "probe"}, verifier, auth, store, "", nil)
	stub := func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }

	publicMux, internalMux, mcpMux := http.NewServeMux(), http.NewServeMux(), http.NewServeMux()
	mountQueryRouteSets(getenv, publicMux, internalMux, mcpMux, queryRouteHandlers{
		Query:        query,
		Registry:     newRegistryHandler("digest", map[string]string{"probe": digestHex(iaDocument)}),
		BuildInfo:    newBuildInfoHandler(verifier),
		Proof:        stub,
		ProofWrite:   stub,
		RunOperation: stub,
		MCP:          newMCPHandlerWithLimits(&countingMCPClient{}, nil, allMCPRootsEnabled(), getenv, mcpDefaultLimits()),
	}, graphQLEdgeDeps{auth: auth, corsOrigins: []string{"https://app.example"}, maxBytes: defaultGraphQLMaxQueryBytes})
	handler := markResponseModelRoutes(publicMux)
	internalMux.Handle("/", handler)
	plane := &Plane{Handler: handler, InternalHandler: internalMux, MCPHandler: mcpMux}

	registry := health.NewRegistry(time.Second)
	registry.SetReady(true)
	operator, err := health.NewServer(health.ServerOptions{Address: "127.0.0.1:0", Registry: registry, Service: "test"})
	if err != nil {
		t.Fatal(err)
	}
	public, internal := Listeners("127.0.0.1:0", "127.0.0.1:0", plane, OperatorCompat(registry, operator, "dev-health-query-api"), nil)
	mcp := MCPListener("127.0.0.1:0", plane, nil)
	if internal == nil || mcp == nil {
		t.Fatal("expected internal and MCP listeners")
	}
	return &behaviourStack{
		handlers: map[string]http.Handler{
			"query": public.server.Handler, "internal": internal.server.Handler, "mcp": mcp.server.Handler,
			"operator": operator.Handler(),
		},
		envelope: iaEnvelope(t, priv, principal.Claims{OrgID: ecOrg.String(), Role: "admin"}),
		edge:     ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), role: "admin", tokenVersion: 5}),
	}
}

// do sends one request to a listener. carrier is "", "envelope", "edge" or
// "headers" (the internal identity headers).
func (s *behaviourStack) do(listener, method, path, carrier string) int {
	var request *http.Request
	switch {
	case path == "/graphql" && (method == http.MethodGet || method == http.MethodHead):
		request = httptest.NewRequest(method, "/graphql?org_id="+ecOrg.String()+"&query="+url.QueryEscape(iaDocument), nil)
	case path == "/query" || path == "/graphql":
		request = httptest.NewRequest(method, path, strings.NewReader(`{"query":`+jsonQuoteString(iaDocument)+`}`))
		request.Header.Set("Content-Type", "application/json")
	default:
		request = httptest.NewRequest(method, path, nil)
	}
	switch carrier {
	case "envelope":
		request.Header.Set("Authorization", "Bearer "+s.envelope)
	case "edge":
		request.Header.Set("Authorization", "Bearer "+s.edge)
	case "headers":
		request.Header.Set(internalidentity.HeaderOrgID, ecOrg.String())
		request.Header.Set(internalidentity.HeaderRole, "admin")
		request.Header.Set(internalidentity.HeaderSuperuser, "false")
		request.Header.Set(internalidentity.HeaderImpersonationActive, "false")
	}
	recorder := httptest.NewRecorder()
	s.handlers[listener].ServeHTTP(recorder, request)
	return recorder.Code
}

func jsonQuoteString(text string) string {
	return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(text) + `"`
}

func served(code int) bool { return code != http.StatusNotFound && code != http.StatusMethodNotAllowed }

// servedMethods are the methods a path answers on a listener with a credential
// the path accepts (so an authentication refusal does not hide a method).
func (s *behaviourStack) servedMethods(listener, path string, carriers []string) []string {
	var out []string
	for _, method := range probeMethods {
		for _, carrier := range carriers {
			if served(s.do(listener, method, path, carrier)) {
				out = append(out, method)
				break
			}
		}
	}
	return out
}

type profileRow struct {
	id        string
	method    string
	route     string
	listeners []string
	public    bool
	classes   []string
}

func queryAPIRowsByRoute(t *testing.T) map[string][]profileRow {
	t.Helper()
	byRoute := map[string][]profileRow{}
	for _, raw := range loadQueryAPIRows(t) {
		id, _ := raw["id"].(string)
		match := rowListeners.FindStringSubmatch(id)
		if match == nil {
			t.Fatalf("row id %q has no @listener suffix", id)
		}
		row := profileRow{
			id: id, method: raw["method"].(string), route: raw["route"].(string),
			listeners: strings.Split(match[1], ","), public: raw["classification"] == "public",
		}
		if list, ok := raw["accepted_credential_classes"].([]any); ok {
			for _, class := range list {
				row.classes = append(row.classes, class.(string))
			}
		}
		sort.Strings(row.classes)
		byRoute[row.route] = append(byRoute[row.route], row)
	}
	return byRoute
}

func rowFor(rows []profileRow, listener string) []profileRow {
	var out []profileRow
	for _, row := range rows {
		for _, candidate := range row.listeners {
			if candidate == listener {
				out = append(out, row)
			}
		}
	}
	return out
}

// TestEndpointProfileRowsMatchServedBehaviour: for every non-REST route the
// binary serves on a listener, the rows equal what the real handlers do.
func TestEndpointProfileRowsMatchServedBehaviour(t *testing.T) {
	stack := newBehaviourStack(t)
	rows := queryAPIRowsByRoute(t)
	restPaths := map[string]bool{}
	for _, route := range RESTRoutes() {
		restPaths[route.Pattern] = true
	}

	probed := 0
	for token := range servedRoutes(t) {
		at := strings.LastIndex(token, "@")
		path, listener := token[:at], token[at+1:]
		if restPaths[path] || path == "/api/v1/" {
			continue
		}
		if reason, stubbed := behaviourStubbed[path]; stubbed {
			if len(rowFor(rows[path], listener)) == 0 {
				t.Errorf("%s: stubbed in the harness (%s) and has no row", token, reason)
			}
			continue
		}
		t.Run(token, func(t *testing.T) {
			forListener := rowFor(rows[path], listener)
			if len(forListener) == 0 {
				t.Fatalf("%s is served with no row", token)
			}
			// Identity headers sent to the public listener are deleted there and
			// counted, with a drop log the identity-header tests assert on
			// (rate-limited to one a second): they are no carrier of that
			// listener, so they are not sent to it.
			carriers := []string{"", "envelope", "edge"}
			if listener != "query" {
				carriers = append(carriers, "headers")
			}
			methods := stack.servedMethods(listener, path, carriers)
			if len(methods) == 0 {
				t.Fatalf("%s answers 404/405 to every method: the harness does not reach it", token)
			}
			var wantMethods []string
			if len(methods) == len(probeMethods) {
				wantMethods = []string{"api_route"}
			} else {
				wantMethods = append(wantMethods, methods...)
				// HEAD rides on GET in net/http routing and in handlers that accept GET.
				wantMethods = withoutHeadWhenGet(wantMethods)
			}
			var gotMethods []string
			for _, row := range forListener {
				gotMethods = append(gotMethods, row.method)
			}
			sort.Strings(gotMethods)
			sort.Strings(wantMethods)
			if strings.Join(gotMethods, ",") != strings.Join(wantMethods, ",") {
				t.Errorf("%s: rows declare methods %v, the handlers serve %v", token, gotMethods, wantMethods)
			}

			method := methods[0]
			for _, row := range forListener {
				if row.public {
					if code := stack.do(listener, method, path, ""); code == http.StatusUnauthorized {
						t.Errorf("%s: row %q is public but an anonymous %s is refused with 401", token, row.id, method)
					}
					continue
				}
				var accepted []string
				for carrier, class := range map[string]string{"envelope": classEnvelope, "edge": classEdgeToken} {
					if code := stack.do(listener, method, path, carrier); code != http.StatusUnauthorized && served(code) {
						accepted = append(accepted, class)
					}
				}
				sort.Strings(accepted)
				if strings.Join(accepted, ",") != strings.Join(row.classes, ",") {
					t.Errorf("%s: row %q accepts %v, the stack authenticates %v", token, row.id, row.classes, accepted)
				}
			}
			probed++
		})
	}
	if probed < 8 {
		t.Fatalf("only %d routes were probed: the harness stopped reaching the listeners", probed)
	}
}

func withoutHeadWhenGet(methods []string) []string {
	hasGet := false
	for _, method := range methods {
		hasGet = hasGet || method == http.MethodGet
	}
	if !hasGet {
		return methods
	}
	var out []string
	for _, method := range methods {
		if method != http.MethodHead {
			out = append(out, method)
		}
	}
	return out
}
