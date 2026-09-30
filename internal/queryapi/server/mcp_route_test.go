package server

// CHAOS-7085 / CHAOS-7091: every refusal of the MCP caller class is sent
// through the REAL listener handlers (MCPListener / Listeners, the same
// middleware stack dho query-api serves) and asserts two things: the
// refusal, AND zero ClickHouse calls (countingMCPClient counts every Query
// the resolvers could make). A refusal that still reached ClickHouse would
// pass a status-only test.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"

	schemav1 "github.com/full-chaos/dev-health-ops/contracts/graphql/v1"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/internalidentity"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

const mcpTestOrg = "70d529e0-0000-4000-8000-000000000001"

// countingMCPClient is a ClickHouse client that answers every query with no
// rows (or with err) and counts the calls.
type countingMCPClient struct {
	calls atomic.Int64
	err   error
	// rowsErr, when set, is returned from the row scanner's Err() -- the
	// shape of a budget exception raised while streaming.
	rowsErr error
}

func (c *countingMCPClient) Query(context.Context, string, []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	c.calls.Add(1)
	if c.err != nil {
		return nil, c.err
	}
	return &emptyRows{err: c.rowsErr}, nil
}

type emptyRows struct{ err error }

func (*emptyRows) Next() bool        { return false }
func (*emptyRows) Scan(...any) error { return nil }
func (r *emptyRows) Err() error      { return r.err }
func (*emptyRows) Close() error      { return nil }

// allMCPRootsEnabled is a class switch with every allowlisted root field's
// row on.
func allMCPRootsEnabled() routeswitch.StaticSwitch {
	sw := routeswitch.StaticSwitch{}
	for root := range mcpRootFieldAllowlist {
		sw[mcpRoutingOperationPrefix+root] = true
	}
	return sw
}

// mcpTestListeners builds the three listener handlers the way Build and
// dho query-api do: the public and internal route sets carry the REAL
// registered-document /query (digest gate, internal carrier), and the MCP
// route set carries only the MCP class's /query. All three share one
// counting ClickHouse client.
type mcpTestListeners struct {
	public, internal, mcp http.Handler
	ch                    *countingMCPClient
}

func newMCPTestListeners(t *testing.T, ch *countingMCPClient, sw routeswitch.Switch, limits mcpLimits) mcpTestListeners {
	t.Helper()
	getenv := getenvFunc(func(string) string { return "" })

	routeMux := routeswitch.NewMux(routeswitch.StaticSwitch{"hotspots": true})
	routeMux.Register("hotspots", newGraphQLServer(&graph.Resolver{ClickHouse: ch}))
	registered := newDocumentDispatchHandler(getenv, routeMux, map[string]string{digestHex(registeredHotspotsDocument): "hotspots"}, nil, nil, nil, "", nil)

	stub := func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) }
	mux, internalMux, mcpMux := http.NewServeMux(), http.NewServeMux(), http.NewServeMux()
	// Build's own mounting, so these route sets are Build's route sets.
	mountQueryRouteSets(getenv, mux, internalMux, mcpMux, queryRouteHandlers{
		Query:     registered,
		Registry:  stub,
		BuildInfo: stub,
		MCP:       newMCPHandlerWithLimits(ch, nil, sw, getenv, limits),
	})
	// Build's tail: the public set is wrapped, the internal set falls through to it.
	handler := markResponseModelRoutes(mux)
	internalMux.Handle("/", handler)
	plane := &Plane{Handler: handler, InternalHandler: internalMux, MCPHandler: mcpMux}

	public, internal := Listeners("127.0.0.1:0", "127.0.0.1:0", plane, nil, nil)
	mcp := MCPListener("127.0.0.1:0", plane, nil)
	if internal == nil || mcp == nil {
		t.Fatal("expected internal and MCP listeners")
	}
	return mcpTestListeners{public: public.server.Handler, internal: internal.server.Handler, mcp: mcp.server.Handler, ch: ch}
}

func mcpHeaders(org, role, superuser, impersonation string) http.Header {
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	h.Set(internalidentity.HeaderOrgID, org)
	h.Set(internalidentity.HeaderRole, role)
	h.Set(internalidentity.HeaderSuperuser, superuser)
	h.Set(internalidentity.HeaderImpersonationActive, impersonation)
	return h
}

func validMCPHeaders() http.Header { return mcpHeaders(mcpTestOrg, "member", "false", "false") }

func mcpDo(handler http.Handler, method string, header http.Header, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, "/query", strings.NewReader(body))
	for key, values := range header {
		req.Header[key] = values
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func mcpBody(t *testing.T, query string, variables map[string]any) string {
	t.Helper()
	payload := map[string]any{"query": query}
	if variables != nil {
		payload["variables"] = variables
	}
	b, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

const mcpHotspotsQuery = `query MCPHotspots($input: HotspotsInput!) { hotspots(input: $input) { rows { filePath repoId riskScore } } }`

func mcpHotspotsVariables(org string) map[string]any {
	return map[string]any{"input": map[string]any{
		"orgId": org, "sinceUtc": "2026-09-01T00:00:00Z", "untilUtc": "2026-09-08T00:00:00Z",
	}}
}

// mcpReason reads the refusal body's typed reason and code.
func mcpReason(t *testing.T, rec *httptest.ResponseRecorder) (reason, code string) {
	t.Helper()
	var body struct {
		Errors []struct {
			Extensions map[string]any `json:"extensions"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || len(body.Errors) == 0 {
		return "", ""
	}
	reason, _ = body.Errors[0].Extensions["reason"].(string)
	code, _ = body.Errors[0].Extensions["code"].(string)
	return reason, code
}

func assertMCPRefused(t *testing.T, rec *httptest.ResponseRecorder, ch *countingMCPClient, wantStatus int, wantReason string) {
	t.Helper()
	if rec.Code != wantStatus {
		t.Fatalf("status = %d, want %d; body %s", rec.Code, wantStatus, rec.Body.String())
	}
	if reason, _ := mcpReason(t, rec); reason != wantReason {
		t.Fatalf("reason = %q, want %q; body %s", reason, wantReason, rec.Body.String())
	}
	if n := ch.calls.Load(); n != 0 {
		t.Fatalf("a refused request made %d ClickHouse calls, want 0", n)
	}
}

// captureLog redirects the standard logger (which the default slog handler
// also writes through) for the duration of fn.
func captureMCPLog(t *testing.T, fn func()) string {
	t.Helper()
	var buf bytes.Buffer
	var mu sync.Mutex
	writer := writerFunc(func(p []byte) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		return buf.Write(p)
	})
	previous := log.Writer()
	log.SetOutput(writer)
	defer log.SetOutput(previous)
	fn()
	mu.Lock()
	defer mu.Unlock()
	return buf.String()
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

// --- Acceptance: served ---

func TestMCPServesAnAllowlistedQueryOnTheMCPListener(t *testing.T) {
	l := newMCPTestListeners(t, &countingMCPClient{}, allMCPRootsEnabled(), mcpDefaultLimits())
	rec := mcpDo(l.mcp, http.MethodPost, validMCPHeaders(), mcpBody(t, mcpHotspotsQuery, mcpHotspotsVariables(mcpTestOrg)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body %s", rec.Code, rec.Body.String())
	}
	var body struct {
		Data   map[string]any `json:"data"`
		Errors []any          `json:"errors"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Errors) != 0 || body.Data["hotspots"] == nil {
		t.Fatalf("want served data with no errors, got %s", rec.Body.String())
	}
	if l.ch.calls.Load() == 0 {
		t.Fatal("a served hotspots query made no ClickHouse call: the resolver never ran")
	}
}

// --- Acceptance: refused by operation type ---

func TestMCPRefusesAMutationByOperationTypeWithZeroClickHouse(t *testing.T) {
	l := newMCPTestListeners(t, &countingMCPClient{}, allMCPRootsEnabled(), mcpDefaultLimits())
	query := fmt.Sprintf(`mutation M { deleteSavedReport(orgId: %q, reportId: "r1") }`, mcpTestOrg)
	rec := mcpDo(l.mcp, http.MethodPost, validMCPHeaders(), mcpBody(t, query, nil))
	assertMCPRefused(t, rec, l.ch, http.StatusMethodNotAllowed, mcpReasonNotAQuery)
}

// --- Acceptance: introspection refused ---

func TestMCPRefusesIntrospection(t *testing.T) {
	for name, query := range map[string]string{
		"__schema": `query I { __schema { queryType { name } } }`,
		"__type":   `query I { __type(name: "Query") { name } }`,
		"nested":   fmt.Sprintf(`query I { hotspots(input: {orgId: %q, sinceUtc: "2026-09-01T00:00:00Z", untilUtc: "2026-09-08T00:00:00Z"}) { rows { filePath } } __schema { types { name } } }`, mcpTestOrg),
	} {
		t.Run(name, func(t *testing.T) {
			l := newMCPTestListeners(t, &countingMCPClient{}, allMCPRootsEnabled(), mcpDefaultLimits())
			rec := mcpDo(l.mcp, http.MethodPost, validMCPHeaders(), mcpBody(t, query, nil))
			assertMCPRefused(t, rec, l.ch, http.StatusBadRequest, mcpReasonIntrospection)
		})
	}
}

// --- Acceptance: depth / alias / complexity refused before dispatch ---

func TestMCPRefusesADocumentDeeperThanTheLimitBeforeDispatch(t *testing.T) {
	limits := mcpDefaultLimits()
	limits.depth = 2 // hotspots { rows { filePath } } has depth 3
	l := newMCPTestListeners(t, &countingMCPClient{}, allMCPRootsEnabled(), limits)
	rec := mcpDo(l.mcp, http.MethodPost, validMCPHeaders(), mcpBody(t, mcpHotspotsQuery, mcpHotspotsVariables(mcpTestOrg)))
	assertMCPRefused(t, rec, l.ch, http.StatusBadRequest, mcpReasonDepth)
}

func TestMCPRefusesADocumentOverTheComplexityLimitBeforeDispatch(t *testing.T) {
	limits := mcpDefaultLimits()
	limits.complexity = 2 // hotspots { rows { filePath repoId riskScore } } costs more
	l := newMCPTestListeners(t, &countingMCPClient{}, allMCPRootsEnabled(), limits)
	rec := mcpDo(l.mcp, http.MethodPost, validMCPHeaders(), mcpBody(t, mcpHotspotsQuery, mcpHotspotsVariables(mcpTestOrg)))
	assertMCPRefused(t, rec, l.ch, http.StatusBadRequest, mcpReasonComplexity)
}

// The PRODUCTION alias cap, over HTTP: 16 aliased copies of one root field
// (each well under the depth and complexity caps together).
func TestMCPRefusesSixteenAliasesAtTheProductionCapBeforeDispatch(t *testing.T) {
	l := newMCPTestListeners(t, &countingMCPClient{}, allMCPRootsEnabled(), mcpDefaultLimits())
	var fields []string
	for i := 0; i < mcpAliasLimit+1; i++ {
		fields = append(fields, fmt.Sprintf("a%d: hotspots(input: $input) { rows { filePath } }", i))
	}
	query := "query A($input: HotspotsInput!) { " + strings.Join(fields, " ") + " }"
	rec := mcpDo(l.mcp, http.MethodPost, validMCPHeaders(), mcpBody(t, query, mcpHotspotsVariables(mcpTestOrg)))
	assertMCPRefused(t, rec, l.ch, http.StatusBadRequest, mcpReasonAliases)
}

// The inverse control: exactly the cap is served.
func TestMCPServesExactlyFifteenAliases(t *testing.T) {
	l := newMCPTestListeners(t, &countingMCPClient{}, allMCPRootsEnabled(), mcpDefaultLimits())
	var fields []string
	for i := 0; i < mcpAliasLimit; i++ {
		fields = append(fields, fmt.Sprintf("a%d: hotspots(input: $input) { rows { filePath } }", i))
	}
	query := "query A($input: HotspotsInput!) { " + strings.Join(fields, " ") + " }"
	rec := mcpDo(l.mcp, http.MethodPost, validMCPHeaders(), mcpBody(t, query, mcpHotspotsVariables(mcpTestOrg)))
	if rec.Code != http.StatusOK {
		t.Fatalf("15 aliases: status %d, want 200; body %s", rec.Code, rec.Body.String())
	}
}

// --- Acceptance: elevated claim refused, loudly ---

func TestMCPRefusesElevatedClaimsLoudlyWithZeroClickHouse(t *testing.T) {
	for name, header := range map[string]http.Header{
		"superuser":     mcpHeaders(mcpTestOrg, "member", "true", "false"),
		"impersonation": mcpHeaders(mcpTestOrg, "member", "false", "true"),
		"role_admin":    mcpHeaders(mcpTestOrg, "admin", "false", "false"),
		"role_owner":    mcpHeaders(mcpTestOrg, "Owner", "false", "false"),
		"role_operator": mcpHeaders(mcpTestOrg, "operator", "false", "false"),
	} {
		t.Run(name, func(t *testing.T) {
			l := newMCPTestListeners(t, &countingMCPClient{}, allMCPRootsEnabled(), mcpDefaultLimits())
			var rec *httptest.ResponseRecorder
			out := captureMCPLog(t, func() {
				rec = mcpDo(l.mcp, http.MethodPost, header, mcpBody(t, mcpHotspotsQuery, mcpHotspotsVariables(mcpTestOrg)))
			})
			assertMCPRefused(t, rec, l.ch, http.StatusForbidden, mcpReasonElevatedClaim)
			if !strings.Contains(out, "REFUSED a request claiming elevated privileges") || !strings.Contains(out, "ERROR") {
				t.Fatalf("no loud ERROR line for the elevated claim; log:\n%s", out)
			}
		})
	}
}

// --- Acceptance: the class exists only on the MCP port ---

func TestMCPOperationIsRefusedOnThePublicAndInternalListeners(t *testing.T) {
	for name, pick := range map[string]func(mcpTestListeners) http.Handler{
		"public":   func(l mcpTestListeners) http.Handler { return l.public },
		"internal": func(l mcpTestListeners) http.Handler { return l.internal },
	} {
		t.Run(name, func(t *testing.T) {
			l := newMCPTestListeners(t, &countingMCPClient{}, allMCPRootsEnabled(), mcpDefaultLimits())
			rec := mcpDo(pick(l), http.MethodPost, validMCPHeaders(), mcpBody(t, mcpHotspotsQuery, mcpHotspotsVariables(mcpTestOrg)))
			if rec.Code == http.StatusOK {
				t.Fatalf("the MCP free-form document was served on the %s listener: %s", name, rec.Body.String())
			}
			// Not even reached: a refusal from the MCP handler itself (its
			// off-listener check) would mean the route set carries the class.
			if _, code := mcpReason(t, rec); code != "" {
				t.Fatalf("the %s listener reached the MCP caller-class handler (code %q): %s", name, code, rec.Body.String())
			}
			if n := l.ch.calls.Load(); n != 0 {
				t.Fatalf("the %s listener made %d ClickHouse calls for an MCP-only document, want 0", name, n)
			}
		})
	}
}

// --- Carrier and org scope ---

func TestMCPRefusesAnyAuthorizationHeader(t *testing.T) {
	l := newMCPTestListeners(t, &countingMCPClient{}, allMCPRootsEnabled(), mcpDefaultLimits())
	header := validMCPHeaders()
	header.Set("Authorization", "Bearer x")
	rec := mcpDo(l.mcp, http.MethodPost, header, mcpBody(t, mcpHotspotsQuery, mcpHotspotsVariables(mcpTestOrg)))
	assertMCPRefused(t, rec, l.ch, http.StatusUnauthorized, mcpReasonAuthorization)
}

func TestMCPRefusesARequestWithNoCarrier(t *testing.T) {
	l := newMCPTestListeners(t, &countingMCPClient{}, allMCPRootsEnabled(), mcpDefaultLimits())
	header := http.Header{}
	header.Set("Content-Type", "application/json")
	rec := mcpDo(l.mcp, http.MethodPost, header, mcpBody(t, mcpHotspotsQuery, mcpHotspotsVariables(mcpTestOrg)))
	assertMCPRefused(t, rec, l.ch, http.StatusUnauthorized, mcpReasonNoCarrier)
}

// hotspots carries its org INSIDE an input object, which OperationOrgGuard
// does not read; the class gate does, literally and through a variable.
func TestMCPRefusesAnotherOrgInsideAnInputObject(t *testing.T) {
	other := "11111111-0000-4000-8000-000000000002"
	t.Run("variable", func(t *testing.T) {
		l := newMCPTestListeners(t, &countingMCPClient{}, allMCPRootsEnabled(), mcpDefaultLimits())
		rec := mcpDo(l.mcp, http.MethodPost, validMCPHeaders(), mcpBody(t, mcpHotspotsQuery, mcpHotspotsVariables(other)))
		assertMCPRefused(t, rec, l.ch, http.StatusForbidden, mcpReasonOrgMismatch)
	})
	t.Run("literal", func(t *testing.T) {
		l := newMCPTestListeners(t, &countingMCPClient{}, allMCPRootsEnabled(), mcpDefaultLimits())
		query := fmt.Sprintf(`query L { hotspots(input: {orgId: %q, sinceUtc: "2026-09-01T00:00:00Z", untilUtc: "2026-09-08T00:00:00Z"}) { rows { filePath } } }`, other)
		rec := mcpDo(l.mcp, http.MethodPost, validMCPHeaders(), mcpBody(t, query, nil))
		assertMCPRefused(t, rec, l.ch, http.StatusForbidden, mcpReasonOrgMismatch)
	})
	t.Run("top_level_argument", func(t *testing.T) {
		l := newMCPTestListeners(t, &countingMCPClient{}, allMCPRootsEnabled(), mcpDefaultLimits())
		query := fmt.Sprintf(`query T { securityOverview(orgId: %q) { __typename } }`, other)
		rec := mcpDo(l.mcp, http.MethodPost, validMCPHeaders(), mcpBody(t, query, nil))
		assertMCPRefused(t, rec, l.ch, http.StatusForbidden, mcpReasonOrgMismatch)
	})
}

// --- Allowlist and class routing rows ---

func TestMCPRefusesARootFieldOutsideTheAllowlist(t *testing.T) {
	// Every document here VALIDATES against the SDL, so only the allowlist
	// can refuse it (an invalid document would be refused earlier and hide
	// a missing allowlist check). The class switch has a row ON for each
	// field too, so the routing row cannot refuse it either.
	sw := allMCPRootsEnabled()
	for name, query := range map[string]string{
		"dataHealth (operator view)":  `query D { dataHealth(team: "t1") { __typename } }`,
		"savedReports (user objects)": fmt.Sprintf(`query S { savedReports(orgId: %q) { __typename } }`, mcpTestOrg),
		"busFactor (person ranking)":  fmt.Sprintf(`query B { busFactor(orgId: %q) { __typename } }`, mcpTestOrg),
		"featureFlags (not in slice)": fmt.Sprintf(`query F { featureFlags(orgId: %q) { __typename } }`, mcpTestOrg),
		"mixed with an allowed field": fmt.Sprintf(`query M { securityOverview(orgId: %q) { __typename } experiments(orgId: %q) { __typename } }`, mcpTestOrg, mcpTestOrg),
	} {
		t.Run(name, func(t *testing.T) {
			for _, root := range []string{"dataHealth", "savedReports", "busFactor", "featureFlags", "experiments"} {
				sw[mcpRoutingOperationPrefix+root] = true
			}
			l := newMCPTestListeners(t, &countingMCPClient{}, sw, mcpDefaultLimits())
			rec := mcpDo(l.mcp, http.MethodPost, validMCPHeaders(), mcpBody(t, query, nil))
			assertMCPRefused(t, rec, l.ch, http.StatusForbidden, mcpReasonRootField)
		})
	}
}

func TestMCPRefusesARootFieldWhoseClassRowIsOff(t *testing.T) {
	sw := allMCPRootsEnabled()
	sw[mcpRoutingOperationPrefix+"hotspots"] = false
	l := newMCPTestListeners(t, &countingMCPClient{}, sw, mcpDefaultLimits())
	rec := mcpDo(l.mcp, http.MethodPost, validMCPHeaders(), mcpBody(t, mcpHotspotsQuery, mcpHotspotsVariables(mcpTestOrg)))
	assertMCPRefused(t, rec, l.ch, http.StatusNotFound, mcpReasonRootFieldNotEnabled)
}

// --- Transport shape ---

func TestMCPRefusesOtherTransportShapes(t *testing.T) {
	good := mcpBody(t, mcpHotspotsQuery, mcpHotspotsVariables(mcpTestOrg))
	cases := []struct {
		name   string
		method string
		header func() http.Header
		body   string
		status int
		reason string
	}{
		{"get", http.MethodGet, validMCPHeaders, "", http.StatusMethodNotAllowed, mcpReasonMethod},
		{"multipart", http.MethodPost, func() http.Header {
			h := validMCPHeaders()
			h.Set("Content-Type", "multipart/form-data; boundary=x")
			return h
		}, good, http.StatusUnsupportedMediaType, mcpReasonContentType},
		{"batch_array", http.MethodPost, validMCPHeaders, "[" + good + "]", http.StatusBadRequest, mcpReasonBadBody},
		{"apq_extensions", http.MethodPost, validMCPHeaders, `{"query":"` + strings.ReplaceAll(mcpHotspotsQuery, `"`, `\"`) + `","extensions":{"persistedQuery":{"version":1,"sha256Hash":"x"}}}`, http.StatusBadRequest, mcpReasonBodyField},
		{"two_operations", http.MethodPost, validMCPHeaders, mcpBody(t, mcpHotspotsQuery+" query Other { __typename }", mcpHotspotsVariables(mcpTestOrg)), http.StatusBadRequest, mcpReasonOperationCount},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			l := newMCPTestListeners(t, &countingMCPClient{}, allMCPRootsEnabled(), mcpDefaultLimits())
			rec := mcpDo(l.mcp, tc.method, tc.header(), tc.body)
			assertMCPRefused(t, rec, l.ch, tc.status, tc.reason)
		})
	}
}

func TestMCPListenerServesNothingButQuery(t *testing.T) {
	l := newMCPTestListeners(t, &countingMCPClient{}, allMCPRootsEnabled(), mcpDefaultLimits())
	for _, path := range []string{"/registry", "/buildinfo", "/metrics", "/healthz", "/api/v1/meta", "/query/proof", "/query/proof-write"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		l.mcp.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("%s on the MCP listener: status %d, want 404", path, rec.Code)
		}
	}
}

// The handler refuses a request that did not arrive through the MCP
// listener's middleware (wired onto another mux by mistake).
func TestMCPHandlerRefusesARequestOffTheMCPListener(t *testing.T) {
	ch := &countingMCPClient{}
	handler := newMCPHandler(ch, nil, allMCPRootsEnabled(), func(string) string { return "" })
	rec := mcpDo(handler, http.MethodPost, validMCPHeaders(), mcpBody(t, mcpHotspotsQuery, mcpHotspotsVariables(mcpTestOrg)))
	assertMCPRefused(t, rec, ch, http.StatusNotFound, mcpReasonOffListener)
}

// --- CHAOS-7091: the read budget is a typed refusal, never truncation ---

func TestMCPBudgetExceptionIsATypedRefusal(t *testing.T) {
	for name, tc := range map[string]struct {
		ch   *countingMCPClient
		want string
	}{
		"bytes_at_query":     {&countingMCPClient{err: fmt.Errorf("ClickHouse query failed: %w", &clickhousedriver.Exception{Code: 307})}, mcpReasonBytesCeiling},
		"bytes_while_stream": {&countingMCPClient{rowsErr: fmt.Errorf("ClickHouse row iteration failed: %w", &clickhousedriver.Exception{Code: 307})}, mcpReasonBytesCeiling},
		"rows":               {&countingMCPClient{err: fmt.Errorf("wrapped: %w", &clickhousedriver.Exception{Code: 158})}, mcpReasonRowsCeiling},
		"result_rows":        {&countingMCPClient{rowsErr: fmt.Errorf("ClickHouse row iteration failed: %w", &clickhousedriver.Exception{Code: 396})}, mcpReasonRowsCeiling},
		"time":               {&countingMCPClient{err: fmt.Errorf("wrapped: %w", &clickhousedriver.Exception{Code: 159})}, mcpReasonTimeCeiling},
		"deadline":           {&countingMCPClient{err: fmt.Errorf("ClickHouse query failed: %w", context.DeadlineExceeded)}, mcpReasonTimeCeiling},
	} {
		ch, want := tc.ch, tc.want
		t.Run(name, func(t *testing.T) {
			l := newMCPTestListeners(t, ch, allMCPRootsEnabled(), mcpDefaultLimits())
			rec := mcpDo(l.mcp, http.MethodPost, validMCPHeaders(), mcpBody(t, mcpHotspotsQuery, mcpHotspotsVariables(mcpTestOrg)))
			if rec.Code != http.StatusUnprocessableEntity {
				t.Fatalf("status %d, want 422; body %s", rec.Code, rec.Body.String())
			}
			reason, code := mcpReason(t, rec)
			if code != "MCP_READ_BUDGET_EXCEEDED" || reason != want {
				t.Fatalf("code %q reason %q, want MCP_READ_BUDGET_EXCEEDED/%s; body %s", code, reason, want, rec.Body.String())
			}
			if strings.Contains(rec.Body.String(), `"data"`) {
				t.Fatalf("a budget refusal carried data: %s", rec.Body.String())
			}
		})
	}
}

// A resolver that swallows the budget error (flowMatrix's shape: execution
// error -> empty result) still cannot hide it: the observed client records
// it where it surfaces.
func TestMCPObservedClientRecordsASwallowedBudgetError(t *testing.T) {
	obs := &mcpObservation{}
	ctx := context.WithValue(context.Background(), mcpObservationKey{}, obs)
	client := mcpObservedClient{next: &countingMCPClient{rowsErr: fmt.Errorf("x: %w", &clickhousedriver.Exception{Code: 307})}}
	rows, err := client.Query(ctx, "SELECT 1", nil)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
	}
	_ = rows.Err() // swallowed by the "resolver"
	_ = rows.Close()
	if got := obs.budgetReason(); got != mcpReasonBytesCeiling {
		t.Fatalf("budget reason %q, want %q", got, mcpReasonBytesCeiling)
	}
	if obs.callCount() != 1 {
		t.Fatalf("calls %d, want 1", obs.callCount())
	}
}

// A non-budget ClickHouse error is NOT a budget refusal.
func TestMCPBudgetReasonIgnoresOtherErrors(t *testing.T) {
	if got := mcpBudgetReason(fmt.Errorf("x: %w", &clickhousedriver.Exception{Code: 60})); got != "" {
		t.Fatalf("code 60 classified as %q, want \"\"", got)
	}
}

func TestMCPClickHouseOptionsCarryTheCeilings(t *testing.T) {
	opts := newMCPClickHouseOptions("clickhouse://localhost:9000/default")
	if opts.MaxBytesToRead == nil || *opts.MaxBytesToRead != 4294967296 {
		t.Fatalf("MaxBytesToRead = %v, want 4294967296 (4 GiB)", opts.MaxBytesToRead)
	}
	if opts.MaxExecutionTime != 10 {
		t.Fatalf("MaxExecutionTime = %d, want 10", opts.MaxExecutionTime)
	}
	if opts.MaxResultRows == nil || *opts.MaxResultRows != queryRouteMaxResultRows {
		t.Fatalf("MaxResultRows = %v, want queryRouteMaxResultRows", opts.MaxResultRows)
	}
	shared := newUnrestrictedReadClickHouseOptions("clickhouse://localhost:9000/default")
	if shared.MaxBytesToRead == nil || *shared.MaxBytesToRead != 0 {
		t.Fatal("building the MCP options changed the shared path's unrestricted setting")
	}
}

// --- Telemetry: one line per request, no variable value, no query text ---

func TestMCPLogsOneLinePerRequestWithoutValues(t *testing.T) {
	l := newMCPTestListeners(t, &countingMCPClient{}, allMCPRootsEnabled(), mcpDefaultLimits())
	const sentinel = "SENTINEL-REPO-7085"
	variables := mcpHotspotsVariables(mcpTestOrg)
	variables["input"].(map[string]any)["repoIds"] = []any{sentinel}
	out := captureMCPLog(t, func() {
		mcpDo(l.mcp, http.MethodPost, validMCPHeaders(), mcpBody(t, mcpHotspotsQuery, variables))
	})
	var lines []string
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, "query-api: mcp request:") {
			lines = append(lines, line)
		}
	}
	if len(lines) != 1 {
		t.Fatalf("want exactly one request line, got %d:\n%s", len(lines), out)
	}
	line := lines[0]
	for _, want := range []string{
		"caller_class=mcp", "outcome=served", "document_digest=" + digestHex(mcpHotspotsQuery),
		"root_fields=hotspots", "aliases=0", "depth=3", "complexity=", "duration_ms=",
	} {
		if !strings.Contains(line, want) {
			t.Fatalf("request line lacks %q: %s", want, line)
		}
	}
	if strings.Contains(out, sentinel) || strings.Contains(out, mcpTestOrg) || strings.Contains(out, "HotspotsInput") {
		t.Fatalf("the log carries a variable value, the org or the query text:\n%s", out)
	}
}

// --- Pins: caps, allowlist, class rows ---

func TestMCPCapsArePinned(t *testing.T) {
	if mcpDepthLimit != 10 || mcpAliasLimit != 15 || mcpComplexityLimit != 150 {
		t.Fatalf("caps = depth %d aliases %d complexity %d; the MCP team builds against 10/15/150 -- change them with that team", mcpDepthLimit, mcpAliasLimit, mcpComplexityLimit)
	}
	if mcpMaxBytesToRead != 4<<30 || mcpMaxExecutionTimeSeconds != 10 {
		t.Fatalf("ClickHouse ceilings = %d bytes, %d s; want 4 GiB, 10 s", mcpMaxBytesToRead, mcpMaxExecutionTimeSeconds)
	}
}

func TestMCPAllowlistIsExactlyTheReviewedSet(t *testing.T) {
	want := []string{
		"analytics", "capacityForecast", "capacityForecasts", "catalog", "cognitiveLoad",
		"complexityTimeseries", "compoundingRisk", "hotspots", "securityAlerts", "securityOverview",
		"throughputForecast", "workGraphArtifacts", "workGraphEdges", "workGraphFlow",
	}
	var got []string
	for root := range mcpRootFieldAllowlist {
		got = append(got, root)
	}
	sort.Strings(got)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("allowlist = %v, want %v", got, want)
	}
	schema := gqlparser.MustLoadSchema(&ast.Source{Name: "schema.graphql", Input: string(schemav1.SDL)})
	for _, root := range got {
		if schema.Query.Fields.ForName(root) == nil {
			t.Fatalf("allowlisted root field %q is not a Query field in the SDL", root)
		}
	}
}

func TestMCPAllowlistExcludesPersonOperatorAndWriteFields(t *testing.T) {
	schema := gqlparser.MustLoadSchema(&ast.Source{Name: "schema.graphql", Input: string(schemav1.SDL)})
	for _, field := range schema.Mutation.Fields {
		if mcpRootFieldAllowlist[field.Name] {
			t.Fatalf("mutation %q is allowlisted", field.Name)
		}
	}
	for _, root := range []string{
		"home", "recommendations", "workItemTeamAttributions", "pr", "busFactor", "reviewEdges",
		"dataHealth", "productTelemetryDashboard", "productTelemetryPlatformDashboard", "experiments",
		"savedReports", "savedReport", "reportRuns", "operatingReview", "featureFlags", "featureFlagEvents",
		"workUnitTeamAttributions", "testopsRisk", "aiImpactSummary", "aiAttributedPrs",
	} {
		if mcpRootFieldAllowlist[root] {
			t.Fatalf("%q must not be allowlisted for the MCP caller class", root)
		}
	}
}

func TestMCPRoutingRowsAreOnePerRootFieldUnderTheClassDigest(t *testing.T) {
	digests := mcpRoutingDigests()
	if len(digests) != len(mcpRootFieldAllowlist) {
		t.Fatalf("%d class rows, want %d", len(digests), len(mcpRootFieldAllowlist))
	}
	classDigest := digestHex("dev-health-ops/mcp-freeform-class/v1")
	for root := range mcpRootFieldAllowlist {
		if digests["mcp:"+root] != classDigest {
			t.Fatalf("class row for %q = %q, want %q", root, digests["mcp:"+root], classDigest)
		}
	}
}

// The second wall: the class's own gqlgen server refuses a mutation and a
// non-allowlisted root field by itself, with no handler gate in front, and
// runs no resolver for either.
func TestMCPGraphQLServerRefusesMutationsAndUnlistedRootsByItself(t *testing.T) {
	for name, tc := range map[string]struct{ query, want string }{
		"mutation":      {fmt.Sprintf(`mutation M { deleteSavedReport(orgId: %q, reportId: "r1") }`, mcpTestOrg), "serves query operations only"},
		"unlisted_root": {fmt.Sprintf(`query F { featureFlags(orgId: %q) { __typename } }`, mcpTestOrg), "featureFlags\\\" is not served to the MCP caller class"},
	} {
		query := tc.query
		t.Run(name, func(t *testing.T) {
			ch := &countingMCPClient{}
			resolver := &graph.Resolver{ClickHouse: ch}
			gql := newMCPGraphQLServer(graph.NewExecutableSchema(graph.Config{Resolvers: resolver}), mcpDefaultLimits())
			rec := gqlPost(t, gql, mcpBody(t, query, nil))
			var body struct {
				Data   any   `json:"data"`
				Errors []any `json:"errors"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if len(body.Errors) == 0 || body.Data != nil {
				t.Fatalf("the MCP gqlgen server ran %s: %s", name, rec.Body.String())
			}
			// The refusal must be the guard's own, not a later wall (the org
			// guard refuses a request with no claims, too).
			if !strings.Contains(rec.Body.String(), tc.want) {
				t.Fatalf("%s refused for another reason, want %q: %s", name, tc.want, rec.Body.String())
			}
			if ch.calls.Load() != 0 {
				t.Fatalf("%d ClickHouse calls, want 0", ch.calls.Load())
			}
		})
	}
}

// acr-api sends role "viewer" (and superuser/impersonation "false") on every
// call: that exact header set is served, and the role is forced to "" before
// dispatch, so no resolver can grant it anything.
func TestMCPServesTheViewerRoleAcrSends(t *testing.T) {
	l := newMCPTestListeners(t, &countingMCPClient{}, allMCPRootsEnabled(), mcpDefaultLimits())
	rec := mcpDo(l.mcp, http.MethodPost, mcpHeaders(mcpTestOrg, "viewer", "false", "false"), mcpBody(t, mcpHotspotsQuery, mcpHotspotsVariables(mcpTestOrg)))
	if rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), `"errors"`) {
		t.Fatalf("role viewer: status %d, want 200 with no errors; body %s", rec.Code, rec.Body.String())
	}
	claims, status, reason := mcpAuthenticate(func() *http.Request {
		r := httptest.NewRequest(http.MethodPost, "/query", nil)
		for key, values := range mcpHeaders(mcpTestOrg, "viewer", "false", "false") {
			r.Header[key] = values
		}
		return r
	}())
	if reason != "" || status != 0 || claims.Role != "" || claims.IsSuperuser || claims.ImpersonationActive || claims.OrgID != mcpTestOrg {
		t.Fatalf("viewer claims = %+v (status %d reason %q), want org only with role forced to \"\"", claims, status, reason)
	}
}

// The header org must be a real org id: empty, and padded (a value that
// would name a different org than the one it compares equal to after a
// trim), are both refused as invalid_org before anything else runs. Each
// clause is pinned by its own row. (Over the wire net/http trims header
// values, so the padded row is reachable only in-process; the clause keeps
// the guarantee independent of that.)
func TestMCPRefusesAnInvalidHeaderOrg(t *testing.T) {
	for name, org := range map[string]string{
		"empty":  "",
		"padded": " " + mcpTestOrg + " ",
	} {
		t.Run(name, func(t *testing.T) {
			l := newMCPTestListeners(t, &countingMCPClient{}, allMCPRootsEnabled(), mcpDefaultLimits())
			rec := mcpDo(l.mcp, http.MethodPost, mcpHeaders(org, "viewer", "false", "false"), mcpBody(t, mcpHotspotsQuery, mcpHotspotsVariables(mcpTestOrg)))
			assertMCPRefused(t, rec, l.ch, http.StatusUnauthorized, mcpReasonInvalidOrg)
		})
	}
}

// Every remaining refusal reason, one row per reason or per clause of a
// compound check, each through the real MCP listener with zero ClickHouse.
func TestMCPRefusesEveryOtherMalformedRequest(t *testing.T) {
	good := mcpHotspotsVariables(mcpTestOrg)
	headersWithout := func(drop string) func() http.Header {
		return func() http.Header {
			h := validMCPHeaders()
			h.Del(drop)
			return h
		}
	}
	headersWith := func(key string, values ...string) func() http.Header {
		return func() http.Header {
			h := validMCPHeaders()
			h[http.CanonicalHeaderKey(key)] = values
			return h
		}
	}
	literal := func(org string) string {
		return mcpBody(t, fmt.Sprintf(`query L { hotspots(input: {orgId: %q, sinceUtc: "2026-09-01T00:00:00Z", untilUtc: "2026-09-08T00:00:00Z"}) { rows { filePath } } }`, org), nil)
	}
	cases := []struct {
		name   string
		header func() http.Header
		body   string
		status int
		reason string
	}{
		{"no_content_type", headersWithout("Content-Type"), mcpBody(t, mcpHotspotsQuery, good), http.StatusUnsupportedMediaType, mcpReasonContentType},
		// mime.ParseMediaType returns "application/json" WITH an error for a
		// malformed parameter; the error alone must refuse it.
		{"content_type_bad_parameter", headersWith("Content-Type", "application/json; charset"), mcpBody(t, mcpHotspotsQuery, good), http.StatusUnsupportedMediaType, mcpReasonContentType},
		// json.Unmarshal fills query and still returns an error for variables
		// of the wrong type; the error alone must refuse it.
		{"variables_not_an_object", validMCPHeaders, `{"query":` + strconvQuote(mcpHotspotsQuery) + `,"variables":5}`, http.StatusBadRequest, mcpReasonBadBody},
		{"body_too_large", validMCPHeaders, `{"query":"` + strings.Repeat(" ", defaultGraphQLMaxQueryBytes) + `"}`, http.StatusRequestEntityTooLarge, mcpReasonBodyTooLarge},
		{"body_null", validMCPHeaders, `null`, http.StatusBadRequest, mcpReasonBadBody},
		{"query_empty", validMCPHeaders, `{"query":""}`, http.StatusBadRequest, mcpReasonBadBody},
		{"query_not_a_string", validMCPHeaders, `{"query":5}`, http.StatusBadRequest, mcpReasonBadBody},
		{"missing_header", headersWithout(internalidentity.HeaderRole), mcpBody(t, mcpHotspotsQuery, good), http.StatusUnauthorized, internalidentity.ReasonMissing},
		{"duplicate_header", headersWith(internalidentity.HeaderOrgID, mcpTestOrg, mcpTestOrg), mcpBody(t, mcpHotspotsQuery, good), http.StatusUnauthorized, internalidentity.ReasonDuplicate},
		{"flag_not_a_boolean", headersWith(internalidentity.HeaderSuperuser, "yes"), mcpBody(t, mcpHotspotsQuery, good), http.StatusUnauthorized, internalidentity.ReasonBoolean},
		{"document_syntax", validMCPHeaders, mcpBody(t, "query {", nil), http.StatusBadRequest, mcpReasonInvalidDocument},
		{"operation_name_mismatch", validMCPHeaders, `{"query":` + strconvQuote(mcpHotspotsQuery) + `,"operationName":"Other","variables":{"input":{"orgId":"` + mcpTestOrg + `","sinceUtc":"2026-09-01T00:00:00Z","untilUtc":"2026-09-08T00:00:00Z"}}}`, http.StatusBadRequest, mcpReasonOperationName},
		{"variables_missing", validMCPHeaders, mcpBody(t, mcpHotspotsQuery, nil), http.StatusBadRequest, mcpReasonInvalidVariables},
		{"org_argument_empty", validMCPHeaders, literal(""), http.StatusForbidden, mcpReasonInvalidOrgArgument},
		{"org_argument_padded", validMCPHeaders, literal(" " + mcpTestOrg + " "), http.StatusForbidden, mcpReasonInvalidOrgArgument},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			l := newMCPTestListeners(t, &countingMCPClient{}, allMCPRootsEnabled(), mcpDefaultLimits())
			rec := mcpDo(l.mcp, http.MethodPost, tc.header(), tc.body)
			assertMCPRefused(t, rec, l.ch, tc.status, tc.reason)
		})
	}
}

func strconvQuote(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

// __typename at the root is not introspection and not a root field needing
// a class row: served beside an allowlisted field.
func TestMCPServesTypenameAtTheRoot(t *testing.T) {
	l := newMCPTestListeners(t, &countingMCPClient{}, allMCPRootsEnabled(), mcpDefaultLimits())
	query := `query T($input: HotspotsInput!) { __typename hotspots(input: $input) { rows { filePath } } }`
	rec := mcpDo(l.mcp, http.MethodPost, validMCPHeaders(), mcpBody(t, query, mcpHotspotsVariables(mcpTestOrg)))
	if rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), `"errors"`) {
		t.Fatalf("status %d, want 200 with no errors; body %s", rec.Code, rec.Body.String())
	}
}

// The org_id spelling (the Python plane's, and graph.OperationOrgGuard's) is
// checked too. Today's SDL has no org_id argument or input field, so the
// schema validator refuses one before this gate; this pins the gate itself
// for the day one is added.
func TestMCPOrgCheckReadsTheOrgIDSpellingToo(t *testing.T) {
	field := &ast.Field{Name: "f", Alias: "f", Arguments: ast.ArgumentList{{
		Name:  "org_id",
		Value: &ast.Value{Kind: ast.StringValue, Raw: "other-org"},
	}}}
	if got := mcpCheckOrgArguments(ast.SelectionSet{field}, nil, nil, mcpTestOrg); got != mcpReasonOrgMismatch {
		t.Fatalf("argument org_id: reason %q, want %q", got, mcpReasonOrgMismatch)
	}
	nested := &ast.Field{Name: "f", Alias: "f", Arguments: ast.ArgumentList{{
		Name: "input",
		Value: &ast.Value{Kind: ast.ObjectValue, Children: ast.ChildValueList{{
			Name:  "org_id",
			Value: &ast.Value{Kind: ast.StringValue, Raw: "other-org"},
		}}},
	}}}
	if got := mcpCheckOrgArguments(ast.SelectionSet{nested}, nil, nil, mcpTestOrg); got != mcpReasonOrgMismatch {
		t.Fatalf("input field org_id: reason %q, want %q", got, mcpReasonOrgMismatch)
	}
}
