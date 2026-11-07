package apiservice

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/pep"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/full-chaos/dev-health-go/authverify"
	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/api/billing"
	"github.com/full-chaos/dev-health-ops/internal/api/orgs"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
	"github.com/full-chaos/dev-health-ops/internal/authzenforce"
)

// The billing and licensing routes by enforcement class. A: the profile row names a catalog action, so the PDP
// decides the route once its namespace is switched on. B: no action; the handler admits only admin.billing holders.
// C: no action; every organization role passes. D: the anonymous plan catalog. E: a non-user principal: the Stripe
// webhook, whose signature is its only credential. F: public listener health and 405 stubs. Every route of the area
// is in exactly one class; an edge-listener route carries the "@edge" suffix.
var (
	billingClassA = map[string]string{
		"GET /api/v1/billing/invoices":                    "billing.read",
		"GET /api/v1/billing/invoices/{invoice_id}":       "billing.read",
		"POST /api/v1/billing/invoices/{invoice_id}/void": "billing.manage",
		"GET /api/v1/billing/subscriptions/list":          "billing.read",
		"GET /api/v1/billing/subscriptions":               "billing.read",
		"GET /api/v1/billing/subscriptions/history":       "billing.read",
		"POST /api/v1/billing/subscriptions/change-plan":  "billing.manage",
		"POST /api/v1/billing/subscriptions/cancel":       "billing.manage",
		"POST /api/v1/billing/subscriptions/reactivate":   "billing.manage",
		"GET /api/v1/billing/entitlements/{org_id}":       "license.read",
		"GET /api/v1/licensing/entitlements/{org_id}":     "license.read",
		"POST /api/v1/billing/plans/pull-stripe":          "admin.billing",
	}
	billingClassB = []string{
		"POST /api/v1/billing/plans", "PUT /api/v1/billing/plans/{plan_id}", "DELETE /api/v1/billing/plans/{plan_id}",
		"POST /api/v1/billing/plans/{plan_id}/sync-stripe", "POST /api/v1/billing/refunds", "GET /api/v1/billing/refunds",
		"GET /api/v1/billing/refunds/{refund_id}", "GET /api/v1/billing/audit", "GET /api/v1/billing/audit/{audit_id}",
		"POST /api/v1/billing/audit/{audit_id}/resolve", "POST /api/v1/billing/reconcile",
	}
	billingClassC = []string{"POST /api/v1/billing/checkout", "POST /api/v1/billing/portal"}
	billingClassD = []string{"GET /api/v1/billing/plans", "GET /api/v1/billing/plans/{plan_id}"}
	billingClassE = []string{"POST /api/v1/billing/webhooks/stripe", "POST /api/v1/billing/webhooks/stripe @edge"}
	billingClassF = []string{"GET /health @edge", "HEAD /health @edge", "HEAD /api/v1/billing/plans/pull-stripe", "POST /api/v1/billing/plans/{plan_id}"}
)

// billingAreaRoutes is every route of the area: billing.Routes, billing.EdgeRoutes and orgs.Routes' licensing route.
func billingAreaRoutes(guard *policy.Guard, pool *pgxpool.Pool) (api, edge []httpapi.Route) {
	api = billing.Routes(billing.Deps{Pool: pool, Guard: guard, Logger: quietLogger()})
	for _, route := range orgs.Routes(pool, guard, quietLogger()) {
		if strings.HasPrefix(route.Pattern, "/api/v1/licensing/") {
			api = append(api, route)
		}
	}
	return api, billing.EdgeRoutes(billing.Deps{Pool: pool, Logger: quietLogger()})
}

// The census: each route of the area is in exactly one class, no class names a route the area does not mount, and
// class A is exactly the area's rows that name a catalog action in the go-api profile (the PDP's row map).
func TestEveryBillingAndLicensingRouteHasOneEnforcementClass(t *testing.T) {
	api, edge := billingAreaRoutes(policy.NewGuard(nil, quietLogger()), nil)
	var mounted []string
	for _, route := range api {
		mounted = append(mounted, route.Method+" "+route.Pattern)
	}
	for _, route := range edge {
		mounted = append(mounted, route.Method+" "+route.Pattern+" @edge")
	}
	classOf := map[string]string{}
	add := func(class string, keys []string) {
		for _, key := range keys {
			if previous, seen := classOf[key]; seen {
				t.Errorf("%s is in class %s and %s", key, previous, class)
			}
			classOf[key] = class
		}
	}
	var a []string
	for key := range billingClassA {
		a = append(a, key)
	}
	add("A", a)
	add("B", billingClassB)
	add("C", billingClassC)
	add("D", billingClassD)
	add("E", billingClassE)
	add("F", billingClassF)
	for _, key := range mounted {
		if _, ok := classOf[key]; !ok {
			t.Errorf("%s is mounted but has no enforcement class", key)
		}
	}
	for key := range classOf {
		if !slices.Contains(mounted, key) {
			t.Errorf("class row %s names no mounted route", key)
		}
	}
	rows, err := policy.ParseShadowRows(authv1.GoAPIEndpointProfiles, goAPIAccessService)
	if err != nil {
		t.Fatal(err)
	}
	for key, row := range rows {
		if !strings.HasPrefix(key, "GET /api/v1/billing/") && !strings.HasPrefix(key, "POST /api/v1/billing/") &&
			!strings.HasPrefix(key, "PUT /api/v1/billing/") && !strings.HasPrefix(key, "DELETE /api/v1/billing/") &&
			!strings.Contains(key, " /api/v1/licensing/") {
			continue
		}
		if want, ok := billingClassA[key]; !ok || want != row.Action {
			t.Errorf("profile row %s names %s; class A says %q", key, row.Action, want)
		}
	}
	for key, action := range billingClassA {
		if row, ok := rows[key]; !ok || row.Action != action {
			t.Errorf("class A %s (%s) has no profile row naming that action", key, action)
		}
	}
}

// billingCellPDP is a decision point over the wire: it records each request and answers with verdict.
type billingCellPDP struct {
	srv     *httptest.Server
	verdict string // "allow", "deny" or "error"
	mu      sync.Mutex
	seen    []cellSeen
}

type cellSeen struct{ principal, action, kind, key, org string }

func newBillingCellPDP(t *testing.T, verdict string) *billingCellPDP {
	t.Helper()
	p := &billingCellPDP{verdict: verdict}
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			PrincipalReference string `json:"principal_reference"`
			Action             string `json:"action"`
			Resource           struct {
				Type, Key string
				Org       *string `json:"organization_id"`
			} `json:"resource"`
		}
		raw, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(raw, &body); err != nil {
			http.Error(w, "bad", http.StatusBadRequest)
			return
		}
		seen := cellSeen{principal: body.PrincipalReference, action: body.Action, kind: body.Resource.Type, key: body.Resource.Key}
		var org any
		if body.Resource.Org != nil {
			seen.org, org = *body.Resource.Org, *body.Resource.Org
		}
		p.mu.Lock()
		p.seen = append(p.seen, seen)
		p.mu.Unlock()
		if p.verdict == "error" {
			http.Error(w, "down", http.StatusBadGateway)
			return
		}
		allowed, reason := p.verdict == "allow", "role_lacks_action"
		if allowed {
			reason = "allowed_by_role"
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"schema_version": "authorization_decision.v1", "decision_id": "dec_" + strings.Repeat("A", 26), "allowed": allowed,
			"reason_code": reason, "principal_id": "prn_TESTPRINCIPAL", "organization_id": org, "policy_revision": 1,
			"expires_at": time.Now().Add(time.Hour).UTC().Format("2006-01-02T15:04:05Z"),
		})
	}))
	t.Cleanup(p.srv.Close)
	return p
}

func (p *billingCellPDP) client(t *testing.T) *pep.Client {
	t.Helper()
	c, err := pep.New(pep.Config{BaseURL: p.srv.URL, AllowPlainHTTP: true, HTTPClient: p.srv.Client(), ClientService: goAPIAccessService, Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func (p *billingCellPDP) calls() []cellSeen {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.seen)
}

// cellRig is the api listener over the area's real routes and guard levels: the real profile-routed access
// validator, the real Enforcer over the real profile row map, the probe store (probeUser is an admin of probeOrg),
// and a pool whose every connection is refused, so a handler the guard lets through answers the generic 500.
type cellRig struct {
	handler http.Handler
	minter  *eddsaMinter
}

func newCellRig(t *testing.T, enforced string, decider pep.Decider, unconfigured bool) *cellRig {
	t.Helper()
	minter := newEdDSAMinter(t)
	store := newProbeStore()
	store.sessions[cellAdminSID] = policy.SessionRow{PrincipalID: probeAdminUser, LegacyTokenVersion: i64ptr(0)}
	auth, err := policy.NewAuthenticator(store, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	eddsa, err := authverify.NewVerifier(authverify.VerifierConfig{Issuer: probeIssuer, Audience: accessTokenAudienceAPI, Keys: minter.keys})
	if err != nil {
		t.Fatal(err)
	}
	validator, err := accessverify.New(accessverify.Config{Rows: realRows(t), EdDSA: eddsa,
		Meter: sdkmetric.NewMeterProvider().Meter("test"), Logger: quietLogger(), Now: func() time.Time { return probeNow }})
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.RouteAccess(routedAccessVerify{validator}, goAPIAccessService); err != nil {
		t.Fatal(err)
	}
	guard := policy.NewGuard(auth, quietLogger())
	if enforced != "" {
		rows, err := policy.ParseShadowRows(authv1.GoAPIEndpointProfiles, goAPIAccessService)
		if err != nil {
			t.Fatal(err)
		}
		enforcer, err := policy.NewEnforcer(policy.EnforceConfig{Decider: decider, Unconfigured: unconfigured, Rows: rows,
			Switch: authzenforce.Parse(authzenforce.GoAPI, enforced), Timeout: time.Second, Logger: quietLogger()})
		if err != nil {
			t.Fatal(err)
		}
		guard.SetEnforcer(enforcer)
	}
	pool, err := pgxpool.New(context.Background(), "postgres://127.0.0.1:1/refused?connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	api, _ := billingAreaRoutes(guard, pool)
	server, err := NewServer(apiTestConfig(), quietLogger(), api, policy.NewScope(auth, quietLogger()).Layers()...)
	if err != nil {
		t.Fatal(err)
	}
	return &cellRig{handler: server.Handler(), minter: minter}
}

// cellAdminSID is a live session of probeAdminUser, the superuser whose impersonation row the probe store holds.
var cellAdminSID = uuid.MustParse("00000000-0000-4000-8000-0000000000c9")

var cellParam = regexp.MustCompile(`\{[a-z_]+\}`)

// cellRequest is the request of route key: path ids are probeOrg for {org_id} (the token's org), else a fixed UUID;
// a POST carries a body the route's own decoder accepts.
func cellRequest(key string, headers map[string]string) *http.Request {
	method, route, _ := strings.Cut(key, " ")
	route = strings.TrimSuffix(route, " @edge")
	path := cellParam.ReplaceAllStringFunc(route, func(p string) string {
		if p == "{org_id}" {
			return probeOrg.String()
		}
		return "00000000-0000-4000-8000-0000000000e1"
	})
	var body io.Reader
	if method == http.MethodPost || method == http.MethodPut {
		payload := "{}"
		if strings.HasSuffix(route, "/change-plan") {
			payload = `{"price_id":"price_cell"}`
		}
		body = strings.NewReader(payload)
	}
	request := httptest.NewRequest(method, path, body)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	return request
}

func (rig *cellRig) serve(key string, headers map[string]string) (int, string, http.Header) {
	recorder := httptest.NewRecorder()
	rig.handler.ServeHTTP(recorder, cellRequest(key, headers))
	return recorder.Code, strings.TrimSpace(recorder.Body.String()), recorder.Header()
}

func (rig *cellRig) userBearer() map[string]string { return bearer(rig.minter.mint(probeUser, nil)) }

const cellAllNamespaces = "billing,license,admin"

// handlerReached is the answer of a handler the guard let through: its dependencies are unreachable, so it fails
// with the generic 500, which no guard refusal ever is.
func handlerReached(status int, body string) bool {
	return status == http.StatusInternalServerError && body == `{"detail":"Internal Server Error"}`
}

// cellDenyBody is the body an enforced deny gives at the route's own guard level.
var cellDenyBody = map[string]string{
	"POST /api/v1/billing/invoices/{invoice_id}/void": `{"detail":"Admin access required"}`,
	"POST /api/v1/billing/subscriptions/change-plan":  `{"detail":"Admin access required"}`,
	"POST /api/v1/billing/subscriptions/cancel":       `{"detail":"Admin access required"}`,
	"POST /api/v1/billing/subscriptions/reactivate":   `{"detail":"Admin access required"}`,
	"POST /api/v1/billing/plans/pull-stripe":          `{"detail":"Superuser access required"}`,
}

func classAKeys() []string {
	var keys []string
	for key := range billingClassA {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

// Class A with the switch off: no PDP call, the legacy answer, both with no Enforcer (no namespace on) and with an
// Enforcer whose only namespace is another area's. probeUser is an admin of probeOrg without admin.billing, so every
// org row reaches its handler and the platform row is refused by the legacy gate.
func TestBillingClassARowsAskNothingWithTheSwitchOff(t *testing.T) {
	pdp := newBillingCellPDP(t, "deny")
	for name, rig := range map[string]*cellRig{
		"no enforcer":          newCellRig(t, "", nil, false),
		"another namespace on": newCellRig(t, "teams", pdp.client(t), false),
	} {
		for _, key := range classAKeys() {
			status, body, _ := rig.serve(key, rig.userBearer())
			if key == "POST /api/v1/billing/plans/pull-stripe" {
				if status != http.StatusForbidden || body != `{"detail":"Superuser access required"}` {
					t.Errorf("%s, %s: %d %s, want the legacy platform refusal", name, key, status, body)
				}
				continue
			}
			if !handlerReached(status, body) {
				t.Errorf("%s, %s: %d %s, want the handler reached", name, key, status, body)
			}
		}
	}
	if n := len(pdp.calls()); n != 0 {
		t.Errorf("%d PDP calls with the area's namespaces off, want 0", n)
	}
}

// Class A with its namespace on: a caller with a live go-api impersonation row asks no PDP. A write is refused with a
// 403 (12814 option A, as built); a read keeps the answer the row gives with its cell off (product rule 1).
func TestBillingClassARowsRefuseAnImpersonatingCallerBeforeThePDP(t *testing.T) {
	pdp := newBillingCellPDP(t, "allow")
	rig, off := newCellRig(t, cellAllNamespaces, pdp.client(t), false), newCellRig(t, "", nil, false)
	impersonating := func(rig *cellRig) map[string]string {
		return bearer(rig.minter.mint(probeAdminUser, map[string]any{"sid": sessionid.Wire(cellAdminSID), "org_id": nil}))
	}
	for _, key := range classAKeys() {
		status, body, header := rig.serve(key, impersonating(rig))
		if method, _, _ := strings.Cut(key, " "); method == http.MethodGet {
			legacyStatus, legacyBody, legacyHeader := off.serve(key, impersonating(off))
			if status != legacyStatus || body != legacyBody || header.Get("X-Impersonating") != legacyHeader.Get("X-Impersonating") {
				t.Errorf("%s: %d %s X-Impersonating %q, want the cells-off %d %s %q", key, status, body, header.Get("X-Impersonating"),
					legacyStatus, legacyBody, legacyHeader.Get("X-Impersonating"))
			}
			continue
		}
		if status != http.StatusForbidden || body != `{"detail":"Not allowed in an impersonation session"}` || header.Get("X-Impersonating") != "true" {
			t.Errorf("%s: %d %s X-Impersonating %q, want 403 for the impersonation session", key, status, body, header.Get("X-Impersonating"))
		}
	}
	if n := len(pdp.calls()); n != 0 {
		t.Errorf("%d PDP calls for an impersonating caller, want 0", n)
	}
}

// Class A with its namespace on: one PDP call per request, on the row's action, the row's resource kind and the
// token's org (the platform row: no org); an allow reaches the handler, the legacy platform refusal included; a deny
// is a 403 with the level's body; a PDP failure is a 503 with a retry hint.
func TestBillingClassARowsAreDecidedByThePDPWithTheSwitchOn(t *testing.T) {
	for _, key := range classAKeys() {
		t.Run(key, func(t *testing.T) {
			action := billingClassA[key]
			platform := action == "admin.billing"
			for _, verdict := range []string{"allow", "deny", "error"} {
				pdp := newBillingCellPDP(t, verdict)
				rig := newCellRig(t, cellAllNamespaces, pdp.client(t), false)
				headers := rig.userBearer()
				status, body, header := rig.serve(key, headers)
				calls := pdp.calls()
				if len(calls) != 1 {
					t.Fatalf("%s: %d PDP calls, want 1", verdict, len(calls))
				}
				want := cellSeen{principal: strings.TrimPrefix(headers["Authorization"], "Bearer "), action: action, kind: "organization",
					key: probeOrg.String(), org: probeOrg.String()}
				if platform {
					want.kind, want.key, want.org = "platform", calls[0].key, ""
				}
				if calls[0] != want {
					t.Errorf("%s: PDP request %+v, want %+v", verdict, calls[0], want)
				}
				switch verdict {
				case "allow":
					if !handlerReached(status, body) {
						t.Errorf("allow: %d %s, want the handler reached", status, body)
					}
				case "deny":
					wantBody := `{"detail":"Access denied"}`
					if b, ok := cellDenyBody[key]; ok {
						wantBody = b
					}
					if status != http.StatusForbidden || body != wantBody {
						t.Errorf("deny: %d %s, want 403 %s", status, body, wantBody)
					}
				case "error":
					if status != http.StatusServiceUnavailable || header.Get("Retry-After") != "5" {
						t.Errorf("error: %d %s Retry-After %q, want 503 with Retry-After 5", status, body, header.Get("Retry-After"))
					}
				}
			}
		})
	}
}

// cellQueryOrgRows are the class A rows whose handler serves the organization an org_id query value names; the other
// org rows name it in the {org_id} path segment.
var cellQueryOrgRows = []string{
	"GET /api/v1/billing/invoices", "GET /api/v1/billing/invoices/{invoice_id}", "POST /api/v1/billing/invoices/{invoice_id}/void",
	"GET /api/v1/billing/subscriptions/list", "GET /api/v1/billing/subscriptions", "GET /api/v1/billing/subscriptions/history",
	"POST /api/v1/billing/subscriptions/change-plan", "POST /api/v1/billing/subscriptions/cancel", "POST /api/v1/billing/subscriptions/reactivate",
}

// cellNamingOrg is the request of key that names org for its handler to serve: the org_id query value of a query
// row, the {org_id} path segment of a path row.
func cellNamingOrg(key string, org uuid.UUID, headers map[string]string) *http.Request {
	request := cellRequest(key, headers)
	if slices.Contains(cellQueryOrgRows, key) {
		query := request.URL.Query()
		query.Set("org_id", org.String())
		request.URL.RawQuery = query.Encode()
		return request
	}
	request.URL.Path = strings.Replace(request.URL.Path, probeOrg.String(), org.String(), 1)
	return request
}

// Class A organization rows with their namespace on: a request that names another organization than the token's
// (probeUser is a member of probeOtherOrg) is refused with a 403 and the level's body before any PDP call; naming
// the token's own organization is decided by the PDP once, on that organization.
func TestBillingClassAOrgRowsServeOnlyTheTokenOrganization(t *testing.T) {
	var orgRows []string
	for _, key := range classAKeys() {
		if billingClassA[key] != "admin.billing" {
			orgRows = append(orgRows, key)
		}
	}
	if len(orgRows) != 11 {
		t.Fatalf("class A organization rows %v, want the 9 query rows and the 2 {org_id} path rows", orgRows)
	}
	for _, key := range orgRows {
		if !slices.Contains(cellQueryOrgRows, key) && !strings.Contains(key, "{org_id}") {
			t.Fatalf("%s names its organization neither in org_id nor in {org_id}", key)
		}
		pdp := newBillingCellPDP(t, "allow")
		rig := newCellRig(t, cellAllNamespaces, pdp.client(t), false)
		recorder := httptest.NewRecorder()
		rig.handler.ServeHTTP(recorder, cellNamingOrg(key, probeOtherOrg, rig.userBearer()))
		wantBody := `{"detail":"Access denied"}`
		if b, ok := cellDenyBody[key]; ok {
			wantBody = b
		}
		if status, body := recorder.Code, strings.TrimSpace(recorder.Body.String()); status != http.StatusForbidden || body != wantBody {
			t.Errorf("%s naming another org: %d %s, want 403 %s", key, status, body, wantBody)
		}
		if n := len(pdp.calls()); n != 0 {
			t.Errorf("%s naming another org: %d PDP calls, want 0", key, n)
		}
		recorder = httptest.NewRecorder()
		rig.handler.ServeHTTP(recorder, cellNamingOrg(key, probeOrg, rig.userBearer()))
		if status, body := recorder.Code, strings.TrimSpace(recorder.Body.String()); !handlerReached(status, body) {
			t.Errorf("%s naming the token org: %d %s, want the handler reached", key, status, body)
		}
		if calls := pdp.calls(); len(calls) != 1 || calls[0].org != probeOrg.String() {
			t.Errorf("%s naming the token org: PDP saw %+v, want one call on the token org", key, calls)
		}
	}
}

// Class A with its namespace on and no decision point client: every request is refused with a 503 and the reason
// code, never served by the legacy check.
func TestBillingClassARowsRefuseWithNoDecisionPoint(t *testing.T) {
	rig := newCellRig(t, cellAllNamespaces, nil, true)
	for _, key := range classAKeys() {
		status, body, _ := rig.serve(key, rig.userBearer())
		if status != http.StatusServiceUnavailable || !strings.Contains(body, `"reason_code":"authz_enforce_no_decision_point"`) {
			t.Errorf("%s: %d %s, want 503 authz_enforce_no_decision_point", key, status, body)
		}
	}
}

// Class A with its namespace on: a retired HS256 caller is refused at the token contract (401), the same answer as
// with the switch off, and the PDP is never asked.
func TestBillingClassARowsRefuseAnHS256CallerBeforeThePDP(t *testing.T) {
	hs256 := bearer(probeHS256(t, probeKey, map[string]any{"sub": probeUser.String(), "org_id": probeOrg.String(), "role": "admin", "tv": 3}))
	off := newCellRig(t, "", nil, false)
	pdp := newBillingCellPDP(t, "deny")
	on := newCellRig(t, cellAllNamespaces, pdp.client(t), false)
	for _, key := range classAKeys() {
		offStatus, offBody, _ := off.serve(key, hs256)
		onStatus, onBody, _ := on.serve(key, hs256)
		if onStatus != offStatus || onBody != offBody {
			t.Errorf("%s: on %d %s, off %d %s; want the same answer", key, onStatus, onBody, offStatus, offBody)
		}
		if onStatus != http.StatusUnauthorized || onBody != `{"detail":{"message":"Invalid or expired token"}}` {
			t.Errorf("%s: %d %s, want the 401 token refusal", key, onStatus, onBody)
		}
	}
	if n := len(pdp.calls()); n != 0 {
		t.Errorf("%d PDP calls for an HS256 caller, want 0", n)
	}
}

// Classes B to F with every namespace on: no PDP call, and the answer is the switch-off answer. B and C keep their
// legacy checks until their rows name an action; D, E and F are never PDP rows.
func TestBillingRowsWithoutAnActionAreNeverDecidedByThePDP(t *testing.T) {
	off := newCellRig(t, "", nil, false)
	pdp := newBillingCellPDP(t, "deny")
	on := newCellRig(t, cellAllNamespaces, pdp.client(t), false)
	var keys []string
	for _, class := range [][]string{billingClassB, billingClassC, billingClassD, billingClassE, billingClassF} {
		for _, key := range class {
			if !strings.HasSuffix(key, " @edge") {
				keys = append(keys, key)
			}
		}
	}
	for _, key := range keys {
		offStatus, offBody, _ := off.serve(key, off.userBearer())
		onStatus, onBody, _ := on.serve(key, on.userBearer())
		if onStatus != offStatus || onBody != offBody {
			t.Errorf("%s: on %d %s, off %d %s; want the same answer", key, onStatus, onBody, offStatus, offBody)
		}
	}
	if n := len(pdp.calls()); n != 0 {
		t.Errorf("%d PDP calls on rows with no action, want 0", n)
	}
}
