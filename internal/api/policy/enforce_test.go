package policy

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/pep"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
	"github.com/full-chaos/dev-health-ops/internal/authzenforce"
)

type enforceRecorder struct {
	mu       sync.Mutex
	hits     []string
	unjudged []string
}

func recordEnforce(t *testing.T) *enforceRecorder {
	t.Helper()
	rec := &enforceRecorder{}
	previous := enforceDecisionAdd
	enforceDecisionAdd = func(_ context.Context, namespace, outcome string) {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		rec.hits = append(rec.hits, namespace+"/"+outcome)
	}
	previousUnjudged := enforceUnjudgedAdd
	enforceUnjudgedAdd = func(_ context.Context, route, reason string) {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		rec.unjudged = append(rec.unjudged, route+"/"+reason)
	}
	t.Cleanup(func() { enforceDecisionAdd, enforceUnjudgedAdd = previous, previousUnjudged })
	return rec
}

func (r *enforceRecorder) unjudgedSnapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.unjudged)
}

func (r *enforceRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.hits)
}

func onFor(namespaces ...string) authzenforce.Switch {
	list := ""
	for i, namespace := range namespaces {
		if i > 0 {
			list += ","
		}
		list += namespace
	}
	return authzenforce.Parse(authzenforce.GoAPI, list)
}

func newEnforcerFor(t *testing.T, decider pep.Decider, sw authzenforce.Switch) *Enforcer {
	t.Helper()
	enf, err := NewEnforcer(EnforceConfig{Decider: decider, Rows: shadowTestRows, Switch: sw, Timeout: time.Second, Logger: quiet()})
	if err != nil {
		t.Fatal(err)
	}
	return enf
}

func enforceHandler(t *testing.T, store *accessStore, claims *RoutedResult, enf *Enforcer) http.Handler {
	t.Helper()
	auth := routed(t, store, &fakeValidator{result: claims})
	guard := NewGuard(auth, quiet())
	guard.SetEnforcer(enf)
	mux := http.NewServeMux()
	mux.Handle("GET /api/v1/admin/teams", guard.Wrap(AdminOrg, echo))
	mux.Handle("POST /api/v1/admin/teams", guard.Wrap(AdminOrg, echo))
	mux.Handle("GET /api/v1/billing/invoices", guard.Wrap(Authenticated, echo))
	mux.Handle("GET /api/v1/admin/orgs", guard.WrapPlatform(ActionAdminOrgs, echo))
	mux.Handle("GET /api/v1/admin/other", guard.Wrap(Admin, echo))
	return mux
}

func enforceRequest(h http.Handler, method, path string) response {
	return serve(h, method, path, map[string]string{"Authorization": "Bearer " + shadowBearer})
}

type enforceCell struct{ namespace, method, path string }

// The refusal body is the one the route level gives, so a client sees no new shape.
var enforceDenyBodies = map[string]string{
	"teams":   `{"detail":"Admin access required"}`,
	"billing": `{"detail":"Access denied"}`,
	"admin":   `{"detail":"Superuser access required"}`,
}

var enforceCells = []enforceCell{
	{"teams", "GET", "/api/v1/admin/teams"},
	{"billing", "GET", "/api/v1/billing/invoices"},
	{"admin", "GET", "/api/v1/admin/orgs"},
}

func TestAnEnforcedCellTakesThePDPDecisionInsteadOfTheLegacyOne(t *testing.T) {
	for _, cell := range enforceCells {
		t.Run(cell.namespace, func(t *testing.T) {
			recordEnforce(t)
			deny := newTestPDP(t, denyWith("role_lacks_action"))
			got := enforceRequest(enforceHandler(t, personaStore(personaSuperuser), userClaims(nil), newEnforcerFor(t, deny.client(t), onFor(cell.namespace))), cell.method, cell.path)
			if got.status != http.StatusForbidden || got.body != enforceDenyBodies[cell.namespace] {
				t.Fatalf("PDP deny, legacy allow: %d %q, want 403 %q", got.status, got.body, enforceDenyBodies[cell.namespace])
			}
			if n := len(deny.calls()); n != 1 {
				t.Fatalf("%d PDP calls, want 1", n)
			}
			allow := newTestPDP(t, allowAll)
			got = enforceRequest(enforceHandler(t, personaStore(personaMember), userClaims(nil), newEnforcerFor(t, allow.client(t), onFor(cell.namespace))), cell.method, cell.path)
			if got.status != http.StatusOK {
				t.Fatalf("PDP allow, legacy deny: %d %q, want 200", got.status, got.body)
			}
		})
	}
}

func TestACellThatIsOffAsksNothingAndKeepsTheLegacyAnswer(t *testing.T) {
	for _, cell := range enforceCells {
		for _, persona := range []persona{personaAdmin, personaMember, personaSuperuser} {
			rec := recordEnforce(t)
			pdp := newTestPDP(t, denyWith("role_lacks_action"))
			base := enforceRequest(enforceHandler(t, personaStore(persona), userClaims(nil), nil), cell.method, cell.path)
			others := []string{}
			for _, other := range enforceCells {
				if other.namespace != cell.namespace {
					others = append(others, other.namespace)
				}
			}
			for name, sw := range map[string]authzenforce.Switch{"all off": {}, "other namespaces on": onFor(others...)} {
				got := enforceRequest(enforceHandler(t, personaStore(persona), userClaims(nil), newEnforcerFor(t, pdp.client(t), sw)), cell.method, cell.path)
				if got.status != base.status || got.body != base.body {
					t.Fatalf("%s/%s/%s: %d %q, want the legacy %d %q", cell.namespace, persona.role, name, got.status, got.body, base.status, base.body)
				}
			}
			if n := len(pdp.calls()); n != 0 && len(others) == 0 {
				t.Fatalf("%d PDP calls with the cell off", n)
			}
			for _, hit := range rec.snapshot() {
				if hit[:len(cell.namespace)] == cell.namespace {
					t.Fatalf("a counted decision for the cell that is off: %v", rec.snapshot())
				}
			}
		}
	}
}

// CHAOS-13596 F-5: after the fallback returns are gone, the operator rollback still holds. A namespace that was ON and is
// switched OFF serves the legacy answer, asks nothing and counts nothing, for every persona and for a non-UUID served
// organization too; with it ON the same request is decided (the decision point is asked).
func TestANamespaceSwitchedBackOffServesTheLegacyAnswer(t *testing.T) {
	for _, cell := range enforceCells {
		for _, persona := range []persona{personaAdmin, personaMember, personaSuperuser} {
			base := enforceRequest(enforceHandler(t, personaStore(persona), userClaims(nil), nil), cell.method, cell.path)
			on := newTestPDP(t, allowAll)
			enforceRequest(enforceHandler(t, personaStore(persona), userClaims(nil), newEnforcerFor(t, on.client(t), onFor(cell.namespace))), cell.method, cell.path)
			if len(on.calls()) == 0 {
				t.Fatalf("%s/%s: the cell ON asked nothing", cell.namespace, persona.role)
			}
			rec := recordEnforce(t)
			off := newTestPDP(t, allowAll)
			got := enforceRequest(enforceHandler(t, personaStore(persona), userClaims(nil), newEnforcerFor(t, off.client(t), authzenforce.Switch{})), cell.method, cell.path)
			if got.status != base.status || got.body != base.body {
				t.Fatalf("%s/%s switched off: %d %q, want the legacy %d %q", cell.namespace, persona.role, got.status, got.body, base.status, base.body)
			}
			if n := len(off.calls()); n != 0 || len(rec.snapshot()) != 0 || len(rec.unjudgedSnapshot()) != 0 {
				t.Fatalf("%s/%s switched off: %d PDP calls, outcomes %v, unjudged %v, want none", cell.namespace, persona.role, n,
					rec.snapshot(), rec.unjudgedSnapshot())
			}
		}
	}
}

func TestEnforcementDeniesOnAnyPDPFailure(t *testing.T) {
	deciders := map[string]funcDecider{
		"local deny": {func(context.Context, pep.Request) pep.Decision { return pep.Decision{} }},
		"panic":      {func(context.Context, pep.Request) pep.Decision { panic("pdp client bug") }},
	}
	for name, decider := range deciders {
		for _, persona := range []persona{personaAdmin, personaSuperuser} {
			t.Run(name+"/"+persona.role, func(t *testing.T) {
				rec := recordEnforce(t)
				got := enforceRequest(enforceHandler(t, personaStore(persona), userClaims(nil), newEnforcerFor(t, decider, onFor("teams"))), "GET", "/api/v1/admin/teams")
				if got.status != http.StatusServiceUnavailable || got.header.Get("Retry-After") == "" {
					t.Fatalf("%d %q retry-after %q, want 503 with a retry hint", got.status, got.body, got.header.Get("Retry-After"))
				}
				if !slices.Equal(rec.snapshot(), []string{"teams/error_deny"}) {
					t.Fatalf("counted %v, want [teams/error_deny]", rec.snapshot())
				}
			})
		}
	}
}

const enforceNoDecisionPointBody = `{"detail":{"message":"Authorization service temporarily unavailable","reason_code":"authz_enforce_no_decision_point"}}`

func newUnconfiguredEnforcerFor(t *testing.T, sw authzenforce.Switch) *Enforcer {
	t.Helper()
	enf, err := NewEnforcer(EnforceConfig{Unconfigured: true, Rows: shadowTestRows, Switch: sw, Logger: quiet()})
	if err != nil {
		t.Fatal(err)
	}
	return enf
}

// No decision point client: every request of a switched-on namespace is refused fail-closed, with a reason code.
func TestAnEnforcerWithNoDecisionPointRefusesEveryRequestOfItsNamespacesWithA503(t *testing.T) {
	for _, cell := range enforceCells {
		t.Run(cell.namespace, func(t *testing.T) {
			rec := recordEnforce(t)
			got := enforceRequest(enforceHandler(t, personaStore(personaSuperuser), userClaims(nil), newUnconfiguredEnforcerFor(t, onFor(cell.namespace))), cell.method, cell.path)
			if got.status != http.StatusServiceUnavailable || got.body != enforceNoDecisionPointBody {
				t.Fatalf("no decision point, legacy allow: %d %q, want 503 %q", got.status, got.body, enforceNoDecisionPointBody)
			}
			if !slices.Equal(rec.snapshot(), []string{cell.namespace + "/error_deny"}) {
				t.Fatalf("counted %v, want [%s/error_deny]", rec.snapshot(), cell.namespace)
			}
		})
	}
	got := serve(fixtureBearerHandler(t, newUnconfiguredEnforcerFor(t, onFor("teams"))), http.MethodGet, "/api/v1/admin/teams", validAuth(t, func(c jwt.MapClaims) { c["role"] = "admin" }))
	if got.status != http.StatusServiceUnavailable || got.body != enforceNoDecisionPointBody {
		t.Fatalf("no decision point, signed fixture caller: %d %q, want the 503: fail-closed refuses every request", got.status, got.body)
	}
}

func TestAnEnforcerWithNoDecisionPointKeepsTheLegacyAnswerOffItsNamespaces(t *testing.T) {
	rec := recordEnforce(t)
	base := enforceRequest(enforceHandler(t, personaStore(personaSuperuser), userClaims(nil), nil), "GET", "/api/v1/billing/invoices")
	got := enforceRequest(enforceHandler(t, personaStore(personaSuperuser), userClaims(nil), newUnconfiguredEnforcerFor(t, onFor("teams"))), "GET", "/api/v1/billing/invoices")
	if got.status != base.status || got.body != base.body || len(rec.snapshot()) != 0 {
		t.Fatalf("billing off, teams on with no decision point: %d %q counted %v, want the legacy %d %q and nothing counted", got.status, got.body, rec.snapshot(), base.status, base.body)
	}
}

func TestADeadPDPIsA503NotALegacyAllow(t *testing.T) {
	pdp := newTestPDP(t, allowAll)
	client := pdp.client(t)
	pdp.srv.Close()
	got := enforceRequest(enforceHandler(t, personaStore(personaSuperuser), userClaims(nil), newEnforcerFor(t, client, onFor("teams"))), "GET", "/api/v1/admin/teams")
	if got.status != http.StatusServiceUnavailable {
		t.Fatalf("dead PDP: %d %q, want 503", got.status, got.body)
	}
}

func fixtureBearerHandler(t *testing.T, enf *Enforcer) http.Handler {
	t.Helper()
	store := newStore()
	withClaimRole(store, claims(func(c jwt.MapClaims) { c["role"] = "admin" }))
	guard := NewGuard(authenticator(t, store), quiet())
	guard.SetEnforcer(enf)
	mux := http.NewServeMux()
	mux.Handle("GET /api/v1/admin/teams", guard.Wrap(AdminOrg, echo))
	mux.Handle("POST /api/v1/admin/teams", guard.Wrap(AdminOrg, echo))
	return mux
}

// impersonating serves next with a live go-api impersonation row for the caller: the admin's own bearer, acting in
// targetOrgID. The decision point has no delegated session for that bearer to judge.
func impersonating(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		session := &Impersonation{ID: uuid.New(), AdminUserID: userID, TargetUserID: targetUserID, TargetOrgID: targetOrgID, TargetRole: "admin",
			ExpiresAt: time.Now().Add(time.Hour)}
		next.ServeHTTP(w, r.WithContext(WithImpersonation(r.Context(), session)))
	})
}

const impersonationRefusalBody = `{"detail":"Not allowed in an impersonation session"}`

// An enforced write row, or a platform wrap, refuses a caller with a live go-api impersonation row before any decision
// point call: the bearer is the actor's own, so the decision point would judge the actor in the actor's organization,
// not the delegation. Its reads of other rows keep the legacy answer (impersonation_read_parity_test.go).
func TestAnImpersonatingAdminOnAnEnforcedRowIsRefusedBeforeThePDP(t *testing.T) {
	for _, cell := range []struct {
		namespace, method, path string
		who                     persona
	}{
		{"teams", http.MethodPost, "/api/v1/admin/teams", personaAdmin},
		{"admin", http.MethodGet, "/api/v1/admin/orgs", personaSuperuser},
	} {
		t.Run(cell.namespace, func(t *testing.T) {
			rec := recordEnforce(t)
			pdp := newTestPDP(t, allowAll)
			h := impersonating(enforceHandler(t, personaStore(cell.who), userClaims(nil), newEnforcerFor(t, pdp.client(t), onFor(cell.namespace))))
			got := enforceRequest(h, cell.method, cell.path)
			if got.status != http.StatusForbidden || got.body != impersonationRefusalBody {
				t.Errorf("impersonating caller on an enforced row: %d %q, want 403 %q", got.status, got.body, impersonationRefusalBody)
			}
			if n := len(pdp.calls()); n != 0 {
				t.Errorf("%d PDP calls, want 0", n)
			}
			if d, u := rec.snapshot(), rec.unjudgedSnapshot(); !slices.Equal(d, []string{cell.namespace + "/impersonation_refused"}) || len(u) != 0 {
				t.Errorf("decisions %v unjudged %v, want [%s/impersonation_refused] and none", d, u, cell.namespace)
			}
		})
	}
}

// A signed fixture bearer with a live go-api impersonation row is refused on a write too: the row, not the token class,
// makes the caller one the decision point cannot judge as the delegation.
func TestAnImpersonatingFixtureCallerOnAnEnforcedRowIsRefused(t *testing.T) {
	rec := recordEnforce(t)
	pdp := newTestPDP(t, allowAll)
	got := serve(impersonating(fixtureBearerHandler(t, newEnforcerFor(t, pdp.client(t), onFor("teams")))), http.MethodPost, "/api/v1/admin/teams",
		validAuth(t, func(c jwt.MapClaims) { c["role"] = "admin" }))
	if got.status != http.StatusForbidden || got.body != impersonationRefusalBody || len(pdp.calls()) != 0 {
		t.Fatalf("impersonating fixture caller: %d %q, %d PDP calls, want 403 %q and 0", got.status, got.body, len(pdp.calls()), impersonationRefusalBody)
	}
	if d, u := rec.snapshot(), rec.unjudgedSnapshot(); !slices.Equal(d, []string{"teams/impersonation_refused"}) || len(u) != 0 {
		t.Fatalf("decisions %v unjudged %v, want [teams/impersonation_refused] and none", d, u)
	}
}

const stopImpersonationRoute = "/api/v1/admin/impersonate/stop"

// stopRouteRow is the stop route's row as the catalog gives it.
func stopRouteRow(t *testing.T) ShadowRow {
	t.Helper()
	action, err := policycatalog.Default().Action(ActionAdminImpersonate)
	if err != nil {
		t.Fatal(err)
	}
	return shadowRowOf(action)
}

func stopRouteHandler(t *testing.T, decider pep.Decider) http.Handler {
	t.Helper()
	rows := maps.Clone(shadowTestRows)
	rows[stopImpersonationPattern] = stopRouteRow(t)
	enf, err := NewEnforcer(EnforceConfig{Decider: decider, Rows: rows, Switch: onFor("admin"), Timeout: time.Second, Logger: quiet()})
	if err != nil {
		t.Fatal(err)
	}
	guard := NewGuard(routed(t, personaStore(personaSuperuser), &fakeValidator{result: userClaims(nil)}), quiet())
	guard.SetEnforcer(enf)
	mux := http.NewServeMux()
	mux.Handle(stopImpersonationPattern, guard.WrapPlatform(ActionAdminImpersonate, echo))
	return impersonating(mux)
}

// The stop route is the one way out of an impersonation, as in the legacy guard: the admin acts on their own session,
// so the decision point judges the admin's own bearer there. Its answer stands: a deny is the route level's 403 and no
// verdict is a 503, never the legacy allow the admin would get.
func TestTheStopRouteOfAnImpersonatingAdminIsDecidedByThePDP(t *testing.T) {
	for _, cell := range []struct {
		name        string
		verdict     func(string) (bool, string)
		obligations map[string]any
		dead        bool
		status      int
		body        string
		counted     string
	}{
		{"allow", allowAll, nil, false, http.StatusOK, "", "admin/allow"},
		{"deny", denyWith("platform_role_absent"), nil, false, http.StatusForbidden, enforceDenyBodies["admin"], "admin/deny"},
		{"allow with an obligation go-api does not carry out", allowAll, map[string]any{"step_up": true}, false, http.StatusServiceUnavailable, "", "admin/error_deny"},
		{"no decision point answer", allowAll, nil, true, http.StatusServiceUnavailable, "", "admin/error_deny"},
	} {
		t.Run(cell.name, func(t *testing.T) {
			rec := recordEnforce(t)
			pdp := newTestPDP(t, cell.verdict)
			pdp.obligations = cell.obligations
			client := pdp.client(t)
			if cell.dead {
				pdp.srv.Close()
			}
			got := enforceRequest(stopRouteHandler(t, client), http.MethodPost, stopImpersonationRoute)
			if got.status != cell.status || (cell.body != "" && got.body != cell.body) {
				t.Errorf("stop route: %d %q, want %d %q", got.status, got.body, cell.status, cell.body)
			}
			if !cell.dead {
				if calls := pdp.calls(); len(calls) != 1 || calls[0].Action != ActionAdminImpersonate || calls[0].Principal != shadowBearer {
					t.Errorf("PDP saw %+v, want one admin.impersonate question with the admin's bearer", calls)
				}
			}
			if d := rec.snapshot(); !slices.Equal(d, []string{cell.counted}) {
				t.Errorf("decisions %v, want [%s]", d, cell.counted)
			}
		})
	}
}

// stopPatternVariants are route templates that are not the stop route's: a prefix, a suffix, a trailing slash,
// percent-encoded and dot segments, another case, a wildcard or subtree that would also match its path, and other
// methods. Only the exact template is exempt.
var stopPatternVariants = []string{
	"POST /api/v1/admin/impersonate/stop/",
	"POST /api/v1/admin/impersonate/stopx",
	"POST /api/v1/admin/impersonate/sto",
	"POST /api/v1/admin/impersonate/stop%2F",
	"POST /api/v1/admin/impersonate%2Fstop",
	"POST /api/v1/admin/impersonate/%73top",
	"POST /api/v1/admin/impersonate/./stop",
	"POST /api/v1/admin/impersonate/x/../stop",
	"POST /api/v1/admin//impersonate/stop",
	"POST /API/v1/admin/impersonate/stop",
	"post /api/v1/admin/impersonate/stop",
	"POST  /api/v1/admin/impersonate/stop",
	"POST /api/v1/admin/impersonate/stop ",
	" POST /api/v1/admin/impersonate/stop",
	"/api/v1/admin/impersonate/stop",
	"POST /api/v1/admin/impersonate/{action}",
	"POST /api/v1/admin/impersonate/",
	"POST /api/v1/admin/impersonate",
	"GET /api/v1/admin/impersonate/stop",
	"PUT /api/v1/admin/impersonate/stop",
	"PATCH /api/v1/admin/impersonate/stop",
	"DELETE /api/v1/admin/impersonate/stop",
}

// An enforced row whose template is not exactly the stop route's refuses an impersonating caller unasked.
func TestTheStopExemptionIsTheExactTemplateOnly(t *testing.T) {
	rows := maps.Clone(shadowTestRows)
	rows[stopImpersonationPattern] = stopRouteRow(t)
	for _, v := range stopPatternVariants {
		rows[v] = stopRouteRow(t)
	}
	for _, pattern := range append([]string{stopImpersonationPattern}, stopPatternVariants...) {
		t.Run(pattern, func(t *testing.T) {
			rec := recordEnforce(t)
			pdp := newTestPDP(t, allowAll)
			enf, err := NewEnforcer(EnforceConfig{Decider: pdp.client(t), Rows: rows, Switch: onFor("admin"), Timeout: time.Second, Logger: quiet()})
			if err != nil {
				t.Fatal(err)
			}
			got := enf.decide(context.Background(), enforceObservation{pattern: pattern, bearer: shadowBearer, userAccessToken: true, impersonating: true})
			want, wantCalls, wantCounted := enforceImpersonationRefused, 0, "admin/impersonation_refused"
			if pattern == stopImpersonationPattern {
				want, wantCalls, wantCounted = enforceAllow, 1, "admin/allow"
			}
			if got != want || len(pdp.calls()) != wantCalls || !slices.Equal(rec.snapshot(), []string{wantCounted}) {
				t.Errorf("template %q: verdict %d, %d PDP calls, counted %v; want %d, %d, [%s]", pattern, got, len(pdp.calls()), rec.snapshot(), want, wantCalls, wantCounted)
			}
		})
	}
}

// Through the mux: a request whose path or method is not the stop route's either reaches another enforced row and is
// refused unasked, or reaches no handler at all. Only the exact stop request is served.
func TestTheStopExemptionThroughTheMux(t *testing.T) {
	const subtree = "POST /api/v1/admin/impersonate/"
	rows := maps.Clone(shadowTestRows)
	rows[stopImpersonationPattern] = stopRouteRow(t)
	rows[subtree] = stopRouteRow(t)
	for _, cell := range []struct {
		method, path string
		refused      bool
	}{
		{http.MethodPost, "/api/v1/admin/impersonate/stop/", true},
		{http.MethodPost, "/api/v1/admin/impersonate/stopx", true},
		{http.MethodPost, "/api/v1/admin/impersonate/stop%2F", true},
		{http.MethodPost, "/api/v1/admin/impersonate/stop/x", true},
		{http.MethodPost, "/api/v1/admin/impersonate/x/../stop", false},
		{http.MethodPost, "/api/v1/admin/impersonate/./stop", false},
		{http.MethodPost, "/api/v1/admin//impersonate/stop", false},
		{http.MethodGet, "/api/v1/admin/impersonate/stop", false},
		{http.MethodPut, "/api/v1/admin/impersonate/stop", false},
		{http.MethodDelete, "/api/v1/admin/impersonate/stop", false},
	} {
		t.Run(cell.method+" "+cell.path, func(t *testing.T) {
			rec := recordEnforce(t)
			pdp := newTestPDP(t, allowAll)
			enf, err := NewEnforcer(EnforceConfig{Decider: pdp.client(t), Rows: rows, Switch: onFor("admin"), Timeout: time.Second, Logger: quiet()})
			if err != nil {
				t.Fatal(err)
			}
			guard := NewGuard(routed(t, personaStore(personaSuperuser), &fakeValidator{result: userClaims(nil)}), quiet())
			guard.SetEnforcer(enf)
			mux := http.NewServeMux()
			mux.Handle(stopImpersonationPattern, guard.WrapPlatform(ActionAdminImpersonate, echo))
			mux.Handle(subtree, guard.WrapPlatform(ActionAdminImpersonate, echo))
			got := enforceRequest(impersonating(mux), cell.method, cell.path)
			t.Logf("%s %s -> %d", cell.method, cell.path, got.status)
			if n := len(pdp.calls()); n != 0 || strings.Contains(got.body, "user=") {
				t.Errorf("%d PDP calls, served %q; want 0 and not served", n, got.body)
			}
			if cell.refused {
				if got.status != http.StatusForbidden || got.body != impersonationRefusalBody || !slices.Equal(rec.snapshot(), []string{"admin/impersonation_refused"}) {
					t.Errorf("%d %q counted %v, want 403 %q and [admin/impersonation_refused]", got.status, got.body, rec.snapshot(), impersonationRefusalBody)
				}
			} else if got.status == http.StatusOK || len(rec.snapshot()) != 0 {
				t.Errorf("%d %q counted %v, want no handler and nothing counted", got.status, got.body, rec.snapshot())
			}
		})
	}
	rec := recordEnforce(t)
	pdp := newTestPDP(t, allowAll)
	got := enforceRequest(stopRouteHandler(t, pdp.client(t)), http.MethodPost, stopImpersonationRoute)
	if got.status != http.StatusOK || len(pdp.calls()) != 1 || !slices.Equal(rec.snapshot(), []string{"admin/allow"}) {
		t.Errorf("exact stop request: %d, %d PDP calls, counted %v; want 200, 1, [admin/allow]", got.status, len(pdp.calls()), rec.snapshot())
	}
}

// profilePath is a request path that the mux routes to pattern's route: each wildcard becomes one segment.
func profilePath(route string) string {
	route = strings.ReplaceAll(route, "{$}", "")
	var b strings.Builder
	for {
		open := strings.IndexByte(route, '{')
		if open < 0 {
			b.WriteString(route)
			return b.String()
		}
		end := strings.IndexByte(route[open:], '}')
		b.WriteString(route[:open] + "x")
		route = route[open+end+1:]
	}
}

// With enforcement and the shadow both on, as go-api installs them on one guard, every non-stop go-api profile row
// that is not a read refuses an impersonating caller with the 403 and counts it once in each: the enforcement outcome
// and the shadow result impersonation_refused, with no decision point call.
func TestAnEnforcedImpersonationRefusalIsAlsoAShadowResultOnEveryProfileRow(t *testing.T) {
	rows, err := ParseShadowRows(authv1.GoAPIEndpointProfiles, "dev-health-ops-api")
	if err != nil {
		t.Fatal(err)
	}
	cells := 0
	for pattern, row := range rows {
		if method, _, _ := strings.Cut(pattern, " "); liveRowExempt(pattern) || readMethod(method) {
			continue
		}
		cells++
		t.Run(pattern, func(t *testing.T) {
			erec, srec := recordEnforce(t), recordShadow(t)
			pdp := newTestPDP(t, allowAll)
			enf, err := NewEnforcer(EnforceConfig{Decider: pdp.client(t), Rows: rows, Switch: onFor(row.Namespace), Timeout: time.Second, Logger: quiet()})
			if err != nil {
				t.Fatal(err)
			}
			sh, _ := newShadowFor(t, pdp.client(t), func(c *ShadowConfig) { c.Rows = rows })
			guard := NewGuard(routed(t, personaStore(personaSuperuser), &fakeValidator{result: userClaims(nil)}), quiet())
			guard.SetEnforcer(enf)
			guard.SetShadow(sh)
			mux := http.NewServeMux()
			mux.Handle(pattern, guard.Wrap(Authenticated, echo))
			method, route, _ := strings.Cut(pattern, " ")
			got := enforceRequest(impersonating(mux), method, profilePath(route))
			sh.wg.Wait()
			if got.status != http.StatusForbidden || got.body != impersonationRefusalBody {
				t.Errorf("%d %q, want 403 %q", got.status, got.body, impersonationRefusalBody)
			}
			_, results := srec.snapshot()
			if !slices.Equal(erec.snapshot(), []string{row.Namespace + "/impersonation_refused"}) || !slices.Equal(results, []string{"impersonation_refused"}) {
				t.Errorf("enforcement %v shadow %v, want [%s/impersonation_refused] and [impersonation_refused]", erec.snapshot(), results, row.Namespace)
			}
			if n := len(pdp.calls()); n != 0 {
				t.Errorf("%d PDP calls, want 0", n)
			}
		})
	}
	if cells < 30 {
		t.Fatalf("%d non-stop write profile rows, want the go-api profile's (30 or more)", cells)
	}
	t.Logf("non-stop write profile rows: %d", cells)
}

var delegatedActorWire = "prn_" + wireID(uuid.MustParse("77777777-7777-4777-8777-777777777777"))

// delegatedBearerHandler serves the teams rows to a delegated session's bearer (an act claim) whose row admits it.
func delegatedBearerHandler(t *testing.T, enf *Enforcer) http.Handler {
	t.Helper()
	auth := routed(t, personaStore(personaAdmin), &fakeValidator{result: userClaims(func(c *AccessTokenClaims) { c.Actor = &delegatedActorWire })})
	auth.DelegatedRows(func(string) bool { return true })
	guard := NewGuard(auth, quiet())
	guard.SetEnforcer(enf)
	mux := http.NewServeMux()
	mux.Handle("GET /api/v1/admin/teams", guard.Wrap(AdminOrg, echo))
	return mux
}

// A delegated session's bearer is decided by the decision point: the bearer is the delegated session's own, so the
// decision point reads its delegation. Its allow carries the delegated obligations; go-api does not carry out
// minimum_assurance, so that allow is no verdict (503).
func TestADelegatedBearerOnAnEnforcedRowIsDecidedByThePDP(t *testing.T) {
	for _, cell := range []struct {
		name        string
		verdict     func(string) (bool, string)
		obligations map[string]any
		status      int
		counted     string
	}{
		{"allow", allowAll, nil, http.StatusOK, "teams/allow"},
		{"allow with the delegated obligations", allowAll, map[string]any{"tenant_scope": ownOrg, "minimum_assurance": "aal2"}, http.StatusServiceUnavailable, "teams/error_deny"},
		{"deny", denyWith("impersonation_denied"), nil, http.StatusForbidden, "teams/deny"},
		{"default deny", denyWith("role_lacks_action"), nil, http.StatusForbidden, "teams/deny"},
	} {
		t.Run(cell.name, func(t *testing.T) {
			rec := recordEnforce(t)
			pdp := newTestPDP(t, cell.verdict)
			pdp.obligations = cell.obligations
			got := enforceRequest(delegatedBearerHandler(t, newEnforcerFor(t, pdp.client(t), onFor("teams"))), http.MethodGet, "/api/v1/admin/teams")
			if got.status != cell.status {
				t.Errorf("delegated bearer: %d %q, want %d", got.status, got.body, cell.status)
			}
			if cell.status == http.StatusForbidden && got.body != enforceDenyBodies["teams"] {
				t.Errorf("delegated bearer deny body %q, want %q", got.body, enforceDenyBodies["teams"])
			}
			if calls := pdp.calls(); len(calls) != 1 || calls[0].Principal != shadowBearer || calls[0].Org != ownOrg || calls[0].Action != "teams.read" {
				t.Errorf("PDP saw %+v, want one teams.read question with the delegated bearer on its organization", calls)
			}
			if d, u := rec.snapshot(), rec.unjudgedSnapshot(); !slices.Equal(d, []string{cell.counted}) || len(u) != 0 {
				t.Errorf("decisions %v unjudged %v, want [%s] and none", d, u, cell.counted)
			}
		})
	}
}

// collectSum reads every data point of the int64 sum name from reader: the description and each value of label.
func collectSum(t *testing.T, reader *sdkmetric.ManualReader, name, label string) (description string, values []string) {
	t.Helper()
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			description = m.Description
			sum, _ := m.Data.(metricdata.Sum[int64])
			for _, point := range sum.DataPoints {
				value, _ := point.Attributes.Value(attribute.Key(label))
				values = append(values, value.AsString())
			}
		}
	}
	sort.Strings(values)
	return description, values
}

// A delegated session's bearer with an organization gives the unjudged counter no reason: it is judged. Its closed
// reason set is org_required, delegated_orgless (a delegated bearer with NO organization, CHAOS-10872 S1) and
// impersonation (a live-row read; the live-row platform wrap is refused, not unjudged). hs256, no_org and served_org left
// it with the fallback returns (CHAOS-13596).
func TestTheUnjudgedCounterHasNoDelegatedReason(t *testing.T) {
	recordEnforce(t)
	reader := sdkmetric.NewManualReader()
	previous := enforceUnjudgedAdd
	enforceUnjudgedAdd = newEnforceUnjudgedAdder(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("enforce-test"))
	t.Cleanup(func() { enforceUnjudgedAdd = previous })
	pdp := newTestPDP(t, allowAll)
	enforceRequest(impersonating(enforceHandler(t, personaStore(personaAdmin), userClaims(nil), newEnforcerFor(t, pdp.client(t), onFor("teams")))), http.MethodGet, "/api/v1/admin/teams")
	enforceRequest(impersonating(enforceHandler(t, personaStore(personaSuperuser), userClaims(nil), newEnforcerFor(t, pdp.client(t), onFor("admin")))), http.MethodGet, "/api/v1/admin/orgs")
	enforceRequest(delegatedBearerHandler(t, newEnforcerFor(t, pdp.client(t), onFor("teams"))), http.MethodGet, "/api/v1/admin/teams")
	noOrg := userClaims(func(c *AccessTokenClaims) { c.OrgID = nil })
	enforceRequest(enforceHandler(t, personaStore(personaAdmin), noOrg, newEnforcerFor(t, pdp.client(t), onFor("teams"))), http.MethodGet, "/api/v1/admin/teams")
	description, reasons := collectSum(t, reader, enforceUnjudgedMetric, "reason")
	if !slices.Equal(reasons, []string{"impersonation", "org_required"}) {
		t.Errorf("unjudged reasons %v, want [impersonation org_required] (no delegated point)", reasons)
	}
	if want := "by route row and reason: org_required, delegated_orgless, impersonation"; !strings.HasSuffix(description, want) {
		t.Errorf("unjudged description %q, want it to end %q", description, want)
	}
}

// The decision counter's outcome set is closed: allow, deny, error_deny and impersonation_refused, each reached, and
// the description names exactly those. A request that serves another organization counts no outcome (it is unjudged).
func TestTheEnforcementOutcomeSetIsClosed(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	previous := enforceDecisionAdd
	enforceDecisionAdd = newEnforceDecisionAdder(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("enforce-test"))
	t.Cleanup(func() { enforceDecisionAdd = previous })
	allow, deny, dead := newTestPDP(t, allowAll), newTestPDP(t, denyWith("role_lacks_action")), newTestPDP(t, allowAll)
	deadClient := dead.client(t)
	dead.srv.Close()
	for _, h := range []http.Handler{
		enforceHandler(t, personaStore(personaAdmin), userClaims(nil), newEnforcerFor(t, allow.client(t), onFor("teams"))),
		enforceHandler(t, personaStore(personaAdmin), userClaims(nil), newEnforcerFor(t, deny.client(t), onFor("teams"))),
		enforceHandler(t, personaStore(personaAdmin), userClaims(nil), newEnforcerFor(t, deadClient, onFor("teams"))),
	} {
		enforceRequest(h, http.MethodGet, "/api/v1/admin/teams")
	}
	enforceRequest(impersonating(enforceHandler(t, personaStore(personaAdmin), userClaims(nil), newEnforcerFor(t, allow.client(t), onFor("teams")))),
		http.MethodPost, "/api/v1/admin/teams")
	members := servedOrgCensusRow{"GET /api/v1/admin/orgs/{org_id}/members", Admin, false}
	servedOrgServe(t, members, servedOrgSetup{who: personaAdmin, enforcer: servedOrgEnforcer(t, allow, "members")}, strangerOrg)
	description, outcomes := collectSum(t, reader, enforceDecisionMetric, "outcome")
	want := []string{"allow", "deny", "error_deny", "impersonation_refused"}
	if outcomes = slices.Compact(outcomes); !slices.Equal(outcomes, want) {
		t.Errorf("outcomes %v, want %v", outcomes, want)
	}
	if suffix := "outcome: allow, deny, error_deny, impersonation_refused"; !strings.HasSuffix(description, suffix) {
		t.Errorf("decision description %q, want it to end %q", description, suffix)
	}
}

// An AdminOrg route refuses a token with no organization whoever it is: the decision point is not asked, the answer is
// the legacy one, counted unjudged org_required (CHAOS-10872 S1).
func TestAnOrganizationRouteWithNoOrganizationKeepsTheLegacyAnswerAndIsCountedUnjudged(t *testing.T) {
	rec := recordEnforce(t)
	pdp := newTestPDP(t, denyWith("role_lacks_action"))
	noOrg := userClaims(func(c *AccessTokenClaims) { c.OrgID = nil })
	base := enforceRequest(enforceHandler(t, personaStore(personaAdmin), noOrg, nil), "GET", "/api/v1/admin/teams")
	got := enforceRequest(enforceHandler(t, personaStore(personaAdmin), noOrg, newEnforcerFor(t, pdp.client(t), onFor("teams"))), "GET", "/api/v1/admin/teams")
	if got.status != base.status || got.body != base.body {
		t.Fatalf("no organization: %d %q, want the legacy %d %q", got.status, got.body, base.status, base.body)
	}
	if n := len(pdp.calls()); n != 0 || !slices.Equal(rec.unjudgedSnapshot(), []string{"GET /api/v1/admin/teams/org_required"}) {
		t.Fatalf("%d PDP calls, unjudged %v, want 0 and [GET /api/v1/admin/teams/org_required]", n, rec.unjudgedSnapshot())
	}
}

// A platform route needs no organization, so a caller with none is judged.
func TestAPlatformRouteWithNoOrganizationIsJudged(t *testing.T) {
	rec := recordEnforce(t)
	pdp := newTestPDP(t, allowAll)
	noOrg := userClaims(func(c *AccessTokenClaims) { c.OrgID = nil })
	got := enforceRequest(enforceHandler(t, personaStore(personaSuperuser), noOrg, newEnforcerFor(t, pdp.client(t), onFor("admin"))), "GET", "/api/v1/admin/orgs")
	if got.status != http.StatusOK || len(rec.unjudgedSnapshot()) != 0 || !slices.Equal(rec.snapshot(), []string{"admin/allow"}) {
		t.Fatalf("%d %q unjudged %v decisions %v, want 200, none and [admin/allow]", got.status, got.body, rec.unjudgedSnapshot(), rec.snapshot())
	}
	calls := pdp.calls()
	if len(calls) != 1 || calls[0].Type != "platform" || calls[0].HasOrg {
		t.Fatalf("PDP saw %+v, want one platform question with no organization", calls)
	}
}

func TestAnUnjudgedRequestLogsOneDebugLineThatQuotesNoToken(t *testing.T) {
	recordEnforce(t)
	var logs bytes.Buffer
	enf, err := NewEnforcer(EnforceConfig{Decider: newTestPDP(t, allowAll).client(t), Rows: shadowTestRows, Switch: onFor("teams"),
		Logger: slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))})
	if err != nil {
		t.Fatal(err)
	}
	noOrg := userClaims(func(c *AccessTokenClaims) { c.OrgID = nil })
	enforceRequest(enforceHandler(t, personaStore(personaAdmin), noOrg, enf), http.MethodGet, "/api/v1/admin/teams")
	line := logs.String()
	if strings.Count(line, "level=DEBUG") != 1 || !strings.Contains(line, "reason=org_required") || !strings.Contains(line, "route=\"GET /api/v1/admin/teams\"") {
		t.Fatalf("log %q: want one DEBUG line with the route and reason org_required", line)
	}
	if strings.Contains(line, shadowBearer) {
		t.Fatalf("the log quotes the token: %q", line)
	}
}

// Both counters carry closed label sets: the decision counter has a namespace, the service and an outcome; the
// unjudged counter a route row and one of its reasons.
func TestEnforcementCounterLabelSetsAreClosed(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("enforce-test")
	newEnforceDecisionAdder(meter)(context.Background(), "teams", "allow")
	newEnforceUnjudgedAdder(meter)(context.Background(), "GET /api/v1/admin/teams", "impersonation")
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	keys := map[string][]string{}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, point := range sum.DataPoints {
				var names []string
				for _, kv := range point.Attributes.ToSlice() {
					names = append(names, string(kv.Key))
				}
				sort.Strings(names)
				keys[m.Name] = names
			}
		}
	}
	if got := keys["dev_health_authz_enforcement_decisions_total"]; !slices.Equal(got, []string{"namespace", "outcome", "service"}) {
		t.Fatalf("decision counter labels %v", got)
	}
	if got := keys["dev_health_authz_enforcement_unjudged_total"]; !slices.Equal(got, []string{"reason", "route"}) {
		t.Fatalf("unjudged counter labels %v", got)
	}
	if !slices.Equal(enforceUnjudgedReasons, []string{"org_required", "delegated_orgless", "impersonation"}) {
		t.Fatalf("reasons %v, want org_required, delegated_orgless and impersonation", enforceUnjudgedReasons)
	}
}

// The five obligations an allow can carry, each in a form go-api cannot carry out on a route that has a repository
// dimension: a tenant scope other than the request's organization, any repository allowlist, and every redaction,
// minimum assurance and step-up.
var enforceObligationKinds = map[string]map[string]any{
	"tenant_scope":         {"tenant_scope": strangerOrg},
	"repository_allowlist": {"repository_allowlist": []any{"acme/widget"}},
	"repository_scope":     {"repository_scope": "organization"},
	"redaction":            {"redaction": []any{"personal_identifiers"}},
	"minimum_assurance":    {"minimum_assurance": "aal2"},
	"step_up":              {"step_up": true},
}

const enforceUnavailableBody = `{"detail":"Authorization service temporarily unavailable"}`

// An allow that carries an obligation go-api cannot carry out is no verdict: 503 with Retry-After, counted
// error_deny. It is neither served nor refused with the route-level 403.
func TestAnAllowWithAnObligationGoAPICannotCarryOutIsNoVerdictThroughTheGuard(t *testing.T) {
	for _, kind := range slices.Sorted(maps.Keys(enforceObligationKinds)) {
		for _, cell := range enforceCells {
			t.Run(kind+"/"+cell.namespace, func(t *testing.T) {
				rec := recordEnforce(t)
				pdp := newTestPDP(t, allowAll)
				pdp.obligations = enforceObligationKinds[kind]
				got := enforceRequest(enforceHandler(t, personaStore(personaSuperuser), userClaims(nil), newEnforcerFor(t, pdp.client(t), onFor(cell.namespace))), cell.method, cell.path)
				if got.status != http.StatusServiceUnavailable || got.header.Get("Retry-After") != enforceRetryAfter || got.body != enforceUnavailableBody {
					t.Fatalf("allow with %s: %d %q retry-after %q, want 503 %q with Retry-After %s", kind, got.status, got.body, got.header.Get("Retry-After"), enforceUnavailableBody, enforceRetryAfter)
				}
				if !slices.Equal(rec.snapshot(), []string{cell.namespace + "/error_deny"}) {
					t.Fatalf("allow with %s: counted %v, want [%s/error_deny]", kind, rec.snapshot(), cell.namespace)
				}
			})
		}
	}
}

// answerOf is the guard's answer to a verdict, as guard.wrap gives it; "legacy" is the legacy check's turn.
func answerOf(verdict enforceVerdict) (int, string, http.Header) {
	switch verdict {
	case enforceDeny, enforceUnavailable, enforceNoDecisionPoint:
		w := httptest.NewRecorder()
		writeEnforceRefusal(w, nil, Admin, verdict, DenyText{})
		return w.Code, w.Body.String(), w.Header()
	case enforceAllow:
		return http.StatusOK, "", http.Header{}
	}
	return 0, "legacy", http.Header{}
}

// Every mapped go-api row, every obligation kind: the allow is no verdict. The control is the same allow with no
// obligation, which every row serves.
func TestAnAllowWithAnObligationGoAPICannotCarryOutIsNoVerdictOnEveryMappedRow(t *testing.T) {
	rows, err := ParseShadowRows(authv1.GoAPIEndpointProfiles, "dev-health-ops-api")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) == 0 {
		t.Fatal("no mapped row")
	}
	keys := slices.Sorted(maps.Keys(rows))
	sw := onFor(authzenforce.Namespaces()...)
	observe := func(key string) enforceObservation {
		return enforceObservation{pattern: key, bearer: shadowBearer, orgID: ownOrg, userAccessToken: true}
	}
	t.Run("control", func(t *testing.T) {
		rec := recordEnforce(t)
		enf, err := NewEnforcer(EnforceConfig{Decider: newTestPDP(t, allowAll).client(t), Rows: rows, Switch: sw, Timeout: time.Second, Logger: quiet()})
		if err != nil {
			t.Fatal(err)
		}
		for _, key := range keys {
			if status, _, _ := answerOf(enf.decide(context.Background(), observe(key))); status != http.StatusOK {
				t.Errorf("%s: plain allow answered %d, want 200", key, status)
			}
		}
		if n := len(rec.snapshot()); n != len(keys) {
			t.Fatalf("counted %d decisions over %d rows", n, len(keys))
		}
	})
	for _, kind := range slices.Sorted(maps.Keys(enforceObligationKinds)) {
		t.Run(kind, func(t *testing.T) {
			rec := recordEnforce(t)
			pdp := newTestPDP(t, allowAll)
			pdp.obligations = enforceObligationKinds[kind]
			enf, err := NewEnforcer(EnforceConfig{Decider: pdp.client(t), Rows: rows, Switch: sw, Timeout: time.Second, Logger: quiet()})
			if err != nil {
				t.Fatal(err)
			}
			served, status403, status503, retryAfter, errorDeny := 0, 0, 0, 0, 0
			for _, key := range keys {
				before := len(rec.snapshot())
				status, body, header := answerOf(enf.decide(context.Background(), observe(key)))
				switch status {
				case http.StatusOK:
					served++
				case http.StatusForbidden:
					status403++
				case http.StatusServiceUnavailable:
					if body == enforceUnavailableBody {
						status503++
					}
				}
				if status == http.StatusServiceUnavailable && header.Get("Retry-After") == enforceRetryAfter {
					retryAfter++
				}
				if got := rec.snapshot()[before:]; slices.Equal(got, []string{rows[key].Namespace + "/error_deny"}) {
					errorDeny++
				}
			}
			if n := len(keys); status503 != n || retryAfter != n || errorDeny != n {
				t.Fatalf("obligation=%s rows=%d served=%d status403=%d status503=%d retry_after=%d error_deny=%d, want every row 503 with Retry-After and counted error_deny",
					kind, n, served, status403, status503, retryAfter, errorDeny)
			}
			t.Logf("obligation=%s rows=%d status503=%d retry_after=%d error_deny=%d", kind, len(keys), status503, retryAfter, errorDeny)
		})
	}
}

// A tenant scope that is the request's organization is carried out: every organization row serves. A platform row
// asks for no organization, so the scope names none of its request and is not carried out.
func TestATenantScopeOfTheRequestOrganizationIsCarriedOutOnEveryOrganizationRow(t *testing.T) {
	rows, err := ParseShadowRows(authv1.GoAPIEndpointProfiles, "dev-health-ops-api")
	if err != nil {
		t.Fatal(err)
	}
	rec := recordEnforce(t)
	pdp := newTestPDP(t, allowAll)
	pdp.obligations = map[string]any{"tenant_scope": ownOrg}
	enf, err := NewEnforcer(EnforceConfig{Decider: pdp.client(t), Rows: rows, Switch: onFor(authzenforce.Namespaces()...), Timeout: time.Second, Logger: quiet()})
	if err != nil {
		t.Fatal(err)
	}
	organization, platform := 0, 0
	for _, key := range slices.Sorted(maps.Keys(rows)) {
		row := rows[key]
		before := len(rec.snapshot())
		status, _, header := answerOf(enf.decide(context.Background(), enforceObservation{pattern: key, bearer: shadowBearer, orgID: ownOrg, userAccessToken: true}))
		counted := rec.snapshot()[before:]
		switch row.ResourceType {
		case "organization":
			organization++
			if status != http.StatusOK || !slices.Equal(counted, []string{row.Namespace + "/allow"}) {
				t.Errorf("%s: tenant scope = the request organization answered %d counted %v, want 200 and [%s/allow]", key, status, counted, row.Namespace)
			}
		default:
			platform++
			if status != http.StatusServiceUnavailable || header.Get("Retry-After") != enforceRetryAfter || !slices.Equal(counted, []string{row.Namespace + "/error_deny"}) {
				t.Errorf("%s: platform row with a tenant scope answered %d counted %v, want 503 with Retry-After and [%s/error_deny]", key, status, counted, row.Namespace)
			}
		}
	}
	if organization == 0 || platform == 0 {
		t.Fatalf("organization rows %d, platform rows %d: the matrix needs both", organization, platform)
	}
}

func newEnforcerWithoutRepositoryDimension(t *testing.T, decider pep.Decider, sw authzenforce.Switch, routes ...string) *Enforcer {
	t.Helper()
	flagged := map[string]bool{}
	for _, route := range routes {
		flagged[route] = true
	}
	enf, err := NewEnforcer(EnforceConfig{Decider: decider, Rows: shadowTestRows, Switch: sw, Timeout: time.Second, Logger: quiet(), NoRepositoryDimension: flagged})
	if err != nil {
		t.Fatal(err)
	}
	return enf
}

// The carried-out rule: a tenant scope that is the request's organization, and a repository allowlist on a route the
// table marks as having no repository dimension, are carried out and the allow serves. Any other obligation, or a
// set with one obligation not carried out, is no verdict: 503 with Retry-After, counted error_deny.
func TestTheCarriedOutRuleThroughTheGuard(t *testing.T) {
	const teamsRead = "GET /api/v1/admin/teams"
	cells := []struct {
		name        string
		obligations map[string]any
		flagged     []string
		method      string
		path        string
		serves      bool
	}{
		{"tenant scope is the request organization", map[string]any{"tenant_scope": ownOrg}, nil, "GET", "/api/v1/admin/teams", true},
		{"tenant scope is another organization", map[string]any{"tenant_scope": strangerOrg}, nil, "GET", "/api/v1/admin/teams", false},
		{"tenant scope on a platform route", map[string]any{"tenant_scope": ownOrg}, nil, "GET", "/api/v1/admin/orgs", false},
		{"repository allowlist on a route with no repository dimension", map[string]any{"repository_allowlist": []any{"acme/widget"}}, []string{teamsRead}, "GET", "/api/v1/admin/teams", true},
		{"empty repository allowlist on a route with no repository dimension", map[string]any{"repository_allowlist": []any{}}, []string{teamsRead}, "GET", "/api/v1/admin/teams", true},
		{"repository allowlist on a route not flagged", map[string]any{"repository_allowlist": []any{"acme/widget"}}, []string{"POST /api/v1/admin/teams"}, "GET", "/api/v1/admin/teams", false},
		{"redaction on a flagged route", map[string]any{"redaction": []any{"personal_identifiers"}}, []string{teamsRead}, "GET", "/api/v1/admin/teams", false},
		{"minimum assurance on a flagged route", map[string]any{"minimum_assurance": "aal2"}, []string{teamsRead}, "GET", "/api/v1/admin/teams", false},
		{"step up on a flagged route", map[string]any{"step_up": true}, []string{teamsRead}, "GET", "/api/v1/admin/teams", false},
		{"tenant scope and repository allowlist, both carried out", map[string]any{"tenant_scope": ownOrg, "repository_allowlist": []any{"acme/widget"}}, []string{teamsRead}, "GET", "/api/v1/admin/teams", true},
		{"carried-out set plus redaction", map[string]any{"tenant_scope": ownOrg, "repository_allowlist": []any{"acme/widget"}, "redaction": []any{"personal_identifiers"}}, []string{teamsRead}, "GET", "/api/v1/admin/teams", false},
		{"carried-out set plus minimum assurance", map[string]any{"tenant_scope": ownOrg, "repository_allowlist": []any{"acme/widget"}, "minimum_assurance": "aal2"}, []string{teamsRead}, "GET", "/api/v1/admin/teams", false},
		{"carried-out set plus step up", map[string]any{"tenant_scope": ownOrg, "repository_allowlist": []any{"acme/widget"}, "step_up": true}, []string{teamsRead}, "GET", "/api/v1/admin/teams", false},
		{"tenant scope carried out, repository allowlist on a route not flagged", map[string]any{"tenant_scope": ownOrg, "repository_allowlist": []any{"acme/widget"}}, nil, "GET", "/api/v1/admin/teams", false},
		{"repository allowlist carried out, tenant scope another organization", map[string]any{"tenant_scope": strangerOrg, "repository_allowlist": []any{"acme/widget"}}, []string{teamsRead}, "GET", "/api/v1/admin/teams", false},
		{"repository scope organization on a route with no repository dimension", map[string]any{"repository_scope": "organization"}, []string{teamsRead}, "GET", "/api/v1/admin/teams", true},
		{"repository scope organization on a route not flagged", map[string]any{"repository_scope": "organization"}, []string{"POST /api/v1/admin/teams"}, "GET", "/api/v1/admin/teams", false},
		{"repository scope organization, flagged route, tenant scope is the request organization", map[string]any{"repository_scope": "organization", "tenant_scope": ownOrg}, []string{teamsRead}, "GET", "/api/v1/admin/teams", true},
		{"repository scope organization, flagged route, tenant scope another organization", map[string]any{"repository_scope": "organization", "tenant_scope": strangerOrg}, []string{teamsRead}, "GET", "/api/v1/admin/teams", false},
		{"repository scope organization, tenant scope carried out, route not flagged", map[string]any{"repository_scope": "organization", "tenant_scope": ownOrg}, nil, "GET", "/api/v1/admin/teams", false},
		{"repository scope organization plus redaction on a flagged route", map[string]any{"repository_scope": "organization", "tenant_scope": ownOrg, "redaction": []any{"personal_identifiers"}}, []string{teamsRead}, "GET", "/api/v1/admin/teams", false},
		{"repository scope organization plus step up on a flagged route", map[string]any{"repository_scope": "organization", "tenant_scope": ownOrg, "step_up": true}, []string{teamsRead}, "GET", "/api/v1/admin/teams", false},
	}
	for _, cell := range cells {
		t.Run(cell.name, func(t *testing.T) {
			rec := recordEnforce(t)
			pdp := newTestPDP(t, allowAll)
			pdp.obligations = cell.obligations
			namespace := shadowTestRows[cell.method+" "+cell.path].Namespace
			// A served cell runs as a plain member, whom the legacy check refuses: only the carried-out allow serves it.
			// A refused cell runs as a superuser, whom the legacy check serves: only the no-verdict refusal refuses it.
			who := personaSuperuser
			if cell.serves {
				who = personaMember
			}
			got := enforceRequest(enforceHandler(t, personaStore(who), userClaims(nil), newEnforcerWithoutRepositoryDimension(t, pdp.client(t), onFor(namespace), cell.flagged...)), cell.method, cell.path)
			if cell.serves {
				if got.status != http.StatusOK || !slices.Equal(rec.snapshot(), []string{namespace + "/allow"}) {
					t.Fatalf("%d %q counted %v, want 200 and [%s/allow]: every obligation is carried out", got.status, got.body, rec.snapshot(), namespace)
				}
				return
			}
			if got.status != http.StatusServiceUnavailable || got.header.Get("Retry-After") != enforceRetryAfter || got.body != enforceUnavailableBody {
				t.Fatalf("%d %q retry-after %q, want 503 %q with Retry-After %s: an obligation is not carried out", got.status, got.body, got.header.Get("Retry-After"), enforceUnavailableBody, enforceRetryAfter)
			}
			if !slices.Equal(rec.snapshot(), []string{namespace + "/error_deny"}) {
				t.Fatalf("counted %v, want [%s/error_deny]", rec.snapshot(), namespace)
			}
		})
	}
}

func TestTheNoRepositoryDimensionTableNamesOnlyMappedRows(t *testing.T) {
	if _, err := NewEnforcer(EnforceConfig{Decider: funcDecider{}, Rows: shadowTestRows, NoRepositoryDimension: map[string]bool{"GET /api/v1/not-a-row": true}}); err == nil {
		t.Fatal("a no-repository-dimension flag on a route with no row was accepted")
	}
	rows, err := ParseShadowRows(authv1.GoAPIEndpointProfiles, "dev-health-ops-api")
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range slices.Sorted(maps.Keys(goAPIRoutesWithoutRepositoryDimension)) {
		if _, ok := rows[key]; !ok {
			t.Errorf("%s is flagged with no repository dimension but is not a mapped go-api row", key)
		}
	}
}

// The question the decision point sees is the row's action on the caller's organization, for the caller's token.
func TestTheQuestionIsTheRowsActionOnTheCallersOrganization(t *testing.T) {
	recordEnforce(t)
	pdp := newTestPDP(t, allowAll)
	enforceRequest(enforceHandler(t, personaStore(personaAdmin), userClaims(nil), newEnforcerFor(t, pdp.client(t), onFor("teams"))), "GET", "/api/v1/admin/teams")
	calls := pdp.calls()
	if len(calls) != 1 || calls[0].Action != "teams.read" || calls[0].Type != "organization" || !calls[0].HasOrg || calls[0].Org != ownOrg || calls[0].Key != ownOrg || calls[0].Principal != shadowBearer {
		t.Fatalf("PDP saw %+v, want teams.read on organization %s for the bearer", calls, ownOrg)
	}
}

func TestARouteWithNoActionRowIsNeverEnforced(t *testing.T) {
	rec := recordEnforce(t)
	pdp := newTestPDP(t, denyWith("role_lacks_action"))
	sw := authzenforce.Parse(authzenforce.GoAPI, "acr,admin,analytics,authz,billing,credentials,git,integrations,license,members,metrics,org,settings,teams,work_items")
	base := enforceRequest(enforceHandler(t, personaStore(personaAdmin), userClaims(nil), nil), "GET", "/api/v1/admin/other")
	got := enforceRequest(enforceHandler(t, personaStore(personaAdmin), userClaims(nil), newEnforcerFor(t, pdp.client(t), sw)), "GET", "/api/v1/admin/other")
	if got.status != base.status || got.body != base.body || len(pdp.calls()) != 0 || len(rec.snapshot()) != 0 {
		t.Fatalf("a route without an action row: %d %q calls %d counted %v, want the legacy %d %q and nothing", got.status, got.body, len(pdp.calls()), rec.snapshot(), base.status, base.body)
	}
}

func TestEnforcementCountsOneOutcomeFromTheClosedSet(t *testing.T) {
	rec := recordEnforce(t)
	allow, deny := newTestPDP(t, allowAll), newTestPDP(t, denyWith("role_lacks_action"))
	enforceRequest(enforceHandler(t, personaStore(personaAdmin), userClaims(nil), newEnforcerFor(t, allow.client(t), onFor("teams"))), "GET", "/api/v1/admin/teams")
	enforceRequest(enforceHandler(t, personaStore(personaAdmin), userClaims(nil), newEnforcerFor(t, deny.client(t), onFor("teams"))), "GET", "/api/v1/admin/teams")
	if got := rec.snapshot(); !slices.Equal(got, []string{"teams/allow", "teams/deny"}) {
		t.Fatalf("counted %v, want [teams/allow teams/deny]", got)
	}
}

// Flipping a cell on, revoking, and flipping it off again never gives the access back: the legacy path reads the
// same live rows the decision point's answer rests on, so a revoked caller is refused with the cell off.
func TestRollbackCannotReactivateRevokedAccess(t *testing.T) {
	revoke := map[string]func(*accessStore){
		"membership removed": func(s *accessStore) { delete(s.roles, [2]uuid.UUID{userID, ownOrgID}) },
		"user deactivated":   func(s *accessStore) { s.users[userID] = UserState{IsActive: false} },
	}
	for name, apply := range revoke {
		t.Run(name, func(t *testing.T) {
			recordEnforce(t)
			store := personaStore(personaAdmin)
			pdp := newTestPDP(t, denyWith("no_membership"))
			on := enforceHandler(t, store, userClaims(nil), newEnforcerFor(t, pdp.client(t), onFor("teams")))
			if got := enforceRequest(on, "GET", "/api/v1/admin/teams"); got.status != http.StatusForbidden && got.status != http.StatusUnauthorized {
				t.Fatalf("revoked, cell on: %d %q, want a refusal", got.status, got.body)
			}
			apply(store)
			off := enforceHandler(t, store, userClaims(nil), newEnforcerFor(t, pdp.client(t), authzenforce.Switch{}))
			if got := enforceRequest(off, "GET", "/api/v1/admin/teams"); got.status == http.StatusOK {
				t.Fatalf("revoked, cell flipped off: %d %q, the legacy path gave the access back", got.status, got.body)
			}
		})
	}
}

// The decision point judges user principals only. A route whose profile row names an action is enforceable, so its
// row may accept the two user access-token classes and nothing else: a row that gains a service-account, workload or
// push class must keep its namespace off until the decision point judges that kind. The one other class a mapped row
// may carry is org_context_header, and only next to a user-token class: it names the X-Org-Id request header the
// telemetry rows read, travels with the user token, is no credential, and the decision point never reads it (the
// platform question carries no organization). A row with that class and no user-token class fails.
func mappedRowClassProblems(classes []string) []string {
	userToken := func(class string) bool { return class == "ops_access_token_hs256" || class == "user_access_token" }
	hasUserToken := slices.ContainsFunc(classes, userToken)
	var problems []string
	for _, class := range classes {
		if !userToken(class) && !(class == "org_context_header" && hasUserToken) {
			problems = append(problems, class)
		}
	}
	return problems
}

// The planted rows: org_context_header alone, or next to a class that is not a user token, is refused.
func TestAMappedRowWithOrgContextHeaderAndNoUserTokenClassFails(t *testing.T) {
	if got := mappedRowClassProblems([]string{"org_context_header"}); len(got) != 1 {
		t.Errorf("org_context_header alone: problems %v, want one", got)
	}
	if got := mappedRowClassProblems([]string{"org_context_header", "refresh_credential"}); len(got) != 2 {
		t.Errorf("org_context_header with a non-user class: problems %v, want two", got)
	}
	if got := mappedRowClassProblems([]string{"org_context_header", "user_access_token"}); len(got) != 0 {
		t.Errorf("org_context_header with a user token: problems %v, want none", got)
	}
	if got := mappedRowClassProblems([]string{"refresh_credential", "user_access_token"}); len(got) != 1 {
		t.Errorf("a non-user class: problems %v, want one", got)
	}
}

// Every row that names an action carries only the classes mappedRowClassProblems accepts.
func TestEveryMappedRowAcceptsOnlyUserTokenClasses(t *testing.T) {
	var document struct {
		Rows []struct {
			Method, Route, Action string
			Classes               []string `json:"accepted_credential_classes"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(authv1.GoAPIEndpointProfiles, &document); err != nil {
		t.Fatal(err)
	}
	mapped := 0
	for _, row := range document.Rows {
		if row.Action == "" {
			continue
		}
		mapped++
		for _, class := range mappedRowClassProblems(row.Classes) {
			t.Errorf("%s %s (%s) accepts %s: keep the %s namespace off, or teach the decision point that principal kind", row.Method, row.Route, row.Action, class, strings.SplitN(row.Action, ".", 2)[0])
		}
	}
	if mapped == 0 {
		t.Fatal("no row names an action")
	}
}

// CHAOS-10872 S3: a present bearer that is not a user access token (an HS256 token, a workload token, a push token: whatever the
// routed verifier refuses, and an authenticator with no access check at all) is refused at authentication, before the enforcer is
// asked anything: 401, no decision point question, no decision outcome and no unjudged count. So no bearer that reaches the guard on
// an enforced row is a non-user token; the enforcer's own refusal of one (TestANonUserTokenThatReachesTheEnforcerIsRefused) is a
// guard only.
func TestANonUserTokenIsRefusedAtAuthenticationBeforeTheEnforcerDecides(t *testing.T) {
	const refusal = `{"detail":{"message":"Invalid or expired token"}}`
	for _, tc := range []struct {
		name string
		auth func(t *testing.T) *Authenticator
	}{
		{"the verifier refuses the token", func(t *testing.T) *Authenticator {
			return routed(t, personaStore(personaAdmin), &fakeValidator{err: errRefusedByValidator})
		}},
		{"the verifier answers no result", func(t *testing.T) *Authenticator {
			return routed(t, personaStore(personaAdmin), &fakeValidator{})
		}},
		{"the verifier answers a result with no claims", func(t *testing.T) *Authenticator {
			return routed(t, personaStore(personaAdmin), &fakeValidator{result: &RoutedResult{}})
		}},
		{"the authenticator has no access check", func(t *testing.T) *Authenticator {
			return unrouted(t, personaStore(personaAdmin))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := recordEnforce(t)
			pdp := newTestPDP(t, allowAll)
			guard := NewGuard(tc.auth(t), quiet())
			guard.SetEnforcer(newEnforcerFor(t, pdp.client(t), onFor("teams")))
			mux := http.NewServeMux()
			mux.Handle("GET /api/v1/admin/teams", guard.Wrap(AdminOrg, echo))
			for _, bearer := range []string{shadowBearer, "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ4In0.c2ln"} {
				got := serve(mux, http.MethodGet, "/api/v1/admin/teams", map[string]string{"Authorization": "Bearer " + bearer})
				if got.status != http.StatusUnauthorized || got.body != refusal {
					t.Errorf("%s: %d %q, want 401 %s", tc.name, got.status, got.body, refusal)
				}
			}
			if calls := pdp.calls(); len(calls) != 0 {
				t.Errorf("%s: the decision point saw %+v, want no question", tc.name, calls)
			}
			if d, u := rec.snapshot(), rec.unjudgedSnapshot(); len(d) != 0 || len(u) != 0 {
				t.Errorf("%s: decisions %v unjudged %v, want none and none (the enforcer is never reached)", tc.name, d, u)
			}
		})
	}
}
