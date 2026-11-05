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
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
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
	got := serve(hs256Handler(t, newUnconfiguredEnforcerFor(t, onFor("teams"))), http.MethodGet, "/api/v1/admin/teams", validAuth(t, func(c jwt.MapClaims) { c["role"] = "admin" }))
	if got.status != http.StatusServiceUnavailable || got.body != enforceNoDecisionPointBody {
		t.Fatalf("no decision point, legacy HS256 caller: %d %q, want the 503: fail-closed refuses every request", got.status, got.body)
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

func hs256Handler(t *testing.T, enf *Enforcer) http.Handler {
	t.Helper()
	store := newStore()
	withClaimRole(store, claims(func(c jwt.MapClaims) { c["role"] = "admin" }))
	guard := NewGuard(authenticator(t, store), quiet())
	guard.SetEnforcer(enf)
	mux := http.NewServeMux()
	mux.Handle("GET /api/v1/admin/teams", guard.Wrap(AdminOrg, echo))
	return mux
}

// A caller the decision point cannot judge keeps the legacy answer, counted with its own counter by route and reason.
func TestALegacyHS256CallerKeepsTheLegacyAnswerAndIsCountedUnjudged(t *testing.T) {
	rec := recordEnforce(t)
	pdp := newTestPDP(t, denyWith("role_lacks_action"))
	got := serve(hs256Handler(t, newEnforcerFor(t, pdp.client(t), onFor("teams"))), http.MethodGet, "/api/v1/admin/teams", validAuth(t, func(c jwt.MapClaims) { c["role"] = "admin" }))
	if got.status != http.StatusOK {
		t.Fatalf("legacy HS256 token on an enforced cell: %d %q, want the legacy 200", got.status, got.body)
	}
	if n := len(pdp.calls()); n != 0 || len(rec.snapshot()) != 0 || !slices.Equal(rec.unjudgedSnapshot(), []string{"GET /api/v1/admin/teams/hs256"}) {
		t.Fatalf("%d PDP calls, decisions %v, unjudged %v, want 0, none and [GET /api/v1/admin/teams/hs256]", n, rec.snapshot(), rec.unjudgedSnapshot())
	}
}

func TestADelegatedSessionKeepsTheLegacyAnswerAndIsCountedUnjudged(t *testing.T) {
	rec := recordEnforce(t)
	pdp := newTestPDP(t, denyWith("role_lacks_action"))
	inner := enforceHandler(t, personaStore(personaAdmin), userClaims(nil), newEnforcerFor(t, pdp.client(t), onFor("teams")))
	delegated := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		session := &Impersonation{ID: uuid.New(), AdminUserID: userID, TargetUserID: uuid.New(), TargetOrgID: ownOrgID, TargetRole: "admin", ExpiresAt: time.Now().Add(time.Hour)}
		inner.ServeHTTP(w, r.WithContext(WithImpersonation(r.Context(), session)))
	})
	base := enforceRequest(enforceHandler(t, personaStore(personaAdmin), userClaims(nil), nil), "GET", "/api/v1/admin/teams")
	got := enforceRequest(delegated, "GET", "/api/v1/admin/teams")
	if got.status != base.status {
		t.Fatalf("delegated: %d %q, want the legacy %d", got.status, got.body, base.status)
	}
	if n := len(pdp.calls()); n != 0 || !slices.Equal(rec.unjudgedSnapshot(), []string{"GET /api/v1/admin/teams/delegated"}) {
		t.Fatalf("%d PDP calls, unjudged %v, want 0 and [GET /api/v1/admin/teams/delegated]", n, rec.unjudgedSnapshot())
	}
}

func TestAnOrganizationRouteWithNoOrganizationKeepsTheLegacyAnswerAndIsCountedUnjudged(t *testing.T) {
	rec := recordEnforce(t)
	pdp := newTestPDP(t, denyWith("role_lacks_action"))
	noOrg := userClaims(func(c *AccessTokenClaims) { c.OrgID = nil })
	base := enforceRequest(enforceHandler(t, personaStore(personaAdmin), noOrg, nil), "GET", "/api/v1/admin/teams")
	got := enforceRequest(enforceHandler(t, personaStore(personaAdmin), noOrg, newEnforcerFor(t, pdp.client(t), onFor("teams"))), "GET", "/api/v1/admin/teams")
	if got.status != base.status || got.body != base.body {
		t.Fatalf("no organization: %d %q, want the legacy %d %q", got.status, got.body, base.status, base.body)
	}
	if n := len(pdp.calls()); n != 0 || !slices.Equal(rec.unjudgedSnapshot(), []string{"GET /api/v1/admin/teams/no_org"}) {
		t.Fatalf("%d PDP calls, unjudged %v, want 0 and [GET /api/v1/admin/teams/no_org]", n, rec.unjudgedSnapshot())
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
	token := validAuth(t, func(c jwt.MapClaims) { c["role"] = "admin" })
	serve(hs256Handler(t, enf), http.MethodGet, "/api/v1/admin/teams", token)
	line := logs.String()
	if strings.Count(line, "level=DEBUG") != 1 || !strings.Contains(line, "reason=hs256") || !strings.Contains(line, "route=\"GET /api/v1/admin/teams\"") {
		t.Fatalf("log %q: want one DEBUG line with the route and reason hs256", line)
	}
	if strings.Contains(line, strings.TrimPrefix(token["Authorization"], "Bearer ")) {
		t.Fatalf("the log quotes the token: %q", line)
	}
}

// Both counters carry closed label sets: the decision counter has a namespace, the service and an outcome; the
// unjudged counter a route row and one of three reasons.
func TestEnforcementCounterLabelSetsAreClosed(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("enforce-test")
	newEnforceDecisionAdder(meter)(context.Background(), "teams", "allow")
	newEnforceUnjudgedAdder(meter)(context.Background(), "GET /api/v1/admin/teams", "hs256")
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
	reasons := []string{enforceUnjudgedHS256, enforceUnjudgedDelegated, enforceUnjudgedNoOrg}
	if !slices.Equal(reasons, []string{"hs256", "delegated", "no_org"}) {
		t.Fatalf("reasons %v, want the closed set hs256, delegated, no_org", reasons)
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
		writeEnforceRefusal(w, Admin, verdict)
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
// push class must keep its namespace off until the decision point judges that kind.
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
		for _, class := range row.Classes {
			if class != "ops_access_token_hs256" && class != "user_access_token" {
				t.Errorf("%s %s (%s) accepts %s: keep the %s namespace off, or teach the decision point that principal kind", row.Method, row.Route, row.Action, class, strings.SplitN(row.Action, ".", 2)[0])
			}
		}
	}
	if mapped == 0 {
		t.Fatal("no row names an action")
	}
}
