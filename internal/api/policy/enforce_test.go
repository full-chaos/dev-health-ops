package policy

import (
	"context"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/pep"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/authzenforce"
)

type enforceRecorder struct {
	mu   sync.Mutex
	hits []string
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
	t.Cleanup(func() { enforceDecisionAdd = previous })
	return rec
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
			if got.status != http.StatusForbidden {
				t.Fatalf("PDP deny, legacy allow: %d %q, want 403", got.status, got.body)
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

func TestADeadPDPIsA503NotALegacyAllow(t *testing.T) {
	pdp := newTestPDP(t, allowAll)
	client := pdp.client(t)
	pdp.srv.Close()
	got := enforceRequest(enforceHandler(t, personaStore(personaSuperuser), userClaims(nil), newEnforcerFor(t, client, onFor("teams"))), "GET", "/api/v1/admin/teams")
	if got.status != http.StatusServiceUnavailable {
		t.Fatalf("dead PDP: %d %q, want 503", got.status, got.body)
	}
}

func TestAnAllowThatCarriesAnObligationIsADeny(t *testing.T) {
	pdp := newTestPDP(t, allowAll)
	pdp.obligations = map[string]any{"tenant_scope": ownOrg}
	rec := recordEnforce(t)
	got := enforceRequest(enforceHandler(t, personaStore(personaAdmin), userClaims(nil), newEnforcerFor(t, pdp.client(t), onFor("teams"))), "GET", "/api/v1/admin/teams")
	if got.status != http.StatusForbidden || !slices.Equal(rec.snapshot(), []string{"teams/deny"}) {
		t.Fatalf("%d %q counted %v, want 403 and [teams/deny]: go-api enforces no obligation, so an allow that carries one is a deny", got.status, got.body, rec.snapshot())
	}
}

func TestACallerThePDPCannotJudgeKeepsTheLegacyAnswer(t *testing.T) {
	rec := recordEnforce(t)
	pdp := newTestPDP(t, denyWith("role_lacks_action"))
	store := newStore()
	withClaimRole(store, claims(func(c jwt.MapClaims) { c["role"] = "admin" }))
	auth := authenticator(t, store)
	guard := NewGuard(auth, quiet())
	guard.SetEnforcer(newEnforcerFor(t, pdp.client(t), onFor("teams")))
	mux := http.NewServeMux()
	mux.Handle("GET /api/v1/admin/teams", guard.Wrap(AdminOrg, echo))
	got := serve(mux, http.MethodGet, "/api/v1/admin/teams", validAuth(t, func(c jwt.MapClaims) { c["role"] = "admin" }))
	if got.status != http.StatusOK {
		t.Fatalf("legacy HS256 token on an enforced cell: %d %q, want the legacy 200", got.status, got.body)
	}
	if n := len(pdp.calls()); n != 0 || !slices.Equal(rec.snapshot(), []string{"teams/unjudged_legacy"}) {
		t.Fatalf("%d PDP calls, counted %v, want 0 and [teams/unjudged_legacy]", n, rec.snapshot())
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
