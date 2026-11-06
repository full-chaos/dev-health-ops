package policy

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

// goAPIRows are the real go-api profile rows as go-api installs them.
func goAPIRows(t *testing.T) map[string]ShadowRow {
	t.Helper()
	rows, err := ParseShadowRows(authv1.GoAPIEndpointProfiles, "dev-health-ops-api")
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

// parityMount is one route as go-api mounts it: the pattern and the guard wrap of its handler.
type parityMount struct {
	pattern string
	wrap    func(g *Guard) http.Handler
	path    string
}

// parityMux mounts m behind a guard over auth, with enf installed (nil: every cell off).
func parityMux(t *testing.T, auth *Authenticator, enf *Enforcer, m parityMount) http.Handler {
	t.Helper()
	guard := NewGuard(auth, quiet())
	if enf != nil {
		guard.SetEnforcer(enf)
	}
	mux := http.NewServeMux()
	mux.Handle(m.pattern, m.wrap(guard))
	return mux
}

func parityEnforcer(t *testing.T, decider *testPDP, rows map[string]ShadowRow, namespace string) *Enforcer {
	t.Helper()
	enf, err := NewEnforcer(EnforceConfig{Decider: decider.client(t), Rows: rows, Switch: onFor(namespace), Timeout: time.Second, Logger: quiet()})
	if err != nil {
		t.Fatal(err)
	}
	return enf
}

// The read rows of the impersonation parity table, mounted with the level of their go-api route (billing/routes.go,
// apiservice/admin/orgs.go). The members row serves the target organization through its path.
func impersonationReadMounts() []parityMount {
	return []parityMount{
		{"GET /api/v1/billing/invoices", func(g *Guard) http.Handler { return g.Wrap(Authenticated, echo) }, "/api/v1/billing/invoices"},
		{"GET /api/v1/admin/orgs/{org_id}/members", func(g *Guard) http.Handler { return g.Wrap(Admin, echo) },
			"/api/v1/admin/orgs/" + targetOrgID.String() + "/members"},
		{"GET /api/v1/admin/orgs", func(g *Guard) http.Handler { return g.WrapPlatform(ActionAdminOrgs, echo) }, "/api/v1/admin/orgs"},
	}
}

// An admin with a live go-api impersonation row (its own bearer, EdDSA or HS256) reads an enforced row: the answer
// is the one the row gives with its cell off, served as the target organization (product rule 1). The platform row
// keeps its legacy impersonation refusal.
func TestAnImpersonatingAdminsReadOnAnEnforcedRowEqualsTheLegacyAnswer(t *testing.T) {
	rows := goAPIRows(t)
	tokens := []struct {
		name    string
		auth    func(t *testing.T) *Authenticator
		headers map[string]string
	}{
		{"EdDSA", func(t *testing.T) *Authenticator {
			return routed(t, personaStore(personaSuperuser), &fakeValidator{result: userClaims(nil)})
		}, map[string]string{"Authorization": "Bearer " + shadowBearer}},
		{"HS256", func(t *testing.T) *Authenticator {
			store := newStore()
			store.users[userID] = UserState{IsActive: true, IsSuperuser: true}
			withClaimRole(store, claims(func(c jwt.MapClaims) { c["role"] = "admin" }))
			return authenticator(t, store)
		}, validAuth(t, func(c jwt.MapClaims) { c["role"], c["is_superuser"] = "admin", true })},
	}
	for _, token := range tokens {
		for _, m := range impersonationReadMounts() {
			t.Run(token.name+" "+m.pattern, func(t *testing.T) {
				row, ok := rows[m.pattern]
				if !ok {
					t.Fatalf("%s: no go-api profile row", m.pattern)
				}
				pdp := newTestPDP(t, allowAll)
				legacy := serve(impersonating(parityMux(t, token.auth(t), nil, m)), http.MethodGet, m.path, token.headers)
				enforced := serve(impersonating(parityMux(t, token.auth(t), parityEnforcer(t, pdp, rows, row.Namespace), m)), http.MethodGet, m.path, token.headers)
				if enforced.status != legacy.status || enforced.body != legacy.body {
					t.Errorf("enforce-on %d %q, cells-off %d %q: want the cells-off answer", enforced.status, enforced.body, legacy.status, legacy.body)
				}
				if n := len(pdp.calls()); n != 0 {
					t.Errorf("%d PDP calls, want 0: the decision point would judge the admin's own bearer, not the session", n)
				}
			})
		}
	}
	if got := serve(impersonating(parityMux(t, tokens[0].auth(t), nil, impersonationReadMounts()[0])), http.MethodGet, "/api/v1/billing/invoices", tokens[0].headers); got.status != http.StatusOK || !strings.Contains(got.body, "imp="+targetOrgID.String()) {
		t.Fatalf("cells-off yardstick: %d %q, want 200 as the target", got.status, got.body)
	}
}

// Every read (GET, HEAD) go-api profile row except the live-row exemptions, its namespace on, answers an impersonating
// admin as the row does with its cell off, with no decision point call. A GET row also takes HEAD (net/http ServeMux),
// which RefuseImpersonation reads as a read too.
func TestEveryEnforcedReadRowAnswersAnImpersonatingAdminAsTheLegacyGuard(t *testing.T) {
	rows := goAPIRows(t)
	cells := 0
	for pattern, row := range rows {
		method, route, _ := strings.Cut(pattern, " ")
		if liveRowExempt(pattern) || !readMethod(method) {
			continue
		}
		cells++
		t.Run(pattern, func(t *testing.T) {
			m := parityMount{pattern, func(g *Guard) http.Handler { return g.Wrap(Authenticated, echo) }, profilePath(route)}
			auth := func() *Authenticator {
				return routed(t, personaStore(personaSuperuser), &fakeValidator{result: userClaims(nil)})
			}
			pdp := newTestPDP(t, allowAll)
			for _, verb := range []string{method, http.MethodHead} {
				legacy := enforceRequest(impersonating(parityMux(t, auth(), nil, m)), verb, m.path)
				enforced := enforceRequest(impersonating(parityMux(t, auth(), parityEnforcer(t, pdp, rows, row.Namespace), m)), verb, m.path)
				if enforced.status != legacy.status || enforced.body != legacy.body {
					t.Errorf("%s: enforce-on %d %q, cells-off %d %q: want the cells-off answer", verb, enforced.status, enforced.body, legacy.status, legacy.body)
				}
			}
			if n := len(pdp.calls()); n != 0 {
				t.Errorf("%d PDP calls, want 0", n)
			}
		})
	}
	if cells < 20 {
		t.Fatalf("%d read rows, want the go-api read rows (20 or more)", cells)
	}
}

// delegatedBillingRead is the delegated bearer (act claim) of a target admin on GET /api/v1/billing/invoices, whose
// row admits a delegated bearer; pdp answers its allow with obligations.
func delegatedBillingRead(t *testing.T, pdp *testPDP, enforce bool) response {
	t.Helper()
	rows := goAPIRows(t)
	actor := "prn_" + wireID(actorID)
	auth := routed(t, personaStore(personaAdmin), &fakeValidator{result: userClaims(func(c *AccessTokenClaims) { c.Actor = &actor })})
	auth.DelegatedRows(func(profile string) bool { return profile == "GET /api/v1/billing/invoices" })
	var enf *Enforcer
	if enforce {
		enf = parityEnforcer(t, pdp, rows, "billing")
	}
	m := impersonationReadMounts()[0]
	return enforceRequest(parityMux(t, auth, enf, m), http.MethodGet, m.path)
}

// A level no go-api session meets stays no verdict: an aal2 allow on a delegated read is a 503.
func TestADelegatedReadWithAnAAL2AllowStaysUnavailable(t *testing.T) {
	pdp := newTestPDP(t, allowAll)
	pdp.obligations = map[string]any{"tenant_scope": ownOrg, "minimum_assurance": "aal2"}
	if got := delegatedBillingRead(t, pdp, true); got.status != http.StatusServiceUnavailable {
		t.Fatalf("delegated read, aal2 allow: %d %q, want 503", got.status, got.body)
	}
}

// An admin with a live go-api impersonation row reads an admin-or row (GET /api/v1/admin/users, WrapAdminOr, with the
// row naming admin.users as adminUsersRows does): enforce-on answers as the cells-off guard, with no PDP call. The
// impersonation clause runs before the admin-or batch.
func TestAnImpersonatingAdminsReadOnAnAdminOrRowEqualsTheLegacyAnswer(t *testing.T) {
	rows := adminUsersRows(t)
	m := parityMount{"GET /api/v1/admin/users", func(g *Guard) http.Handler { return g.WrapAdminOr(rule.ActionAdminUsers, echo) }, "/api/v1/admin/users"}
	auth := func() *Authenticator {
		return routed(t, personaStore(personaSuperuser), &fakeValidator{result: userClaims(nil)})
	}
	pdp := newTestPDP(t, allowAll)
	legacy := enforceRequest(impersonating(parityMux(t, auth(), nil, m)), http.MethodGet, m.path)
	enforced := enforceRequest(impersonating(parityMux(t, auth(), parityEnforcer(t, pdp, rows, "admin"), m)), http.MethodGet, m.path)
	if enforced.status != legacy.status || enforced.body != legacy.body || legacy.status != http.StatusOK {
		t.Errorf("enforce-on %d %q, cells-off %d %q: want the cells-off 200", enforced.status, enforced.body, legacy.status, legacy.body)
	}
	if n := len(pdp.calls()); n != 0 {
		t.Errorf("%d PDP calls, want 0", n)
	}
}
