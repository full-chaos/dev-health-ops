package apiservice

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

// The SSO provider CRUD routes with auth-service owning the providers gate on the caller organization's enterprise
// entitlement (product rule C1), the token's organization, with the process tier unlicensed. The switch-off path keeps the
// process-only gate. Each cell drives the real route table, guard and handler over the fake entitlement read below.

const (
	ssoList   = "GET /api/v1/auth/sso/providers"
	ssoGet    = "GET /api/v1/auth/sso/providers/{provider_id}"
	ssoCreate = "POST /api/v1/auth/sso/providers"
	ssoPatch  = "PATCH /api/v1/auth/sso/providers/{provider_id}"
	ssoDelete = "DELETE /api/v1/auth/sso/providers/{provider_id}"
)

var ssoCRUD = []string{ssoList, ssoGet, ssoCreate, ssoPatch, ssoDelete}

// ssoUnlicensedBody is the 402 of an unlicensed community process: the bytes main answers on every provider route.
const ssoUnlicensedBody = `{"detail":{"error":"feature_not_licensed","feature":"sso_saml","required_tier":"enterprise","current_tier":"community"}}`

// orgEntitlement is the provider routes' entitlement read: the organizations it grants, or err for every read.
type orgEntitlement struct {
	mu      sync.Mutex
	granted map[string]bool
	err     error
	asked   []string
}

func (e *orgEntitlement) read(_ context.Context, orgID string) (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.asked = append(e.asked, orgID)
	if e.err != nil {
		return false, e.err
	}
	return e.granted[orgID], nil
}

func (e *orgEntitlement) take() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	asked := e.asked
	e.asked = nil
	return asked
}

func entitledProbeOrg() *orgEntitlement {
	return &orgEntitlement{granted: map[string]bool{probeOrg.String(): true}}
}

// ssoRoutesWith is ssoRoutes with the entitlement read and a logger of the test's own (nil client = the switch off).
func ssoRoutesWith(client authprovision.SSOProviders, entitlement *orgEntitlement, logger *slog.Logger) func(*testing.T, Deps) []httpapi.Route {
	return func(_ *testing.T, deps Deps) []httpapi.Route {
		deps.SSOProviders = client
		deps.SSOOrgEntitled = entitlement.read
		return Routes(deps, logger)
	}
}

// ssoWant is the answer of a caller class on a provider route when its organization is entitled.
func ssoWant(class, key string) (int, string) {
	write := key == ssoCreate || key == ssoPatch || key == ssoDelete
	if write && class != "org admin" {
		return http.StatusForbidden, `{"detail":"Admin access required"}`
	}
	switch key {
	case ssoCreate:
		return http.StatusCreated, ""
	case ssoDelete:
		return http.StatusNoContent, ""
	}
	return http.StatusOK, ""
}

func TestSSOProviderRoutesAdmitTheEntitledOrganizationWithTheSwitchOn(t *testing.T) {
	withSSOLicence(t, false)
	client := &ssoCellClient{}
	entitlement := entitledProbeOrg()
	handler, minter := adminConfigServerScoped(t, nil, withBoundaryCallers, ssoRoutesWith(client, entitlement, quietLogger()), nil)
	callers := boundaryCallers(minter)
	admitted := 0
	for _, key := range ssoCRUD {
		for _, class := range boundaryClasses {
			got := ssoDo(handler, key, callers[class].token)
			status, body := ssoWant(class, key)
			if got.status != status || (body != "" && got.body != body) {
				t.Errorf("%s / %s: %d %s, want %d %s", key, class, got.status, got.body, status, body)
			}
			if status < 300 {
				admitted++
			}
			if asked := entitlement.take(); len(asked) != 1 || asked[0] != probeOrg.String() {
				t.Errorf("%s / %s: entitlement asked for %v, want the token organization %s once", key, class, asked, probeOrg)
			}
			t.Logf("CELL\tentitled-on\t%s\t%s\t%d:%s", key, class, got.status, short(got.body))
		}
	}
	if len(client.calls) != admitted {
		t.Fatalf("auth-service calls %v, want %d (one per admitted request)", client.calls, admitted)
	}
}

func TestSSOProviderRoutesRefuseTheUnentitledOrganizationWithTheSwitchOn(t *testing.T) {
	withSSOLicence(t, false)
	client := &ssoCellClient{}
	entitlement := &orgEntitlement{granted: map[string]bool{}}
	handler, minter := adminConfigServerScoped(t, nil, withBoundaryCallers, ssoRoutesWith(client, entitlement, quietLogger()), nil)
	callers := boundaryCallers(minter)
	for _, key := range ssoCRUD {
		for _, class := range boundaryClasses {
			if got := ssoDo(handler, key, callers[class].token); got.status != http.StatusPaymentRequired || got.body != ssoUnlicensedBody {
				t.Errorf("%s / %s: %d %s, want the 402 %s", key, class, got.status, got.body, ssoUnlicensedBody)
			}
		}
	}
	if len(client.calls) != 0 {
		t.Fatalf("an unentitled organization reached auth-service: %v", client.calls)
	}
}

// A process licence that grants sso_saml admits every organization, entitled or not (either source satisfies the gate, as on
// the sign-in routes), without a failed read: the org entitlement is not asked.
func TestSSOProviderRoutesAdmitEveryOrganizationOnAProcessLicence(t *testing.T) {
	withSSOLicence(t, true)
	client := &ssoCellClient{}
	entitlement := &orgEntitlement{granted: map[string]bool{}}
	handler, minter := adminConfigServerScoped(t, nil, withBoundaryCallers, ssoRoutesWith(client, entitlement, quietLogger()), nil)
	admin := boundaryCallers(minter)["org admin"].token
	for _, key := range ssoCRUD {
		if got := ssoDo(handler, key, admin); got.status >= 300 {
			t.Errorf("%s: %d %s, want the route served", key, got.status, got.body)
		}
	}
	if len(client.calls) != len(ssoCRUD) {
		t.Fatalf("auth-service calls %v, want %d", client.calls, len(ssoCRUD))
	}
	if asked := entitlement.take(); len(asked) != 0 {
		t.Fatalf("a process licence still read the org entitlement: %v", asked)
	}
}

// With the switch off the provider routes keep main's process-only gate: an entitled organization gets the same 402 and the
// entitlement is never read, so turning the switch back off (B-back) restores today's answers exactly.
func TestSSOProviderRoutesWithTheSwitchOffKeepTheProcessOnlyGate(t *testing.T) {
	withSSOLicence(t, false)
	entitlement := entitledProbeOrg()
	handler, minter := adminConfigServerScoped(t, nil, withBoundaryCallers, ssoRoutesWith(nil, entitlement, quietLogger()), nil)
	callers := boundaryCallers(minter)
	for _, key := range ssoCRUD {
		for _, class := range boundaryClasses {
			if got := ssoDo(handler, key, callers[class].token); got.status != http.StatusPaymentRequired || got.body != ssoUnlicensedBody {
				t.Errorf("%s / %s: %d %s, want the 402 %s", key, class, got.status, got.body, ssoUnlicensedBody)
			}
		}
	}
	if asked := entitlement.take(); len(asked) != 0 {
		t.Fatalf("the switch-off path read the org entitlement: %v", asked)
	}
}

// A failed entitlement read denies with the 402 and one ERROR line per request; it never admits.
func TestSSOProviderRoutesDenyWhenTheEntitlementReadFails(t *testing.T) {
	withSSOLicence(t, false)
	client := &ssoCellClient{}
	entitlement := &orgEntitlement{err: errors.New("entitlement store down")}
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	handler, minter := adminConfigServerScoped(t, nil, withBoundaryCallers, ssoRoutesWith(client, entitlement, logger), nil)
	admin := boundaryCallers(minter)["org admin"].token
	for _, key := range ssoCRUD {
		if got := ssoDo(handler, key, admin); got.status != http.StatusPaymentRequired || got.body != ssoUnlicensedBody {
			t.Errorf("%s: %d %s, want the 402 %s", key, got.status, got.body, ssoUnlicensedBody)
		}
	}
	if n := strings.Count(logs.String(), "api sso: org entitlement check failed; denying"); n != len(ssoCRUD) {
		t.Fatalf("%d entitlement failure lines, want %d:\n%s", n, len(ssoCRUD), logs.String())
	}
	if len(client.calls) != 0 {
		t.Fatalf("a failed entitlement read reached auth-service: %v", client.calls)
	}
}

// A token without an organization is asked about as the empty organization and refused, and a create body that names an
// entitled organization does not move the gate: the gate reads the token organization only.
func TestSSOProviderGateReadsTheTokenOrganizationOnly(t *testing.T) {
	withSSOLicence(t, false)
	client := &ssoCellClient{}
	other := "00000000-0000-4000-8000-0000000000e1"
	entitlement := &orgEntitlement{granted: map[string]bool{other: true, "": true}}
	handler, minter := adminConfigServerScoped(t, nil, withBoundaryCallers, ssoRoutesWith(client, entitlement, quietLogger()), nil)
	admin := boundaryCallers(minter)["org admin"].token
	got := ssoDoWith(handler, ssoCreate, admin, "", `{"name":"Acme","protocol":"oidc","org_id":"`+other+`","oidc_config":{"client_id":"c","client_secret":"s","issuer":"https://i.example"}}`)
	if got.status != http.StatusPaymentRequired || got.body != ssoUnlicensedBody {
		t.Errorf("body names an entitled org: %d %s, want the 402", got.status, got.body)
	}
	if asked := entitlement.take(); len(asked) != 1 || asked[0] != probeOrg.String() {
		t.Errorf("entitlement asked for %v, want the token organization %s", asked, probeOrg)
	}
	// A platform admin may name any org in X-Org-Id (the scope's org); the gate still reads the token's.
	request := httptest.NewRequest(http.MethodGet, "/api/v1/auth/sso/providers", nil)
	request.Header.Set("Authorization", "Bearer "+boundaryCallers(minter)["platform admin + org member"].token)
	request.Header.Set("X-Org-Id", other)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusPaymentRequired || recorder.Body.String() != ssoUnlicensedBody {
		t.Errorf("X-Org-Id names an entitled org: %d %s, want the 402", recorder.Code, recorder.Body)
	}
	if asked := entitlement.take(); len(asked) != 1 || asked[0] != probeOrg.String() {
		t.Errorf("X-Org-Id: entitlement asked for %v, want the token organization %s", asked, probeOrg)
	}
	noOrg := minter.mint(cellPlatformNoRow, map[string]any{"sid": sessionid.Wire(cellNoRowSID), "org_id": nil})
	entitlement.granted[""] = false
	for _, key := range ssoCRUD {
		if got := ssoDo(handler, key, noOrg); got.status != http.StatusPaymentRequired || got.body != ssoUnlicensedBody {
			t.Errorf("platform admin, token without an org, %s: %d %s, want the 402", key, got.status, got.body)
		}
	}
	if asked := entitlement.take(); len(asked) != len(ssoCRUD) || asked[0] != "" {
		t.Errorf("token without an org: entitlement asked for %v, want the empty organization per request", asked)
	}
	if len(client.calls) != 0 {
		t.Fatalf("a refused request reached auth-service: %v", client.calls)
	}
}

// Activate and deactivate have no licence gate (legacy router.py had none): an unentitled organization's admin is served and
// the entitlement is never read.
func TestSSOProviderActivateAndDeactivateTakeNoEntitlementGate(t *testing.T) {
	withSSOLicence(t, false)
	client := &ssoCellClient{}
	entitlement := &orgEntitlement{granted: map[string]bool{}}
	handler, minter := adminConfigServerScoped(t, nil, withBoundaryCallers, ssoRoutesWith(client, entitlement, quietLogger()), nil)
	admin := boundaryCallers(minter)["org admin"].token
	for _, key := range []string{"POST /api/v1/auth/sso/providers/{provider_id}/activate", "POST /api/v1/auth/sso/providers/{provider_id}/deactivate"} {
		if got := ssoDo(handler, key, admin); got.status != http.StatusOK {
			t.Errorf("%s: %d %s, want 200", key, got.status, got.body)
		}
	}
	if asked := entitlement.take(); len(asked) != 0 {
		t.Fatalf("activate/deactivate read the org entitlement: %v", asked)
	}
}

// With the namespaces enforced and the decision point refusing, the route's late check runs first and must see the caller: an
// entitled organization gets the decision point's refusal, an unentitled one the 402 (the legacy order).
func TestSSOProviderLateGateSeesTheCallerWhenTheDecisionPointRefuses(t *testing.T) {
	withSSOLicence(t, false)
	pdp := newCellPDP(t)
	for _, mode := range []string{"deny", "fail"} {
		for name, entitlement := range map[string]*orgEntitlement{"entitled": entitledProbeOrg(), "unentitled": {granted: map[string]bool{}}} {
			client := &ssoCellClient{}
			on, minter := adminConfigServerScoped(t, func(t *testing.T) *policy.Enforcer {
				return newCellEnforcer(t, pdp, ssoRowNamespaces, false)
			}, withBoundaryCallers, ssoRoutesWith(client, entitlement, quietLogger()), nil)
			admin := boundaryCallers(minter)["org admin"].token
			for _, key := range ssoCRUD {
				pdp.reset(mode)
				got := ssoDo(on, key, admin)
				want := http.StatusPaymentRequired
				if name == "entitled" {
					want = map[string]int{"deny": http.StatusForbidden, "fail": http.StatusServiceUnavailable}[mode]
				}
				if got.status != want || (want == http.StatusPaymentRequired && got.body != ssoUnlicensedBody) {
					t.Errorf("pdp %s / %s / %s: %d %s, want %d", mode, name, key, got.status, got.body, want)
				}
				if asked := entitlement.take(); len(asked) != 1 || asked[0] != probeOrg.String() {
					t.Errorf("pdp %s / %s / %s: entitlement asked for %v, want the token organization once", mode, name, key, asked)
				}
				t.Logf("CELL\tlate-under-refusal\t%s\t%s\t%s\t%d:%s", mode, name, key, got.status, short(got.body))
			}
			if len(client.calls) != 0 {
				t.Fatalf("pdp %s / %s: a refused request reached auth-service: %v", mode, name, client.calls)
			}
		}
	}
}

// Production wiring leaves the entitlement seam nil, so the routes read licensing.OrgHasFeature over the api pool.
func TestBuildDepsLeavesTheSSOEntitlementSeamNil(t *testing.T) {
	deps, components, err := buildDeps(context.Background(), config.Config{}, health.NewRegistry(time.Second), quietLogger(), AuthProxyConfig{}, false)
	closeComponents(components)
	if err != nil {
		t.Fatalf("buildDeps: %v", err)
	}
	if deps.SSOOrgEntitled != nil {
		t.Fatal("buildDeps set SSOOrgEntitled; production must read the org entitlement from the api pool")
	}
}
