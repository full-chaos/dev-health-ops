package admin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/pep"
	"github.com/full-chaos/dev-health-go/pep/pepfake"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

const pdpBearer = "aaaa.bbbb.cccc-dddd-eeee-ffff"

// pdpCtx is a request context with the admin cell on: platform questions go to a decision point that allows exactly
// the named actions; "bogus" makes every question a local (no verdict) decision.
func pdpCtx(actions ...string) context.Context {
	var rules []pepfake.Rule
	for _, action := range actions {
		r := pepfake.Rule{PrincipalReference: pdpBearer, Action: action, ResourceType: "platform", ResourceKey: "platform"}
		if action == "bogus" {
			for _, a := range rule.PlatformActions {
				rules = append(rules, pepfake.Rule{PrincipalReference: pdpBearer, Action: a, ResourceType: "platform", ResourceKey: "platform", MinimumAssurance: "bogus"})
			}
			continue
		}
		rules = append(rules, r)
	}
	var decider pep.Decider = pepfake.New(rules...)
	return policy.WithPlatformDecider(context.Background(), decider, pdpBearer)
}

// pdpCase is one answer of the decision point against go-api's own flag: the decision point wins.
type pdpCase struct {
	name      string
	allowed   []string
	superuser bool
	want      bool
	status    int
}

func pdpCases(action string) []pdpCase {
	return []pdpCase{
		{"pdp allows, flag off", []string{action}, false, true, 0},
		{"pdp denies, flag on", nil, true, false, 0},
		{"pdp allows another action, flag on", []string{rule.ActionAdminStats}, true, false, 0},
		{"no verdict", []string{"bogus"}, true, false, http.StatusServiceUnavailable},
	}
}

// Mix 3 with the admin cell on: a user outside the caller's org is in scope only when the decision point says the
// caller holds admin.users; no verdict is a 503.
func TestPDPAction_UserInScopeIsTheDecisionPointsAdminUsers(t *testing.T) {
	for _, c := range pdpCases(rule.ActionAdminUsers) {
		w := httptest.NewRecorder()
		user := &policy.User{ID: uuid.New(), IsSuperuser: c.superuser}
		got := newHandlers(t).ensureUserInScope(pdpCtx(c.allowed...), w, user, uuid.New(), uuid.New())
		if got != c.want || (c.status != 0 && w.Code != c.status) {
			t.Errorf("%s: in scope = %v (status %d), want %v", c.name, got, w.Code, c.want)
		}
	}
}

// Mix 3 with the admin cell on: no platform action stands in for an org admin membership (fail closed until the
// superuser cross-org question is answered), whatever the decision point and the flag say.
func TestPDPAction_OrgAdminAccessHasNoPlatformBypass(t *testing.T) {
	for _, c := range pdpCases(rule.ActionAdminOrgs) {
		w := httptest.NewRecorder()
		user := &policy.User{ID: uuid.New(), IsSuperuser: c.superuser}
		if got := newHandlers(t).ensureOrgAdminAccess(pdpCtx(c.allowed...), w, user, uuid.New()); got || w.Code != http.StatusForbidden {
			t.Errorf("%s: access = %v (status %d), want false and 403", c.name, got, w.Code)
		}
	}
}

// Mix 3 with the admin cell on: a caller with no org claim is served without an org context only when the decision
// point says admin.users.
func TestPDPAction_NoOrgContextIsTheDecisionPointsAdminUsers(t *testing.T) {
	for _, c := range pdpCases(rule.ActionAdminUsers) {
		w := httptest.NewRecorder()
		user := &policy.User{IsSuperuser: c.superuser}
		_, got := orgIDForNonSuperuser(pdpCtx(c.allowed...), w, user)
		if got != c.want || (c.status != 0 && w.Code != c.status) {
			t.Errorf("%s: served = %v (status %d), want %v", c.name, got, w.Code, c.want)
		}
	}
}

// Mix 8 with the admin cell on: a create with no X-Org-Id and no org claim is a platform create only when the
// decision point says admin.users.
func TestPDPAction_PlatformCreateIsTheDecisionPointsAdminUsers(t *testing.T) {
	for _, c := range pdpCases(rule.ActionAdminUsers) {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, "/users", nil)
		user := &policy.User{ID: uuid.New(), IsSuperuser: c.superuser}
		org, got := newHandlers(t).createUserOrg(pdpCtx(c.allowed...), w, r, user)
		if got != c.want || (got && org != uuid.Nil) || (c.status != 0 && w.Code != c.status) {
			t.Errorf("%s: org %s ok %v (status %d), want ok %v", c.name, org, got, w.Code, c.want)
		}
	}
}

// Mix 8 with the admin cell on: the all-users list reaches the store only when the decision point says admin.users.
func TestPDPAction_ListAllUsersIsTheDecisionPointsAdminUsers(t *testing.T) {
	for _, c := range pdpCases(rule.ActionAdminUsers) {
		h, reads := newPlatformHandlers()
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/users", nil)
		user := &policy.User{ID: uuid.New(), IsSuperuser: c.superuser}
		h.listUsers(w, r.WithContext(policy.WithUser(pdpCtx(c.allowed...), user)))
		if got := *reads > 0; got != c.want || (c.status != 0 && w.Code != c.status) {
			t.Errorf("%s: store reached = %v (status %d), want %v", c.name, got, w.Code, c.want)
		}
	}
}

// With the admin cell on, the impersonation status looks a session up only when the decision point says
// admin.impersonate: a denied caller is answered "not impersonating" whatever its flag; no verdict is a 503.
func TestPDPAction_ImpersonationStatusIsTheDecisionPointsAdminImpersonate(t *testing.T) {
	for _, c := range []struct {
		name      string
		allowed   []string
		superuser bool
		status    int
		body      string
	}{
		{"pdp denies, flag on", nil, true, http.StatusOK, `"is_impersonating":false`},
		{"no verdict", []string{"bogus"}, true, http.StatusServiceUnavailable, ""},
	} {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/impersonate/status", nil)
		user := &policy.User{ID: uuid.New(), UserID: "not-a-uuid", IsSuperuser: c.superuser}
		newHandlers(t).impersonationStatus(w, r.WithContext(policy.WithUser(pdpCtx(c.allowed...), user)))
		if w.Code != c.status || !strings.Contains(w.Body.String(), c.body) {
			t.Errorf("%s: %d %q, want %d %q", c.name, w.Code, w.Body.String(), c.status, c.body)
		}
	}
	// An allowed caller goes on to the session lookup: here its id is not a UUID, which that lookup refuses with 400.
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/impersonate/status", nil)
	user := &policy.User{ID: uuid.New(), UserID: "not-a-uuid"}
	newHandlers(t).impersonationStatus(w, r.WithContext(policy.WithUser(pdpCtx(rule.ActionAdminImpersonate), user)))
	if w.Code != http.StatusBadRequest {
		t.Errorf("pdp allows, flag off: %d %q, want the lookup's 400", w.Code, w.Body.String())
	}
}
