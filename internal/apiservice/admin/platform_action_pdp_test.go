package admin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/pep"
	"github.com/full-chaos/dev-health-go/pep/pepfake"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp/pdptest"
)

const pdpBearer = "aaaa.bbbb.cccc-dddd-eeee-ffff"

// denyingFake allows what its rules allow and answers every other request with the decision point's deny. pepfake
// alone answers an unmatched request with a local no_decision, which go-api must treat as no verdict (503), so a deny
// case built on it would pass through the no-verdict path.
type denyingFake struct {
	rules *pepfake.Fake
	deny  pep.Decision
}

func newDenyingFake(t *testing.T, rules ...pepfake.Rule) denyingFake {
	t.Helper()
	pdp := pdptest.New(t)
	pdp.Deny("platform_role_absent")
	client, err := pep.New(pep.Config{BaseURL: pdp.URL, AllowPlainHTTP: true, HTTPClient: &http.Client{}, ClientService: "dev-health-ops-api", Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	deny := client.Decide(context.Background(), pep.Request{PrincipalReference: pdpBearer, Action: rule.ActionAdminUsers, ResourceType: "platform", ResourceKey: "platform"})
	if deny.Allowed() || deny.Local() {
		t.Fatalf("the test decision point's deny: allowed=%v local=%v reason=%s", deny.Allowed(), deny.Local(), deny.Reason())
	}
	return denyingFake{rules: pepfake.New(rules...), deny: deny}
}

func (f denyingFake) Decide(ctx context.Context, req pep.Request) pep.Decision {
	if d := f.rules.Decide(ctx, req); d.Allowed() || d.Reason() != pep.ReasonNoDecision {
		return d
	}
	return f.deny
}

func (f denyingFake) DecideBatch(ctx context.Context, reqs []pep.Request) []pep.Decision {
	out := make([]pep.Decision, len(reqs))
	for i, req := range reqs {
		out[i] = f.Decide(ctx, req)
	}
	return out
}

// pdpCtx is a request context with the admin cell on: platform questions go to a decision point that allows exactly
// the named actions and denies the rest; "bogus" makes every question a local (no verdict) decision.
func pdpCtx(t *testing.T, actions ...string) context.Context {
	t.Helper()
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
	return policy.WithPlatformDecider(context.Background(), newDenyingFake(t, rules...), pdpBearer)
}

// pdpCase is one answer of the decision point against go-api's own flag: the decision point wins.
type pdpCase struct {
	name      string
	allowed   []string
	superuser bool
	want      bool
	status    int
}

// statusWrong reports a status the case must not answer: its own status when it names one, else the no-verdict 503
// (a deny or an allow is a verdict).
func (c pdpCase) statusWrong(code int) bool {
	if c.status != 0 {
		return code != c.status
	}
	return code == http.StatusServiceUnavailable
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
		got := newHandlers(t).ensureUserInScope(pdpCtx(t, c.allowed...), w, user, uuid.New(), uuid.New())
		if got != c.want || c.statusWrong(w.Code) {
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
		if got := newHandlers(t).ensureOrgAdminAccess(pdpCtx(t, c.allowed...), w, user, uuid.New()); got || w.Code != http.StatusForbidden {
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
		_, got := orgIDForNonSuperuser(pdpCtx(t, c.allowed...), w, user)
		if got != c.want || c.statusWrong(w.Code) {
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
		org, got := newHandlers(t).createUserOrg(pdpCtx(t, c.allowed...), w, r, user)
		if got != c.want || (got && org != uuid.Nil) || c.statusWrong(w.Code) {
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
		h.listUsers(w, r.WithContext(policy.WithUser(pdpCtx(t, c.allowed...), user)))
		if got := *reads > 0; got != c.want || c.statusWrong(w.Code) {
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
		newHandlers(t).impersonationStatus(w, r.WithContext(policy.WithUser(pdpCtx(t, c.allowed...), user)))
		if w.Code != c.status || !strings.Contains(w.Body.String(), c.body) {
			t.Errorf("%s: %d %q, want %d %q", c.name, w.Code, w.Body.String(), c.status, c.body)
		}
	}
	// An allowed caller goes on to the session lookup: here its id is not a UUID, which that lookup refuses with 400.
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/impersonate/status", nil)
	user := &policy.User{ID: uuid.New(), UserID: "not-a-uuid"}
	newHandlers(t).impersonationStatus(w, r.WithContext(policy.WithUser(pdpCtx(t, rule.ActionAdminImpersonate), user)))
	if w.Code != http.StatusBadRequest {
		t.Errorf("pdp allows, flag off: %d %q, want the lookup's 400", w.Code, w.Body.String())
	}
}

// Where 6.10 meets the selection-only org rule: with the admin cell on, an admin of the token's org A who holds
// members.manage there, asking about the members of another org B named in the path, is refused, whatever go-api's
// own superuser flag says: no platform action stands in for an admin membership in B.
func TestPDPAction_TokenOrgAdminIsRefusedAnotherPathOrg(t *testing.T) {
	tokenOrg, pathOrg := uuid.New(), uuid.New()
	decider := pepfake.New(pepfake.Rule{PrincipalReference: pdpBearer, Action: "members.manage", ResourceType: "organization",
		ResourceKey: tokenOrg.String(), OrganizationID: tokenOrg.String(), TenantScope: tokenOrg.String()})
	for _, superuser := range []bool{false, true} {
		h, reads := newPlatformHandlers()
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/orgs/"+pathOrg.String()+"/members", nil)
		r.SetPathValue("org_id", pathOrg.String())
		user := &policy.User{ID: uuid.New(), UserID: uuid.NewString(), OrgID: tokenOrg.String(), Role: "admin", IsSuperuser: superuser}
		ctx := policy.WithUser(policy.WithPlatformDecider(context.Background(), decider, pdpBearer), user)
		h.listMembers(w, r.WithContext(ctx))
		if w.Code != http.StatusForbidden || *reads != 0 {
			t.Errorf("superuser %v: %d %q, store reached %d times; want 403 and no list read", superuser, w.Code, w.Body.String(), *reads)
		}
	}
}

// With the admin cell on, a live go-api impersonation row gets no platform action in the users handlers (D6714): even
// when the decision point would allow the admin's own admin.users, a user outside the org is not in scope and the
// caller's own org is not served; both answer 403 in an impersonation session.
func TestPDPAction_ImpersonationSessionGetsNoAdminUsers(t *testing.T) {
	session := &policy.Impersonation{ID: uuid.New(), AdminUserID: uuid.New(), TargetUserID: uuid.New(), TargetOrgID: uuid.New(),
		TargetRole: "member", ExpiresAt: time.Now().Add(time.Hour)}
	user := &policy.User{ID: uuid.New(), IsSuperuser: true}
	w := httptest.NewRecorder()
	ctx := policy.WithImpersonation(pdpCtx(t, rule.ActionAdminUsers), session)
	if got := newHandlers(t).ensureUserInScope(ctx, w, user, uuid.New(), uuid.New()); got || w.Code != http.StatusForbidden ||
		!strings.Contains(w.Body.String(), "impersonation session") {
		t.Errorf("user scope: in scope = %v, %d %q; want false and 403 in an impersonation session", got, w.Code, w.Body.String())
	}
	w = httptest.NewRecorder()
	if _, got := orgIDForNonSuperuser(ctx, w, user); got || w.Code != http.StatusForbidden || !strings.Contains(w.Body.String(), "impersonation session") {
		t.Errorf("no org context: served = %v, %d %q; want false and 403 in an impersonation session", got, w.Code, w.Body.String())
	}
}
