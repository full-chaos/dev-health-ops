package policy

import (
	"maps"
	"net/http"
	"net/http/httptest"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

// platformAdminRoute is how the admin package mounts a platform-admin row: the platform action of its wrap and
// whether the body is read first. TestPlatformAction_AdminRoutesAreWrappedByTheirAction (apiservice/admin) pins the
// action of each real mount; this table must equal the production profile's platform-admin rows.
type platformAdminRoute struct {
	action    string
	bodyFirst bool
}

var platformAdminRoutes = map[string]platformAdminRoute{
	"GET /api/v1/admin/orgs":                                               {rule.ActionAdminOrgs, false},
	"GET /api/v1/admin/orgs/{org_id}":                                      {rule.ActionAdminOrgs, false},
	"POST /api/v1/admin/orgs":                                              {rule.ActionAdminOrgs, true},
	"PATCH /api/v1/admin/orgs/{org_id}":                                    {rule.ActionAdminOrgs, true},
	"DELETE /api/v1/admin/orgs/{org_id}":                                   {rule.ActionAdminOrgs, false},
	"GET /api/v1/admin/feature-flags":                                      {rule.ActionAdminFlags, false},
	"PATCH /api/v1/admin/feature-flags/{flag_id}":                          {rule.ActionAdminFlags, true},
	"GET /api/v1/admin/orgs/{org_id}/feature-overrides":                    {rule.ActionAdminFlags, false},
	"POST /api/v1/admin/orgs/{org_id}/feature-overrides":                   {rule.ActionAdminFlags, true},
	"PATCH /api/v1/admin/orgs/{org_id}/feature-overrides/{override_id}":    {rule.ActionAdminFlags, true},
	"DELETE /api/v1/admin/orgs/{org_id}/feature-overrides/{override_id}":   {rule.ActionAdminFlags, false},
	"GET /api/v1/admin/platform/audit-logs":                                {rule.ActionAdminAudit, false},
	"GET /api/v1/admin/platform/stats":                                     {rule.ActionAdminStats, false},
	"POST /api/v1/admin/impersonate":                                       {rule.ActionAdminImpersonate, true},
	"POST /api/v1/admin/impersonate/stop":                                  {rule.ActionAdminImpersonate, false},
	"POST /api/v1/admin/impersonate/handoff/consume":                       {rule.ActionAdminImpersonate, true},
	"POST /api/v1/telemetry/report":                                        {rule.ActionAdminTelemetry, false},
	"GET /api/v1/admin/service-accounts/{service_account_id}/keys":         {rule.ActionAdminUsers, false},
	"POST /api/v1/admin/service-accounts/{service_account_id}/keys":        {rule.ActionAdminUsers, false},
	"POST /api/v1/admin/service-accounts/{service_account_id}/keys/rotate": {rule.ActionAdminUsers, false},
	"POST /api/v1/admin/service-accounts/keys/{credential_id}/revoke":      {rule.ActionAdminUsers, false},
}

// adminOrUsersRoutes are the /admin/users rows: mounted WrapAdminOr(admin.users) (an org owner or admin passes too),
// with whether the body is read first. TestPlatformAction_AdminRoutesAreWrappedByTheirAction (apiservice/admin) pins
// the real mounts.
var adminOrUsersRoutes = map[string]platformAdminRoute{
	"GET /api/v1/admin/users":                     {rule.ActionAdminUsers, false},
	"GET /api/v1/admin/users/{user_id}":           {rule.ActionAdminUsers, false},
	"POST /api/v1/admin/users":                    {rule.ActionAdminUsers, true},
	"PATCH /api/v1/admin/users/{user_id}":         {rule.ActionAdminUsers, true},
	"POST /api/v1/admin/users/{user_id}/password": {rule.ActionAdminUsers, true},
	"DELETE /api/v1/admin/users/{user_id}":        {rule.ActionAdminUsers, false},
}

// platformAdminGapRoutes are the platform-admin rows whose profile row names no action yet, with the guard level they
// are mounted at. With the admin cell on, the decision point does not judge them.
var platformAdminGapRoutes = map[string]Authz{
	"POST /api/v1/telemetry/opt-in":                                    Authenticated,
	"POST /api/v1/admin/service-accounts":                              Superuser,
	"POST /api/v1/admin/service-accounts/{service_account_id}/disable": Superuser,
	"POST /api/v1/admin/service-accounts/{service_account_id}/enable":  Superuser,
	"POST /api/v1/admin/service-accounts/{service_account_id}/owner":   Superuser,
}

// productionGoAPIRows is the guard's action map as go-api builds it (apiservice/enforcer.go).
func productionGoAPIRows(t *testing.T) map[string]ShadowRow {
	t.Helper()
	rows, err := ParseShadowRows(authv1.GoAPIEndpointProfiles, "dev-health-ops-api")
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

// The cell tables cover exactly the production profile's platform-admin rows: every admin-namespace row except the
// admin.billing rows (6.9), each with the action of its mount (the platform wraps and the admin-or /admin/users rows).
func TestPlatformAdminCellsCoverTheProductionPlatformRows(t *testing.T) {
	rows := productionGoAPIRows(t)
	got := map[string]string{}
	for pattern, row := range rows {
		if row.Namespace == "admin" && row.Action != rule.ActionAdminBilling {
			got[pattern] = row.Action
			if row.ResourceType != "platform" {
				t.Errorf("%s: resource type %q, want platform", pattern, row.ResourceType)
			}
		}
	}
	want := map[string]string{}
	for pattern, route := range platformAdminRoutes {
		want[pattern] = route.action
	}
	for pattern, route := range adminOrUsersRoutes {
		want[pattern] = route.action
	}
	if !maps.Equal(got, want) {
		t.Fatalf("production platform-admin rows %v, cell table %v", got, want)
	}
	for pattern := range platformAdminGapRoutes {
		if _, ok := rows[pattern]; ok {
			t.Errorf("%s now names an action: move it from platformAdminGapRoutes into platformAdminRoutes", pattern)
		}
	}
}

var pathParam = regexp.MustCompile(`\{[a-z_]+\}`)

// concretePath fills a route template: {org_id} is targetOrgID (an org the caller has no membership in), any other
// parameter a fixed id.
func concretePath(pattern string) (method, path string) {
	method, template, _ := strings.Cut(pattern, " ")
	path = pathParam.ReplaceAllStringFunc(template, func(p string) string {
		if p == "{org_id}" {
			return targetOrgID.String()
		}
		return "99999999-9999-4999-8999-999999999999"
	})
	return method, path
}

func platformAdminMux(guard *Guard) *http.ServeMux {
	mux := http.NewServeMux()
	for pattern, route := range platformAdminRoutes {
		if route.bodyFirst {
			mux.Handle(pattern, guard.BodyFirstPlatform(route.action, echo))
		} else {
			mux.Handle(pattern, guard.WrapPlatform(route.action, echo))
		}
	}
	for pattern, route := range adminOrUsersRoutes {
		if route.bodyFirst {
			mux.Handle(pattern, guard.BodyFirstAdminOr(route.action, echo))
		} else {
			mux.Handle(pattern, guard.WrapAdminOr(route.action, echo))
		}
	}
	for pattern, level := range platformAdminGapRoutes {
		if level == Superuser {
			// The service account lifecycle rows: WrapPlatform(admin.users), as admin/serviceaccounts.go mounts them.
			mux.Handle(pattern, guard.WrapPlatform(rule.ActionAdminUsers, echo))
			continue
		}
		mux.Handle(pattern, guard.Wrap(level, echo))
	}
	return mux
}

// serveCell sends one request; a body-first row gets an empty JSON object.
func serveCell(h http.Handler, pattern string, headers map[string]string) response {
	method, path := concretePath(pattern)
	var request *http.Request
	if method == http.MethodPost || method == http.MethodPatch {
		request = httptest.NewRequest(method, path, strings.NewReader("{}"))
		request.Header.Set("Content-Type", "application/json")
	} else {
		request = httptest.NewRequest(method, path, nil)
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, request)
	return response{recorder.Code, recorder.Body.String(), recorder.Header()}
}

func cellBearer() map[string]string {
	return map[string]string{"Authorization": "Bearer " + shadowBearer}
}

// pdpState is the principal as the decision point's state reads it.
type pdpState struct {
	status authorization.Status
	role   string
	member bool
}

func (s pdpState) PrincipalStatus() authorization.Status { return s.status }
func (s pdpState) PlatformRole() string                  { return s.role }
func (s pdpState) OrganizationStatus(string) authorization.Status {
	return authorization.StatusActive
}
func (s pdpState) Membership(org string) (authorization.Membership, bool) {
	if !s.member {
		return authorization.Membership{}, false
	}
	return authorization.Membership{OrgID: org, Role: "admin", Status: authorization.StatusActive}, true
}

// evaluatorVerdict answers as auth-service does: the production evaluator over state, with the platform question
// mapped as authorize.go maps it (a platform resource carries no key and no organization).
func evaluatorVerdict(state pdpState) func(string) (bool, string) {
	evaluator := authorization.NewEvaluator()
	return func(action string) (bool, string) {
		d := evaluator.Evaluate(authorization.Request{Action: action, ResourceKind: "platform"}, state)
		return d.Allowed, string(d.Reason)
	}
}

// legacyStore is the caller as go-api's own state reads it: superuser or not, and its role in its token's org ("" is
// no membership there).
func legacyStore(superuser bool, role string) *accessStore {
	store := newAccessStore()
	store.users[userID] = UserState{IsActive: true, IsSuperuser: superuser}
	if role == "" {
		delete(store.members, [2]uuid.UUID{userID, ownOrgID})
		delete(store.roles, [2]uuid.UUID{userID, ownOrgID})
	} else {
		store.roles[[2]uuid.UUID{userID, ownOrgID}] = role
	}
	return store
}

// orglessAccessClaims is a user access token with no organization claim.
func orglessAccessClaims() *RoutedResult {
	return userClaims(func(c *AccessTokenClaims) { c.OrgID = nil })
}

func cellHandler(t *testing.T, store *accessStore, claims *RoutedResult, enf *Enforcer) http.Handler {
	t.Helper()
	guard := NewGuard(routed(t, store, &fakeValidator{result: claims}), quiet())
	guard.SetEnforcer(enf)
	return platformAdminMux(guard)
}

func productionEnforcer(t *testing.T, client *testPDP, namespaces ...string) *Enforcer {
	t.Helper()
	enf, err := NewEnforcer(EnforceConfig{Decider: client.client(t), Rows: productionGoAPIRows(t), Switch: onFor(namespaces...), Logger: quiet()})
	if err != nil {
		t.Fatal(err)
	}
	return enf
}

const superuserRefusalBody = `{"detail":"Superuser access required"}`

// Each platform-admin row, with the admin cell on, takes the decision point's verdict: the production evaluator
// allows a platform_admin (with or without a membership anywhere) and denies everyone else, whatever go-api's own
// state says (D6659: the two sources may disagree in a mixed window; the decision point wins). The question is a
// platform one: no organization, the platform key.
func TestPlatformAdminRowsTakeThePDPVerdict(t *testing.T) {
	personas := []struct {
		name      string
		pdp       pdpState
		superuser bool
		role      string
		claims    func() *RoutedResult
		want      int
	}{
		{"platform_admin", pdpState{authorization.StatusActive, rule.PlatformAdmin, true}, true, "admin", nil, http.StatusOK},
		{"platform_admin_orgless_token", pdpState{authorization.StatusActive, rule.PlatformAdmin, false}, true, "", orglessAccessClaims, http.StatusOK},
		{"org_admin", pdpState{authorization.StatusActive, "", true}, false, "admin", nil, http.StatusForbidden},
		{"member", pdpState{authorization.StatusActive, "", true}, false, "member", nil, http.StatusForbidden},
		{"platform_owner_only", pdpState{authorization.StatusActive, rule.PlatformOwner, true}, false, "owner", nil, http.StatusForbidden},
		{"suspended_platform_admin", pdpState{authorization.StatusSuspended, rule.PlatformAdmin, true}, true, "admin", nil, http.StatusForbidden},
		{"pdp_admin_legacy_not", pdpState{authorization.StatusActive, rule.PlatformAdmin, true}, false, "admin", nil, http.StatusOK},
		{"legacy_admin_pdp_not", pdpState{authorization.StatusActive, "", true}, true, "admin", nil, http.StatusForbidden},
	}
	for pattern, route := range platformAdminRoutes {
		for _, p := range personas {
			t.Run(pattern+"/"+p.name, func(t *testing.T) {
				rec := recordEnforce(t)
				pdp := newTestPDP(t, evaluatorVerdict(p.pdp))
				claims := userClaims(nil)
				if p.claims != nil {
					claims = p.claims()
				}
				got := serveCell(cellHandler(t, legacyStore(p.superuser, p.role), claims, productionEnforcer(t, pdp, "admin")), pattern,
					map[string]string{"Authorization": "Bearer " + shadowBearer, "X-Org-Id": targetOrgID.String()})
				if got.status != p.want || (p.want == http.StatusForbidden && got.body != superuserRefusalBody) {
					t.Fatalf("%d %q, want %d", got.status, got.body, p.want)
				}
				calls := pdp.calls()
				want := pdpSeen{Principal: shadowBearer, Action: route.action, Type: "platform", Key: "platform", Service: "dev-health-ops-api"}
				if len(calls) != 1 || calls[0] != want {
					t.Fatalf("PDP saw %+v, want one platform question %+v (no organization)", calls, want)
				}
				outcome := "admin/allow"
				if p.want != http.StatusOK {
					outcome = "admin/deny"
				}
				if !slices.Equal(rec.snapshot(), []string{outcome}) {
					t.Fatalf("counted %v, want [%s]", rec.snapshot(), outcome)
				}
			})
		}
	}
}

// A live go-api impersonation row is refused on every platform-admin row before any decision point call (12735,
// D6714: platform actions stay refused); the live-row exempt routes (stop, handoff consume) alone ask the decision
// point, as the admin.
func TestPlatformAdminRowsRefuseImpersonationExceptStop(t *testing.T) {
	for pattern := range platformAdminRoutes {
		t.Run(pattern, func(t *testing.T) {
			rec := recordEnforce(t)
			pdp := newTestPDP(t, allowAll)
			h := impersonating(cellHandler(t, legacyStore(true, "admin"), userClaims(nil), productionEnforcer(t, pdp, "admin")))
			got := serveCell(h, pattern, cellBearer())
			if liveRowExempt(pattern) {
				if got.status != http.StatusOK || len(pdp.calls()) != 1 || !slices.Equal(rec.snapshot(), []string{"admin/allow"}) {
					t.Fatalf("exempt row while impersonating: %d %q, %d PDP calls, counted %v; want 200, 1, [admin/allow]", got.status, got.body, len(pdp.calls()), rec.snapshot())
				}
				return
			}
			if got.status != http.StatusForbidden || got.body != impersonationRefusalBody || len(pdp.calls()) != 0 {
				t.Fatalf("%d %q, %d PDP calls; want 403 %q and 0", got.status, got.body, len(pdp.calls()), impersonationRefusalBody)
			}
			if !slices.Equal(rec.snapshot(), []string{"admin/impersonation_refused"}) {
				t.Fatalf("counted %v, want [admin/impersonation_refused]", rec.snapshot())
			}
		})
	}
}

// A delegated session's bearer (act claim) is refused on every platform-admin row, with no decision point call.
func TestPlatformAdminRowsRefuseADelegatedBearer(t *testing.T) {
	actor := "prn_" + wireID(actorID)
	for pattern := range platformAdminRoutes {
		t.Run(pattern, func(t *testing.T) {
			pdp := newTestPDP(t, allowAll)
			claims := userClaims(func(c *AccessTokenClaims) { c.Actor = &actor })
			got := serveCell(cellHandler(t, legacyStore(true, "admin"), claims, productionEnforcer(t, pdp, "admin")), pattern, cellBearer())
			if got.status != http.StatusForbidden || got.body != delegatedRefusal || len(pdp.calls()) != 0 {
				t.Fatalf("%d %q, %d PDP calls; want 403 %q and 0", got.status, got.body, len(pdp.calls()), delegatedRefusal)
			}
		})
	}
}

// No verdict is never the legacy answer: a dead decision point, an allow that carries an obligation, and a go-api with
// no decision point client each refuse a legacy-allowed platform admin with a 503.
func TestPlatformAdminRowsFailClosedWithoutAVerdict(t *testing.T) {
	for pattern := range platformAdminRoutes {
		t.Run(pattern, func(t *testing.T) {
			dead := newTestPDP(t, allowAll)
			deadEnforcer := productionEnforcer(t, dead, "admin")
			dead.srv.Close()
			got := serveCell(cellHandler(t, legacyStore(true, "admin"), userClaims(nil), deadEnforcer), pattern, cellBearer())
			if got.status != http.StatusServiceUnavailable || got.header.Get("Retry-After") != enforceRetryAfter {
				t.Fatalf("dead PDP: %d %q Retry-After %q, want 503 and %q", got.status, got.body, got.header.Get("Retry-After"), enforceRetryAfter)
			}
			obligated := newTestPDP(t, allowAll)
			obligated.obligations = map[string]any{"tenant_scope": ownOrg}
			got = serveCell(cellHandler(t, legacyStore(true, "admin"), userClaims(nil), productionEnforcer(t, obligated, "admin")), pattern, cellBearer())
			if got.status != http.StatusServiceUnavailable {
				t.Fatalf("allow with an obligation: %d %q, want 503", got.status, got.body)
			}
			unconfigured, err := NewEnforcer(EnforceConfig{Unconfigured: true, Rows: productionGoAPIRows(t), Switch: onFor("admin"), Logger: quiet()})
			if err != nil {
				t.Fatal(err)
			}
			got = serveCell(cellHandler(t, legacyStore(true, "admin"), userClaims(nil), unconfigured), pattern, cellBearer())
			if got.status != http.StatusServiceUnavailable || got.body != enforceNoDecisionPointBody {
				t.Fatalf("no decision point: %d %q, want 503 %q", got.status, got.body, enforceNoDecisionPointBody)
			}
		})
	}
}

// With the admin cell off the decision point is never asked and the legacy answer stands, byte for byte.
func TestPlatformAdminRowsKeepTheLegacyAnswerWithTheCellOff(t *testing.T) {
	for pattern := range platformAdminRoutes {
		t.Run(pattern, func(t *testing.T) {
			for _, superuser := range []bool{true, false} {
				base := serveCell(cellHandler(t, legacyStore(superuser, "admin"), userClaims(nil), nil), pattern, cellBearer())
				pdp := newTestPDP(t, denyWith("platform_role_absent"))
				got := serveCell(cellHandler(t, legacyStore(superuser, "admin"), userClaims(nil), productionEnforcer(t, pdp, "teams")), pattern, cellBearer())
				if got.status != base.status || got.body != base.body || len(pdp.calls()) != 0 {
					t.Fatalf("superuser %v, admin cell off: %d %q, %d PDP calls; want the legacy %d %q and 0", superuser, got.status, got.body, len(pdp.calls()), base.status, base.body)
				}
				want := http.StatusForbidden
				if superuser {
					want = http.StatusOK
				}
				if base.status != want {
					t.Fatalf("legacy answer for superuser %v: %d %q, want %d", superuser, base.status, base.body, want)
				}
			}
		})
	}
}

// Rule 1 parity yardstick (product rules, D6773): a platform admin on a platform-admin row gets the answer the legacy
// HS256 path gave at base ad73c1cc1 with the admin cell on: 200, the route reached as the caller (logs/13154-legacy-b-1.log
// 833044a56374). The routed caller is the same principal (superuser on its users row, role "member" in its token's org)
// and is judged by the production evaluator over a platform_admin state, as auth-service judges it. HS256 skipped the
// decision point; a users-row superuser without an auth platform role is held in CHAOS-13209.
const legacyPlatformAdminBody = "org= user=11111111-1111-4111-8111-111111111111 imp=-"

func TestPlatformAdminRowsKeepTheLegacyAnswerForARoutedPlatformAdmin(t *testing.T) {
	for pattern, route := range platformAdminRoutes {
		t.Run(pattern, func(t *testing.T) {
			pdp := newTestPDP(t, evaluatorVerdict(pdpState{authorization.StatusActive, rule.PlatformAdmin, true}))
			got := serveCell(cellHandler(t, legacyStore(true, "member"), userClaims(nil), productionEnforcer(t, pdp, "admin")), pattern, cellBearer())
			if got.status != http.StatusOK || got.body != legacyPlatformAdminBody {
				t.Fatalf("routed platform admin: %d %q; want the legacy 200 %q", got.status, got.body, legacyPlatformAdminBody)
			}
			want := pdpSeen{Principal: shadowBearer, Action: route.action, Type: "platform", Key: "platform", Service: "dev-health-ops-api"}
			if calls := pdp.calls(); len(calls) != 1 || calls[0] != want {
				t.Fatalf("PDP saw %+v, want one platform question %+v", calls, want)
			}
		})
	}
}

// The stand-in rows with no action (POST /api/v1/telemetry/opt-in, the route of the former telemetry report stand-in, whose
// handler asks admin.telemetry) are not decided by a route question with the admin cell on: a superuser is answered by
// main's checks with no question. The telemetry report row has its own action, mounted WrapPlatformDeny: it is in
// platformAdminRoutes here, and its cells on the real handler are in internal/apiservice.
func TestPlatformAdminGapRowsKeepMainsAnswerAndAskTheWrapAction(t *testing.T) {
	for pattern, level := range platformAdminGapRoutes {
		t.Run(pattern, func(t *testing.T) {
			rec := recordEnforce(t)
			pdp := newTestPDP(t, denyWith("platform_role_absent"))
			got := serveCell(cellHandler(t, legacyStore(true, "member"), userClaims(nil), productionEnforcer(t, pdp, "admin", "members")), pattern, cellBearer())
			if level == Superuser {
				// A WrapPlatform gap row (the service account lifecycle rows): no route question, but the platform gate asks
				// the decision point its own admin.users once, so a decision point that denies refuses a local superuser.
				// The guard obeys a PDP deny (no fall-through). See
				// TestServiceAccountKeyRowsAdminCellOnSuperuserWithoutMembershipIs200: the real evaluator allows superusers.
				if got.status != http.StatusForbidden || len(pdp.calls()) != 1 || len(rec.snapshot()) != 0 {
					t.Fatalf("superuser, PDP deny: %d %q, %d PDP calls, counted %v; want 403 after the one admin.users question", got.status, got.body, len(pdp.calls()), rec.snapshot())
				}
			} else if got.status != http.StatusOK || len(pdp.calls()) != 0 || len(rec.snapshot()) != 0 {
				t.Fatalf("superuser: %d %q, %d PDP calls, counted %v; want main's 200 with 0 calls", got.status, got.body, len(pdp.calls()), rec.snapshot())
			}
		})
	}
}

// GAP (flips in 10863 PR-1; Q1 fail closed): the org scope lets a superuser with no membership select another org by
// X-Org-Id from go-api's own platform actions (scope.go mayUseOrg), with the admin cell on and a decision point that
// denies admin.orgs. Nothing asks the decision point.
func TestPlatformAdminGapOrgScopeSelectsAnOrgByTheLocalFlag(t *testing.T) {
	rec := recordEnforce(t)
	pdp := newTestPDP(t, denyWith("platform_role_absent"))
	store := legacyStore(true, "member")
	auth := routed(t, store, &fakeValidator{result: userClaims(nil)})
	guard := NewGuard(auth, quiet())
	guard.SetEnforcer(productionEnforcer(t, pdp, "admin"))
	scope := NewScope(auth, quiet())
	mux := http.NewServeMux()
	mux.Handle("GET /api/v1/telemetry/status", guard.Wrap(Authenticated, echo))
	got := serve(scope.Principal(scope.OrgScope(mux)), http.MethodGet, "/api/v1/telemetry/status",
		map[string]string{"Authorization": "Bearer " + shadowBearer, "X-Org-Id": targetOrgID.String()})
	if got.status != http.StatusOK || !strings.Contains(got.body, "org="+targetOrgID.String()) || len(pdp.calls()) != 0 || len(rec.snapshot()) != 0 {
		t.Fatalf("GAP pin: %d %q, %d PDP calls, counted %v; today the scope takes the foreign org with 0 calls", got.status, got.body, len(pdp.calls()), rec.snapshot())
	}
}

// Product rule 1 (D6773): a platform admin whose access token names an org it holds no membership row in gets, on
// every platform-admin row, the answer its HS256 token gave for the same org, and is decided by the decision point.
// The HS256 answer is the executed legacy run on main 98c7d4f56 (log 13154-legacy-rec-98c7d4f5.log sha256
// 8ba2f74cdd64, LEGACY-REC lines): 200 "org= user=<id> imp=-" on every row, 0 decision-point calls.
func TestPlatformAdminRowsAnswerAnOrgClaimWithNoMembershipAsLegacy(t *testing.T) {
	for pattern := range platformAdminRoutes {
		t.Run(pattern, func(t *testing.T) {
			routedPDP := newTestPDP(t, allowAll)
			store := legacyStore(true, "")
			got := serveCell(cellHandler(t, store, userClaims(nil), productionEnforcer(t, routedPDP, "admin")), pattern, cellBearer())
			// The HS256 class never asked the decision point; the user access token, like the org-less one, does.
			if got.status != http.StatusOK || got.body != legacyPlatformAdminBody || len(routedPDP.calls()) == 0 {
				t.Fatalf("user access token %d %q (%d PDP calls), HS256 200 %q (0 PDP calls)", got.status, got.body, len(routedPDP.calls()), legacyPlatformAdminBody)
			}
		})
	}
}
