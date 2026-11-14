package policy

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

// CHAOS-13594 (10872 S1): a token with no organization on an organization row. The legacy guard admits that caller only
// when it holds admin.orgs (Guard.wrap's org-less check; a plain wrap's org-less action), and an AdminOrg row refuses it
// whoever it is. With the row's cell on, the decision point is asked that one platform action, never the row's
// organization action; an AdminOrg row refuses before any question.

const orglessOCRBody = `{"detail":"Organization context required"}`

// orglessOrgNamespaces are the namespaces of every organization row of the production profile.
var orglessOrgNamespaces = []string{"acr", "billing", "integrations", "license", "members", "org", "settings", "sso", "teams"}

type orglessOrgRow struct {
	pattern string
	level   Authz
	row     ShadowRow
}

// orglessOrgRows is every organization row of the production profile with the guard level its profile row names
// (primary_validator), read from the profile and the action map, never typed.
func orglessOrgRows(t *testing.T) []orglessOrgRow {
	t.Helper()
	var document struct {
		Rows []struct {
			Service   string `json:"service"`
			Method    string `json:"method"`
			Route     string `json:"route"`
			Validator struct {
				Description string `json:"description"`
			} `json:"primary_validator"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(authv1.GoAPIEndpointProfiles, &document); err != nil {
		t.Fatal(err)
	}
	levels := map[string]string{}
	for _, r := range document.Rows {
		if r.Service == "dev-health-ops-api" {
			level, _, _ := strings.Cut(r.Validator.Description, ":")
			levels[r.Method+" "+r.Route] = level
		}
	}
	var out []orglessOrgRow
	for pattern, row := range productionGoAPIRows(t) {
		if row.ResourceType != policycatalog.ScopeOrganization {
			continue
		}
		var level Authz
		switch levels[pattern] {
		case "policy.Admin":
			level = Admin
		case "policy.AdminOrg":
			level = AdminOrg
		case "policy.Authenticated":
			level = Authenticated
		default:
			t.Fatalf("%s: profile level %q, want Admin, AdminOrg or Authenticated", pattern, levels[pattern])
		}
		if !slices.Contains(orglessOrgNamespaces, row.Namespace) {
			t.Fatalf("%s: namespace %q is not in orglessOrgNamespaces", pattern, row.Namespace)
		}
		out = append(out, orglessOrgRow{pattern, level, row})
	}
	slices.SortFunc(out, func(a, b orglessOrgRow) int { return strings.Compare(a.pattern, b.pattern) })
	return out
}

// orglessEcho reports what the handler acts on: the user, its token org, its role and the request org.
var orglessEcho = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	user := UserFrom(r.Context())
	_, _ = io.WriteString(w, "user="+user.UserID+" org="+user.OrgID+" role="+user.Role+" scope="+OrgIDFrom(r.Context()))
})

type orglessCaller struct {
	name  string
	state UserState
	// holds: the caller holds admin.orgs by go-api's own state (the legacy answer).
	holds bool
	// memberRole is the caller's role in ownOrg ("" = no membership row anywhere).
	memberRole string
}

func platformPtr(p rule.Platform) *rule.Platform { return &p }

var orglessCallers = []orglessCaller{
	{"platform admin by the local flag, no membership", UserState{IsActive: true, IsSuperuser: true}, true, ""},
	{"platform admin by a platform set, no membership", UserState{IsActive: true, Platform: platformPtr(rule.PlatformOf(rule.ActionAdminOrgs))}, true, ""},
	{"platform role without admin.orgs", UserState{IsActive: true, Platform: platformPtr(rule.PlatformOf(rule.ActionAdminUsers))}, false, ""},
	{"fresh user, no membership", UserState{IsActive: true}, false, ""},
	{"org admin elsewhere, org-less token", UserState{IsActive: true}, false, "admin"},
}

func orglessStore(c orglessCaller) *accessStore {
	store := legacyStore(false, c.memberRole)
	store.users[userID] = c.state
	return store
}

// orglessMux mounts every organization row at its profile level over orglessEcho.
func orglessMux(t *testing.T, store *accessStore, claims *RoutedResult, enf *Enforcer, rows []orglessOrgRow, delegated bool, next http.Handler) http.Handler {
	t.Helper()
	auth := routed(t, store, &fakeValidator{result: claims})
	if delegated {
		auth.DelegatedRows(func(string) bool { return true })
	}
	guard := NewGuard(auth, quiet())
	guard.SetEnforcer(enf)
	mux := http.NewServeMux()
	for _, row := range rows {
		mux.Handle(row.pattern, guard.Wrap(row.level, next))
	}
	return mux
}

// orglessTargets are the request paths of a row: its concrete path, and for a query row also the form that names
// targetOrgID in org_id.
func orglessTargets(row orglessOrgRow) []string {
	_, path := concretePath(row.pattern)
	if row.row.OrgFromQuery {
		return []string{path, path + "?org_id=" + targetOrgID.String()}
	}
	return []string{path}
}

func orglessServe(h http.Handler, pattern, path string) response {
	method, _, _ := strings.Cut(pattern, " ")
	var request *http.Request
	if method == http.MethodPost || method == http.MethodPatch || method == http.MethodPut {
		request = httptest.NewRequest(method, path, strings.NewReader("{}"))
		request.Header.Set("Content-Type", "application/json")
	} else {
		request = httptest.NewRequest(method, path, nil)
	}
	request.Header.Set("Authorization", "Bearer "+shadowBearer)
	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, request)
	return response{recorder.Code, recorder.Body.String(), recorder.Header()}
}

// orglessPDP answers admin.orgs with allowAdminOrgs and denies every other action.
func orglessPDP(t *testing.T, allowAdminOrgs bool) *testPDP {
	t.Helper()
	return newTestPDP(t, func(action string) (bool, string) {
		if action == rule.ActionAdminOrgs && allowAdminOrgs {
			return true, "allowed_by_role"
		}
		return false, "platform_role_absent"
	})
}

func deadPDP(t *testing.T) *testPDP {
	t.Helper()
	pdp := newTestPDP(t, allowAll)
	pdp.srv.Close()
	return pdp
}

func orglessSwitches() map[string][]string {
	return map[string][]string{
		"organization cells on":           orglessOrgNamespaces,
		"organization and admin cells on": append(slices.Clone(orglessOrgNamespaces), "admin"),
	}
}

// The census the cells below run over: 137 organization rows, 70 Admin, 50 AdminOrg, 17 Authenticated, and none of them
// is an org-less route (OrglessMayReach), so the org-less check applies to every one.
func TestOrglessOrgRowsCensus(t *testing.T) {
	counts := map[Authz]int{}
	rows := orglessOrgRows(t)
	for _, row := range rows {
		counts[row.level]++
		method, path := concretePath(row.pattern)
		if OrglessMayReach(method, path) {
			t.Errorf("%s is an org-less route", row.pattern)
		}
	}
	if len(rows) != 137 || counts[Admin] != 70 || counts[AdminOrg] != 50 || counts[Authenticated] != 17 {
		t.Fatalf("%d organization rows %v, want 137: 70 Admin, 50 AdminOrg, 17 Authenticated", len(rows), counts)
	}
}

// Product rule 1, every organization row, every org-less caller: with the cell off the answer is the legacy one (the
// handler for a holder of admin.orgs, 403 Organization context required for anyone else, and that 403 on every AdminOrg
// row); with the cell on and the decision point agreeing with go-api's own state it is the same answer, byte for byte.
// The decision point is asked exactly one platform question, admin.orgs with no organization, on an Admin or
// Authenticated row, and none on an AdminOrg row.
func TestOrglessOrgRowsKeepTheLegacyAnswerUnderThePlatformCell(t *testing.T) {
	rows := orglessOrgRows(t)
	actionRows := productionGoAPIRows(t)
	claims := orglessAccessClaims
	for _, c := range orglessCallers {
		base := orglessMux(t, orglessStore(c), claims(), nil, rows, false, orglessEcho)
		off := orglessMux(t, orglessStore(c), claims(), seamEnforcer(t, orglessPDP(t, c.holds).client(t), actionRows, "admin"), rows, false, orglessEcho)
		for switchName, namespaces := range orglessSwitches() {
			pdp := orglessPDP(t, c.holds)
			on := orglessMux(t, orglessStore(c), claims(), seamEnforcer(t, pdp.client(t), actionRows, namespaces...), rows, false, orglessEcho)
			for _, row := range rows {
				for _, path := range orglessTargets(row) {
					name := c.name + "/" + switchName + "/" + path
					want := orglessServe(base, row.pattern, path)
					if row.level == AdminOrg || !c.holds {
						if want.status != http.StatusForbidden || want.body != orglessOCRBody {
							t.Fatalf("%s %s: legacy %d %q, want 403 %s", name, row.pattern, want.status, want.body, orglessOCRBody)
						}
					} else if want.status != http.StatusOK || !strings.Contains(want.body, "org= role=member") {
						t.Fatalf("%s %s: legacy %d %q, want the handler with no token org", name, row.pattern, want.status, want.body)
					}
					if got := orglessServe(off, row.pattern, path); got.status != want.status || got.body != want.body {
						t.Fatalf("%s %s cell off: %d %q, want the legacy %d %q", name, row.pattern, got.status, got.body, want.status, want.body)
					}
					before := len(pdp.calls())
					got := orglessServe(on, row.pattern, path)
					if got.status != want.status || got.body != want.body {
						t.Errorf("%s %s cell on: %d %q, want the legacy %d %q", name, row.pattern, got.status, got.body, want.status, want.body)
					}
					calls := pdp.calls()[before:]
					if row.level == AdminOrg {
						if len(calls) != 0 {
							t.Errorf("%s %s: PDP saw %+v, want no question on an AdminOrg row", name, row.pattern, calls)
						}
						continue
					}
					if len(calls) != 1 || calls[0].Action != rule.ActionAdminOrgs || calls[0].Type != policycatalog.ScopePlatform || calls[0].HasOrg {
						t.Errorf("%s %s: PDP saw %+v, want one platform question admin.orgs with no organization", name, row.pattern, calls)
					}
				}
			}
		}
	}
}

// F-9 (the S1 outage row): with no verdict an org-less caller on an Admin or Authenticated organization row gets the
// enforced 503, where the legacy guard answers without a question; an AdminOrg row keeps its 403.
func TestOrglessOrgRowsWithNoVerdictAre503ExceptAdminOrg(t *testing.T) {
	rows := orglessOrgRows(t)
	actionRows := productionGoAPIRows(t)
	for _, c := range orglessCallers {
		dead := deadPDP(t)
		on := orglessMux(t, orglessStore(c), orglessAccessClaims(), seamEnforcer(t, dead.client(t), actionRows, orglessOrgNamespaces...), rows, false, orglessEcho)
		for _, row := range rows {
			got := orglessServe(on, row.pattern, orglessTargets(row)[0])
			if row.level == AdminOrg {
				if got.status != http.StatusForbidden || got.body != orglessOCRBody {
					t.Errorf("%s %s: %d %q, want the AdminOrg 403 %s", c.name, row.pattern, got.status, got.body, orglessOCRBody)
				}
				continue
			}
			if got.status != http.StatusServiceUnavailable || !strings.Contains(got.body, enforceUnavailableDetail) || got.header.Get("Retry-After") != enforceRetryAfter {
				t.Errorf("%s %s: %d %q, want the enforced 503", c.name, row.pattern, got.status, got.body)
			}
		}
	}
}

// Known stricter (ci/auth_exception_ledger.tsv orgless-org-row-local-flag-pdp-deny), the 13366 form on organization
// rows: the decision point's platform roles are the authority. A caller with no organization whose local platform flag is
// set but whom the decision point refuses admin.orgs gets 403 Organization context required where the legacy guard
// serves the handler.
func TestOrglessOrgRowLocalFlagRefusedByThePDPIsKnownStricter(t *testing.T) {
	rows := orglessOrgRows(t)
	actionRows := productionGoAPIRows(t)
	c := orglessCallers[0]
	base := orglessMux(t, orglessStore(c), orglessAccessClaims(), nil, rows, false, orglessEcho)
	on := orglessMux(t, orglessStore(c), orglessAccessClaims(), seamEnforcer(t, orglessPDP(t, false).client(t), actionRows, orglessOrgNamespaces...), rows, false, orglessEcho)
	for _, row := range rows {
		if row.level == AdminOrg {
			continue
		}
		path := orglessTargets(row)[0]
		if legacy := orglessServe(base, row.pattern, path); legacy.status != http.StatusOK {
			t.Fatalf("%s legacy: %d %q, want the local flag's 200", row.pattern, legacy.status, legacy.body)
		}
		if got := orglessServe(on, row.pattern, path); got.status != http.StatusForbidden || got.body != orglessOCRBody {
			t.Errorf("%s cell on: %d %q, want 403 %s", row.pattern, got.status, got.body, orglessOCRBody)
		}
	}
}

// Known wider (ci/auth_exception_ledger.tsv orgless-org-row-pdp-platform-role-no-local-flag), acp REVIEW-13594-plan-1
// Q1: the decision point is the authority. A caller with no organization whose local state holds no admin.orgs but whose
// platform role at the decision point does is served the handler where the legacy guard refuses it 403. Before the S1
// GO the prod count of such users is a named row.
func TestOrglessOrgRowPDPPlatformRoleWithoutLocalFlagIsServed(t *testing.T) {
	rows := orglessOrgRows(t)
	actionRows := productionGoAPIRows(t)
	c := orglessCallers[3]
	base := orglessMux(t, orglessStore(c), orglessAccessClaims(), nil, rows, false, orglessEcho)
	on := orglessMux(t, orglessStore(c), orglessAccessClaims(), seamEnforcer(t, orglessPDP(t, true).client(t), actionRows, orglessOrgNamespaces...), rows, false, orglessEcho)
	for _, row := range rows {
		if row.level == AdminOrg {
			continue
		}
		path := orglessTargets(row)[0]
		if legacy := orglessServe(base, row.pattern, path); legacy.status != http.StatusForbidden || legacy.body != orglessOCRBody {
			t.Fatalf("%s legacy: %d %q, want 403 %s", row.pattern, legacy.status, legacy.body, orglessOCRBody)
		}
		if got := orglessServe(on, row.pattern, path); got.status != http.StatusOK {
			t.Errorf("%s cell on: %d %q, want the handler", row.pattern, got.status, got.body)
		}
	}
}

// F1 (acp REVIEW-13594-plan-1 Q2): a delegated session's bearer with no organization keeps the legacy answer, with no
// question, counted under its own reason: the decision point refuses a delegated session every platform action
// (authorization/delegation.go delegatedAllow), so judging it would narrow impersonation. It reaches only the rows that
// admit a delegated bearer: the Authenticated organization rows.
func TestOrglessDelegatedBearerKeepsTheLegacyAnswer(t *testing.T) {
	rows := orglessOrgRows(t)
	actionRows := productionGoAPIRows(t)
	delegatedOrgless := func() *RoutedResult {
		return userClaims(func(claims *AccessTokenClaims) { claims.OrgID, claims.Actor = nil, &delegatedActorWire })
	}
	for _, c := range []orglessCaller{orglessCallers[0], orglessCallers[3]} {
		base := orglessMux(t, orglessStore(c), delegatedOrgless(), nil, rows, true, orglessEcho)
		for _, row := range rows {
			if row.level != Authenticated {
				continue
			}
			rec := recordEnforce(t)
			pdp := newTestPDP(t, allowAll)
			on := orglessMux(t, orglessStore(c), delegatedOrgless(), seamEnforcer(t, pdp.client(t), actionRows, orglessOrgNamespaces...), rows, true, orglessEcho)
			path := orglessTargets(row)[0]
			want := orglessServe(base, row.pattern, path)
			got := orglessServe(on, row.pattern, path)
			if got.status != want.status || got.body != want.body {
				t.Errorf("%s %s: %d %q, want the legacy %d %q", c.name, row.pattern, got.status, got.body, want.status, want.body)
			}
			if n := len(pdp.calls()); n != 0 || !slices.Equal(rec.unjudgedSnapshot(), []string{row.pattern + "/delegated_orgless"}) || len(rec.snapshot()) != 0 {
				t.Errorf("%s %s: %d PDP calls, unjudged %v, decisions %v, want 0, [%s/delegated_orgless] and none",
					c.name, row.pattern, n, rec.unjudgedSnapshot(), rec.snapshot(), row.pattern)
			}
		}
	}
}

// The impersonation residual is untouched: an org-less platform admin with a live go-api impersonation row keeps the
// legacy read answer (served as the target organization) with no question, and its write is refused before any.
func TestOrglessImpersonationResidualIsUntouched(t *testing.T) {
	rows := orglessOrgRows(t)
	actionRows := productionGoAPIRows(t)
	c := orglessCallers[0]
	var read, write orglessOrgRow
	for _, row := range rows {
		method, _, _ := strings.Cut(row.pattern, " ")
		if row.level == Admin && !row.row.OrgFromPath && !row.row.OrgFromQuery {
			if method == http.MethodGet && read.pattern == "" {
				read = row
			}
			if method == http.MethodPost && write.pattern == "" {
				write = row
			}
		}
	}
	if read.pattern == "" || write.pattern == "" {
		t.Fatal("no Admin session-org GET and POST rows")
	}
	base := impersonating(orglessMux(t, orglessStore(c), orglessAccessClaims(), nil, rows, false, orglessEcho))
	rec := recordEnforce(t)
	pdp := newTestPDP(t, allowAll)
	on := impersonating(orglessMux(t, orglessStore(c), orglessAccessClaims(), seamEnforcer(t, pdp.client(t), actionRows, orglessOrgNamespaces...), rows, false, orglessEcho))
	want := orglessServe(base, read.pattern, orglessTargets(read)[0])
	got := orglessServe(on, read.pattern, orglessTargets(read)[0])
	if want.status != http.StatusOK || got.status != want.status || got.body != want.body {
		t.Errorf("read %s: %d %q, want the legacy %d %q", read.pattern, got.status, got.body, want.status, want.body)
	}
	if got := orglessServe(on, write.pattern, orglessTargets(write)[0]); got.status != http.StatusForbidden || got.body != impersonationRefusalBody {
		t.Errorf("write %s: %d %q, want 403 %s", write.pattern, got.status, got.body, impersonationRefusalBody)
	}
	if n := len(pdp.calls()); n != 0 || !slices.Equal(rec.unjudgedSnapshot(), []string{read.pattern + "/impersonation"}) {
		t.Errorf("%d PDP calls, unjudged %v, want 0 and [%s/impersonation]", n, rec.unjudgedSnapshot(), read.pattern)
	}
}

// The counters: a judged org-less request counts its row's namespace and outcome; the AdminOrg refusal asks nothing and
// is no deny (acp Q3): it counts only the unjudged reason org_required.
func TestOrglessOrgRowCounters(t *testing.T) {
	rows := orglessOrgRows(t)
	actionRows := productionGoAPIRows(t)
	var admin, adminOrg orglessOrgRow
	for _, row := range rows {
		if row.level == Admin && admin.pattern == "" {
			admin = row
		}
		if row.level == AdminOrg && adminOrg.pattern == "" {
			adminOrg = row
		}
	}
	for _, cell := range []struct {
		name      string
		caller    orglessCaller
		pdp       *testPDP
		row       orglessOrgRow
		decisions []string
		unjudged  []string
	}{
		{"holder, allow", orglessCallers[0], orglessPDP(t, true), admin, []string{admin.row.Namespace + "/allow"}, nil},
		{"non-holder, deny", orglessCallers[3], orglessPDP(t, false), admin, []string{admin.row.Namespace + "/deny"}, nil},
		{"no verdict", orglessCallers[0], deadPDP(t), admin, []string{admin.row.Namespace + "/error_deny"}, nil},
		{"AdminOrg, holder", orglessCallers[0], orglessPDP(t, true), adminOrg, nil, []string{adminOrg.pattern + "/org_required"}},
		{"AdminOrg, no verdict", orglessCallers[3], deadPDP(t), adminOrg, nil, []string{adminOrg.pattern + "/org_required"}},
	} {
		rec := recordEnforce(t)
		on := orglessMux(t, orglessStore(cell.caller), orglessAccessClaims(), seamEnforcer(t, cell.pdp.client(t), actionRows, orglessOrgNamespaces...), rows, false, orglessEcho)
		orglessServe(on, cell.row.pattern, orglessTargets(cell.row)[0])
		if got := rec.snapshot(); !slices.Equal(got, cell.decisions) {
			t.Errorf("%s: decisions %v, want %v", cell.name, got, cell.decisions)
		}
		if got := rec.unjudgedSnapshot(); !slices.Equal(got, cell.unjudged) {
			t.Errorf("%s: unjudged %v, want %v", cell.name, got, cell.unjudged)
		}
	}
}

// With the admin cell on, the guard's admin.orgs answer is the request's platform answer: a handler that asks
// HoldsPlatform(admin.orgs) again is not a second question.
func TestOrglessOrgRowSeedsTheRequestsPlatformAnswer(t *testing.T) {
	rows := orglessOrgRows(t)
	var admin orglessOrgRow
	for _, row := range rows {
		if row.level == Admin {
			admin = row
			break
		}
	}
	pdp := orglessPDP(t, true)
	on := orglessMux(t, orglessStore(orglessCallers[0]), orglessAccessClaims(), seamEnforcer(t, pdp.client(t), productionGoAPIRows(t), append(slices.Clone(orglessOrgNamespaces), "admin")...),
		[]orglessOrgRow{admin}, false, seamEcho(rule.ActionAdminOrgs))
	got := orglessServe(on, admin.pattern, orglessTargets(admin)[0])
	if got.status != http.StatusOK || len(pdp.calls()) != 1 {
		t.Fatalf("%s: %d %q with %d PDP calls, want the handler and one question", admin.pattern, got.status, got.body, len(pdp.calls()))
	}
}

// A platform row is not an organization row: an org-less caller there is still asked the row's own platform action
// (13366), never admin.orgs in its place.
func TestOrglessPlatformRowStillAsksItsOwnAction(t *testing.T) {
	rows := productionGoAPIRows(t)
	pattern := ""
	for _, p := range profilePlatformRows(t) {
		if _, plain := platformAdminRoutes[p]; plain && rows[p].Action != rule.ActionAdminOrgs && !platformAdminRoutes[p].bodyFirst {
			pattern = p
			break
		}
	}
	if pattern == "" {
		t.Fatal("no platform wrap row with another action than admin.orgs")
	}
	pdp := newTestPDP(t, allowAll)
	guard := NewGuard(routed(t, legacyStore(true, ""), &fakeValidator{result: orglessAccessClaims()}), quiet())
	guard.SetEnforcer(seamEnforcer(t, pdp.client(t), rows, "admin"))
	serveCell(platformRowsMux(guard), pattern, cellBearer())
	if calls := pdp.calls(); len(calls) != 1 || calls[0].Action != rows[pattern].Action {
		t.Fatalf("%s: PDP saw %+v, want one question for %s", pattern, calls, rows[pattern].Action)
	}
}

// A token that is not a user access token comes first (unjudgedReason): with no organization on an organization row it is
// still the hs256 reason and the legacy answer, never a platform question. No such token reaches the guard today
// (Authenticate builds user access tokens only); this pins the order if one ever does.
func TestOrglessNonUserTokenKeepsTheHS256Reason(t *testing.T) {
	rows := orglessOrgRows(t)
	rec := recordEnforce(t)
	pdp := newTestPDP(t, allowAll)
	enf := seamEnforcer(t, pdp.client(t), productionGoAPIRows(t), orglessOrgNamespaces...)
	for _, bearer := range []string{"", shadowBearer} {
		verdict := enf.decide(t.Context(), enforceObservation{pattern: rows[0].pattern, bearer: bearer, userAccessToken: false, read: true})
		if verdict != enforceLegacy {
			t.Errorf("bearer %q: verdict %v, want the legacy answer", bearer, verdict)
		}
	}
	if n := len(pdp.calls()); n != 0 || !slices.Equal(rec.unjudgedSnapshot(), []string{rows[0].pattern + "/hs256", rows[0].pattern + "/hs256"}) {
		t.Fatalf("%d PDP calls, unjudged %v, want 0 and hs256 twice", n, rec.unjudgedSnapshot())
	}
}
