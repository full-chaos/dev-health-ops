package apiservice

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/pep"
	"github.com/google/uuid"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
)

const orglessOCRBody = `{"detail":"Organization context required"}`

// orglessOrgNamespaces turns on the cell of every organization row of the go-api profile.
const orglessOrgNamespaces = "acr,billing,integrations,license,members,org,settings,sso,teams"

// orglessRealRows is every organization row of the go-api profile, keyed "METHOD /route", with the guard level its
// profile row names (primary_validator).
func orglessRealRows(t *testing.T) map[string]string {
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
		if r.Service == goAPIAccessService {
			level, _, _ := strings.Cut(r.Validator.Description, ":")
			levels[r.Method+" "+r.Route] = level
		}
	}
	rows, err := policy.ParseShadowRows(authv1.GoAPIEndpointProfiles, goAPIAccessService)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for key, row := range rows {
		if row.ResourceType == policycatalog.ScopeOrganization {
			out[key] = levels[key]
		}
	}
	if len(out) != 137 {
		t.Fatalf("%d organization rows, want 137", len(out))
	}
	return out
}

// orglessPaths are the rows whose handler serves one fixed value of a path parameter (cellPath fills the others).
var orglessPaths = map[string]string{"POST /api/v1/admin/ip-allowlist/{entry_id}": "/api/v1/admin/ip-allowlist/check"}

// orglessNoBody are the rows that take no body (EmptyBody: any body is a 413 before the guard).
var orglessNoBody = map[string]bool{"POST /api/v1/acr/credentials/{credential_id}/revoke": true}

// On orglessOrgsMe the org-less caller gets the handler's 400, cell off and on. 13604 row 10 there (legacy 200, decision
// point 403) is a caller WITH a token organization and no membership row, pinned by
// TestNoRowTokenOrgCallerOnOrgsMeIsDeniedByTheDecisionPoint.
const orglessOrgsMe = "GET /api/v1/orgs/me"

const orglessNoOrgContextBody = `{"detail":"No organization context"}`

// orglessDo is cellDo with the fixed paths of orglessPaths and no body on the rows of orglessNoBody.
func orglessDo(handler http.Handler, key, token string) cellAnswer {
	method, _, _ := strings.Cut(key, " ")
	path, fixed := orglessPaths[key]
	if !fixed {
		path = cellPath(key)
	}
	var body io.Reader
	if (method == http.MethodPost || method == http.MethodPut || method == http.MethodPatch) && !orglessNoBody[key] {
		body = strings.NewReader("{}")
	}
	request := httptest.NewRequest(method, path, body)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	request.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return cellAnswer{status: recorder.Code, body: recorder.Body.String(), header: recorder.Header()}
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// CHAOS-13594 (10872 S1), product rule 1, over the REAL go-api routes and handlers: a token with no organization gets,
// on every organization row with its cell on and the production evaluator behind the decision point, the answer the
// legacy guard and the same handler give it with the cell off. A platform admin with no membership row reaches the
// handler on every Admin and Authenticated row (the handler then answers as it does today); an org member is refused
// 403 Organization context required; every AdminOrg row refuses both, with no question.
func TestOrglessOrgRowsRealHandlersKeepTheLegacyAnswer(t *testing.T) {
	rows := orglessRealRows(t)
	pdp := newCellPDP(t)
	off, offMinter := adminConfigServerWith(t, nil, withPlatformAdmins)
	on, onMinter := adminConfigServerWith(t, func(t *testing.T) *policy.Enforcer { return newCellEnforcer(t, pdp, orglessOrgNamespaces, false) }, withPlatformAdmins)
	orgless := func(minter *eddsaMinter, user, sid uuid.UUID) string {
		return minter.mint(user, map[string]any{"sid": sessionid.Wire(sid), "org_id": nil})
	}
	type orglessRealCaller struct {
		name              string
		offToken, onToken string
		snap              cellSnapshot
	}
	admin := orglessRealCaller{"platform admin, no membership", orgless(offMinter, cellPlatformNoRow, cellNoRowSID), orgless(onMinter, cellPlatformNoRow, cellNoRowSID), cellSnapshot{platformRole: "platform_admin"}}
	member := orglessRealCaller{"org member", orgless(offMinter, probeMember, probeOtherSID), orgless(onMinter, probeMember, probeOtherSID), cellSnapshot{orgRole: "member"}}
	pdp.register(map[string]cellCaller{admin.name: {token: admin.onToken, snap: admin.snap}, member.name: {token: member.onToken, snap: member.snap}})
	handlerAnswers := map[bool]int{}
	orgsMeChecked := 0
	for _, key := range sortedKeys(rows) {
		level := rows[key]
		for _, c := range []orglessRealCaller{admin, member} {
			want := orglessDo(off, key, c.offToken)
			if key == orglessOrgsMe && c.name == admin.name {
				orgsMeChecked++
				if want.status != http.StatusBadRequest || want.body != orglessNoOrgContextBody {
					t.Fatalf("%s / %s off: %d %s, want 400 %s", key, c.name, want.status, want.body, orglessNoOrgContextBody)
				}
			}
			if level == "policy.AdminOrg" || c.name == member.name {
				if want.status != http.StatusForbidden || want.body != orglessOCRBody {
					t.Fatalf("%s / %s off: %d %s, want 403 %s", key, c.name, want.status, want.body, orglessOCRBody)
				}
			} else if want.status == http.StatusUnauthorized || want.status == http.StatusMethodNotAllowed || want.body == "404 page not found\n" {
				t.Fatalf("%s / %s off: %d %s, want the handler's answer", key, c.name, want.status, want.body)
			} else {
				// The guard admits; many handlers then refuse the missing organization with the same 403 themselves.
				handlerAnswers[want.status == http.StatusForbidden && want.body == orglessOCRBody]++
			}
			pdp.reset("evaluate")
			got := orglessDo(on, key, c.onToken)
			if got.status != want.status || got.body != want.body || got.header.Get("Content-Type") != want.header.Get("Content-Type") ||
				got.header.Get("Retry-After") != want.header.Get("Retry-After") {
				t.Errorf("%s / %s on: %d %s %v, want the legacy %d %s %v", key, c.name, got.status, got.body, got.header, want.status, want.body, want.header)
			}
			calls := pdp.calls()
			if level == "policy.AdminOrg" {
				if len(calls) != 0 {
					t.Errorf("%s / %s: calls %+v, want none on an AdminOrg row", key, c.name, calls)
				}
				continue
			}
			if len(calls) != 1 || calls[0].Action != policy.ActionAdminOrgs || calls[0].ResourceType != policycatalog.ScopePlatform || calls[0].OrganizationID != "" {
				t.Errorf("%s / %s: calls %+v, want one platform question admin.orgs with no organization", key, c.name, calls)
			}
		}
	}
	if orgsMeChecked != 1 {
		t.Errorf("%s checked %d times for the org-less platform admin, want 1", orglessOrgsMe, orgsMeChecked)
	}
	if handlerAnswers[true]+handlerAnswers[false] != 87 {
		t.Errorf("%d Admin and Authenticated rows reached the handler, want 87", handlerAnswers[true]+handlerAnswers[false])
	}
	t.Logf("platform admin with no organization, legacy handler answers: %d rows refuse 403 Organization context required, %d rows answer otherwise", handlerAnswers[true], handlerAnswers[false])
}

// F-9 over the real routes: with no verdict, every Admin and Authenticated organization row answers the org-less
// platform admin the enforced 503; every AdminOrg row keeps the 403.
func TestOrglessOrgRowsRealHandlersWithNoVerdict(t *testing.T) {
	rows := orglessRealRows(t)
	pdp := newCellPDP(t)
	on, minter := adminConfigServerWith(t, func(t *testing.T) *policy.Enforcer { return newCellEnforcer(t, pdp, orglessOrgNamespaces, false) }, withPlatformAdmins)
	token := minter.mint(cellPlatformNoRow, map[string]any{"sid": sessionid.Wire(cellNoRowSID), "org_id": nil})
	for _, key := range sortedKeys(rows) {
		pdp.reset("fail")
		got := orglessDo(on, key, token)
		if rows[key] == "policy.AdminOrg" {
			if got.status != http.StatusForbidden || got.body != orglessOCRBody {
				t.Errorf("%s: %d %s, want 403 %s", key, got.status, got.body, orglessOCRBody)
			}
			continue
		}
		if got.status != http.StatusServiceUnavailable || !strings.Contains(got.body, "Authorization service temporarily unavailable") {
			t.Errorf("%s: %d %s, want the enforced 503", key, got.status, got.body)
		}
	}
}

// orglessAskedProblems serves the org-less platform admin (no membership row) on every organization row of handler and
// names each row whose decision point question is not the one the legacy guard reads for that caller: admin.orgs on an
// Admin or Authenticated row, none on an AdminOrg row.
func orglessAskedProblems(handler http.Handler, pdp *cellPDP, token string, rows map[string]string) (problems []string, checked int) {
	for _, key := range sortedKeys(rows) {
		pdp.reset("evaluate")
		orglessDo(handler, key, token)
		calls := pdp.calls()
		checked++
		if rows[key] == "policy.AdminOrg" {
			if len(calls) != 0 {
				problems = append(problems, key+": a question on an AdminOrg row")
			}
			continue
		}
		if len(calls) != 1 || calls[0].Action != policy.ActionAdminOrgs || calls[0].ResourceType != policycatalog.ScopePlatform || calls[0].OrganizationID != "" {
			problems = append(problems, key+": asked "+orglessCallsText(calls)+", want admin.orgs on the platform")
		}
	}
	return problems, checked
}

func orglessCallsText(calls []pep.Request) string {
	parts := make([]string, 0, len(calls))
	for _, c := range calls {
		parts = append(parts, c.Action+"@"+c.ResourceType)
	}
	return "[" + strings.Join(parts, " ") + "]"
}

// The census (acp F-4): over the production mounts of the 137 organization rows, the org-less caller is asked exactly
// the platform action the legacy guard admits it by. Every row is a plain level mount today, so that action is
// admin.orgs (Guard.wrap orglessAction); a row mounted with its own gate action would be asked that action instead.
func TestOrglessOrgRowsAskTheLegacyOrglessActionOnEveryRow(t *testing.T) {
	rows := orglessRealRows(t)
	pdp := newCellPDP(t)
	handler, minter := adminConfigServerWith(t, func(t *testing.T) *policy.Enforcer { return newCellEnforcer(t, pdp, orglessOrgNamespaces, false) }, withPlatformAdmins)
	token := minter.mint(cellPlatformNoRow, map[string]any{"sid": sessionid.Wire(cellNoRowSID), "org_id": nil})
	pdp.register(map[string]cellCaller{"admin": {token: token, snap: cellSnapshot{platformRole: "platform_admin"}}})
	problems, checked := orglessAskedProblems(handler, pdp, token, rows)
	for _, problem := range problems {
		t.Error(problem)
	}
	if checked != 137 {
		t.Fatalf("%d rows checked, want 137", checked)
	}
}

// The census fails on a planted row: one Admin organization row mounted WrapAdminOr(admin.users), as an admin-or row
// is, is asked admin.users (its own gate action, the one the legacy guard reads), and the census names that row only.
func TestOrglessCensusNamesAPlantedRowWithItsOwnAction(t *testing.T) {
	rows := orglessRealRows(t)
	planted := ""
	for _, key := range sortedKeys(rows) {
		if rows[key] == "policy.Admin" && strings.HasPrefix(key, "GET ") {
			planted = key
			break
		}
	}
	method, pattern, _ := strings.Cut(planted, " ")
	pdp := newCellPDP(t)
	handler, minter := adminConfigServerOver(t, func(t *testing.T) *policy.Enforcer { return newCellEnforcer(t, pdp, orglessOrgNamespaces, false) }, withPlatformAdmins,
		func(_ *testing.T, deps Deps) []httpapi.Route {
			routes := Routes(deps, quietLogger())
			for i, route := range routes {
				if route.Method == method && route.Pattern == pattern {
					routes[i].Handler = deps.Guard.WrapAdminOr(policy.ActionAdminUsers, http.HandlerFunc(principalEcho))
				}
			}
			return routes
		})
	token := minter.mint(cellPlatformNoRow, map[string]any{"sid": sessionid.Wire(cellNoRowSID), "org_id": nil})
	pdp.register(map[string]cellCaller{"admin": {token: token, snap: cellSnapshot{platformRole: "platform_admin"}}})
	problems, _ := orglessAskedProblems(handler, pdp, token, rows)
	if len(problems) != 1 || !strings.HasPrefix(problems[0], planted+": asked [admin.users@platform]") {
		t.Fatalf("problems %v, want exactly the planted row %s asked admin.users", problems, planted)
	}
}
