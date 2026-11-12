package apiservice

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/google/uuid"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
)

const orglessOCRBody = `{"detail":"Organization context required"}`

// orglessOrgNamespaces turns on the cell of every organization row of the go-api profile.
const orglessOrgNamespaces = "billing,integrations,license,members,settings,teams"

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
	if len(out) != 127 {
		t.Fatalf("%d organization rows, want 127", len(out))
	}
	return out
}

// orglessPaths are the rows whose handler serves one fixed value of a path parameter (cellPath fills the others).
var orglessPaths = map[string]string{"POST /api/v1/admin/ip-allowlist/{entry_id}": "/api/v1/admin/ip-allowlist/check"}

// orglessDo is cellDo with the fixed paths of orglessPaths.
func orglessDo(handler http.Handler, key, token string) cellAnswer {
	method, _, _ := strings.Cut(key, " ")
	path, fixed := orglessPaths[key]
	if !fixed {
		path = cellPath(key)
	}
	var body io.Reader
	if method == http.MethodPost || method == http.MethodPut || method == http.MethodPatch {
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
	for _, key := range sortedKeys(rows) {
		level := rows[key]
		for _, c := range []orglessRealCaller{admin, member} {
			want := orglessDo(off, key, c.offToken)
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
			if got.status != want.status || got.body != want.body {
				t.Errorf("%s / %s on: %d %s, want the legacy %d %s", key, c.name, got.status, got.body, want.status, want.body)
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
	if handlerAnswers[true]+handlerAnswers[false] != 77 {
		t.Errorf("%d Admin and Authenticated rows reached the handler, want 77", handlerAnswers[true]+handlerAnswers[false])
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
