package apiservice

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
)

// ssoRowActions are the go-api SSO provider rows the catalog 20 PR gives an action: reads settings.read, writes sso.write. The
// target is the legacy answer for every caller class, status and body (product rule 1), over the real handler with the
// auth-service client behind it.
var ssoRowActions = map[string]string{
	"GET /api/v1/auth/sso/providers":                           "settings.read",
	"GET /api/v1/auth/sso/providers/{provider_id}":             "settings.read",
	"POST /api/v1/auth/sso/providers":                          "sso.write",
	"PATCH /api/v1/auth/sso/providers/{provider_id}":           "sso.write",
	"DELETE /api/v1/auth/sso/providers/{provider_id}":          "sso.write",
	"POST /api/v1/auth/sso/providers/{provider_id}/activate":   "sso.write",
	"POST /api/v1/auth/sso/providers/{provider_id}/deactivate": "sso.write",
}

const ssoRowNamespaces = "org,settings,sso"

var ssoCellProvider = uuid.MustParse("5a5a5a5a-0000-4000-8000-000000000001")

type ssoCellClient struct{ calls []string }

func (c *ssoCellClient) provider(org uuid.UUID) json.RawMessage {
	return json.RawMessage(`{"id":"` + ssoCellProvider.String() + `","org_id":"` + org.String() + `","name":"Acme","protocol":"oidc","status":"active","is_default":false,` +
		`"allow_idp_initiated":false,"auto_provision_users":true,"default_role":"member","config":{"issuer":"https://i.example"},"allowed_domains":[],` +
		`"last_metadata_sync_at":null,"last_login_at":null,"last_error":null,"last_error_at":null,"created_at":"2026-11-13T01:02:03Z","updated_at":"2026-11-13T01:02:03Z"}`)
}

func (c *ssoCellClient) List(_ context.Context, in authprovision.SSOListRequest) (json.RawMessage, error) {
	c.calls = append(c.calls, "list")
	return json.RawMessage(`{"items":[` + string(c.provider(in.Org)) + `],"total":1,"limit":1000,"offset":0}`), nil
}

func (c *ssoCellClient) Get(_ context.Context, org, _, _ uuid.UUID) (json.RawMessage, error) {
	c.calls = append(c.calls, "get")
	return c.provider(org), nil
}

func (c *ssoCellClient) Create(_ context.Context, in authprovision.SSOCreateRequest) (json.RawMessage, error) {
	c.calls = append(c.calls, "create")
	return c.provider(in.Org), nil
}

func (c *ssoCellClient) Update(_ context.Context, in authprovision.SSOUpdateRequest) (json.RawMessage, error) {
	c.calls = append(c.calls, "update")
	return c.provider(in.Org), nil
}

func (c *ssoCellClient) Delete(context.Context, uuid.UUID, uuid.UUID, uuid.UUID) error {
	c.calls = append(c.calls, "delete")
	return nil
}

func (c *ssoCellClient) Activate(_ context.Context, org, _, _ uuid.UUID) (json.RawMessage, error) {
	c.calls = append(c.calls, "activate")
	return c.provider(org), nil
}

func (c *ssoCellClient) Deactivate(_ context.Context, org, _, _ uuid.UUID) (json.RawMessage, error) {
	c.calls = append(c.calls, "deactivate")
	return c.provider(org), nil
}

func ssoRoutes(client authprovision.SSOProviders) func(*testing.T, Deps) []httpapi.Route {
	return func(_ *testing.T, deps Deps) []httpapi.Route {
		deps.SSOProviders = client
		return Routes(deps, quietLogger())
	}
}

func ssoDo(handler http.Handler, key, token string) cellAnswer {
	return ssoDoWith(handler, key, token, "", "")
}

// ssoDoWith is ssoDo with a query string and a body of its own (empty: the route's valid one).
func ssoDoWith(handler http.Handler, key, token, query, override string) cellAnswer {
	method, route, _ := strings.Cut(key, " ")
	path := strings.ReplaceAll(route, "{provider_id}", ssoCellProvider.String()) + query
	var body *strings.Reader
	switch {
	case override != "":
		body = strings.NewReader(override)
	case key == "POST /api/v1/auth/sso/providers":
		body = strings.NewReader(`{"name":"Acme","protocol":"oidc","oidc_config":{"client_id":"c","client_secret":"s","issuer":"https://i.example"}}`)
	case key == "PATCH /api/v1/auth/sso/providers/{provider_id}":
		body = strings.NewReader(`{"name":"Renamed"}`)
	default:
		body = strings.NewReader("")
	}
	request := httptest.NewRequest(method, path, body)
	request.Header.Set("Authorization", "Bearer "+token)
	if body.Len() > 0 {
		request.Header.Set("Content-Type", "application/json")
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return cellAnswer{status: recorder.Code, body: recorder.Body.String(), header: recorder.Header()}
}

func TestSSORowsMatchTheProfile(t *testing.T) {
	got := map[string]string{}
	for _, row := range loadGoAPIRows(t) {
		route, _ := row["route"].(string)
		method, _ := row["method"].(string)
		key := method + " " + route
		if _, ok := ssoRowActions[key]; ok && row["service"] == goAPIAccessService {
			got[key], _ = row["action"].(string)
		}
	}
	for key, want := range ssoRowActions {
		if got[key] != want {
			t.Errorf("%s: profile action %q, table %q", key, got[key], want)
		}
	}
}

// For each SSO row and caller class, the answer with the cells on (the real evaluator decides) equals the cells-off answer,
// status and body. Each cell prints one CELL line.
func TestSSORowsAnswerEveryCallerClassAsLegacy(t *testing.T) {
	withSSOLicence(t, true)
	pdp := newCellPDP(t)
	client := &ssoCellClient{}
	off, offMinter := adminConfigServerScoped(t, nil, withBoundaryCallers, ssoRoutes(client), nil)
	on, onMinter := adminConfigServerScoped(t, func(t *testing.T) *policy.Enforcer {
		return newCellEnforcer(t, pdp, ssoRowNamespaces, false)
	}, withBoundaryCallers, ssoRoutes(client), nil)
	offCallers, onCallers := boundaryCallers(offMinter), boundaryCallers(onMinter)
	for key, action := range ssoRowActions {
		for _, class := range boundaryClasses {
			pdp.register(onCallers)
			pdp.reset("evaluate")
			legacy := ssoDo(off, key, offCallers[class].token)
			if calls := pdp.calls(); len(calls) != 0 {
				t.Fatalf("%s / %s: %d decision point calls with the cell off", key, class, len(calls))
			}
			got := ssoDo(on, key, onCallers[class].token)
			calls := pdp.calls()
			result := "EQUAL"
			if got.status != legacy.status || got.body != legacy.body {
				result = "DIFF"
				t.Errorf("%s / %s: new %d %s, legacy %d %s", key, class, got.status, got.body, legacy.status, legacy.body)
			}
			t.Logf("CELL\t%s\t%s\tlegacy=%d:%s\tnew=%d:%s\t%s", key, class, legacy.status, short(legacy.body), got.status, short(got.body), result)
			if len(calls) != 1 || calls[0].Action != action || calls[0].OrganizationID != probeOrg.String() {
				t.Errorf("%s / %s: calls %+v, want one %s call on %s", key, class, calls, action, probeOrg)
			}
		}
	}
}

func TestSSORowsFailClosed(t *testing.T) {
	withSSOLicence(t, true)
	pdp := newCellPDP(t)
	client := &ssoCellClient{}
	on, minter := adminConfigServerScoped(t, func(t *testing.T) *policy.Enforcer {
		return newCellEnforcer(t, pdp, ssoRowNamespaces, false)
	}, withBoundaryCallers, ssoRoutes(client), nil)
	admin := boundaryCallers(minter)["org admin"].token
	for key := range ssoRowActions {
		pdp.reset("fail")
		got := ssoDo(on, key, admin)
		if got.status != http.StatusServiceUnavailable || got.header.Get("Retry-After") != "5" {
			t.Errorf("%s: %d %s, want 503 Retry-After 5", key, got.status, got.body)
		}
	}
	if len(client.calls) != 0 {
		t.Fatalf("a failed decision reached auth-service: %v", client.calls)
	}
}

// With the sso namespace enforced, a refused caller still gets the legacy ORDER of answers: the invalid body or query (422) and
// the unlicensed process (402) come before the role check, so before the decision point's refusal (the legacy order: the guard's
// legacy checks, then the route's 422, then the licence's 402, then the handler's role check: internal/api/sso/sso.go gate;
// router.py create_provider :262, update_provider :333, delete_provider :383, the role check after the decorators). Each cell is
// the cells-off answer for every caller class, and that answer is the 422 or the 402 even for a member and a viewer.
func TestSSORowsKeepTheLegacyOrderOfAnswersUnderEnforcement(t *testing.T) {
	const list, create, patch, get, del = "GET /api/v1/auth/sso/providers", "POST /api/v1/auth/sso/providers",
		"PATCH /api/v1/auth/sso/providers/{provider_id}", "GET /api/v1/auth/sso/providers/{provider_id}", "DELETE /api/v1/auth/sso/providers/{provider_id}"
	scenarios := []struct {
		name            string
		licensed        bool
		key, query, bad string
		want            int
	}{
		{"unlicensed list", false, list, "", "", http.StatusPaymentRequired},
		{"unlicensed create", false, create, "", "", http.StatusPaymentRequired},
		{"unlicensed get", false, get, "", "", http.StatusPaymentRequired},
		{"unlicensed patch", false, patch, "", "", http.StatusPaymentRequired},
		{"unlicensed delete", false, del, "", "", http.StatusPaymentRequired},
		{"invalid list query", true, list, "?limit=abc", "", http.StatusUnprocessableEntity},
		{"invalid create body", true, create, "", `{"name":1}`, http.StatusUnprocessableEntity},
		{"invalid patch body", true, patch, "", `{"name":1}`, http.StatusUnprocessableEntity},
		{"invalid body and unlicensed: the 422 first", false, create, "", `{"name":1}`, http.StatusUnprocessableEntity},
	}
	pdp := newCellPDP(t)
	client := &ssoCellClient{}
	for _, c := range scenarios {
		withSSOLicence(t, c.licensed)
		off, offMinter := adminConfigServerScoped(t, nil, withBoundaryCallers, ssoRoutes(client), nil)
		on, onMinter := adminConfigServerScoped(t, func(t *testing.T) *policy.Enforcer {
			return newCellEnforcer(t, pdp, ssoRowNamespaces, false)
		}, withBoundaryCallers, ssoRoutes(client), nil)
		offCallers, onCallers := boundaryCallers(offMinter), boundaryCallers(onMinter)
		for _, mode := range []string{"deny", "fail", "evaluate"} {
			for _, class := range boundaryClasses {
				pdp.register(onCallers)
				pdp.reset(mode)
				legacy := ssoDoWith(off, c.key, offCallers[class].token, c.query, c.bad)
				got := ssoDoWith(on, c.key, onCallers[class].token, c.query, c.bad)
				result := "EQUAL"
				if got.status != legacy.status || got.body != legacy.body || legacy.status != c.want {
					result = "DIFF"
					t.Errorf("%s / %s / pdp %s: enforced %d %s, legacy %d %s, want %d", c.name, class, mode, got.status, got.body, legacy.status, legacy.body, c.want)
				}
				t.Logf("CELL\t%s\t%s\t%s\tlegacy=%d:%s\tnew=%d:%s\t%s", c.name, mode, class, legacy.status, short(legacy.body), got.status, short(got.body), result)
			}
		}
	}
	if len(client.calls) != 0 {
		t.Fatalf("a refused request reached auth-service: %v", client.calls)
	}
}

// An impersonated SSO write is refused with the impersonation text before the route's own 422 and 402 and before any call to
// auth-service, with the cell off and on, licensed and not (the cell of 5d6679a9 extended to the provider client).
func TestAnImpersonatedSSOWriteIsRefusedBeforeGatedAndCallsNothing(t *testing.T) {
	writes := []string{"POST /api/v1/auth/sso/providers", "PATCH /api/v1/auth/sso/providers/{provider_id}", "DELETE /api/v1/auth/sso/providers/{provider_id}",
		"POST /api/v1/auth/sso/providers/{provider_id}/activate", "POST /api/v1/auth/sso/providers/{provider_id}/deactivate"}
	pdp := newCellPDP(t)
	for _, licensed := range []bool{true, false} {
		withSSOLicence(t, licensed)
		for name, enforcer := range map[string]func(*testing.T) *policy.Enforcer{
			"cells off": nil,
			"cells on":  func(t *testing.T) *policy.Enforcer { return newCellEnforcer(t, pdp, ssoRowNamespaces, false) },
		} {
			client := &ssoCellClient{}
			handler, minter := adminConfigServerScoped(t, enforcer, withLegacyImpersonationCaller, ssoRoutes(client), refuseLegacyCaller(true))
			token := minter.mint(probeAdminUser, map[string]any{"sid": sessionid.Wire(cellLegacyCallerSID)})
			for _, key := range writes {
				got := ssoDo(handler, key, token)
				if got.status != http.StatusForbidden || !strings.Contains(got.body, impersonationRefusalBody) {
					t.Errorf("licensed=%v %s %s: %d %s, want the 403 %s", licensed, name, key, got.status, got.body, impersonationRefusalBody)
				}
				if got.header.Get("X-Impersonating") != "" {
					t.Errorf("licensed=%v %s %s: X-Impersonating %q on a refused request", licensed, name, key, got.header.Get("X-Impersonating"))
				}
			}
			if len(client.calls) != 0 {
				t.Errorf("licensed=%v %s: an impersonated write reached auth-service: %v", licensed, name, client.calls)
			}
		}
	}
}
