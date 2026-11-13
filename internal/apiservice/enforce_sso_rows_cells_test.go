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
	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
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

func withSSOLicence(t *testing.T) {
	t.Helper()
	licensing.SetProcessLicense(&licensing.ProcessLicense{Tier: "enterprise", Features: map[string]bool{"sso_saml": true}})
	t.Cleanup(func() { licensing.SetProcessLicense(nil) })
}

func ssoDo(handler http.Handler, key, token string) cellAnswer {
	method, route, _ := strings.Cut(key, " ")
	path := strings.ReplaceAll(route, "{provider_id}", ssoCellProvider.String())
	var body *strings.Reader
	switch key {
	case "POST /api/v1/auth/sso/providers":
		body = strings.NewReader(`{"name":"Acme","protocol":"oidc","oidc_config":{"client_id":"c","client_secret":"s","issuer":"https://i.example"}}`)
	case "PATCH /api/v1/auth/sso/providers/{provider_id}":
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
	withSSOLicence(t)
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
	withSSOLicence(t)
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
