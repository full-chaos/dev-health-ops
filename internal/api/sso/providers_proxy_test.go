package sso

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"github.com/full-chaos/dev-health-ops/internal/auth/ssoprovider"
)

const proxySecret = "s3ntinel-proxy-client-secret"

type proxyCall struct {
	op                   string
	org, actor, provider uuid.UUID
	list                 authprovision.SSOListRequest
	create               authprovision.SSOCreateRequest
	update               authprovision.SSOUpdateRequest
}

type fakeProviders struct {
	calls  []proxyCall
	err    error
	answer string
	list   string
}

func (f *fakeProviders) provider(org uuid.UUID) json.RawMessage {
	if f.answer != "" {
		return json.RawMessage(f.answer)
	}
	return json.RawMessage(`{"id":"` + uuid.NewString() + `","org_id":"` + org.String() + `","name":"Acme","protocol":"oidc","status":"pending_setup",` +
		`"is_default":false,"allow_idp_initiated":false,"auto_provision_users":true,"default_role":"member","config":{"issuer":"https://i.example","client_secret":"********"},` +
		`"allowed_domains":[],"last_metadata_sync_at":null,"last_login_at":"2026-11-13T01:02:03.5Z","last_error":null,"last_error_at":null,` +
		`"created_at":"2026-11-13T01:02:03Z","updated_at":"2026-11-13T01:02:03.250Z"}`)
}

func (f *fakeProviders) List(_ context.Context, in authprovision.SSOListRequest) (json.RawMessage, error) {
	f.calls = append(f.calls, proxyCall{op: "list", list: in})
	if f.err != nil {
		return nil, f.err
	}
	if f.list != "" {
		return json.RawMessage(f.list), nil
	}
	return json.RawMessage(`{"items":[` + string(f.provider(in.Org)) + `],"total":1,"limit":1000,"offset":0}`), nil
}

func (f *fakeProviders) Get(_ context.Context, org, actor, provider uuid.UUID) (json.RawMessage, error) {
	f.calls = append(f.calls, proxyCall{op: "get", org: org, actor: actor, provider: provider})
	return f.provider(org), f.err
}

func (f *fakeProviders) Create(_ context.Context, in authprovision.SSOCreateRequest) (json.RawMessage, error) {
	f.calls = append(f.calls, proxyCall{op: "create", create: in})
	return f.provider(in.Org), f.err
}

func (f *fakeProviders) Update(_ context.Context, in authprovision.SSOUpdateRequest) (json.RawMessage, error) {
	f.calls = append(f.calls, proxyCall{op: "update", update: in})
	return f.provider(in.Org), f.err
}

func (f *fakeProviders) Delete(_ context.Context, org, actor, provider uuid.UUID) error {
	f.calls = append(f.calls, proxyCall{op: "delete", org: org, actor: actor, provider: provider})
	return f.err
}

func (f *fakeProviders) Activate(_ context.Context, org, actor, provider uuid.UUID) (json.RawMessage, error) {
	f.calls = append(f.calls, proxyCall{op: "activate", org: org, actor: actor, provider: provider})
	return f.provider(org), f.err
}

func (f *fakeProviders) Deactivate(_ context.Context, org, actor, provider uuid.UUID) (json.RawMessage, error) {
	f.calls = append(f.calls, proxyCall{op: "deactivate", org: org, actor: actor, provider: provider})
	return f.provider(org), f.err
}

type proxyFixture struct {
	h    handlers
	fake *fakeProviders
	logs *bytes.Buffer
	org  uuid.UUID
	user uuid.UUID
}

func newProxyFixture(t *testing.T) *proxyFixture {
	t.Helper()
	logs := &bytes.Buffer{}
	fake := &fakeProviders{}
	return &proxyFixture{h: newHandlers(Deps{Providers: fake, Logger: slog.New(slog.NewTextHandler(logs, nil))}), fake: fake, logs: logs, org: uuid.New(), user: uuid.New()}
}

// do serves handler as a caller with the role and org given (org "" = the fixture's), a body (nil = none) and a path id.
func (f *proxyFixture) do(handler http.HandlerFunc, role, org, target, query string, body any) *httptest.ResponseRecorder {
	if org == "" {
		org = f.org.String()
	}
	request := httptest.NewRequest(http.MethodPost, "/x"+query, nil)
	request.SetPathValue("provider_id", target)
	ctx := policy.WithUser(request.Context(), &policy.User{ID: f.user, UserID: f.user.String(), OrgID: org, Role: role})
	if body != nil {
		raw, _ := json.Marshal(body)
		value, _ := pyjson.DecodeString(string(raw))
		ctx = policy.WithBody(ctx, pybody.Body{Value: value})
	}
	recorder := httptest.NewRecorder()
	handler(recorder, request.WithContext(ctx))
	return recorder
}

func okBody() map[string]any {
	return map[string]any{"name": "Acme", "protocol": "oidc", "oidc_config": map[string]any{"client_id": "c", "client_secret": proxySecret, "issuer": "https://i.example"}}
}

func TestProviderWritesNeedOwnerOrAdminBeforeAnyCall(t *testing.T) {
	f := newProxyFixture(t)
	id := uuid.NewString()
	writes := map[string]func() *httptest.ResponseRecorder{}
	for _, role := range []string{"viewer", "member", "", "superuser"} {
		role := role
		writes["create/"+role] = func() *httptest.ResponseRecorder { return f.do(f.h.createProvider, role, "", id, "", okBody()) }
		writes["update/"+role] = func() *httptest.ResponseRecorder {
			return f.do(f.h.updateProvider, role, "", id, "", map[string]any{"name": "n"})
		}
		writes["delete/"+role] = func() *httptest.ResponseRecorder { return f.do(f.h.deleteProvider, role, "", id, "", nil) }
		writes["activate/"+role] = func() *httptest.ResponseRecorder {
			return f.do(f.h.setStatusViaProviders("active").ServeHTTP, role, "", id, "", nil)
		}
		writes["deactivate/"+role] = func() *httptest.ResponseRecorder {
			return f.do(f.h.setStatusViaProviders("inactive").ServeHTTP, role, "", id, "", nil)
		}
	}
	for name, call := range writes {
		response := call()
		if response.Code != http.StatusForbidden || !strings.Contains(response.Body.String(), `"detail":"Admin access required"`) {
			t.Errorf("%s: %d %s, want 403 Admin access required", name, response.Code, response.Body)
		}
	}
	if len(f.fake.calls) != 0 {
		t.Fatalf("a refused role reached auth-service: %+v", f.fake.calls)
	}
	for _, role := range []string{"owner", "admin"} {
		if response := f.do(f.h.createProvider, role, "", id, "", okBody()); response.Code != http.StatusCreated {
			t.Errorf("create as %s: %d %s, want 201", role, response.Code, response.Body)
		}
	}
}

func TestProviderCallsCarryTheTokenOrganizationAndNeverTheBodys(t *testing.T) {
	f := newProxyFixture(t)
	other := uuid.NewString()
	body := okBody()
	body["organization_id"], body["org_id"] = other, other
	if response := f.do(f.h.createProvider, "admin", "", uuid.NewString(), "", body); response.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", response.Code, response.Body)
	}
	id := uuid.New()
	f.do(f.h.getProvider, "member", "", id.String(), "", nil)
	f.do(f.h.updateProvider, "admin", "", id.String(), "", map[string]any{"organization_id": other, "name": "n"})
	f.do(f.h.deleteProvider, "admin", "", id.String(), "", nil)
	f.do(f.h.listProviders, "member", "", "", "", nil)
	for _, call := range f.fake.calls {
		got := map[string]uuid.UUID{"create": call.create.Org, "get": call.org, "update": call.update.Org, "delete": call.org, "list": call.list.Org}[call.op]
		if got != f.org {
			t.Errorf("%s asked organization %s, want the token's %s (the body named %s)", call.op, got, f.org, other)
		}
		if actor := map[string]uuid.UUID{"create": call.create.Actor, "get": call.actor, "update": call.update.Actor, "delete": call.actor, "list": call.list.Actor}[call.op]; actor != f.user {
			t.Errorf("%s acted as %s, want the caller %s", call.op, actor, f.user)
		}
	}
	if len(f.fake.calls) != 5 {
		t.Fatalf("%d calls, want 5", len(f.fake.calls))
	}
}

func TestProviderRefusalsAreMappedToTheLegacyTextsAndFaultsToTheNamed503(t *testing.T) {
	id := uuid.NewString()
	refusal := func(status int, reason string) error { return &authprovision.Refused{Status: status, Reason: reason} }
	for name, c := range map[string]struct {
		err    error
		status int
		detail string
	}{
		"not found":     {refusal(404, authprovision.ReasonSSOProviderNotFound), 404, `"detail":"SSO provider not found"`},
		"saml missing":  {refusal(400, authprovision.ReasonSAMLConfigRequired), 400, `"detail":"SAML config required for SAML providers"`},
		"oidc missing":  {refusal(400, authprovision.ReasonOIDCConfigRequired), 400, `"detail":"OIDC config required for OIDC providers"`},
		"other refusal": {refusal(400, "malformed"), 503, providersUnavailableText},
		"org missing":   {refusal(404, authprovision.ReasonOrganizationMissing), 503, providersUnavailableText},
		"unavailable":   {errors.New("authprovision: auth-service provisioning route unavailable"), 503, providersUnavailableText},
		"unauthorised":  {refusal(401, "invalid_credentials"), 503, providersUnavailableText},
	} {
		f := newProxyFixture(t)
		f.fake.err = c.err
		for op, call := range map[string]func() *httptest.ResponseRecorder{
			"get":    func() *httptest.ResponseRecorder { return f.do(f.h.getProvider, "admin", "", id, "", nil) },
			"delete": func() *httptest.ResponseRecorder { return f.do(f.h.deleteProvider, "admin", "", id, "", nil) },
			"create": func() *httptest.ResponseRecorder { return f.do(f.h.createProvider, "admin", "", id, "", okBody()) },
		} {
			response := call()
			if response.Code != c.status || !strings.Contains(response.Body.String(), c.detail) {
				t.Errorf("%s/%s: %d %s, want %d containing %s", name, op, response.Code, response.Body, c.status, c.detail)
			}
		}
	}
}

func TestProviderCreateRefusesAProtocolWithoutItsConfigBeforeAnyCall(t *testing.T) {
	f := newProxyFixture(t)
	for protocol, detail := range map[string]string{"saml": "SAML config required for SAML providers", "oidc": "OIDC config required for OIDC providers"} {
		response := f.do(f.h.createProvider, "owner", "", "", "", map[string]any{"name": "n", "protocol": protocol, "saml_config": nil, "oidc_config": nil})
		if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), detail) {
			t.Errorf("%s: %d %s, want 400 %q", protocol, response.Code, response.Body, detail)
		}
	}
	if len(f.fake.calls) != 0 {
		t.Fatalf("a refused create reached auth-service: %+v", f.fake.calls)
	}
}

func TestProviderAnswersAreRenderedInPydanticFormAndNeverCarryTheSecret(t *testing.T) {
	f := newProxyFixture(t)
	response := f.do(f.h.createProvider, "admin", "", "", "", okBody())
	body := response.Body.String()
	if response.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", response.Code, body)
	}
	for _, want := range []string{`"last_login_at":"2026-11-13T01:02:03.500000Z"`, `"updated_at":"2026-11-13T01:02:03.250000Z"`, `"created_at":"2026-11-13T01:02:03Z"`,
		`"client_secret":"********"`} {
		if !strings.Contains(body, want) {
			t.Errorf("answer %s lacks %s", body, want)
		}
	}
	if i, j := strings.Index(body, `"id"`), strings.Index(body, `"org_id"`); i < 0 || j < i || strings.Index(body, `"name"`) < j {
		t.Errorf("answer key order is not the model's: %s", body)
	}
	if strings.Contains(body, proxySecret) || strings.Contains(f.logs.String(), proxySecret) {
		t.Fatal("the client secret is in an answer or a log")
	}
	call := f.fake.calls[0].create
	if string(call.OIDCConfig) == "" || !strings.Contains(string(call.OIDCConfig), proxySecret) {
		t.Fatalf("the secret did not reach auth-service in the oidc_config: %s", call.OIDCConfig)
	}
}

func TestProviderDeleteAnswers204WithNoBody(t *testing.T) {
	f := newProxyFixture(t)
	response := f.do(f.h.deleteProvider, "admin", "", uuid.NewString(), "", nil)
	if response.Code != http.StatusNoContent || response.Body.Len() != 0 {
		t.Fatalf("delete: %d %q, want 204 and no body", response.Code, response.Body)
	}
}

func TestProviderListEchoesTheLimitAndOffsetAsked(t *testing.T) {
	for name, c := range map[string]struct {
		query         string
		limit, offset string
		call          bool
	}{
		"absent":   {"", "50", "0", true},
		"zero":     {"?limit=0", "0", "0", true},
		"max":      {"?limit=1000&offset=7", "1000", "7", true},
		"over max": {"?limit=5000", "5000", "0", true},
	} {
		f := newProxyFixture(t)
		response := f.do(f.h.listProviders, "member", "", "", c.query, nil)
		var answer struct {
			Items  []json.RawMessage `json:"items"`
			Total  int64             `json:"total"`
			Limit  json.Number       `json:"limit"`
			Offset json.Number       `json:"offset"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &answer); err != nil || response.Code != http.StatusOK {
			t.Fatalf("%s: %d %s (%v)", name, response.Code, response.Body, err)
		}
		if answer.Limit.String() != c.limit || answer.Offset.String() != c.offset || answer.Total != 1 || len(answer.Items) != 1 {
			t.Errorf("%s: limit %s offset %s total %d items %d, want limit %s offset %s", name, answer.Limit, answer.Offset, answer.Total, len(answer.Items), c.limit, c.offset)
		}
		if len(f.fake.calls) != 1 || f.fake.calls[0].list.Limit == nil || *f.fake.calls[0].list.Limit != mustInt(c.limit) {
			t.Errorf("%s: auth-service was asked %+v, want limit %s", name, f.fake.calls, c.limit)
		}
	}
}

func mustInt(text string) int64 {
	var n int64
	for _, c := range text {
		n = n*10 + int64(c-'0')
	}
	return n
}

func TestProviderListOutOfRangeIsTheLegacyBare500WithNoCall(t *testing.T) {
	for _, query := range []string{"?limit=-1", "?offset=-1", "?limit=9223372036854775808", "?offset=9223372036854775808"} {
		f := newProxyFixture(t)
		response := f.do(f.h.listProviders, "member", "", "", query, nil)
		if response.Code != http.StatusInternalServerError || !strings.Contains(response.Body.String(), "Internal Server Error") || len(f.fake.calls) != 0 {
			t.Errorf("%s: %d %s calls %d, want the bare 500 and no call", query, response.Code, response.Body, len(f.fake.calls))
		}
	}
}

func TestProviderCallersWithAnUnparsableOrganizationOrIDGetTheLegacyBare500(t *testing.T) {
	f := newProxyFixture(t)
	for name, response := range map[string]*httptest.ResponseRecorder{
		"no org, list":   f.do(f.h.listProviders, "admin", "no-org", "", "", nil),
		"no org, create": f.do(f.h.createProvider, "admin", "no-org", "", "", okBody()),
		"bad id, get":    f.do(f.h.getProvider, "admin", "", "not-a-uuid", "", nil),
		"bad id, delete": f.do(f.h.deleteProvider, "admin", "", "not-a-uuid", "", nil),
		"bad id, update": f.do(f.h.updateProvider, "admin", "", "not-a-uuid", "", map[string]any{"name": "n"}),
		"bad id, status": f.do(f.h.setStatusViaProviders("active").ServeHTTP, "admin", "", "not-a-uuid", "", nil),
	} {
		if response.Code != http.StatusInternalServerError {
			t.Errorf("%s: %d %s, want 500", name, response.Code, response.Body)
		}
	}
	if len(f.fake.calls) != 0 {
		t.Fatalf("an unparsable id reached auth-service: %+v", f.fake.calls)
	}
}

func TestProviderUpdateSendsOnlyTheMembersTheBodyNamed(t *testing.T) {
	f := newProxyFixture(t)
	f.do(f.h.updateProvider, "admin", "", uuid.NewString(), "", map[string]any{"name": "Renamed", "is_default": true, "allowed_domains": []string{"a.example"}, "default_role": nil})
	u := f.fake.calls[0].update
	if u.Name == nil || *u.Name != "Renamed" || u.IsDefault == nil || !*u.IsDefault || u.AllowedDomains == nil || len(*u.AllowedDomains) != 1 {
		t.Fatalf("update = %+v", u)
	}
	if u.AllowIdpInitiated != nil || u.AutoProvision != nil || u.DefaultRole != nil || len(u.SAMLConfig) != 0 || len(u.OIDCConfig) != 0 {
		t.Fatalf("update sent members the body did not name: %+v", u)
	}
}

// 422 first, then 402, then the call: the order the legacy route gave.
func TestProviderGateOrderIsValidationThenLicenceThenTheCall(t *testing.T) {
	f := newProxyFixture(t)
	handler := f.h.gatedTo(body(ssoProviderCreate), http.HandlerFunc(f.h.createProvider))
	serve := func(t *testing.T, payload map[string]any, licensed bool) *httptest.ResponseRecorder {
		t.Helper()
		if licensed {
			licensing.SetProcessLicense(&licensing.ProcessLicense{Tier: "enterprise", Features: map[string]bool{"sso_saml": true}})
		} else {
			licensing.SetProcessLicense(nil)
		}
		t.Cleanup(func() { licensing.SetProcessLicense(nil) })
		raw, _ := json.Marshal(payload)
		value, _ := pyjson.DecodeString(string(raw))
		request := httptest.NewRequest(http.MethodPost, "/x", nil)
		ctx := policy.WithUser(request.Context(), &policy.User{ID: f.user, OrgID: f.org.String(), Role: "admin"})
		ctx = policy.WithBody(ctx, pybody.Body{Value: value})
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request.WithContext(ctx))
		return recorder
	}
	if response := serve(t, map[string]any{"protocol": "oidc"}, false); response.Code != http.StatusUnprocessableEntity {
		t.Errorf("invalid body, unlicensed: %d %s, want 422 before the 402", response.Code, response.Body)
	}
	if response := serve(t, okBody(), false); response.Code != http.StatusPaymentRequired {
		t.Errorf("valid body, unlicensed: %d %s, want 402", response.Code, response.Body)
	}
	if len(f.fake.calls) != 0 {
		t.Fatalf("a refused request reached auth-service: %+v", f.fake.calls)
	}
	if response := serve(t, okBody(), true); response.Code != http.StatusCreated || len(f.fake.calls) != 1 {
		t.Errorf("valid body, licensed: %d %s calls %d, want 201 and one call", response.Code, response.Body, len(f.fake.calls))
	}
}

func TestActivateAndDeactivateCallAuthServiceAndKeepTheirTexts(t *testing.T) {
	f := newProxyFixture(t)
	id := uuid.New()
	for op, handler := range map[string]http.Handler{"activate": f.h.setStatusViaProviders("active"), "deactivate": f.h.setStatusViaProviders("inactive")} {
		f.fake.calls = nil
		response := f.do(handler.ServeHTTP, "owner", "", id.String(), "", nil)
		if response.Code != http.StatusOK || len(f.fake.calls) != 1 || f.fake.calls[0].op != op || f.fake.calls[0].provider != id || f.fake.calls[0].org != f.org {
			t.Errorf("%s: %d %s calls %+v", op, response.Code, response.Body, f.fake.calls)
		}
	}
	f.fake.err = &authprovision.Refused{Status: 404, Reason: authprovision.ReasonSSOProviderNotFound}
	if response := f.do(f.h.setStatusViaProviders("active").ServeHTTP, "admin", "", id.String(), "", nil); response.Code != http.StatusNotFound ||
		!strings.Contains(response.Body.String(), `"detail":"SSO provider not found"`) {
		t.Errorf("unknown provider: %d %s, want 404 SSO provider not found", response.Code, response.Body)
	}
}

// The config is auth-service's bytes, passed on unchanged: a decode and re-marshal here would turn 1.50 into 1.5 and the
// escape into the character, and would reorder nothing but still change bytes.
func TestProviderConfigIsPassedThroughAsAuthServiceWroteIt(t *testing.T) {
	f := newProxyFixture(t)
	config := `{"z":1.50,"a":"\u00e9","m":{"y":[1,2.0],"b":null},"client_secret":"********"}`
	f.fake.answer = `{"id":"` + uuid.NewString() + `","org_id":"` + f.org.String() + `","name":"Acme","protocol":"oidc","status":"active","is_default":false,` +
		`"allow_idp_initiated":false,"auto_provision_users":true,"default_role":"member","config":` + config + `,"allowed_domains":[],` +
		`"last_metadata_sync_at":null,"last_login_at":null,"last_error":null,"last_error_at":null,"created_at":"2026-11-13T01:02:03Z","updated_at":"2026-11-13T01:02:03.250000Z"}`
	for name, response := range map[string]*httptest.ResponseRecorder{
		"get":      f.do(f.h.getProvider, "member", "", uuid.NewString(), "", nil),
		"activate": f.do(f.h.setStatusViaProviders("active").ServeHTTP, "admin", "", uuid.NewString(), "", nil),
		"create":   f.do(f.h.createProvider, "admin", "", "", "", okBody()),
	} {
		if !strings.Contains(response.Body.String(), `"config":`+config+`,"allowed_domains"`) {
			t.Errorf("%s: config not passed through: %s", name, response.Body)
		}
	}
	f.fake.answer = strings.Replace(f.fake.answer, config, `{"a":`, 1)
	if response := f.do(f.h.getProvider, "member", "", uuid.NewString(), "", nil); response.Code != http.StatusServiceUnavailable {
		t.Errorf("a config that is not JSON: %d %s, want the named 503", response.Code, response.Body)
	}
}

// Whole body: the go-api answer for an auth-service provider (PR1d's form: pydantic times, config as stored, masked) is byte-equal
// to what the legacy path renders from the same row (providerResponse over the stored text). Micros zero and not zero.
func TestProviderAnswerBodyIsByteEqualToTheLegacyRender(t *testing.T) {
	stored := `{"scopes": ["openid", "profile"], "client_id": "c", "issuer": "https://\u00e9.example", "claim_mapping": {"z": "1", "a": "2"}, "client_secret": "plain", "certificate": "` + strings.Repeat("Q", 70) + `"}`
	micros := time.Date(2026, 11, 13, 1, 2, 3, 250000000, time.UTC)
	whole := time.Date(2026, 11, 13, 1, 2, 3, 0, time.UTC)
	for name, times := range map[string][2]time.Time{"micros": {micros, whole}, "whole": {whole, whole}, "both": {micros, micros}} {
		f := newProxyFixture(t)
		id := uuid.NewString()
		row := &providerRow{ID: id, OrgID: f.org.String(), Name: "Acme <&>", Protocol: "oidc", Status: "active", DefaultRole: "member", AutoProvision: true,
			Config: &stored, AllowedDomains: ptr(`["a.example", null, "b.example"]`), LastLoginAt: &times[0], LastErrorAt: &times[1], CreatedAt: times[0], UpdatedAt: times[1]}
		want, err := providerResponse(row)
		if err != nil {
			t.Fatal(err)
		}
		wantBytes, err := pyjson.MarshalModel(want)
		if err != nil {
			t.Fatal(err)
		}
		masked, err := ssoprovider.MaskedConfig(&stored)
		if err != nil {
			t.Fatal(err)
		}
		pyd := func(at time.Time) string { return pytime.Pydantic(pytime.UTC(at)) }
		f.fake.answer = `{"id":"` + id + `","org_id":"` + f.org.String() + `","name":"Acme <&>","protocol":"oidc","status":"active","is_default":false,` +
			`"allow_idp_initiated":false,"auto_provision_users":true,"default_role":"member","config":` + string(masked) + `,"allowed_domains":["a.example","b.example"],` +
			`"last_metadata_sync_at":null,"last_login_at":"` + pyd(times[0]) + `","last_error":null,"last_error_at":"` + pyd(times[1]) + `","created_at":"` + pyd(times[0]) + `","updated_at":"` + pyd(times[1]) + `"}`
		for op, response := range map[string]*httptest.ResponseRecorder{
			"get":      f.do(f.h.getProvider, "member", "", id, "", nil),
			"activate": f.do(f.h.setStatusViaProviders("active").ServeHTTP, "admin", "", id, "", nil),
		} {
			if response.Code != http.StatusOK || response.Body.String() != string(wantBytes) {
				t.Errorf("%s/%s:\n got  %s\n want %s", name, op, response.Body, wantBytes)
			}
		}
	}
}

func ptr(s string) *string { return &s }

func TestProviderListItemsCarryTheConfigAsAuthServiceWroteIt(t *testing.T) {
	f := newProxyFixture(t)
	config := `{"z":1.50,"a":"é","client_secret":"********"}`
	item := `{"id":"` + uuid.NewString() + `","org_id":"` + f.org.String() + `","name":"Acme","protocol":"oidc","status":"active","is_default":false,` +
		`"allow_idp_initiated":false,"auto_provision_users":true,"default_role":"member","config":` + config + `,"allowed_domains":[],` +
		`"last_metadata_sync_at":null,"last_login_at":null,"last_error":null,"last_error_at":null,"created_at":"2026-11-13T01:02:03Z","updated_at":"2026-11-13T01:02:03Z"}`
	f.fake.list = `{"items":[` + item + `,` + item + `],"total":2,"limit":1000,"offset":0}`
	response := f.do(f.h.listProviders, "member", "", "", "", nil)
	if response.Code != http.StatusOK || strings.Count(response.Body.String(), `"config":`+config+`,"allowed_domains"`) != 2 {
		t.Fatalf("list: %d %s, want both configs passed through", response.Code, response.Body)
	}
}

// The fault paths carry no client secret either: the body of the named 503 and the WARN of every refusal and fault of a create
// and an update that sent the sentinel.
func TestProviderFaultPathsNeverCarryTheClientSecret(t *testing.T) {
	faults := map[string]error{
		"unavailable":   authprovision.ErrUnavailable,
		"refused other": &authprovision.Refused{Status: 400, Reason: "malformed"},
		"org missing":   &authprovision.Refused{Status: 404, Reason: authprovision.ReasonOrganizationMissing},
		"plain":         errors.New("dial tcp: connection refused"),
	}
	for name, fault := range faults {
		f := newProxyFixture(t)
		f.fake.err = fault
		body := map[string]any{"name": "Acme", "protocol": "oidc", "oidc_config": map[string]any{"client_id": "c", "client_secret": proxySecret, "issuer": "https://i.example"}}
		for op, response := range map[string]*httptest.ResponseRecorder{
			"create": f.do(f.h.createProvider, "admin", "", uuid.NewString(), "", body),
			"update": f.do(f.h.updateProvider, "admin", "", uuid.NewString(), "", map[string]any{"oidc_config": body["oidc_config"]}),
		} {
			if response.Code != http.StatusServiceUnavailable {
				t.Errorf("%s/%s: %d %s, want the named 503", name, op, response.Code, response.Body)
			}
			if strings.Contains(response.Body.String(), proxySecret) {
				t.Errorf("%s/%s: the secret is in the answer: %s", name, op, response.Body)
			}
		}
		if strings.Contains(f.logs.String(), proxySecret) || !strings.Contains(f.logs.String(), "level=WARN") {
			t.Errorf("%s: logs %q: want a WARN without the secret", name, f.logs.String())
		}
	}
}
