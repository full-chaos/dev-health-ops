package authapi

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/auth/ssoprovider"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/usertokens"
)

const ssoSecretSentinel = "s3ntinel-client-secret-VALUE"

type fakeSealer struct{}

func (fakeSealer) Seal(plain string) (string, error) {
	return "sealed(" + strings.ToUpper(plain) + ")", nil
}

type ssoCall struct {
	op     string
	org    uuid.UUID
	id     uuid.UUID
	create ssoprovider.CreateInput
	update ssoprovider.UpdateInput
	status string
	filter ssoprovider.Filter
	actor  ssoprovider.Actor
}

type fakeSSOProviders struct {
	calls  []ssoCall
	err    error
	answer *ssoprovider.Provider
}

func (f *fakeSSOProviders) provider(org uuid.UUID) *ssoprovider.Provider {
	if f.answer != nil {
		return f.answer
	}
	now := time.Date(2026, 11, 13, 1, 2, 3, 0, time.UTC)
	return &ssoprovider.Provider{ID: uuid.NewString(), OrgID: org.String(), Name: "Acme", Protocol: "oidc", Status: "pending_setup", DefaultRole: "member",
		AutoProvision: true, Config: json.RawMessage(`{"client_secret":"********"}`), AllowedDomains: []string{}, CreatedAt: now, UpdatedAt: now}
}

func (f *fakeSSOProviders) List(_ context.Context, org uuid.UUID, filter ssoprovider.Filter) ([]ssoprovider.Provider, int64, error) {
	f.calls = append(f.calls, ssoCall{op: "list", org: org, filter: filter})
	return []ssoprovider.Provider{*f.provider(org)}, 1, f.err
}

func (f *fakeSSOProviders) Get(_ context.Context, org, id uuid.UUID) (*ssoprovider.Provider, error) {
	f.calls = append(f.calls, ssoCall{op: "get", org: org, id: id})
	return f.provider(org), f.err
}

func (f *fakeSSOProviders) Create(_ context.Context, org uuid.UUID, in ssoprovider.CreateInput, a ssoprovider.Actor) (*ssoprovider.Provider, error) {
	f.calls = append(f.calls, ssoCall{op: "create", org: org, create: in, actor: a})
	return f.provider(org), f.err
}

func (f *fakeSSOProviders) Update(_ context.Context, org, id uuid.UUID, in ssoprovider.UpdateInput, a ssoprovider.Actor) (*ssoprovider.Provider, error) {
	f.calls = append(f.calls, ssoCall{op: "update", org: org, id: id, update: in, actor: a})
	return f.provider(org), f.err
}

func (f *fakeSSOProviders) SetStatus(_ context.Context, org, id uuid.UUID, status string, a ssoprovider.Actor) (*ssoprovider.Provider, error) {
	f.calls = append(f.calls, ssoCall{op: "status", org: org, id: id, status: status, actor: a})
	return f.provider(org), f.err
}

func (f *fakeSSOProviders) Delete(_ context.Context, org, id uuid.UUID, a ssoprovider.Actor) error {
	f.calls = append(f.calls, ssoCall{op: "delete", org: org, id: id, actor: a})
	return f.err
}

type ssoRouteCase struct {
	op, path string
	body     func(f *provFixture, actor uuid.UUID) string
	status   int
}

func ssoOIDCConfig() map[string]any {
	return map[string]any{"client_id": "cid", "client_secret": ssoSecretSentinel, "issuer": "https://idp.example.com"}
}

func ssoRouteCases() []ssoRouteCase {
	byID := func(f *provFixture, actor uuid.UUID) string {
		return encode(map[string]any{"organization_id": f.org.String(), "actor_principal_id": actor.String(), "provider_id": uuid.NewString()})
	}
	return []ssoRouteCase{
		{opSSOProviderList, PatternSSOProviderList, func(f *provFixture, actor uuid.UUID) string {
			return encode(map[string]any{"organization_id": f.org.String(), "actor_principal_id": actor.String()})
		}, http.StatusOK},
		{opSSOProviderCreate, PatternSSOProviderCreate, func(f *provFixture, actor uuid.UUID) string {
			return encode(map[string]any{"organization_id": f.org.String(), "actor_principal_id": actor.String(), "name": "Acme", "protocol": "oidc", "oidc_config": ssoOIDCConfig()})
		}, http.StatusCreated},
		{opSSOProviderGet, PatternSSOProviderGet, byID, http.StatusOK},
		{opSSOProviderUpdate, PatternSSOProviderUpdate, func(f *provFixture, actor uuid.UUID) string {
			return encode(map[string]any{"organization_id": f.org.String(), "actor_principal_id": actor.String(), "provider_id": uuid.NewString(), "name": "Renamed"})
		}, http.StatusOK},
		{opSSOProviderDelete, PatternSSOProviderDelete, byID, http.StatusNoContent},
		{opSSOProviderActivate, PatternSSOProviderActivate, byID, http.StatusOK},
		{opSSOProviderDeactivate, PatternSSOProviderDeactivate, byID, http.StatusOK},
	}
}

func TestSSOProviderRoutesAcceptTheGoAPIWorkloadToken(t *testing.T) {
	t.Parallel()
	for _, c := range ssoRouteCases() {
		f := newProvFixtureFor(t, nil, nil)
		expectRefusal(t, f.post(c.path, "{}", nil), http.StatusBadRequest, reasonMalformed)
		if len(f.sso.calls) != 0 {
			t.Fatalf("%s: a {} body reached the store", c.op)
		}
		if response := f.post(c.path, c.body(f, f.owner), nil); response.Code != c.status {
			t.Fatalf("%s: %d %s, want %d", c.op, response.Code, response.Body, c.status)
		}
		if len(f.sso.calls) != 1 {
			t.Fatalf("%s: %d store calls, want 1", c.op, len(f.sso.calls))
		}
	}
}

func TestSSOProviderRoutesRefuseUserTokens(t *testing.T) {
	t.Parallel()
	tokens := userTokensWithIssuer(t)
	f := newProvFixtureFor(t, nil, nil)
	for _, c := range ssoRouteCases() {
		for _, tok := range tokens {
			response := f.post(c.path, c.body(f, f.owner), map[string]string{"Authorization": "Bearer " + tok.Value})
			if response.Code != http.StatusUnauthorized || !strings.Contains(response.Body.String(), reasonInvalidCredentials) {
				t.Errorf("%s %s: %d %s, want 401 %s", c.op, tok.Kind, response.Code, response.Body, reasonInvalidCredentials)
			}
			usertokens.NoLeak(t, c.op+" "+tok.Kind+" body", response.Body.String(), tokens)
		}
	}
	usertokens.NoLeak(t, "logs", f.logs.String(), tokens)
	if len(f.sso.calls) != 0 || f.users.reads != 0 || f.w.keys.reads != 0 {
		t.Fatalf("a user token reached the store (%d), an actor (%d) or the key set (%d)", len(f.sso.calls), f.users.reads, f.w.keys.reads)
	}
}

func TestSSOProviderRoutesMountOnlyWithTheirRows(t *testing.T) {
	t.Parallel()
	for _, c := range ssoRouteCases() {
		f := newProvFixtureFor(t, embeddedDocWithoutRoutes(t, c.path), nil)
		if response := f.post(c.path, c.body(f, f.owner), nil); response.Code != http.StatusNotFound && response.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%s without its row: %d, want the route unmounted", c.op, response.Code)
		}
		if len(f.sso.calls) != 0 {
			t.Fatalf("%s without its row reached the store", c.op)
		}
	}
}

func TestSSOProviderCreateRefusesAProtocolWithoutItsConfig(t *testing.T) {
	t.Parallel()
	f := newProvFixtureFor(t, nil, nil)
	for protocol, reason := range map[string]string{"saml": reasonSAMLConfigRequired, "oidc": reasonOIDCConfigRequired} {
		body := encode(map[string]any{"organization_id": f.org.String(), "actor_principal_id": f.owner.String(), "name": "Acme", "protocol": protocol})
		expectRefusal(t, f.post(PatternSSOProviderCreate, body, nil), http.StatusBadRequest, reason)
	}
	body := encode(map[string]any{"organization_id": f.org.String(), "actor_principal_id": f.owner.String(), "name": "Acme", "protocol": "ldap"})
	expectRefusal(t, f.post(PatternSSOProviderCreate, body, nil), http.StatusBadRequest, reasonMalformed)
	if len(f.sso.calls) != 0 {
		t.Fatal("a refused create reached the store")
	}
}

func TestSSOProviderSecretIsSealedAndNeverInAnAnswerOrALog(t *testing.T) {
	t.Parallel()
	f := newProvFixtureFor(t, nil, nil)
	create := ssoRouteCases()[1]
	response := f.post(create.path, create.body(f, f.owner), nil)
	if response.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", response.Code, response.Body)
	}
	call := f.sso.calls[0]
	if call.create.Config.Secrets == nil || !strings.Contains(*call.create.Config.Secrets, "sealed(S3NTINEL-CLIENT-SECRET-VALUE)") ||
		strings.Contains(*call.create.Config.Secrets, ssoSecretSentinel) {
		t.Fatalf("the stored secrets object is not the sealed secret: %v", call.create.Config.Secrets)
	}
	if _, ok := call.create.Config.JSON.Get("client_secret"); ok {
		t.Fatal("the stored config carries client_secret")
	}
	for _, key := range []string{"authorization_endpoint", "token_endpoint", "userinfo_endpoint", "jwks_uri"} {
		if v, ok := call.create.Config.JSON.Get(key); !ok || v != nil {
			t.Fatalf("config %s = %v, %v; want present and null (the legacy model dump)", key, v, ok)
		}
	}
	update := ssoRouteCases()[3]
	body := encode(map[string]any{"organization_id": f.org.String(), "actor_principal_id": f.owner.String(), "provider_id": uuid.NewString(), "oidc_config": ssoOIDCConfig()})
	if response := f.post(update.path, body, nil); response.Code != http.StatusOK {
		t.Fatalf("update: %d %s", response.Code, response.Body)
	}
	if got := f.sso.calls[1].update.Config; got == nil || got.Secrets == nil || strings.Contains(*got.Secrets, ssoSecretSentinel) {
		t.Fatalf("the update did not seal the secret: %v", got)
	}
	if strings.Contains(response.Body.String(), ssoSecretSentinel) || strings.Contains(f.logs.String(), ssoSecretSentinel) ||
		strings.Contains(f.logs.String(), "sealed(") {
		t.Fatal("the secret or its sealed form is in an answer or a log")
	}
	var answer map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &answer); err != nil {
		t.Fatal(err)
	}
}

func TestSSOProviderCreateDefaultsAreTheLegacyOnes(t *testing.T) {
	t.Parallel()
	f := newProvFixtureFor(t, nil, nil)
	create := ssoRouteCases()[1]
	if response := f.post(create.path, create.body(f, f.owner), nil); response.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", response.Code, response.Body)
	}
	in := f.sso.calls[0].create
	if in.IsDefault || in.AllowIdpInitiated || !in.AutoProvision || in.DefaultRole != "member" || in.AllowedDomains == nil || len(in.AllowedDomains) != 0 {
		t.Fatalf("create defaults = %+v", in)
	}
	if stored, err := pyjson.Dumps(in.Config.JSON); err != nil || !strings.Contains(stored, `"scopes": ["openid", "profile", "email"], "claim_mapping": {}`) {
		t.Fatalf("oidc defaults = %v (%v)", stored, err)
	}
}

func TestSSOProviderListBodyBoundsAndDefaults(t *testing.T) {
	t.Parallel()
	f := newProvFixtureFor(t, nil, nil)
	base := map[string]any{"organization_id": f.org.String(), "actor_principal_id": f.owner.String()}
	if response := f.post(PatternSSOProviderList, encode(base), nil); response.Code != http.StatusOK {
		t.Fatalf("list: %d %s", response.Code, response.Body)
	}
	if got := f.sso.calls[0].filter; got.Limit != 50 || got.Offset != 0 || got.Protocol != "" || got.Status != "" {
		t.Fatalf("default filter = %+v, want limit 50 offset 0", got)
	}
	for _, bad := range []map[string]any{{"limit": -1}, {"offset": -1}, {"limit": "5"}} {
		body := map[string]any{"organization_id": base["organization_id"], "actor_principal_id": base["actor_principal_id"]}
		for k, v := range bad {
			body[k] = v
		}
		expectRefusal(t, f.post(PatternSSOProviderList, encode(body), nil), http.StatusBadRequest, reasonMalformed)
	}
	var answer ssoProviderListAnswer
	response := f.post(PatternSSOProviderList, encode(map[string]any{"organization_id": f.org.String(), "actor_principal_id": f.owner.String(), "limit": 7, "offset": 3, "protocol": "oidc"}), nil)
	if err := json.Unmarshal(response.Body.Bytes(), &answer); err != nil || answer.Limit != 7 || answer.Offset != 3 || answer.Total != 1 || len(answer.Items) != 1 {
		t.Fatalf("list answer = %s (%v)", response.Body, err)
	}
	if got := f.sso.calls[len(f.sso.calls)-1].filter; got.Protocol != "oidc" || got.Limit != 7 || got.Offset != 3 {
		t.Fatalf("filter = %+v", got)
	}
}

func TestSSOProviderMissingProviderIsLegacy404(t *testing.T) {
	t.Parallel()
	for _, c := range ssoRouteCases() {
		if c.op == opSSOProviderList || c.op == opSSOProviderCreate {
			continue
		}
		f := newProvFixtureFor(t, nil, nil)
		f.sso.err = ssoprovider.ErrNotFound
		expectRefusal(t, f.post(c.path, c.body(f, f.owner), nil), http.StatusNotFound, reasonSSOProviderNotFound)
	}
}

func TestSSOProviderStoreFaultIs503(t *testing.T) {
	t.Parallel()
	f := newProvFixtureFor(t, nil, nil)
	f.sso.err = ssoprovider.ErrStore
	for _, c := range ssoRouteCases() {
		expectRefusal(t, f.post(c.path, c.body(f, f.owner), nil), http.StatusServiceUnavailable, reasonStoreUnavailable)
	}
}

func TestSSOProviderActivateAndDeactivateSetTheirStatus(t *testing.T) {
	t.Parallel()
	for path, want := range map[string]string{PatternSSOProviderActivate: ssoprovider.StatusActive, PatternSSOProviderDeactivate: ssoprovider.StatusInactive} {
		f := newProvFixtureFor(t, nil, nil)
		body := encode(map[string]any{"organization_id": f.org.String(), "actor_principal_id": f.owner.String(), "provider_id": uuid.NewString()})
		if response := f.post(path, body, nil); response.Code != http.StatusOK {
			t.Fatalf("%s: %d %s", path, response.Code, response.Body)
		}
		call := f.sso.calls[0]
		if call.op != "status" || call.status != want || call.org != f.org || call.actor.PrincipalID == nil || *call.actor.PrincipalID != f.owner {
			t.Fatalf("%s: store call %+v, want status %s for the body's organization and actor", path, call, want)
		}
	}
}

func TestSSOProviderListLimitIsClampedNotRefused(t *testing.T) {
	t.Parallel()
	f := newProvFixtureFor(t, nil, nil)
	body := encode(map[string]any{"organization_id": f.org.String(), "actor_principal_id": f.owner.String(), "limit": 5000})
	var answer ssoProviderListAnswer
	response := f.post(PatternSSOProviderList, body, nil)
	if err := json.Unmarshal(response.Body.Bytes(), &answer); err != nil || response.Code != http.StatusOK || answer.Limit != maxSSOListMax ||
		f.sso.calls[0].filter.Limit != maxSSOListMax {
		t.Fatalf("limit 5000: %d %s, store limit %d, want 200 clamped to %d", response.Code, response.Body, f.sso.calls[0].filter.Limit, maxSSOListMax)
	}
}

func TestSSOProviderRoutesAddNoActorRuleOfTheirOwn(t *testing.T) {
	t.Parallel()
	for _, c := range ssoRouteCases() {
		f := newProvFixtureFor(t, nil, nil)
		if response := f.post(c.path, c.body(f, f.viewer), nil); response.Code != c.status {
			t.Fatalf("%s as a viewer: %d %s, want %d (go-api is the decision point)", c.op, response.Code, response.Body, c.status)
		}
		if f.users.reads != 0 {
			t.Fatalf("%s read the actor (%d reads)", c.op, f.users.reads)
		}
	}
}

func TestSSOProviderRoutesRefuseAnUnknownOrganization(t *testing.T) {
	t.Parallel()
	for _, c := range ssoRouteCases() {
		f := newProvFixtureFor(t, nil, nil)
		f.orgs.err = identity.ErrOrganizationNotFound
		expectRefusal(t, f.post(c.path, c.body(f, f.owner), nil), http.StatusNotFound, reasonOrganizationNotFound)
		if len(f.sso.calls) != 0 {
			t.Fatalf("%s: an unknown organization reached the store", c.op)
		}
	}
}

func TestSSOProviderRoutesRefuseOtherProdWorkloadSubjects(t *testing.T) {
	t.Parallel()
	acrAPI, queryAPI, web := session.WirePrincipalID(uuid.New()), session.WirePrincipalID(uuid.New()), session.WirePrincipalID(uuid.New())
	refused := map[string]func(*contracts.WorkloadTokenClaims){
		"acr-api subject":               func(c *contracts.WorkloadTokenClaims) { c.Sub = acrAPI },
		"query-api subject":             func(c *contracts.WorkloadTokenClaims) { c.Sub = queryAPI },
		"web subject":                   func(c *contracts.WorkloadTokenClaims) { c.Sub = web },
		"acr-api subject, api audience": func(c *contracts.WorkloadTokenClaims) { c.Sub, c.Aud = acrAPI, "dev-health-api" },
		"go-api subject, api audience":  func(c *contracts.WorkloadTokenClaims) { c.Aud = "dev-health-api" },
		"go-api subject, expired":       func(c *contracts.WorkloadTokenClaims) { c.Iat -= 600; c.Nbf -= 600; c.Exp -= 600 },
	}
	for name, mutate := range refused {
		for _, c := range ssoRouteCases() {
			f := newProvFixtureFor(t, nil, nil)
			header, _ := f.w.mint(contracts.WorkloadTokenTyp, mutate)
			expectRefusal(t, f.post(c.path, c.body(f, f.owner), header), http.StatusUnauthorized, reasonInvalidCredentials)
			if len(f.sso.calls) != 0 || f.orgs.calls != 0 {
				t.Fatalf("%s, %s: a refused workload token reached a store", c.op, name)
			}
		}
	}
}

// The embedded endpoint-profile document holds the seven SSO provider rows, each admitting exactly the go-api workload
// token: the routes mount on it, all seven together.
func TestSSOProviderRoutesMountOnTheEmbeddedDocument(t *testing.T) {
	t.Parallel()
	f := newProvFixtureFor(t, nil, nil)
	routes, err := SSOProviderRoutes(ProvisioningDeps{Users: f.users, Reader: noRows{}, Organizations: f.orgs,
		SSOProviderStores: SSOProviderStores{SSOProviders: f.sso, SSOSealer: fakeSealer{}}, Workload: nil})
	if err != nil || len(routes) != SSOProviderRouteCount() {
		t.Fatalf("embedded document: %d routes, err %v; want %d and no error", len(routes), err, SSOProviderRouteCount())
	}
	if err := SSOProviderRowsError(nil); err != nil {
		t.Fatalf("the embedded document fails the SSO provider rows check: %v", err)
	}
}

// The answers are byte-equal to what go-api's providerResponse renders: times in pydantic's form (six fraction digits only when the
// microseconds are not zero), the config as kept (key order, no HTML escaping, no ASCII escaping).
func TestSSOProviderAnswerTimesAndConfigAreInGoAPIsForm(t *testing.T) {
	t.Parallel()
	f := newProvFixtureFor(t, nil, nil)
	config := "{\"scopes\":[\"openid\"],\"client_id\":\"c\",\"note\":\"a<b>&c\\u2028\u00e9\",\"client_secret\":\"********\"}"
	micros := time.Date(2026, 11, 13, 1, 2, 3, 250000000, time.UTC)
	whole := time.Date(2026, 11, 13, 1, 2, 3, 0, time.UTC)
	nanos := time.Date(2026, 11, 13, 1, 2, 3, 123456789, time.UTC)
	f.sso.answer = &ssoprovider.Provider{ID: uuid.NewString(), OrgID: f.org.String(), Name: "Acme", Protocol: "oidc", Status: "active", DefaultRole: "member",
		Config: json.RawMessage(config), AllowedDomains: []string{}, LastLoginAt: &micros, LastErrorAt: &whole, LastMetadataSyncAt: &nanos, CreatedAt: whole, UpdatedAt: micros}
	for _, c := range []ssoRouteCase{ssoRouteCases()[2], ssoRouteCases()[5], ssoRouteCases()[6]} {
		response := f.post(c.path, c.body(f, f.owner), nil)
		body := response.Body.String()
		if strings.HasSuffix(body, "\n") || response.Header().Get("Content-Type") != "application/json" || response.Header().Get("Content-Length") != strconv.Itoa(len(body)) {
			t.Errorf("%s: the answer is not written as go-api writes one (no trailing newline, JSON type, exact length): %q %v", c.op, body, response.Header())
		}
		for _, want := range []string{`"config":` + config, `"last_login_at":"2026-11-13T01:02:03.250000Z"`, `"last_error_at":"2026-11-13T01:02:03Z"`,
			`"last_metadata_sync_at":"2026-11-13T01:02:03.123456Z"`, `"created_at":"2026-11-13T01:02:03Z"`, `"updated_at":"2026-11-13T01:02:03.250000Z"`} {
			if !strings.Contains(body, want) {
				t.Errorf("%s: answer %s lacks %s", c.op, body, want)
			}
		}
	}
}

// A created config is stored in the legacy model's field order (a nested object as sent), as json.dumps writes it.
func TestSSOProviderCreateStoresTheConfigInTheModelFieldOrder(t *testing.T) {
	t.Parallel()
	f := newProvFixtureFor(t, nil, nil)
	body := `{"organization_id":"` + f.org.String() + `","actor_principal_id":"` + f.owner.String() + `","name":"Acme","protocol":"oidc","oidc_config":` +
		`{"claim_mapping":{"zeta":"z","alpha":"a"},"issuer":"https://\u00e9.example","client_secret":"` + ssoSecretSentinel + `","client_id":"cid","scopes":["email","openid"]}}`
	if response := f.post(PatternSSOProviderCreate, body, nil); response.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", response.Code, response.Body)
	}
	stored, err := pyjson.Dumps(f.sso.calls[0].create.Config.JSON)
	want := `{"client_id": "cid", "issuer": "https://\u00e9.example", "authorization_endpoint": null, "token_endpoint": null, "userinfo_endpoint": null, ` +
		`"jwks_uri": null, "scopes": ["email", "openid"], "claim_mapping": {"zeta": "z", "alpha": "a"}}`
	if err != nil || stored != want {
		t.Fatalf("stored oidc config = %s (%v), want %s", stored, err, want)
	}
	saml := `{"organization_id":"` + f.org.String() + `","actor_principal_id":"` + f.owner.String() + `","name":"S","protocol":"saml","saml_config":` +
		`{"certificate":"CERT","sso_url":"https://s.example","entity_id":"e","attribute_mapping":{"b":"2","a":"1"}}}`
	if response := f.post(PatternSSOProviderCreate, saml, nil); response.Code != http.StatusCreated {
		t.Fatalf("create saml: %d %s", response.Code, response.Body)
	}
	stored, err = pyjson.Dumps(f.sso.calls[1].create.Config.JSON)
	want = `{"entity_id": "e", "sso_url": "https://s.example", "certificate": "CERT", "slo_url": null, ` +
		`"name_id_format": "urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress", "attribute_mapping": {"b": "2", "a": "1"}}`
	if err != nil || stored != want {
		t.Fatalf("stored saml config = %s (%v), want %s", stored, err, want)
	}
}
