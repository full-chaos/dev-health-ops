//go:build integration

package sso_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/sso"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/edtokens"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// The provider CRUD routes, switch on, over a real migrated Postgres with the production entitlement read
// (licensing.OrgHasFeature over the pool; sso.Deps.OrgEntitled nil): the org's organizations.tier or org_licenses row
// decides, the token's organization is the one read, and a failed read denies. The process tier is community.

const unlicensedBody = `{"detail":{"error":"feature_not_licensed","feature":"sso_saml","required_tier":"enterprise","current_tier":"community"}}`

type recordingProviders struct {
	mu   sync.Mutex
	orgs []uuid.UUID
}

func (p *recordingProviders) answer(org uuid.UUID) json.RawMessage {
	p.mu.Lock()
	p.orgs = append(p.orgs, org)
	p.mu.Unlock()
	return json.RawMessage(`{"id":"` + uuid.NewString() + `","org_id":"` + org.String() + `","name":"Acme","protocol":"oidc","status":"pending_setup",` +
		`"is_default":false,"allow_idp_initiated":false,"auto_provision_users":true,"default_role":"member","config":{"issuer":"https://i.example"},` +
		`"allowed_domains":[],"last_metadata_sync_at":null,"last_login_at":null,"last_error":null,"last_error_at":null,` +
		`"created_at":"2026-11-15T01:02:03Z","updated_at":"2026-11-15T01:02:03Z"}`)
}

func (p *recordingProviders) List(_ context.Context, in authprovision.SSOListRequest) (json.RawMessage, error) {
	return json.RawMessage(`{"items":[` + string(p.answer(in.Org)) + `],"total":1,"limit":50,"offset":0}`), nil
}

func (p *recordingProviders) Get(_ context.Context, org, _, _ uuid.UUID) (json.RawMessage, error) {
	return p.answer(org), nil
}

func (p *recordingProviders) Create(_ context.Context, in authprovision.SSOCreateRequest) (json.RawMessage, error) {
	return p.answer(in.Org), nil
}

func (p *recordingProviders) Update(_ context.Context, in authprovision.SSOUpdateRequest) (json.RawMessage, error) {
	return p.answer(in.Org), nil
}

func (p *recordingProviders) Delete(_ context.Context, org, _, _ uuid.UUID) error {
	p.answer(org)
	return nil
}

func (p *recordingProviders) Activate(_ context.Context, org, _, _ uuid.UUID) (json.RawMessage, error) {
	return p.answer(org), nil
}

func (p *recordingProviders) Deactivate(_ context.Context, org, _, _ uuid.UUID) (json.RawMessage, error) {
	return p.answer(org), nil
}

func (p *recordingProviders) take() []uuid.UUID {
	p.mu.Lock()
	defer p.mu.Unlock()
	orgs := p.orgs
	p.orgs = nil
	return orgs
}

type entitlementStack struct {
	pool      *pgxpool.Pool
	on, off   *httptest.Server
	tokens    *edtokens.Issuer
	providers *recordingProviders
	logs      *syncBuffer
	superuser uuid.UUID
	owners    map[string]uuid.UUID
	orgs      map[string]uuid.UUID
}

func startEntitlementStack(t *testing.T, ctx context.Context) *entitlementStack {
	t.Helper()
	licensing.SetProcessLicense(nil)
	pg, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pg.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, pg.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	pgschema.Apply(ctx, t, pool)
	st := &entitlementStack{pool: pool, tokens: edtokens.New(t), providers: &recordingProviders{}, logs: &syncBuffer{},
		owners: map[string]uuid.UUID{}, orgs: map[string]uuid.UUID{}}
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, query, args...); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
	}
	user := func(superuser bool) uuid.UUID {
		id := uuid.New()
		exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, $3, 0, now(), now())`, id, "u-"+id.String()[:8]+"@entitlement.test", superuser)
		return id
	}
	// entitled: organizations.tier enterprise. community: community. license-wins: enterprise in organizations, but an
	// org_licenses row at community decides. override: a team-tier org_licenses row whose features_override grants sso_saml.
	for name, tiers := range map[string][2]string{"entitled": {"enterprise", ""}, "community": {"community", ""},
		"license-wins": {"enterprise", "community"}, "override": {"community", "team"}} {
		org := uuid.New()
		exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, settings, created_at, updated_at)
VALUES ($1, $2, $2, $3, 'stripe', true, '{}', now(), now())`, org, name+"-"+org.String()[:8], tiers[0])
		if tiers[1] != "" {
			override := `{}`
			if name == "override" {
				override = `{"sso_saml": true}`
			}
			exec(`INSERT INTO org_licenses (id, org_id, tier, is_valid, license_type, managed_by, features_override, created_at, updated_at)
VALUES ($1, $2, $3, true, 'saas', 'stripe', $4::json, now(), now())`, uuid.New(), org, tiers[1], override)
		}
		owner := user(false)
		exec(`INSERT INTO memberships (id, user_id, org_id, role, joined_at, created_at, updated_at)
VALUES (gen_random_uuid(), $1, $2, 'owner', now(), now(), now())`, owner, org)
		st.orgs[name], st.owners[name] = org, owner
	}
	st.superuser = user(true)
	logger := slog.New(slog.NewTextHandler(st.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	auth := st.tokens.Authenticator(t, policy.PGStore{Pool: pool}, logger)
	serve := func(providers authprovision.SSOProviders) *httptest.Server {
		deps := sso.Deps{Pool: pool, Guard: policy.NewGuard(auth, logger), Logger: logger}
		if providers != nil {
			deps.Providers = providers
		}
		mux := http.NewServeMux()
		for _, route := range sso.Routes(deps) {
			mux.Handle(route.Method+" "+route.Pattern, route.Handler)
		}
		server := httptest.NewServer(mux)
		t.Cleanup(server.Close)
		return server
	}
	st.on, st.off = serve(st.providers), serve(nil)
	return st
}

func (st *entitlementStack) token(t *testing.T, user uuid.UUID, org string) string {
	t.Helper()
	return st.tokens.MustMint(t, edtokens.Claims{UserID: user, OrgID: org})
}

func (st *entitlementStack) do(t *testing.T, server *httptest.Server, method, path, token, body string) (int, string) {
	t.Helper()
	request, err := http.NewRequest(method, server.URL+"/api/v1/auth"+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	raw, _ := io.ReadAll(response.Body)
	return response.StatusCode, string(raw)
}

const createBody = `{"name":"Acme","protocol":"oidc","oidc_config":{"client_id":"c","client_secret":"s","issuer":"https://i.example"}}`

func TestProviderRoutesReadTheOrgEntitlementFromTheDatabase(t *testing.T) {
	ctx := context.Background()
	st := startEntitlementStack(t, ctx)
	for name, admitted := range map[string]bool{"entitled": true, "community": false, "license-wins": false, "override": true} {
		token := st.token(t, st.owners[name], st.orgs[name].String())
		listStatus, listBody := st.do(t, st.on, http.MethodGet, "/sso/providers", token, "")
		createStatus, createBodyGot := st.do(t, st.on, http.MethodPost, "/sso/providers", token, createBody)
		orgs := st.providers.take()
		if admitted {
			if listStatus != http.StatusOK || createStatus != http.StatusCreated {
				t.Errorf("%s: list %d %s, create %d %s, want 200 and 201", name, listStatus, listBody, createStatus, createBodyGot)
			}
			if len(orgs) != 2 || orgs[0] != st.orgs[name] || orgs[1] != st.orgs[name] {
				t.Errorf("%s: auth-service called for %v, want the token organization %s twice", name, orgs, st.orgs[name])
			}
		} else {
			if listStatus != http.StatusPaymentRequired || listBody != unlicensedBody || createStatus != http.StatusPaymentRequired || createBodyGot != unlicensedBody {
				t.Errorf("%s: list %d %s, create %d %s, want the 402 %s", name, listStatus, listBody, createStatus, createBodyGot, unlicensedBody)
			}
			if len(orgs) != 0 {
				t.Errorf("%s: an unentitled organization reached auth-service: %v", name, orgs)
			}
		}
		t.Logf("CELL\treal-pg\t%s\tlist=%d\tcreate=%d", name, listStatus, createStatus)
	}
}

// A platform admin with no membership row: with the token naming the entitled org, it reads (the named widening) and is
// refused writes by its role; with a token without an org, the 402.
func TestProviderRoutesForAPlatformAdminWithoutAMembership(t *testing.T) {
	ctx := context.Background()
	st := startEntitlementStack(t, ctx)
	inOrg := st.token(t, st.superuser, st.orgs["entitled"].String())
	if status, body := st.do(t, st.on, http.MethodGet, "/sso/providers", inOrg, ""); status != http.StatusOK {
		t.Errorf("token org entitled, list: %d %s, want 200", status, body)
	}
	if status, body := st.do(t, st.on, http.MethodPost, "/sso/providers", inOrg, createBody); status != http.StatusForbidden || body != `{"detail":"Admin access required"}` {
		t.Errorf("token org entitled, create: %d %s, want 403 Admin access required", status, body)
	}
	noOrg := st.token(t, st.superuser, "")
	for _, request := range [][2]string{{http.MethodGet, ""}, {http.MethodPost, createBody}} {
		if status, body := st.do(t, st.on, request[0], "/sso/providers", noOrg, request[1]); status != http.StatusPaymentRequired || body != unlicensedBody {
			t.Errorf("token without an org, %s: %d %s, want the 402", request[0], status, body)
		}
	}
	if orgs := st.providers.take(); len(orgs) != 1 || orgs[0] != st.orgs["entitled"] {
		t.Fatalf("auth-service called for %v, want the entitled list only", orgs)
	}
}

// The switch-off route keeps the process-only gate: the entitled org gets main's 402, byte for byte.
func TestProviderRoutesWithTheSwitchOffIgnoreTheOrgEntitlement(t *testing.T) {
	ctx := context.Background()
	st := startEntitlementStack(t, ctx)
	token := st.token(t, st.owners["entitled"], st.orgs["entitled"].String())
	for _, request := range [][3]string{{http.MethodGet, "/sso/providers", ""}, {http.MethodPost, "/sso/providers", createBody},
		{http.MethodGet, "/sso/providers/" + uuid.NewString(), ""}, {http.MethodDelete, "/sso/providers/" + uuid.NewString(), ""}} {
		if status, body := st.do(t, st.off, request[0], request[1], token, request[2]); status != http.StatusPaymentRequired || body != unlicensedBody {
			t.Errorf("switch off, %s %s: %d %s, want the 402 %s", request[0], request[1], status, body, unlicensedBody)
		}
	}
}

// With the entitlement table unreadable the gate denies with the 402 and logs the failure; it never admits.
func TestProviderRoutesDenyWhenTheEntitlementStoreFails(t *testing.T) {
	ctx := context.Background()
	st := startEntitlementStack(t, ctx)
	token := st.token(t, st.owners["entitled"], st.orgs["entitled"].String())
	if _, err := st.pool.Exec(ctx, `ALTER TABLE org_licenses RENAME TO org_licenses_unreadable`); err != nil {
		t.Fatal(err)
	}
	status, body := st.do(t, st.on, http.MethodGet, "/sso/providers", token, "")
	if _, err := st.pool.Exec(ctx, `ALTER TABLE org_licenses_unreadable RENAME TO org_licenses`); err != nil {
		t.Fatal(err)
	}
	if status != http.StatusPaymentRequired || body != unlicensedBody {
		t.Errorf("store down: %d %s, want the 402", status, body)
	}
	if n := strings.Count(st.logs.String(), "api sso: org entitlement check failed; denying"); n != 1 {
		t.Errorf("%d entitlement failure lines, want 1", n)
	}
	if orgs := st.providers.take(); len(orgs) != 0 {
		t.Fatalf("a failed entitlement read reached auth-service: %v", orgs)
	}
	if status, _ := st.do(t, st.on, http.MethodGet, "/sso/providers", token, ""); status != http.StatusOK {
		t.Fatalf("store back: %d, want 200", status)
	}
}
