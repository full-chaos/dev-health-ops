//go:build integration

package oidc

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// The runtime role reads the go-api tables it needs by column, nothing more.
var testGrants = []string{
	`GRANT USAGE ON SCHEMA public TO %[1]s`,
	`GRANT SELECT (id, org_id, protocol, status, auto_provision_users, config, allowed_domains, encrypted_secrets) ON public.sso_providers TO %[1]s`,
	`GRANT SELECT (user_id, org_id, role) ON public.memberships TO %[1]s`,
	`GRANT SELECT (id, email, password_hash, is_active, is_verified, auth_provider, auth_provider_id) ON public.users TO %[1]s`,
	`GRANT SELECT (id, slug, name, is_active, tier) ON public.organizations TO %[1]s`,
	`GRANT SELECT (org_id, tier, features_override) ON public.org_licenses TO %[1]s`,
}

type itEnv struct {
	admin, runtime *pgxpool.Pool
	schema         authschema.ValidatedIdentifier
}

var (
	envOnce     sync.Once
	envShared   *itEnv
	envErr      error
	envInstance *containers.Instance
	envRole     string
)

func TestMain(m *testing.M) {
	code := m.Run()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	if envShared != nil {
		envShared.runtime.Close()
		if envRole != "" {
			containers.DropRole(envShared.admin, envRole, func(string, ...any) {})
		}
		envShared.admin.Close()
	}
	if envInstance != nil {
		_ = envInstance.Close(ctx)
	}
	cancel()
	os.Exit(code)
}

func sharedEnv(t *testing.T) *itEnv {
	t.Helper()
	envOnce.Do(func() { envShared, envErr = buildEnv(t) })
	if envErr != nil {
		t.Fatalf("test database: %v", envErr)
	}
	return envShared
}

func buildEnv(t *testing.T) (*itEnv, error) {
	ctx := context.Background()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		return nil, err
	}
	envInstance = instance
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		return nil, err
	}
	pgschema.Apply(ctx, t, admin)
	role, err := containers.RoleName("oidc_runtime", instance)
	if err != nil {
		return nil, err
	}
	password := uuid.NewString()
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, role, password)); err != nil {
		return nil, err
	}
	envRole = role
	if _, err := authschema.Apply(ctx, admin, authschema.Options{Schema: "auth", RuntimeRole: role}); err != nil {
		return nil, err
	}
	for _, grant := range testGrants {
		if _, err := admin.Exec(ctx, fmt.Sprintf(grant, `"`+role+`"`)); err != nil {
			return nil, fmt.Errorf("%s: %w", grant, err)
		}
	}
	cfg := admin.Config().Copy()
	cfg.ConnConfig.User = role
	cfg.ConnConfig.Password = password
	runtime, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		return nil, err
	}
	return &itEnv{admin: admin, runtime: runtime, schema: schema}, nil
}

// switchIdentities is the shared identity store behind the users-cutover
// switch: off is today (public.users is the authority, new accounts are not
// available), on is after the cutover.
type switchIdentities struct {
	off, on StoreIdentities
	signup  bool
}

func (s *switchIdentities) pick() StoreIdentities {
	if s.signup {
		return s.on
	}
	return s.off
}

func (s *switchIdentities) IdentityBySubject(ctx context.Context, key ExternalKey, legacy LegacyMatch) (Principal, error) {
	return s.pick().IdentityBySubject(ctx, key, legacy)
}

func (s *switchIdentities) EmailInUse(ctx context.Context, email string) (bool, error) {
	return s.pick().EmailInUse(ctx, email)
}

func (s *switchIdentities) ProvisionFederated(ctx context.Context, account FederatedAccount) (Principal, error) {
	return s.pick().ProvisionFederated(ctx, account)
}

type recordingSessions struct {
	mu       sync.Mutex
	requests []SessionRequest
}

func (r *recordingSessions) Issue(_ context.Context, req SessionRequest) (Tokens, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.requests = append(r.requests, req)
	return Tokens{AccessToken: fmt.Sprintf("access-%d", len(r.requests)), RefreshToken: fmt.Sprintf("refresh-%d", len(r.requests)), ExpiresIn: 3600}, nil
}

func (r *recordingSessions) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.requests)
}

// fakeIdP is an OpenID provider over TLS: discovery, JWKS, a token endpoint
// that checks PKCE, and an optional userinfo endpoint.
type fakeIdP struct {
	server *httptest.Server
	key    *rsa.PrivateKey
	kid    string

	mu         sync.Mutex
	codes      map[string]pendingCode
	tokenCalls int
	userinfo   map[string]any
	docIssuer  *string
}

type pendingCode struct{ challenge, idToken string }

func startIdP(t *testing.T) *fakeIdP {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	idp := &fakeIdP{key: key, kid: "k1", codes: map[string]pendingCode{}}
	mux := http.NewServeMux()
	mux.HandleFunc("/jwks", idp.serveJWKS)
	mux.HandleFunc("/token", idp.serveToken)
	mux.HandleFunc("/userinfo", idp.serveUserinfo)
	mux.HandleFunc("/", idp.serveDiscovery)
	idp.server = httptest.NewTLSServer(mux)
	t.Cleanup(idp.server.Close)
	return idp
}

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

func (idp *fakeIdP) serveJWKS(w http.ResponseWriter, _ *http.Request) {
	pub := idp.key.PublicKey
	_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]any{{
		"kty": "RSA", "kid": idp.kid, "use": "sig", "alg": "RS256",
		"n": b64(pub.N.Bytes()), "e": b64(big.NewInt(int64(pub.E)).Bytes()),
	}}})
}

func (idp *fakeIdP) serveToken(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	idp.mu.Lock()
	defer idp.mu.Unlock()
	idp.tokenCalls++
	pending, ok := idp.codes[r.PostForm.Get("code")]
	sum := sha256.Sum256([]byte(r.PostForm.Get("code_verifier")))
	if !ok || b64(sum[:]) != pending.challenge {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"invalid_grant"}`))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "idp-access", "token_type": "Bearer",
		"expires_in": 300, "id_token": pending.idToken})
}

func (idp *fakeIdP) serveUserinfo(w http.ResponseWriter, _ *http.Request) {
	idp.mu.Lock()
	defer idp.mu.Unlock()
	_ = json.NewEncoder(w).Encode(idp.userinfo)
}

func (idp *fakeIdP) serveDiscovery(w http.ResponseWriter, r *http.Request) {
	if !strings.HasSuffix(r.URL.Path, "/.well-known/openid-configuration") {
		http.NotFound(w, r)
		return
	}
	issuer := idp.server.URL + strings.TrimSuffix(r.URL.Path, "/.well-known/openid-configuration")
	idp.mu.Lock()
	if idp.docIssuer != nil {
		issuer = *idp.docIssuer
	}
	idp.mu.Unlock()
	_ = json.NewEncoder(w).Encode(map[string]any{"issuer": issuer, "token_endpoint": idp.server.URL + "/token",
		"jwks_uri": idp.server.URL + "/jwks"})
}

func signRS256(t *testing.T, key *rsa.PrivateKey, header, claims map[string]any) string {
	t.Helper()
	input := encodeSegments(t, header, claims)
	digest := sha256.Sum256([]byte(input))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return input + "." + b64(sig)
}

func encodeSegments(t *testing.T, header, claims map[string]any) string {
	t.Helper()
	h, err := json.Marshal(header)
	if err != nil {
		t.Fatal(err)
	}
	c, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	return b64(h) + "." + b64(c)
}

type harnessOpts struct {
	signup          bool
	noAutoProvision bool
	allowedDomains  []string
	discovery       bool
	userinfo        bool
}

type harness struct {
	env        *itEnv
	idp        *fakeIdP
	svc        *Service
	sessions   *recordingSessions
	ids        *switchIdentities
	store      *identity.Store
	now        time.Time
	orgID      uuid.UUID
	providerID uuid.UUID
	issuer     string
	clientID   string
	requestID  string
}

func newHarness(t *testing.T, opts harnessOpts) *harness {
	t.Helper()
	env := sharedEnv(t)
	idp := startIdP(t)
	h := &harness{env: env, idp: idp, sessions: &recordingSessions{},
		now: time.Now().UTC().Truncate(time.Second), clientID: "client-" + uuid.NewString(),
		requestID: uuid.NewString()}
	h.issuer = idp.server.URL + "/tenant/" + uuid.NewString()
	stores := [2]*identity.Store{}
	for i, owns := range []bool{false, true} {
		store, err := identity.New(identity.Config{Pool: env.runtime, Schema: env.schema, UsersOwnedByAuth: owns})
		if err != nil {
			t.Fatal(err)
		}
		stores[i] = store
	}
	h.store = stores[0]
	h.ids = &switchIdentities{off: StoreIdentities{Store: stores[0], Pool: env.runtime, Schema: env.schema},
		on: StoreIdentities{Store: stores[1], Pool: env.runtime, Schema: env.schema}, signup: opts.signup}
	h.orgID = h.seedOrg(t)
	cfg := map[string]any{"client_id": h.clientID, "issuer": h.issuer}
	if !opts.discovery {
		cfg["token_endpoint"] = idp.server.URL + "/token"
		cfg["jwks_uri"] = idp.server.URL + "/jwks"
	}
	if opts.userinfo {
		cfg["userinfo_endpoint"] = idp.server.URL + "/userinfo"
	}
	h.providerID = h.seedProvider(t, h.orgID, cfg, !opts.noAutoProvision, opts.allowedDomains)
	flows, err := NewPGFlowStore(env.runtime, env.schema)
	if err != nil {
		t.Fatal(err)
	}
	var secret contracts.SecretString
	stateKey, _ := json.Marshal(uuid.NewString())
	if err := json.Unmarshal(stateKey, &secret); err != nil {
		t.Fatal(err)
	}
	h.svc, err = New(Config{
		Providers: PGProviders{Pool: env.runtime}, Flows: flows, Identities: h.ids,
		Memberships: PGMemberships{Pool: env.runtime}, Entitlements: LicensingEntitlements{Pool: env.runtime}, Sessions: h.sessions,
		StateSecret: secret, AppBaseURL: "https://app.example", HTTPClient: idp.server.Client(),
		Now: func() time.Time { return h.now },
	})
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// seedOrg seeds an enterprise organization: entitled to SSO by its own tier.
func (h *harness) seedOrg(t *testing.T) uuid.UUID {
	t.Helper()
	return h.seedOrgTier(t, "enterprise")
}

func (h *harness) seedOrgTier(t *testing.T, tier string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := h.env.admin.Exec(context.Background(), `INSERT INTO public.organizations (id, slug, name, tier) VALUES ($1, $2, $3, $4)`,
		id, "org-"+id.String(), "Org "+id.String(), tier); err != nil {
		t.Fatalf("seed org: %v", err)
	}
	return id
}

func (h *harness) seedProvider(t *testing.T, orgID uuid.UUID, cfg map[string]any, autoProvision bool, domains []string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	config, _ := json.Marshal(cfg)
	var allowed any
	if domains != nil {
		data, _ := json.Marshal(domains)
		allowed = string(data)
	}
	if _, err := h.env.admin.Exec(context.Background(), `INSERT INTO public.sso_providers
	(id, org_id, name, protocol, status, auto_provision_users, config, allowed_domains, encrypted_secrets)
VALUES ($1, $2, $6, 'oidc', 'active', $3, $4::json, $5::json, json_build_object('client_secret', $7::text))`,
		id, orgID, autoProvision, string(config), allowed, "test idp "+id.String(), uuid.NewString()); err != nil {
		t.Fatalf("seed provider: %v", err)
	}
	return id
}

func (h *harness) seedPublicUser(t *testing.T, email, authProvider, authProviderID string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	var providerID any
	if authProviderID != "" {
		providerID = authProviderID
	}
	if _, err := h.env.admin.Exec(context.Background(), `INSERT INTO public.users
	(id, email, auth_provider, auth_provider_id, is_active, is_verified)
VALUES ($1, $2, $3, $4, true, true)`, id, email, authProvider, providerID); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	return id
}

func (h *harness) seedMembership(t *testing.T, userID, orgID uuid.UUID, role string) {
	t.Helper()
	if _, err := h.env.admin.Exec(context.Background(), `INSERT INTO public.memberships (id, user_id, org_id, role)
VALUES ($1, $2, $3, $4)`, uuid.New(), userID, orgID, role); err != nil {
		t.Fatalf("seed membership: %v", err)
	}
}

// link creates a go-api account, projects it and links key to it, through
// the identity store's explicit audited link.
func (h *harness) link(t *testing.T, key ExternalKey, email string) uuid.UUID {
	t.Helper()
	ctx := context.Background()
	id := h.seedPublicUser(t, email, "local", "")
	if err := h.store.Project(ctx, id); err != nil {
		t.Fatalf("project: %v", err)
	}
	if _, err := h.store.LinkIdentity(ctx, identity.ExternalKey{Issuer: key.Issuer, Provider: identity.Provider(key.Provider),
		Subject: key.Subject}, id, identity.Actor{}); err != nil {
		t.Fatalf("link: %v", err)
	}
	return id
}

func (h *harness) count(t *testing.T, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := h.env.admin.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("count %q: %v", sql, err)
	}
	return n
}

func (h *harness) publicUsersDigest(t *testing.T) string {
	t.Helper()
	var digest *string
	if err := h.env.admin.QueryRow(context.Background(),
		`SELECT md5(string_agg(u::text, ',' ORDER BY u.id)) FROM public.users u`).Scan(&digest); err != nil {
		t.Fatal(err)
	}
	if digest == nil {
		return ""
	}
	return *digest
}

// snapshot is every row count a refused callback must leave unchanged.
type snapshot struct {
	principals, identities, auditRows, sessions int
	publicUsers                                 string
}

func (h *harness) snapshot(t *testing.T) snapshot {
	t.Helper()
	return snapshot{
		principals:  h.count(t, `SELECT count(*) FROM auth.principals`),
		identities:  h.count(t, `SELECT count(*) FROM auth.external_identities`),
		auditRows:   h.count(t, `SELECT count(*) FROM auth.security_audit_events`),
		sessions:    h.sessions.count(),
		publicUsers: h.publicUsersDigest(t),
	}
}

type started struct {
	state, nonce, challenge string
	cookie                  *http.Cookie
}

func (h *harness) start(t *testing.T) started {
	t.Helper()
	return h.startFor(t, h.providerID)
}

func (h *harness) startFor(t *testing.T, providerID uuid.UUID) started {
	t.Helper()
	result, err := h.svc.Authorize(context.Background(), providerID, "")
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	u, err := url.Parse(result.AuthorizationURL)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	if q.Get("code_challenge_method") != "S256" || q.Get("state") != result.State {
		t.Fatalf("authorization url %s", result.AuthorizationURL)
	}
	return started{state: result.State, nonce: q.Get("nonce"), challenge: q.Get("code_challenge"), cookie: result.Cookie}
}

func (h *harness) claims(nonce, sub, email string) map[string]any {
	return map[string]any{
		"iss": h.issuer, "aud": h.clientID, "sub": sub, "nonce": nonce,
		"iat": h.now.Unix(), "exp": h.now.Add(5 * time.Minute).Unix(),
		"email": email, "email_verified": true,
	}
}

func (h *harness) sign(t *testing.T, claims map[string]any) string {
	t.Helper()
	return signRS256(t, h.idp.key, map[string]any{"alg": "RS256", "kid": h.idp.kid, "typ": "JWT"}, claims)
}

// finish registers a fresh code for idToken at the IdP and runs the callback
// with the flow's login-nonce cookie.
func (h *harness) finish(t *testing.T, s started, idToken string) (Login, *Error) {
	t.Helper()
	return h.finishWith(t, h.providerID, s, idToken, true)
}

func (h *harness) finishWith(t *testing.T, providerID uuid.UUID, s started, idToken string, withCookie bool) (Login, *Error) {
	t.Helper()
	code := "code-" + uuid.NewString()
	h.idp.mu.Lock()
	h.idp.codes[code] = pendingCode{challenge: s.challenge, idToken: idToken}
	h.idp.mu.Unlock()
	login, clear, err := h.svc.Callback(context.Background(), providerID, CallbackInput{
		Code: code, State: s.state, RequestID: &h.requestID,
		Cookie: func(name string) (string, bool) {
			if withCookie && name == s.cookie.Name {
				return s.cookie.Value, true
			}
			return "", false
		},
	})
	if err == nil {
		if clear == nil || clear.MaxAge >= 0 || clear.Name != s.cookie.Name {
			t.Fatalf("success did not clear the login-nonce cookie: %+v", clear)
		}
		return login, nil
	}
	var failure *Error
	if !errors.As(err, &failure) {
		t.Fatalf("Callback returned %T %v", err, err)
	}
	return Login{}, failure
}

func wantDenied(t *testing.T, failure *Error, status int, reason Reason) {
	t.Helper()
	if failure == nil {
		t.Fatalf("callback succeeded, want %d %s", status, reason)
	}
	if failure.Status != status || failure.Reason != reason {
		t.Fatalf("callback = %d %s (%v), want %d %s", failure.Status, failure.Reason, failure, status, reason)
	}
}

func wantUnchanged(t *testing.T, h *harness, before snapshot) {
	t.Helper()
	if after := h.snapshot(t); after != before {
		t.Fatalf("a refused callback changed state: before %+v after %+v", before, after)
	}
}
