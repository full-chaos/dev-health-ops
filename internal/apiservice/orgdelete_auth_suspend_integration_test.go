//go:build integration

package apiservice

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/workloadtoken"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/grant"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/invite"
	"github.com/full-chaos/dev-health-ops/internal/auth/membership"
	"github.com/full-chaos/dev-health-ops/internal/auth/org"
	"github.com/full-chaos/dev-health-ops/internal/auth/orgdeprovision"
	"github.com/full-chaos/dev-health-ops/internal/auth/serviceaccount"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/edtokens"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

// stubWorkloadExchange stands in for auth-service's workload exchange route only: the Kubernetes TokenReview of the
// projected ServiceAccount token has no in-process harness. It answers every exchange with a workload token signed
// by the key the REAL auth-side verifier (authapi.NewSessionIssueWorkload) checks, and records that a subject
// credential arrived. Everything else on the auth side is real.
type stubWorkloadExchange struct {
	signer  *token.Signer
	subject string

	mu            sync.Mutex
	sawCredential int
}

func (s *stubWorkloadExchange) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil || r.PostForm.Get("subject_token") == "" || r.PostForm.Get("subject_token_type") == "" {
		http.Error(w, `{"error":"invalid_request"}`, http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	s.sawCredential++
	s.mu.Unlock()
	now := time.Now()
	claims := contracts.WorkloadTokenClaims{Iss: testWorkloadIssuer, Aud: r.PostForm.Get("audience"), Sub: s.subject, PrincipalType: "workload",
		Jti: uuid.NewString(), Iat: now.Unix(), Nbf: now.Unix(), Exp: now.Unix() + 300}
	payload, err := json.Marshal(claims)
	if err != nil {
		http.Error(w, `{"error":"server_error"}`, http.StatusInternalServerError)
		return
	}
	compact, _, err := s.signer.Sign(r.Context(), contracts.WorkloadTokenTyp, payload)
	if err != nil {
		http.Error(w, `{"error":"server_error"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"access_token": compact, "issued_token_type": workloadtoken.TokenTypeJWT,
		"token_type": "Bearer", "expires_in": 300})
}

// orgDeleteSigningKey writes a fresh Ed25519 signing key file, the form token.NewFileSigner reads.
func orgDeleteSigningKey(t *testing.T) string {
	t.Helper()
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "workload-signing.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// orgDeleteAuthService is auth-service's provisioning routes as authruntime builds them (internal/auth/authruntime/
// provisioning.go), over the auth schema as the runtime role, users owned, with the go-api workload subject; plus
// stubWorkloadExchange on the exchange path.
func orgDeleteAuthService(t *testing.T, runtime *pgxpool.Pool, usersOwned bool) (*httptest.Server, *identity.Store) {
	t.Helper()
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	sessions, err := session.New(session.Config{Pool: runtime, Schema: schema,
		SuccessorKey: contracts.NewSecretString(base64.RawURLEncoding.EncodeToString([]byte("orgdelete-r1-successor-key-00032")))})
	if err != nil {
		t.Fatal(err)
	}
	identities, err := identity.New(identity.Config{Pool: runtime, Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	// authruntime builds the provisioning routes' identity store with UsersOwnedByAuth (identityConfig); organization
	// create refuses 409 users_not_owned without it. The returned store only projects, as before.
	births := identities
	if usersOwned {
		if births, err = identity.New(identity.Config{Pool: runtime, Schema: schema, UsersOwnedByAuth: true}); err != nil {
			t.Fatal(err)
		}
	}
	members, err := membership.New(membership.Config{Pool: runtime, Schema: schema, OwnsUsers: true, Sessions: sessions})
	if err != nil {
		t.Fatal(err)
	}
	inviteSecret := "orgdelete-r1-invite-" + uuid.NewString()
	invites, err := invite.New(invite.Config{Pool: runtime, Schema: schema, OwnsUsers: true, Memberships: members,
		Secret: inviteSecret, AcceptSecrets: []string{inviteSecret}})
	if err != nil {
		t.Fatal(err)
	}
	orgs, err := org.New(org.Config{Pool: runtime, Schema: schema, Sessions: sessions})
	if err != nil {
		t.Fatal(err)
	}
	deprovision, err := orgdeprovision.New(orgdeprovision.Config{Pool: runtime, Schema: schema, Orgs: orgs, Memberships: members,
		Invites: invites, Sessions: sessions})
	if err != nil {
		t.Fatal(err)
	}
	accounts, err := serviceaccount.New(serviceaccount.Config{Pool: runtime, Schema: schema, Locks: members, OwnsUsers: true})
	if err != nil {
		t.Fatal(err)
	}
	grants, err := grant.New(grant.Config{Pool: runtime, Schema: schema, OwnsUsers: true, Members: members})
	if err != nil {
		t.Fatal(err)
	}
	signer := token.NewFileSigner(token.FileConfig{KeyPath: orgDeleteSigningKey(t), KeyID: "k1"})
	subject := session.WirePrincipalID(uuid.New())
	workload, err := authapi.NewSessionIssueWorkload(authapi.SessionIssueWorkloadConfig{IssuerURL: testWorkloadIssuer, Keys: signer, Subject: subject})
	if err != nil {
		t.Fatal(err)
	}
	routes, err := authapi.ProvisioningRoutes(authapi.ProvisioningDeps{
		Users: authapi.PGUsers{Pool: runtime, AuthSchema: schema, UsersOwned: true}, Reader: runtime, Organizations: births,
		Memberships: members, Invites: invites, Deprovision: deprovision, Orgs: orgs, ServiceAccounts: accounts, Grants: grants,
		Workload: workload, Logger: quietLogger(),
	})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	for _, route := range routes {
		mux.Handle(route.Method+" "+route.Pattern, route.Handler)
	}
	mux.Handle("POST "+workloadtoken.ExchangePath, &stubWorkloadExchange{signer: signer, subject: subject})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server, identities
}

// With AUTH_PROXY_ORG_PROVISIONING off, AUTH_PROXY_URL set and go-api's workload inputs present (the live prod shape),
// a superuser's DELETE /api/v1/admin/orgs/{org_id} leaves the organization SUSPENDED in auth.organizations, with no
// live auth membership or invite. go-api is built by configureWith; only its database, ClickHouse DSN and
// authenticator are pointed at this test's stack. The bearer is a superuser of the test database who is not a member
// of the deleted organization: no deleted-org member signs in or presents a token.
func TestOrgDeleteSwitchOffSuspendsAuthOrganization(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	pg, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pg.Close(context.Background()) })
	admin, err := pgxpool.New(ctx, pg.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	pgschema.Apply(ctx, t, admin)
	role, err := containers.RoleName("orgdelete_runtime", pg)
	if err != nil {
		t.Fatal(err)
	}
	password := "orgdelete-r1-" + uuid.NewString()
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, role, password)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
	if _, err := authschema.Apply(ctx, admin, authschema.Options{Schema: "auth", RuntimeRole: role, UsersOwned: true}); err != nil {
		t.Fatalf("authschema.Apply: %v", err)
	}
	for _, entry := range authschema.PublicReadColumns() {
		if _, err := admin.Exec(ctx, fmt.Sprintf(`GRANT SELECT (%s) ON public.%s TO %q`, strings.Join(entry.Columns, ", "), entry.Table, role)); err != nil {
			t.Fatalf("grant: %v", err)
		}
	}
	runtimeConfig := admin.Config().Copy()
	runtimeConfig.ConnConfig.User = role
	runtimeConfig.ConnConfig.Password = password
	runtime, err := pgxpool.NewWithConfig(ctx, runtimeConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runtime.Close)

	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ch.Close(context.Background()) })
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(ch.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.Exec(ctx, `CREATE TABLE orgdelete_probe (org_id String, n UInt8) ENGINE = MergeTree ORDER BY org_id`); err != nil {
		t.Fatal(err)
	}

	authService, identities := orgDeleteAuthService(t, runtime, false)

	orgID, owner, super := uuid.New(), uuid.New(), uuid.New()
	for _, statement := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO organizations (id, slug, name, tier, is_active, settings, managed_by, created_at, updated_at)
VALUES ($1, $2, 'Deleted Org', 'community', true, '{}', 'self', now(), now())`, []any{orgID, "orgdel-" + orgID.String()[:8]}},
		{`INSERT INTO users (id, email, password_hash, auth_provider, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, NULL, 'local', true, true, false, 0, now(), now())`, []any{owner, "orgdel-owner-" + owner.String()[:8] + "@example.test"}},
		{`INSERT INTO users (id, email, password_hash, auth_provider, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, NULL, 'local', true, true, true, 0, now(), now())`, []any{super, "orgdel-super-" + super.String()[:8] + "@example.test"}},
		{`INSERT INTO memberships (id, user_id, org_id, role, joined_at, created_at) VALUES (gen_random_uuid(), $1, $2, 'owner', now(), now())`, []any{owner, orgID}},
	} {
		if _, err := admin.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	for _, user := range []uuid.UUID{owner, super} {
		if err := identities.Project(ctx, user); err != nil {
			t.Fatalf("project principal: %v", err)
		}
	}
	if err := identities.ProjectOrganization(ctx, orgID); err != nil {
		t.Fatalf("project organization: %v", err)
	}
	joined := time.Now().UTC().Truncate(time.Second)
	if _, err := identities.BackfillMembership(ctx, identity.MembershipBackfillTarget{OrgID: orgID, UserID: owner, Role: "owner", JoinedAt: &joined}, false); err != nil {
		t.Fatalf("backfill membership: %v", err)
	}

	authState := func() (liveOrgs, suspendedOrgs, liveMembers, pendingInvites int) {
		t.Helper()
		if err := admin.QueryRow(ctx, `SELECT
  (SELECT count(*) FROM auth.organizations WHERE id = $1 AND disabled_at IS NULL),
  (SELECT count(*) FROM auth.organizations WHERE id = $1 AND disabled_at IS NOT NULL),
  (SELECT count(*) FROM auth.memberships WHERE organization_id = $1 AND status <> 'removed'),
  (SELECT count(*) FROM auth.invites WHERE organization_id = $1 AND status = 'pending')`, orgID).
			Scan(&liveOrgs, &suspendedOrgs, &liveMembers, &pendingInvites); err != nil {
			t.Fatal(err)
		}
		return
	}
	if live, suspended, members, _ := authState(); live != 1 || suspended != 0 || members != 1 {
		t.Fatalf("before the delete: auth org live=%d suspended=%d, live auth memberships=%d; want 1, 0, 1", live, suspended, members)
	}

	t.Setenv(EnvAuthProxyOrgProvisioning, "")
	t.Setenv(EnvAuthProxyURL, authService.URL)
	t.Setenv(EnvAuthWorkloadSubjectFile, subjectFile(t))
	t.Setenv(EnvAuthWorkloadIssuer, testWorkloadIssuer)
	var deps Deps
	components, err := configureWith(ctx, apiTestConfig(), health.NewRegistry(time.Second), quietLogger(), func(built *Deps) { deps = *built })
	for index := len(components) - 1; index >= 0; index-- {
		_ = components[index].Shutdown(context.Background())
	}
	if err != nil {
		t.Fatalf("configureWith: %v", err)
	}
	issuer := edtokens.New(t)
	auth := issuer.Authenticator(t, policy.PGStore{Pool: admin, AuthSchema: "auth"}, quietLogger())
	deps.Pool, deps.ClickHouseDSN = admin, ch.URI
	deps.Auth, deps.Guard = auth, policy.NewGuard(auth, quietLogger())
	server, err := newServer(config.Config{APIAddress: "127.0.0.1:0"}, quietLogger(), Routes(deps, quietLogger()), nil,
		deps.AuthProxy.ScopeExemptRoutes(), policy.NewScope(auth, quietLogger()).Layers()...)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodDelete, "/api/v1/admin/orgs/"+orgID.String(), nil)
	request.RemoteAddr = "192.0.2.10:4711"
	request.Header.Set("Authorization", "Bearer "+issuer.MustMint(t, edtokens.Claims{UserID: super}))
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("DELETE /api/v1/admin/orgs/{org_id}: %d %s", recorder.Code, recorder.Body.String())
	}

	var publicOrgs int
	if err := admin.QueryRow(ctx, `SELECT count(*) FROM public.organizations WHERE id = $1`, orgID).Scan(&publicOrgs); err != nil {
		t.Fatal(err)
	}
	if publicOrgs != 0 {
		t.Errorf("public.organizations still holds the deleted org (%d rows)", publicOrgs)
	}
	live, suspended, members, invites := authState()
	if live != 0 || suspended != 1 {
		t.Errorf("after the delete: auth.organizations live=%d suspended=%d for the org; want live=0 suspended=1", live, suspended)
	}
	if members != 0 || invites != 0 {
		t.Errorf("after the delete: %d live auth memberships and %d pending auth invites; want 0 and 0", members, invites)
	}
}
