//go:build integration

package apiservice

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/local"
	"github.com/full-chaos/dev-health-ops/internal/auth/org"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/authplane"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/edtokens"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

const slugReusePassword = "slug-reuse-pw"

// slugStack is go-api (switch off, the live prod shape: AUTH_PROXY_URL set, provisioning routes real) and auth-service's
// two sign-in planes over one database: the compatibility login go-api's proxy forwards POST /api/v1/auth/login to (the
// live plane), and the session issue route go-api's own login asks through its real client (the issue plane).
type slugStack struct {
	admin  *pgxpool.Pool
	goAPI  http.Handler
	bearer string
	compat *httptest.Server
	issue  *sessionissue.Client
	orgs   *org.Store
	users  *identity.Store
}

func newSlugStack(t *testing.T, ctx context.Context, switchValue string) *slugStack {
	t.Helper()
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
	role, err := containers.RoleName("slugreuse_runtime", pg)
	if err != nil {
		t.Fatal(err)
	}
	password := "slugreuse-" + uuid.NewString()
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, role, password)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
	if _, err := authschema.Apply(ctx, admin, authschema.Options{Schema: authplane.Schema, RuntimeRole: role, UsersOwned: true}); err != nil {
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
	if err := conn.Exec(ctx, `CREATE TABLE slugreuse_probe (org_id String, n UInt8) ENGINE = MergeTree ORDER BY org_id`); err != nil {
		t.Fatal(err)
	}

	provisioning, identities := orgDeleteAuthService(t, runtime, switchValue != "")
	if switchValue != "" {
		provisioning = logRefusals(t, provisioning)
	}
	super := uuid.New()
	if _, err := admin.Exec(ctx, `INSERT INTO users (id, email, password_hash, auth_provider, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, NULL, 'local', true, true, true, 0, now(), now())`, super, "slugreuse-super-"+super.String()[:8]+"@example.test"); err != nil {
		t.Fatal(err)
	}
	if err := identities.Project(ctx, super); err != nil {
		t.Fatal(err)
	}

	t.Setenv(EnvAuthProxyOrgProvisioning, switchValue)
	t.Setenv(EnvAuthProxyURL, provisioning.URL)
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
	auth := issuer.Authenticator(t, policy.PGStore{Pool: admin, AuthSchema: authplane.Schema}, quietLogger())
	deps.Pool, deps.ClickHouseDSN = admin, ch.URI
	deps.Auth, deps.Guard = auth, policy.NewGuard(auth, quietLogger())
	server, err := newServer(config.Config{APIAddress: "127.0.0.1:0"}, quietLogger(), Routes(deps, quietLogger()), nil,
		deps.AuthProxy.ScopeExemptRoutes(), policy.NewScope(auth, quietLogger()).Layers()...)
	if err != nil {
		t.Fatal(err)
	}
	s := &slugStack{admin: admin, goAPI: server.Handler(), bearer: issuer.MustMint(t, edtokens.Claims{UserID: super}), users: identities}
	s.compat = compatLoginPlane(t, ctx, admin)
	s.issue = authplane.Start(t, ctx, pg.URI, authplane.NewKey(t)).Issuer
	schema, err := authschema.NewValidatedIdentifier(authplane.Schema)
	if err != nil {
		t.Fatal(err)
	}
	if s.orgs, err = org.New(org.Config{Pool: admin, Schema: schema}); err != nil {
		t.Fatal(err)
	}
	return s
}

// compatLoginPlane is auth-service's compatibility login as authruntime mounts it with AUTH_SESSION_LOGIN on
// (authruntime/runtime.go), users read from go-api's tables (users not owned), passwords compared as "hash:"+password.
func compatLoginPlane(t *testing.T, ctx context.Context, pool *pgxpool.Pool) *httptest.Server {
	t.Helper()
	schema, err := authschema.NewValidatedIdentifier(authplane.Schema)
	if err != nil {
		t.Fatal(err)
	}
	sessions, err := session.New(session.Config{Pool: pool, Schema: schema, Logger: quietLogger(),
		SuccessorKey: contracts.NewSecretString(base64.RawURLEncoding.EncodeToString([]byte("slugreuse-successor-key-00000032")))})
	if err != nil {
		t.Fatal(err)
	}
	identities, err := identity.New(identity.Config{Pool: pool, Schema: schema, Logger: quietLogger()})
	if err != nil {
		t.Fatal(err)
	}
	signer, err := authapi.NewEdDSAIssuer(edtokens.Audience, token.NewFileSigner(token.FileConfig{KeyPath: orgDeleteSigningKey(t), KeyID: "k1"}),
		"https://auth.slugreuse.test")
	if err != nil {
		t.Fatal(err)
	}
	limits, err := authapi.NewLimits(httpapi.NewMemoryCounters(nil), "slugreuse-limits-"+uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	routes, err := authapi.CompatRoutes(authapi.CompatDeps{
		Sessions: sessions, Users: authapi.PGUsers{Pool: pool}, Issuer: signer, Logger: quietLogger(), Projector: identities,
		AppendAudit: func(ctx context.Context, event audit.AuditEvent) error { return audit.Append(ctx, pool, schema, event) },
		AppendAuditWith: func(ctx context.Context, event audit.AuditEvent, apply func(ctx context.Context, tx audit.TxOps) error) error {
			return audit.AppendWith(ctx, pool, schema, event, apply)
		},
		Limits: limits,
		Login: &authapi.LoginConfig{Users: local.IdentityUsers{Store: identities}, Attempts: local.NewPGAttempts(pool, schema),
			LockoutKey: contracts.NewSecretString(base64.RawURLEncoding.EncodeToString([]byte("slugreuse-lockout-key-0000000032"))),
			Compare:    func(hash, password string) bool { return hash == "hash:"+password }},
	})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	for _, route := range routes {
		mux.Handle(route.Method+" "+route.Pattern, requestid.Middleware(route.Handler))
	}
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server
}

func (s *slugStack) goAPICall(t *testing.T, method, path, body string) (int, map[string]any) {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	request := httptest.NewRequest(method, path, reader)
	request.RemoteAddr = "192.0.2.11:4711"
	request.Header.Set("Authorization", "Bearer "+s.bearer)
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	recorder := httptest.NewRecorder()
	s.goAPI.ServeHTTP(recorder, request)
	var answer map[string]any
	_ = json.Unmarshal(recorder.Body.Bytes(), &answer)
	return recorder.Code, answer
}

// user seeds a go-api user who signs in with slugReusePassword, projected into the auth schema as a deployed
// auth-service holds every go-api user (the users backfill).
func (s *slugStack) user(t *testing.T, ctx context.Context, label string) (uuid.UUID, string) {
	t.Helper()
	id := uuid.New()
	email := "slugreuse-" + label + "-" + id.String()[:8] + "@example.com"
	if _, err := s.admin.Exec(ctx, `INSERT INTO users (id, email, password_hash, auth_provider, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, $3, 'local', true, true, false, 0, now(), now())`, id, email, "hash:"+slugReusePassword); err != nil {
		t.Fatal(err)
	}
	if err := s.users.Project(context.Background(), id); err != nil {
		t.Fatalf("project user: %v", err)
	}
	return id, email
}

// create is POST /api/v1/admin/orgs with an owner; slug "" lets go-api derive it from the name.
func (s *slugStack) create(t *testing.T, name, slug string, owner uuid.UUID) (uuid.UUID, string) {
	t.Helper()
	body := map[string]any{"name": name, "owner_user_id": owner.String()}
	if slug != "" {
		body["slug"] = slug
	}
	raw, _ := json.Marshal(body)
	status, answer := s.goAPICall(t, http.MethodPost, "/api/v1/admin/orgs", string(raw))
	if status != http.StatusCreated {
		t.Fatalf("POST /api/v1/admin/orgs %q: %d %v", name, status, answer)
	}
	id, err := uuid.Parse(fmt.Sprint(answer["id"]))
	if err != nil {
		t.Fatalf("create answer id: %v (%v)", err, answer)
	}
	return id, fmt.Sprint(answer["slug"])
}

func (s *slugStack) delete(t *testing.T, orgID uuid.UUID) {
	t.Helper()
	if status, answer := s.goAPICall(t, http.MethodDelete, "/api/v1/admin/orgs/"+orgID.String(), ""); status != http.StatusOK {
		t.Fatalf("DELETE /api/v1/admin/orgs/%s: %d %v", orgID, status, answer)
	}
}

// login is the live plane: POST /api/v1/auth/login at auth-service, as go-api's proxy forwards it.
func (s *slugStack) login(t *testing.T, email string, orgID uuid.UUID) (int, string) {
	t.Helper()
	raw, _ := json.Marshal(map[string]any{"email": email, "password": slugReusePassword, "org_id": orgID.String()})
	request, err := http.NewRequest(http.MethodPost, s.compat.URL+authapi.PatternCompatLogin, strings.NewReader(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	return response.StatusCode, string(body)
}

// issue is the issue plane: go-api's real session issue client asking for a session in orgID.
func (s *slugStack) issueIn(t *testing.T, ctx context.Context, principal, orgID uuid.UUID) error {
	t.Helper()
	_, err := s.issue.Issue(ctx, httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", nil),
		sessionissue.Request{PrincipalID: principal, OrganizationID: &orgID, AMR: []string{"pwd"}})
	return err
}

func (s *slugStack) count(t *testing.T, ctx context.Context, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := s.admin.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		t.Fatalf("count %q: %v", sql, err)
	}
	return n
}

// liveSessionsIn counts the principal's live auth sessions whose organization is orgID.
func (s *slugStack) liveSessionsIn(t *testing.T, ctx context.Context, principal, orgID uuid.UUID) int {
	t.Helper()
	return s.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE principal_id = $1 AND organization_id = $2 AND revoked_at IS NULL`, principal, orgID)
}

func wantSignedIn(t *testing.T, s *slugStack, ctx context.Context, label, email string, principal, orgID uuid.UUID) {
	t.Helper()
	before := s.liveSessionsIn(t, ctx, principal, orgID)
	status, body := s.login(t, email, orgID)
	if status != http.StatusOK {
		t.Errorf("%s: live plane sign-in = %d %s, want 200", label, status, body)
	}
	if err := s.issueIn(t, ctx, principal, orgID); err != nil {
		t.Errorf("%s: issue plane sign-in = %v, want a session", label, err)
	}
	if t.Failed() {
		t.FailNow()
	}
	if got := s.liveSessionsIn(t, ctx, principal, orgID); got != before+2 {
		t.Fatalf("%s: live sessions in the org = %d, want %d", label, got, before+2)
	}
}

// The product sequences of the repro record: M (a sign-in before the delete), V (none) and C (a fresh name). After an
// organization is deleted and a new one is created with the same name, a sign-in into the new organization succeeds
// on both planes (it was 500 on the live plane and 503 on the issue plane). Nothing of the deleted organization
// attaches to the new one: the new org has its own auth row, the deleted row keeps its id and suspension, the deleted
// org's former owner, who is no member of the new one, signs in with no organization, never into the new one.
func TestADeletedOrganizationsNameCanBeReused(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	s := newSlugStack(t, ctx, "")
	for _, seq := range []struct {
		name        string
		signInFirst bool
		freshSecond bool
	}{{"M", true, false}, {"V", false, false}, {"C", true, true}} {
		t.Run(seq.name, func(t *testing.T) {
			name := "Slug reuse " + seq.name + " " + uuid.NewString()[:8]
			ownerX, emailX := s.user(t, ctx, "x")
			x, slugX := s.create(t, name, "", ownerX)
			if seq.signInFirst {
				wantSignedIn(t, s, ctx, "R1 X", emailX, ownerX, x)
			}
			s.delete(t, x)
			if n := s.count(t, ctx, `SELECT count(*) FROM auth.organizations WHERE id = $1 AND disabled_at IS NOT NULL AND slug = $2`,
				x, "~deleted~"+x.String()); n != 1 {
				t.Errorf("X's auth row after the delete: %d suspended rows with the freed slug, want 1", n)
			}
			ownerY, emailY := s.user(t, ctx, "y")
			second := name
			if seq.freshSecond {
				second = name + " fresh"
			}
			y, slugY := s.create(t, second, "", ownerY)
			if !seq.freshSecond && slugY != slugX {
				t.Fatalf("Y's slug = %q, want X's slug %q (the sequence needs the same slug)", slugY, slugX)
			}
			wantSignedIn(t, s, ctx, "R4 Y", emailY, ownerY, y)
			if n := s.count(t, ctx, `SELECT count(*) FROM auth.organizations WHERE id = $1 AND slug = $2 AND disabled_at IS NULL`, y, slugY); n != 1 {
				t.Fatalf("Y's own active auth row with slug %q: %d rows, want 1", slugY, n)
			}
			if n := s.count(t, ctx, `SELECT count(*) FROM auth.organizations WHERE id = $1 AND disabled_at IS NOT NULL AND slug = $2`,
				x, "~deleted~"+x.String()); n != 1 {
				t.Fatalf("X's suspended auth row with the freed slug after Y's sign-in: %d rows, want 1", n)
			}
			if n := s.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE organization_id = $1 AND revoked_at IS NULL`, x); n != 0 {
				t.Fatalf("X has %d live sessions after its delete, want 0", n)
			}
			if n := s.count(t, ctx, `SELECT count(*) FROM auth.memberships WHERE organization_id = $1 AND principal_id = $2`, y, ownerX); n != 0 {
				t.Fatalf("X's owner holds %d auth memberships in Y, want 0", n)
			}
			if status, body := s.login(t, emailX, y); status != http.StatusOK {
				t.Fatalf("X's former owner signing in naming Y = %d %s, want go-api's no-organization sign-in", status, body)
			}
			if n := s.liveSessionsIn(t, ctx, ownerX, y); n != 0 {
				t.Fatalf("X's former owner has %d live sessions in Y, want 0", n)
			}
		})
	}
}

// A slug clash that remains (here an organization only auth-service holds, with the slug a new go-api organization
// asks for) is a 409 organization_conflict on both planes, never a 500 or a 503.
func TestARemainingSlugClashIsAConflict(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	s := newSlugStack(t, ctx, "")
	slug := "held-in-auth-" + uuid.NewString()[:8]
	if _, err := s.orgs.Create(ctx, org.CreateInput{Slug: slug, Name: "Held in auth"}, org.Actor{}); err != nil {
		t.Fatalf("org.Create: %v", err)
	}
	owner, email := s.user(t, ctx, "z")
	z, got := s.create(t, "Clash", slug, owner)
	if got != slug {
		t.Fatalf("Z's slug = %q, want %q", got, slug)
	}
	if status, body := s.login(t, email, z); status != http.StatusConflict {
		t.Errorf("live plane sign-in = %d %s, want 409", status, body)
	}
	var refused *sessionissue.RefusedError
	if err := s.issueIn(t, ctx, owner, z); !errors.As(err, &refused) || refused.Status != http.StatusConflict || refused.Reason != "organization_conflict" {
		t.Fatalf("issue plane = %v, want a 409 organization_conflict refusal", err)
	}
}

// A row a delete before this fix left (suspended, slug kept, go-api's row purged) is freed by the product route: a
// repeat DELETE of the deleted id. Then the same-name organization signs in.
func TestARepeatDeleteFreesTheSlugOfAnEarlierDelete(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	s := newSlugStack(t, ctx, "")
	name := "Earlier delete " + uuid.NewString()[:8]
	ownerX, emailX := s.user(t, ctx, "x")
	x, slugX := s.create(t, name, "", ownerX)
	wantSignedIn(t, s, ctx, "X", emailX, ownerX, x)
	if err := s.orgs.Suspend(ctx, x, nil, org.Actor{}); err != nil {
		t.Fatalf("Suspend: %v", err)
	}
	for _, sql := range []string{`DELETE FROM memberships WHERE org_id = $1`, `DELETE FROM organizations WHERE id = $1`} {
		if _, err := s.admin.Exec(ctx, sql, x); err != nil {
			t.Fatalf("pre-fix purge %q: %v", sql, err)
		}
	}
	if n := s.count(t, ctx, `SELECT count(*) FROM auth.organizations WHERE id = $1 AND slug = $2 AND disabled_at IS NOT NULL`, x, slugX); n != 1 {
		t.Fatalf("the pre-fix row: %d rows, want 1 suspended with slug %q", n, slugX)
	}
	s.delete(t, x)
	if n := s.count(t, ctx, `SELECT count(*) FROM auth.organizations WHERE id = $1 AND slug = $2`, x, slugX); n != 0 {
		t.Fatalf("the repeat delete left slug %q on X's row", slugX)
	}
	ownerY, emailY := s.user(t, ctx, "y")
	y, slugY := s.create(t, name, "", ownerY)
	if slugY != slugX {
		t.Fatalf("Y's slug = %q, want %q", slugY, slugX)
	}
	wantSignedIn(t, s, ctx, "Y", emailY, ownerY, y)
}
