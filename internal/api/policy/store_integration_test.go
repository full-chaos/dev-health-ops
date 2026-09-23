//go:build integration

package policy

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const apiRolePassword = "policy_integration_api_password"

// schemaDDL is the slice of the Alembic schema the policy reads, with the
// column types of src/dev_health_ops/models (GUID is a native uuid).
var schemaDDL = []string{
	`CREATE TABLE public.organizations (id uuid PRIMARY KEY, name text NOT NULL)`,
	// The rest of APIPosture(): readiness requires every declared table.
	`CREATE TABLE public.feature_flags (id uuid PRIMARY KEY)`,
	`CREATE TABLE public.org_feature_overrides (id uuid PRIMARY KEY)`,
	`CREATE TABLE public.org_licenses (id uuid PRIMARY KEY)`,
	`CREATE TABLE public.alembic_version (version_num varchar(32) PRIMARY KEY)`,
	`CREATE TABLE public.worker_instances (id uuid PRIMARY KEY)`,
	`CREATE TABLE public.users (
		id uuid PRIMARY KEY, email text NOT NULL UNIQUE,
		is_active boolean, is_superuser boolean, token_version integer NOT NULL DEFAULT 0)`,
	`CREATE TABLE public.memberships (
		id uuid PRIMARY KEY, user_id uuid NOT NULL REFERENCES public.users(id),
		org_id uuid NOT NULL REFERENCES public.organizations(id), role text)`,
	`CREATE TABLE public.impersonation_sessions (
		id uuid PRIMARY KEY, admin_user_id uuid NOT NULL REFERENCES public.users(id),
		target_user_id uuid NOT NULL REFERENCES public.users(id),
		target_org_id uuid NOT NULL REFERENCES public.organizations(id),
		target_role varchar(50) NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
		expires_at timestamptz NOT NULL, ended_at timestamptz)`,
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}

// provision runs the real scripts/worker/provision_river_roles.sql, with the
// api role only when api is non-empty.
func provision(t *testing.T, ctx context.Context, uri string, roles map[string]string, api string) {
	t.Helper()
	args := []string{uri, "--set=ON_ERROR_STOP=1",
		"--set=domain_role=" + roles["domain"], "--set=queue_role=" + roles["queue"],
		"--set=coordinator_role=" + roles["coordinator"],
		"--set=domain_password=unused_domain", "--set=queue_password=unused_queue",
		"--set=coordinator_password=unused_coordinator",
		"--file=" + filepath.Join(repositoryRoot(t), "scripts", "worker", "provision_river_roles.sql"),
	}
	if api != "" {
		args = append(args, "--set=api_role="+api, "--set=api_password="+apiRolePassword)
	}
	if output, err := exec.CommandContext(ctx, "psql", args...).CombinedOutput(); err != nil {
		t.Fatalf("provision_river_roles.sql: %v\n%s", err, output)
	}
}

// migrate runs the River migration exactly as cmd/dev-health-worker-migrate
// builds its api leg: the role name, and grants derived from APIPosture().
func migrate(t *testing.T, ctx context.Context, admin *pgxpool.Pool, roles map[string]string) {
	t.Helper()
	posture := postgres.APIPosture()
	grants := make([]riverstore.TableGrant, 0, len(posture.RequiredTables))
	for _, table := range posture.RequiredTables {
		grants = append(grants, riverstore.TableGrant{TableName: table.TableName,
			AllowInsert: table.AllowInsert, AllowUpdate: table.AllowUpdate, AllowDelete: table.AllowDelete})
	}
	options := riverstore.MigrationOptions{
		Schema: "river", DomainRole: roles["domain"], QueueRole: roles["queue"],
		APIRole: roles["api"], APIGrants: grants,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	if _, err := riverstore.ApplyPinnedMigrations(ctx, admin, options); err != nil {
		t.Fatalf("ApplyPinnedMigrations: %v", err)
	}
}

func connectAs(t *testing.T, ctx context.Context, raw, role, password string) *pgxpool.Pool {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	parsed.User = url.UserPassword(role, password)
	pool, err := pgxpool.New(ctx, parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// TestAPIRoleEndToEnd walks the deploy order -- provision, migrate, api
// readiness, authenticated request -- on a real Postgres, as the api role.
func TestAPIRoleEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })

	roles := map[string]string{}
	for _, name := range []string{"domain", "queue", "coordinator", "api"} {
		role, err := containers.RoleName("policy_e2e_"+name, instance)
		if err != nil {
			t.Fatal(err)
		}
		roles[name] = role
	}
	adminConfig, err := pgxpool.ParseConfig(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	adminConfig.MaxConns = 4
	admin, err := pgxpool.NewWithConfig(ctx, adminConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	t.Cleanup(func() {
		for _, role := range roles {
			containers.DropRole(admin, role, t.Logf)
		}
	})
	for _, statement := range schemaDDL {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	// Every other table the api posture declares (other route areas' tables)
	// gets a stand-in, so readiness can hold exactly the posture.
	for _, table := range postgres.APIPosture().RequiredTables {
		if _, err := admin.Exec(ctx, "CREATE TABLE IF NOT EXISTS public."+table.TableName+" (id uuid PRIMARY KEY)"); err != nil {
			t.Fatalf("stand-in %s: %v", table.TableName, err)
		}
	}

	// 1. Migration before the api role exists: succeeds, grants nothing to it.
	provision(t, ctx, instance.URI, roles, "")
	migrate(t, ctx, admin, roles)

	// 2. The operator provisions the api role: it can connect, and readiness
	// is refused until a migration grants the posture.
	provision(t, ctx, instance.URI, roles, roles["api"])
	api := connectAs(t, ctx, instance.URI, roles["api"], apiRolePassword)
	if err := postgres.CheckAPIAuthorization(ctx, api, roles["api"], "river"); err == nil {
		t.Fatal("api role ready before any migration granted its posture")
	}
	store := PGStore{Pool: api}
	if _, _, err := store.UserState(ctx, uuid.New()); err == nil || isUnavailable(err) {
		t.Fatalf("ungranted read: %v, want a permission error (not unavailable)", err)
	}

	// 3. The next migration grants exactly the posture.
	migrate(t, ctx, admin, roles)
	if err := postgres.CheckAPIAuthorization(ctx, api, roles["api"], "river"); err != nil {
		t.Fatalf("api role not ready after the migration: %v", err)
	}
	var writable bool
	if err := api.QueryRow(ctx, `SELECT has_table_privilege(current_user, 'public.users', 'UPDATE')`).Scan(&writable); err != nil || writable {
		t.Fatalf("api role can update users: %v %v", writable, err)
	}

	// 4. Seed and read through PGStore as the api role.
	org, member, stranger := uuid.New(), uuid.New(), uuid.New()
	admUser, targetUser, inactive, nullUser := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	seed := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO organizations VALUES ($1,'o'),($2,'m'),($3,'s')`, []any{org, member, stranger}},
		{`INSERT INTO users VALUES ($1,'a@x',true,true,2),($2,'t@x',true,false,0),($3,'i@x',false,false,0),($4,'n@x',NULL,NULL,0)`,
			[]any{admUser, targetUser, inactive, nullUser}},
		{`INSERT INTO memberships VALUES ($1,$2,$3,'admin')`, []any{uuid.New(), targetUser, member}},
		{`INSERT INTO impersonation_sessions (id,admin_user_id,target_user_id,target_org_id,target_role,expires_at,ended_at)
			VALUES ($1,$2,$3,$4,'member',now()+interval '1 hour',now()),
			       ($5,$2,$3,$4,'member',now()-interval '1 minute',NULL)`, []any{uuid.New(), admUser, targetUser, member, uuid.New()}},
	}
	for _, row := range seed {
		if _, err := admin.Exec(ctx, row.sql, row.args...); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	if state, found, err := store.UserState(ctx, admUser); err != nil || !found || state != (UserState{IsActive: true, IsSuperuser: true, TokenVersion: 2}) {
		t.Fatalf("admin state %+v %v %v", state, found, err)
	}
	if state, found, err := store.UserState(ctx, nullUser); err != nil || !found || state != (UserState{}) {
		t.Fatalf("NULL flags must read false: %+v %v %v", state, found, err)
	}
	if _, found, err := store.UserState(ctx, uuid.New()); err != nil || found {
		t.Fatalf("unknown user: %v %v", found, err)
	}
	if ok, err := store.IsMember(ctx, targetUser, member); err != nil || !ok {
		t.Fatalf("member: %v %v", ok, err)
	}
	if ok, err := store.IsMember(ctx, targetUser, stranger); err != nil || ok {
		t.Fatalf("stranger: %v %v", ok, err)
	}
	// Only an ended and an expired session exist: none is active.
	if session, err := store.ActiveImpersonation(ctx, admUser); err != nil || session != nil {
		t.Fatalf("ended/expired session reported active: %+v %v", session, err)
	}
	activeID := uuid.New()
	if _, err := admin.Exec(ctx, `INSERT INTO impersonation_sessions (id,admin_user_id,target_user_id,target_org_id,target_role,expires_at)
		VALUES ($1,$2,$3,$4,'member',now()+interval '1 hour')`, activeID, admUser, targetUser, member); err != nil {
		t.Fatal(err)
	}
	session, err := store.ActiveImpersonation(ctx, admUser)
	if err != nil || session == nil || session.ID != activeID || session.TargetOrgID != member ||
		session.TargetEmail == nil || *session.TargetEmail != "t@x" {
		t.Fatalf("active session %+v %v", session, err)
	}

	// 5. One authenticated request through the org scope, impersonation and
	// guard, over HTTP, as the api role.
	auth := authenticator(t, store)
	scope := NewScope(auth, quiet())
	server := httptest.NewServer(scope.OrgScope(scope.Impersonation(NewGuard(auth, quiet()).Wrap(Superuser, echo))))
	t.Cleanup(server.Close)
	token := sign(t, claims(func(c jwt.MapClaims) {
		c["sub"] = admUser.String()
		c["is_superuser"] = true
		c["tv"] = 2
	}))
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/x", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("X-Org-Id", stranger.String())
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if response.StatusCode != 200 || !strings.HasPrefix(string(body), "org="+member.String()+" user="+admUser.String()) ||
		response.Header.Get("X-Impersonated-User-Id") != targetUser.String() {
		t.Fatalf("%d %s %v", response.StatusCode, body, response.Header)
	}
	for _, user := range []uuid.UUID{inactive, nullUser} {
		request, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/x", nil)
		request.Header.Set("Authorization", "Bearer "+sign(t, claims(func(c jwt.MapClaims) { c["sub"] = user.String() })))
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if response.StatusCode != http.StatusUnauthorized {
			t.Fatalf("inactive user %s: %d", user, response.StatusCode)
		}
	}
}
