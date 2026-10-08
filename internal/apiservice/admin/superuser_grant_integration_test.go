//go:build integration

package admin_test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/apiservice/admin"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestOnlySuperusersGrantOrWriteToSuperusers runs the admin user and member
// routes on a migrated Postgres. An org admin can neither grant nor change
// is_superuser, nor write to a superuser account or its memberships (profile,
// password, deletion, add, role, removal, ownership transfer to or from). A
// superuser can do each of these. An org admin's writes that touch no
// superuser are served as before.
func TestOnlySuperusersGrantOrWriteToSuperusers(t *testing.T) {
	ctx := context.Background()
	pool, _ := migratedPostgres(t, ctx)

	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, query, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, query)
		}
	}
	const ownPassword = "caller own password 77"
	ownHash, err := bcrypt.GenerateFromPassword([]byte(ownPassword), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	users := map[string]uuid.UUID{}
	superusers := map[string]bool{"root": true, "orgroot": true, "outsideroot": true, "ownerroot": true,
		"delroot": true, "roleroot": true, "remroot": true, "xferroot": true}
	for _, name := range []string{"admin", "member", "spare", "owner", "outsider", "root", "orgroot", "outsideroot", "ownerroot", "member2", "delroot", "roleroot", "remroot", "xferroot"} {
		users[name] = uuid.New()
		exec(`INSERT INTO users (id, email, password_hash, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, $3, true, true, $4, 0, now(), now())`, users[name], "su-"+name+"@example.com", string(ownHash), superusers[name])
	}
	orgID, org2ID := uuid.New(), uuid.New()
	for slug, id := range map[string]uuid.UUID{"su-org": orgID, "su-org-2": org2ID} {
		exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $2, 'community', 'stripe', true, now(), now())`, id, slug)
	}
	for _, m := range []struct {
		org        uuid.UUID
		user, role string
	}{
		{orgID, "admin", "admin"}, {orgID, "member", "member"}, {orgID, "spare", "member"},
		{orgID, "owner", "owner"}, {orgID, "orgroot", "member"}, {orgID, "delroot", "member"},
		{orgID, "roleroot", "member"}, {orgID, "remroot", "member"}, {orgID, "xferroot", "member"},
		{org2ID, "admin", "admin"}, {org2ID, "ownerroot", "owner"}, {org2ID, "member2", "member"},
	} {
		exec(`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, now(), now(), now())`, uuid.New(), m.org, users[m.user], m.role)
	}

	const jwtKey = "superuser-grant-test-signing-key-32-bytes-long!!"
	const issuer, audience = "dev-health-ops", "dev-health-api"
	verifier, err := edgetoken.New(jwtKey, issuer, audience)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := edgetoken.NewSigner(jwtKey, issuer, audience)
	if err != nil {
		t.Fatal(err)
	}
	logs := &lockedBuffer{}
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	auth, err := policy.NewAuthenticator(verifier, policy.PGStore{Pool: pool}, logger)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	for _, route := range admin.Routes(admin.Deps{Pool: pool, Guard: policy.NewGuard(auth, logger), Logger: logger}) {
		if strings.HasPrefix(route.Pattern, "/api/v1/admin/users") || strings.HasPrefix(route.Pattern, "/api/v1/admin/orgs/{org_id}/") {
			mux.Handle(route.Method+" "+route.Pattern, route.Handler)
		}
	}
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	token := func(name, org, role string) string {
		t.Helper()
		signed, err := signer.Access(edgetoken.AccessClaims{UserID: users[name].String(), Email: "su-" + name + "@example.com",
			OrgID: org, Role: role, IsSuperuser: superusers[name]}, time.Now(), uuid.NewString())
		if err != nil {
			t.Fatal(err)
		}
		return signed
	}
	orgAdmin := token("admin", orgID.String(), "admin")
	root := token("root", "", "member")

	send := func(bearer, method, path, body string) int {
		t.Helper()
		request, err := http.NewRequestWithContext(ctx, method, server.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer "+bearer)
		request.Header.Set("Content-Type", "application/json")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		return response.StatusCode
	}
	// state is every stored fact a refused write must leave unchanged.
	state := func(name string) string {
		t.Helper()
		var out string
		err := pool.QueryRow(ctx, `SELECT coalesce(u.is_superuser::text,'-') || '|' || u.email || '|' || coalesce(u.full_name,'-') || '|' ||
	coalesce(u.password_hash,'-') || '|' || coalesce((SELECT string_agg(m.org_id::text || ':' || m.role, ',' ORDER BY m.org_id) FROM memberships m WHERE m.user_id = u.id),'-')
FROM users u WHERE u.id = $1`, users[name]).Scan(&out)
		if err == pgx.ErrNoRows {
			return "absent"
		}
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	emailState := func(email string) string {
		t.Helper()
		var superuser bool
		err := pool.QueryRow(ctx, `SELECT is_superuser FROM users WHERE email = $1`, email).Scan(&superuser)
		if err == pgx.ErrNoRows {
			return "absent"
		}
		if err != nil {
			t.Fatal(err)
		}
		return fmt.Sprintf("is_superuser=%v", superuser)
	}
	field := func(name string, index int) string {
		parts := strings.Split(state(name), "|")
		if len(parts) <= index {
			return parts[0]
		}
		return parts[index]
	}
	const refusal = "admin: superuser write refused for a non-superuser caller"
	refusals := func() int { return strings.Count(logs.String(), refusal) }
	users2 := "/api/v1/admin/users/"
	members := func(org uuid.UUID) string { return "/api/v1/admin/orgs/" + org.String() + "/members" }
	setPassword := fmt.Sprintf(`{"admin_password":%q,"password":"Another Strong Pass 42!"}`, ownPassword)

	type step struct {
		name, bearer, method, path, body string
		status                           int
		refused                          bool
		// watch names a user whose state a refused write must keep; for an
		// allowed write, want is what the stored fact must read after it.
		watch string
		check func() string
		want  string
	}
	unchanged := func(name string) (func() string, string) { return func() string { return state(name) }, state(name) }
	refused := func(s step) step {
		s.status, s.refused = http.StatusForbidden, true
		if s.check == nil {
			s.check, s.want = unchanged(s.watch)
		}
		return s
	}
	steps := []func() step{
		func() step {
			return refused(step{name: "org admin creates a superuser", bearer: orgAdmin, method: "POST", path: "/api/v1/admin/users",
				body:  `{"email":"su-new-root@example.com","password":"a long enough password 1","is_superuser":true}`,
				check: func() string { return emailState("su-new-root@example.com") }, want: "absent"})
		},
		func() step {
			return step{name: "org admin creates a user without the flag", bearer: orgAdmin, method: "POST", path: "/api/v1/admin/users",
				body: `{"email":"su-plain@example.com","password":"a long enough password 2"}`, status: http.StatusCreated,
				check: func() string { return emailState("su-plain@example.com") }, want: "is_superuser=false"}
		},
		func() step {
			return step{name: "org admin creates a user with the flag false", bearer: orgAdmin, method: "POST", path: "/api/v1/admin/users",
				body: `{"email":"su-false@example.com","is_superuser":false}`, status: http.StatusCreated,
				check: func() string { return emailState("su-false@example.com") }, want: "is_superuser=false"}
		},
		func() step {
			return step{name: "superuser creates a superuser", bearer: root, method: "POST", path: "/api/v1/admin/users",
				body: `{"email":"su-by-root@example.com","password":"a long enough password 3","is_superuser":true}`, status: http.StatusCreated,
				check: func() string { return emailState("su-by-root@example.com") }, want: "is_superuser=true"}
		},
		func() step {
			return refused(step{name: "org admin promotes a member", bearer: orgAdmin, method: "PATCH", path: users2 + users["member"].String(),
				body: `{"is_superuser":true}`, watch: "member"})
		},
		func() step {
			return refused(step{name: "org admin promotes itself", bearer: orgAdmin, method: "PATCH", path: users2 + users["admin"].String(),
				body: `{"full_name":"Self","is_superuser":true}`, watch: "admin"})
		},
		func() step {
			return refused(step{name: "org admin demotes a superuser", bearer: orgAdmin, method: "PATCH", path: users2 + users["orgroot"].String(),
				body: `{"is_superuser":false}`, watch: "orgroot"})
		},
		func() step {
			return refused(step{name: "org admin patches a superuser's profile", bearer: orgAdmin, method: "PATCH", path: users2 + users["orgroot"].String(),
				body: `{"email":"su-moved@example.com","full_name":"Moved"}`, watch: "orgroot"})
		},
		func() step {
			return refused(step{name: "org admin sets a superuser's password", bearer: orgAdmin, method: "POST", path: users2 + users["orgroot"].String() + "/password",
				body: setPassword, watch: "orgroot"})
		},
		func() step {
			return refused(step{name: "org admin deletes a superuser", bearer: orgAdmin, method: "DELETE", path: users2 + users["delroot"].String(),
				watch: "delroot"})
		},
		func() step {
			return refused(step{name: "org admin adds a superuser to its org", bearer: orgAdmin, method: "POST", path: members(orgID),
				body: fmt.Sprintf(`{"user_id":%q,"role":"member"}`, users["outsideroot"]), watch: "outsideroot"})
		},
		func() step {
			return refused(step{name: "org admin changes a superuser's role", bearer: orgAdmin, method: "PATCH", path: members(orgID) + "/" + users["roleroot"].String(),
				body: `{"role":"admin"}`, watch: "roleroot"})
		},
		func() step {
			return refused(step{name: "org admin removes a superuser from its org", bearer: orgAdmin, method: "DELETE", path: members(orgID) + "/" + users["remroot"].String(),
				watch: "remroot"})
		},
		func() step {
			return refused(step{name: "org admin transfers ownership to a superuser", bearer: orgAdmin, method: "POST", path: "/api/v1/admin/orgs/" + orgID.String() + "/transfer-ownership",
				body: fmt.Sprintf(`{"new_owner_user_id":%q}`, users["xferroot"]), watch: "owner"})
		},
		func() step {
			return refused(step{name: "org admin transfers ownership away from a superuser", bearer: orgAdmin, method: "POST", path: "/api/v1/admin/orgs/" + org2ID.String() + "/transfer-ownership",
				body: fmt.Sprintf(`{"new_owner_user_id":%q}`, users["member2"]), watch: "ownerroot"})
		},
		func() step {
			return step{name: "org admin patches a member without the flag", bearer: orgAdmin, method: "PATCH", path: users2 + users["member"].String(),
				body: `{"full_name":"A New Name"}`, status: http.StatusOK,
				check: func() string { return field("member", 2) }, want: "A New Name"}
		},
		func() step {
			return step{name: "org admin sends a member's unchanged false flag", bearer: orgAdmin, method: "PATCH", path: users2 + users["member"].String(),
				body: `{"is_superuser":false}`, status: http.StatusOK,
				check: func() string { return field("member", 0) }, want: "false"}
		},
		func() step {
			before := field("member", 3)
			return step{name: "org admin sets a member's password", bearer: orgAdmin, method: "POST", path: users2 + users["member"].String() + "/password",
				body: setPassword, status: http.StatusOK,
				check: func() string { return fmt.Sprint(field("member", 3) != before) }, want: "true"}
		},
		func() step {
			return step{name: "org admin changes a member's role", bearer: orgAdmin, method: "PATCH", path: members(orgID) + "/" + users["spare"].String(),
				body: `{"role":"admin"}`, status: http.StatusOK,
				check: func() string { return field("spare", 4) }, want: orgID.String() + ":admin"}
		},
		func() step {
			return step{name: "org admin adds a user to its org", bearer: orgAdmin, method: "POST", path: members(orgID),
				body: fmt.Sprintf(`{"user_id":%q,"role":"member"}`, users["outsider"]), status: http.StatusCreated,
				check: func() string { return field("outsider", 4) }, want: orgID.String() + ":member"}
		},
		func() step {
			return step{name: "org admin removes a member from its org", bearer: orgAdmin, method: "DELETE", path: members(orgID) + "/" + users["outsider"].String(),
				status: http.StatusOK, check: func() string { return field("outsider", 4) }, want: "-"}
		},
		func() step {
			return step{name: "org admin deletes a member", bearer: orgAdmin, method: "DELETE", path: users2 + users["spare"].String(),
				status: http.StatusOK, check: func() string { return state("spare") }, want: "absent"}
		},
		func() step {
			return step{name: "superuser patches a superuser's profile", bearer: root, method: "PATCH", path: users2 + users["orgroot"].String(),
				body: `{"full_name":"Root Renamed"}`, status: http.StatusOK,
				check: func() string { return field("orgroot", 2) }, want: "Root Renamed"}
		},
		func() step {
			before := field("orgroot", 3)
			return step{name: "superuser sets a superuser's password", bearer: root, method: "POST", path: users2 + users["orgroot"].String() + "/password",
				body: setPassword, status: http.StatusOK,
				check: func() string { return fmt.Sprint(field("orgroot", 3) != before) }, want: "true"}
		},
		func() step {
			return step{name: "superuser changes a superuser's role", bearer: root, method: "PATCH", path: members(orgID) + "/" + users["orgroot"].String(),
				body: `{"role":"admin"}`, status: http.StatusOK,
				check: func() string { return field("orgroot", 4) }, want: orgID.String() + ":admin"}
		},
		func() step {
			return step{name: "superuser adds a superuser to an org", bearer: root, method: "POST", path: members(orgID),
				body: fmt.Sprintf(`{"user_id":%q,"role":"member"}`, users["outsideroot"]), status: http.StatusCreated,
				check: func() string { return field("outsideroot", 4) }, want: orgID.String() + ":member"}
		},
		func() step {
			return step{name: "superuser removes a superuser from an org", bearer: root, method: "DELETE", path: members(orgID) + "/" + users["outsideroot"].String(),
				status: http.StatusOK, check: func() string { return field("outsideroot", 4) }, want: "-"}
		},
		func() step {
			return step{name: "superuser transfers ownership away from a superuser", bearer: root, method: "POST", path: "/api/v1/admin/orgs/" + org2ID.String() + "/transfer-ownership",
				body: fmt.Sprintf(`{"new_owner_user_id":%q}`, users["member2"]), status: http.StatusOK,
				check: func() string { return field("member2", 4) }, want: org2ID.String() + ":owner"}
		},
		func() step {
			return step{name: "superuser promotes a member", bearer: root, method: "PATCH", path: users2 + users["member"].String(),
				body: `{"is_superuser":true}`, status: http.StatusOK,
				check: func() string { return field("member", 0) }, want: "true"}
		},
		func() step {
			return step{name: "org admin transfers ownership to a member", bearer: orgAdmin, method: "POST", path: "/api/v1/admin/orgs/" + orgID.String() + "/transfer-ownership",
				body: fmt.Sprintf(`{"new_owner_user_id":%q}`, users["admin"]), status: http.StatusOK,
				check: func() string { return fmt.Sprint(strings.Contains(state("admin"), orgID.String()+":owner")) }, want: "true"}
		},
		func() step {
			return step{name: "superuser deletes a superuser", bearer: root, method: "DELETE", path: users2 + users["orgroot"].String(),
				status: http.StatusOK, check: func() string { return state("orgroot") }, want: "absent"}
		},
	}
	for _, build := range steps {
		s := build()
		before := refusals()
		if got := send(s.bearer, s.method, s.path, s.body); got != s.status {
			t.Errorf("%s: status %d, want %d", s.name, got, s.status)
		}
		if got := s.check(); got != s.want {
			t.Errorf("%s: stored state %q, want %q", s.name, got, s.want)
		}
		if logged := refusals() - before; (logged == 1) != s.refused || logged > 1 {
			t.Errorf("%s: %d refusal log lines, want refused=%v", s.name, logged, s.refused)
		}
	}
}

// migratedPostgres is a scratch database with the full migration chain.
func migratedPostgres(t *testing.T, ctx context.Context) (*pgxpool.Pool, *containers.Instance) {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	conn, err := pgx.Connect(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pgmigrate.Upgrade(ctx, conn, baseline, chain); err != nil {
		t.Fatalf("upgrade: %v", err)
	}
	_ = conn.Close(ctx)
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool, instance
}

// TestAFailedSuperuserTargetReadRefusesTheMembershipWrite gives the admin
// routes a database login that can write memberships but cannot read users,
// so the superuser read of a membership write's target fails. The write must
// be refused with a 500, never served as if the target were not a superuser.
func TestAFailedSuperuserTargetReadRefusesTheMembershipWrite(t *testing.T) {
	ctx := context.Background()
	pool, instance := migratedPostgres(t, ctx)
	orgID, adminID, targetID := uuid.New(), uuid.New(), uuid.New()
	for _, statement := range []struct {
		query string
		args  []any
	}{
		{`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, 'su-read-org', 'su-read-org', 'community', 'stripe', true, now(), now())`, []any{orgID}},
		{`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'su-read-admin@example.com', true, true, false, 0, now(), now()), ($2, 'su-read-target@example.com', true, true, false, 0, now(), now())`, []any{adminID, targetID}},
		{`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'admin', now(), now(), now())`, []any{uuid.New(), orgID, adminID}},
	} {
		if _, err := pool.Exec(ctx, statement.query, statement.args...); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	role, err := containers.RoleName("su_noread", instance)
	if err != nil {
		t.Fatal(err)
	}
	const rolePassword = "su-noread-login"
	if _, err := pool.Exec(ctx, fmt.Sprintf(`CREATE ROLE %s LOGIN PASSWORD '%s'`, role, rolePassword)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP OWNED BY "+role)
		_, _ = pool.Exec(context.Background(), "DROP ROLE IF EXISTS "+role)
	})
	if _, err := pool.Exec(ctx, "GRANT SELECT, INSERT, UPDATE, DELETE ON memberships, organizations TO "+role); err != nil {
		t.Fatal(err)
	}
	config, err := pgxpool.ParseConfig(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.User, config.ConnConfig.Password = role, rolePassword
	noUsers, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(noUsers.Close)
	var one int
	if err := noUsers.QueryRow(ctx, `SELECT 1 FROM users LIMIT 1`).Scan(&one); err == nil {
		t.Fatal("the restricted login can read users, so the target read cannot fail")
	}

	const jwtKey = "superuser-grant-test-signing-key-32-bytes-long!!"
	verifier, err := edgetoken.New(jwtKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	signer, err := edgetoken.NewSigner(jwtKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	logs := &lockedBuffer{}
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	auth, err := policy.NewAuthenticator(verifier, policy.PGStore{Pool: pool}, logger)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	for _, route := range admin.Routes(admin.Deps{Pool: noUsers, Guard: policy.NewGuard(auth, logger), Logger: logger}) {
		if strings.HasPrefix(route.Pattern, "/api/v1/admin/orgs/{org_id}/") {
			mux.Handle(route.Method+" "+route.Pattern, route.Handler)
		}
	}
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	token, err := signer.Access(edgetoken.AccessClaims{UserID: adminID.String(), Email: "su-read-admin@example.com",
		OrgID: orgID.String(), Role: "admin"}, time.Now(), uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/api/v1/admin/orgs/"+orgID.String()+"/members",
		strings.NewReader(fmt.Sprintf(`{"user_id":%q,"role":"member"}`, targetID)))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusInternalServerError {
		t.Errorf("status %d, want 500", response.StatusCode)
	}
	var memberships int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM memberships WHERE user_id = $1`, targetID).Scan(&memberships); err != nil {
		t.Fatal(err)
	}
	if memberships != 0 {
		t.Errorf("%d memberships written for the target, want 0", memberships)
	}
	if !strings.Contains(logs.String(), "admin: superuser target check failed") {
		t.Error("no log line for the failed target read")
	}
}
