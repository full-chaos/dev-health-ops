//go:build integration

package admin_test

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/apiservice/admin"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// The API's delete_user and `dho admin users delete` share the store functions softDeleteUser and forceDeleteUser (CHAOS-10019,
// D6051). This runs the REAL handler on a migrated throwaway PostgreSQL: the default is a soft delete (200, the row stays,
// is_active false); ?force=true deletes the row and keeps the impersonation session with a null target; a reference that cannot
// be nulled (a NOT NULL RESTRICT key) fails the force delete (500), the user stays, and the log names the table.
func TestDeleteUserIsSoftByDefaultAndPermanentWithForce(t *testing.T) {
	ctx := context.Background()
	pool, _ := migratedPostgres(t, ctx)
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, query, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, query)
		}
	}
	orgID := uuid.New()
	exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, 'del-org', 'del-org', 'community', 'stripe', true, now(), now())`, orgID)
	users := map[string]uuid.UUID{}
	for name, superuser := range map[string]bool{"root": true, "plain": false, "impersonated": false, "stuck": false} {
		users[name] = uuid.New()
		exec(`INSERT INTO users (id, email, password_hash, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, 'x', true, true, $3, 0, now(), now())`, users[name], "del-"+name+"@example.com", superuser)
	}
	for _, name := range []string{"plain", "impersonated", "stuck"} {
		exec(`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'member', now(), now(), now())`, uuid.New(), orgID, users[name])
	}
	pgseed.ImpersonationSession(ctx, t, pool, uuid.NewString(), users["root"].String(), users["impersonated"].String(), orgID.String(), "member", time.Now().Add(time.Hour), nil)

	const jwtKey = "user-delete-restrict-test-signing-key-32-bytes!!"
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
		if strings.HasPrefix(route.Pattern, "/api/v1/admin/users") {
			mux.Handle(route.Method+" "+route.Pattern, route.Handler)
		}
	}
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	bearer, err := signer.Access(edgetoken.AccessClaims{UserID: users["root"].String(), Email: "del-root@example.com", IsSuperuser: true}, time.Now(), uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	remove := func(name, query string) int {
		t.Helper()
		request, err := http.NewRequestWithContext(ctx, http.MethodDelete, server.URL+"/api/v1/admin/users/"+users[name].String()+query, nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer "+bearer)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		return response.StatusCode
	}
	present := func(name string) bool {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM users WHERE id = $1`, users[name]).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n == 1
	}

	active := func(name string) bool {
		t.Helper()
		var on bool
		if err := pool.QueryRow(ctx, `SELECT is_active FROM users WHERE id = $1`, users[name]).Scan(&on); err != nil {
			t.Fatal(err)
		}
		return on
	}

	if got := remove("plain", ""); got != http.StatusOK || !present("plain") || active("plain") {
		t.Fatalf("default delete: status %d, present %v, active %v; want 200, present, inactive", got, present("plain"), active("plain"))
	}
	if got := remove("plain", "?force=maybe"); got != http.StatusUnprocessableEntity || !present("plain") {
		t.Fatalf("delete with a bad force value: status %d, present %v; want 422 and present", got, present("plain"))
	}
	if got := remove("impersonated", "?force=true"); got != http.StatusOK || present("impersonated") {
		t.Fatalf("force delete of a user an impersonation session points at: status %d, present %v; want 200 and gone", got, present("impersonated"))
	}
	var kept int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM impersonation_sessions WHERE target_user_id IS NULL AND admin_user_id = $1`, users["root"]).Scan(&kept); err != nil || kept != 1 {
		t.Fatalf("the impersonation session was not kept with a null target: %d, %v", kept, err)
	}
	exec(`CREATE TABLE zz_pinning_records (id uuid PRIMARY KEY, user_id uuid NOT NULL REFERENCES users(id) ON DELETE RESTRICT)`)
	exec(`INSERT INTO zz_pinning_records (id, user_id) VALUES ($1, $2)`, uuid.New(), users["stuck"])
	if got := remove("stuck", "?force=true"); got != http.StatusInternalServerError || !present("stuck") {
		t.Fatalf("force delete over a NOT NULL RESTRICT reference: status %d, present %v; want 500 and present", got, present("stuck"))
	}
	if !strings.Contains(logs.String(), "zz_pinning_records") {
		t.Fatalf("the log does not name the table: %s", logs.String())
	}
	if !strings.Contains(logs.String(), "admin: user permanently deleted") || !strings.Contains(logs.String(), "admin: user soft-deleted") {
		t.Fatalf("the log does not carry the outcome lines: %s", logs.String())
	}
	var memberships int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM memberships WHERE user_id = $1`, users["stuck"]).Scan(&memberships); err != nil || memberships != 1 {
		t.Fatalf("the refused delete changed the user's memberships: %d, %v", memberships, err)
	}
}
