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

// The API's delete_user and `dho admin users delete` share one store function (pgStore.deleteUser), so they share its
// refusals. This runs the REAL handler on a migrated throwaway PostgreSQL: a user with only memberships and tokens is deleted
// (200), a user that an impersonation session points at (a RESTRICT foreign key) is not (500), the user stays, and the log
// names the constraint.
func TestDeleteUserIsRefusedByAUserReferencedThroughARestrictForeignKey(t *testing.T) {
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
	for name, superuser := range map[string]bool{"root": true, "plain": false, "impersonated": false} {
		users[name] = uuid.New()
		exec(`INSERT INTO users (id, email, password_hash, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, 'x', true, true, $3, 0, now(), now())`, users[name], "del-"+name+"@example.com", superuser)
	}
	for _, name := range []string{"plain", "impersonated"} {
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
	remove := func(name string) int {
		t.Helper()
		request, err := http.NewRequestWithContext(ctx, http.MethodDelete, server.URL+"/api/v1/admin/users/"+users[name].String(), nil)
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

	if got := remove("plain"); got != http.StatusOK || present("plain") {
		t.Fatalf("delete of a user with only memberships: status %d, still present %v", got, present("plain"))
	}
	if got := remove("impersonated"); got != http.StatusInternalServerError || !present("impersonated") {
		t.Fatalf("delete of a user an impersonation session points at: status %d, present %v; want 500 and present", got, present("impersonated"))
	}
	if !strings.Contains(logs.String(), "impersonation_sessions_target_user_id_fkey") {
		t.Fatalf("the log does not name the constraint: %s", logs.String())
	}
	var memberships int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM memberships WHERE user_id = $1`, users["impersonated"]).Scan(&memberships); err != nil || memberships != 1 {
		t.Fatalf("the refused delete changed the user's memberships: %d, %v", memberships, err)
	}
}
