//go:build integration

package admin_test

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/apiservice/admin"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

// sessionsPosturePool connects as a login holding exactly the api role's
// manifest with AUTH_PROXY_SESSIONS on (postgres.APIPosture(true)).
func sessionsPosturePool(t *testing.T, ctx context.Context, admin *pgxpool.Pool, uri string) *pgxpool.Pool {
	t.Helper()
	suffix := make([]byte, 6)
	if _, err := rand.Read(suffix); err != nil {
		t.Fatal(err)
	}
	role := "api_sessions_" + hex.EncodeToString(suffix)
	password := hex.EncodeToString(suffix) + "-posture-test"
	statements := []string{fmt.Sprintf(`CREATE ROLE %s LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT PASSWORD '%s'`, role, password)}
	posture := postgres.APIPosture(true)
	for _, table := range posture.RequiredTables {
		privileges := []string{"SELECT"}
		if table.AllowInsert {
			privileges = append(privileges, "INSERT")
		}
		if table.AllowUpdate {
			privileges = append(privileges, "UPDATE")
		}
		if table.AllowDelete {
			privileges = append(privileges, "DELETE")
		}
		statements = append(statements, fmt.Sprintf(`GRANT %s ON public.%s TO %s`, strings.Join(privileges, ", "), table.TableName, role))
	}
	for _, column := range posture.ColumnScoped {
		statements = append(statements, fmt.Sprintf(`GRANT %s (%s) ON public.%s TO %s`, column.Privilege, column.ColumnName, column.TableName, role))
	}
	for _, sequence := range posture.RequiredSequences {
		statements = append(statements, fmt.Sprintf(`GRANT USAGE, SELECT ON SEQUENCE public.%s TO %s`, sequence, role))
	}
	for _, statement := range statements {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DROP OWNED BY "+role)
		_, _ = admin.Exec(context.Background(), "DROP ROLE IF EXISTS "+role)
	})
	parsed, err := url.Parse(uri)
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

// The revoke-all writes kept under the flag: with the api role holding the
// AUTH_PROXY_SESSIONS posture, an admin password set and a soft delete still
// revoke the user's go-api refresh tokens.
func TestAdminRevokeAllWritesKeptUnderTheSessionsPosture(t *testing.T) {
	ctx := context.Background()
	pool, instance := migratedPostgres(t, ctx)
	runtime := sessionsPosturePool(t, ctx, pool, instance.URI)

	const rootPassword = "posture root password 91"
	rootHash, err := bcrypt.GenerateFromPassword([]byte(rootPassword), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	users := map[string]uuid.UUID{"root": uuid.New(), "reset": uuid.New(), "removed": uuid.New()}
	for name, id := range users {
		if _, err := pool.Exec(ctx, `INSERT INTO users (id, email, password_hash, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, $3, true, true, $4, 0, now(), now())`, id, "posture-"+name+"@example.com", string(rootHash), name == "root"); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}
	refresh := map[string]string{}
	for _, name := range []string{"reset", "removed"} {
		digest := sha256.Sum256([]byte(uuid.NewString()))
		refresh[name] = hex.EncodeToString(digest[:])
		if _, err := pool.Exec(ctx, `INSERT INTO refresh_tokens (id, user_id, org_id, token_hash, family_id, expires_at, created_at)
VALUES (gen_random_uuid(), $1, NULL, $2, gen_random_uuid(), now() + interval '7 days', now())`, users[name], refresh[name]); err != nil {
			t.Fatalf("seed refresh token: %v", err)
		}
	}

	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	secret := base64.RawURLEncoding.EncodeToString(key)
	verifier, err := edgetoken.New(secret, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	signer, err := edgetoken.NewSigner(secret, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	auth, err := policy.NewAuthenticator(verifier, policy.PGStore{Pool: runtime}, logger)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	for _, route := range admin.Routes(admin.Deps{Pool: runtime, Guard: policy.NewGuard(auth, logger), Logger: logger}) {
		if strings.HasPrefix(route.Pattern, "/api/v1/admin/users") {
			mux.Handle(route.Method+" "+route.Pattern, route.Handler)
		}
	}
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	bearer, err := signer.Access(edgetoken.AccessClaims{UserID: users["root"].String(), Email: "posture-root@example.com", IsSuperuser: true},
		time.Now(), uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	send := func(method, path, body string) (int, string) {
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
		defer response.Body.Close()
		raw, _ := io.ReadAll(response.Body)
		return response.StatusCode, string(raw)
	}
	revoked := func(name string) bool {
		t.Helper()
		var out bool
		if err := pool.QueryRow(ctx, `SELECT revoked_at IS NOT NULL FROM refresh_tokens WHERE token_hash = $1`, refresh[name]).Scan(&out); err != nil {
			t.Fatal(err)
		}
		return out
	}

	if status, body := send(http.MethodPost, "/api/v1/admin/users/"+users["reset"].String()+"/password",
		fmt.Sprintf(`{"admin_password":%q,"password":"Another Strong Pass 42!"}`, rootPassword)); status != http.StatusOK {
		t.Fatalf("admin password set under the sessions posture: %d %s", status, body)
	}
	if !revoked("reset") {
		t.Fatal("an admin password set left the user's go-api refresh token live")
	}
	if status, body := send(http.MethodDelete, "/api/v1/admin/users/"+users["removed"].String(), ""); status != http.StatusOK {
		t.Fatalf("soft delete under the sessions posture: %d %s", status, body)
	}
	if !revoked("removed") {
		t.Fatal("a soft delete left the user's go-api refresh token live")
	}
}
