//go:build integration

package session_test

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/authmail"
	"github.com/full-chaos/dev-health-ops/internal/auth/signedtoken"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/sessionscenario"
)

// sessionsPostureRole is a login holding exactly the api role's manifest with
// AUTH_PROXY_SESSIONS on (postgres.APIPosture(true)), and a pool connected as
// it.
func sessionsPostureRole(t *testing.T, ctx context.Context, admin *pgxpool.Pool, uri string) *pgxpool.Pool {
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

// seedGoAPIRefresh writes a live go-api refresh_tokens row for user, as a
// login before AUTH_PROXY_SESSIONS turned on left it.
func seedGoAPIRefresh(t *testing.T, ctx context.Context, st stack, user string) string {
	t.Helper()
	digest := sha256.Sum256([]byte(uuid.NewString()))
	hash := hex.EncodeToString(digest[:])
	if _, err := st.pool.Exec(ctx, `INSERT INTO refresh_tokens (id, user_id, org_id, token_hash, family_id, expires_at, created_at)
		VALUES (gen_random_uuid(), $1::uuid, NULL, $2, gen_random_uuid(), now() + interval '7 days', now())`, user, hash); err != nil {
		t.Fatalf("seed refresh token: %v", err)
	}
	return hash
}

func goAPIRefreshRevoked(t *testing.T, ctx context.Context, st stack, hash string) bool {
	t.Helper()
	var revoked bool
	if err := st.pool.QueryRow(ctx, `SELECT revoked_at IS NOT NULL FROM refresh_tokens WHERE token_hash = $1`, hash).Scan(&revoked); err != nil {
		t.Fatal(err)
	}
	return revoked
}

// The revoke-all writes kept under the flag: with the api role holding the
// AUTH_PROXY_SESSIONS posture (no INSERT, UPDATE only of revoked_at), a
// password change and a password reset still revoke the user's go-api
// refresh tokens.
func TestRevokeAllWritesKeptUnderTheSessionsPosture(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	t.Setenv("TRUSTED_PROXIES", "127.0.0.1,::1")
	auth := httptest.NewServer(&fakeIssueRoute{})
	defer auth.Close()
	st := startStackWith(t, ctx, issueClient(t, auth.URL), sessionsPostureRole)
	orgA := st.specs["alice"]["org_id"].(string)

	changed := seedGoAPIRefresh(t, ctx, st, changeAlice)
	alice := changeToken(t, st, changeAlice, "alice@example.com", orgA, "owner", 0)
	if status, body := changeRequest(t, ctx, st, http.MethodPost, "/api/v1/auth/change-password", alice,
		changeBody(sessionscenario.Password, newPassword)); status != http.StatusOK {
		t.Fatalf("change-password under the sessions posture: %d %s", status, body)
	}
	if !goAPIRefreshRevoked(t, ctx, st, changed) {
		t.Fatal("a password change left the user's go-api refresh token live")
	}

	reset := seedGoAPIRefresh(t, ctx, st, changeBob)
	token, tokenHash := signedtoken.Build(uuid.New(), authmail.Config{}.Secret())
	if _, err := st.pool.Exec(ctx, `INSERT INTO password_reset_tokens (id, user_id, token_hash, expires_at, created_at)
		VALUES (gen_random_uuid(), $1::uuid, $2, now() + interval '1 hour', now())`, changeBob, tokenHash); err != nil {
		t.Fatalf("seed reset token: %v", err)
	}
	if status, body := changeRequest(t, ctx, st, http.MethodPost, "/api/v1/auth/reset-password", "",
		`{"token": "`+token+`", "new_password": "`+newPassword+`"}`); status != http.StatusOK {
		t.Fatalf("reset-password under the sessions posture: %d %s", status, body)
	}
	if !goAPIRefreshRevoked(t, ctx, st, reset) {
		t.Fatal("a password reset left the user's go-api refresh token live")
	}
	var inserted int
	if err := st.pool.QueryRow(ctx, `SELECT count(*) FROM refresh_tokens WHERE user_id IN ($1::uuid, $2::uuid)`, changeAlice, changeBob).Scan(&inserted); err != nil {
		t.Fatal(err)
	}
	if inserted != 2 {
		t.Fatalf("%d refresh_tokens rows for the two users, want the 2 seeded (go-api inserts none)", inserted)
	}
}
