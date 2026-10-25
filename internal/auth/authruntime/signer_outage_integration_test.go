//go:build integration

package authruntime

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/passwordhash"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

const goAPIUnavailable = `{"detail":{"message":"Authentication service unavailable"}}`

// newSignerOutageEnv is the configure stack under the eddsa class with a key
// file that cannot be read: the service boots fail-soft and the signer check
// fails on every request.
func newSignerOutageEnv(t *testing.T, ctx context.Context) *routesEnv {
	t.Helper()
	return newRoutesEnvWith(t, ctx, map[string]string{
		authconfig.EnvAccessTokenClass: authconfig.AccessClassEdDSA,
		authconfig.EnvTokenIssuerURL:   "https://auth.example.test",
	})
}

// stateDigest is a digest of every row of every auth table and of the go-api
// users table: any insert, update or delete changes it.
func (e *routesEnv) stateDigest(t *testing.T, ctx context.Context) map[string]string {
	t.Helper()
	rows, err := e.admin.Query(ctx, `SELECT table_name FROM information_schema.tables
		WHERE table_schema = 'auth' AND table_type = 'BASE TABLE' ORDER BY table_name`)
	if err != nil {
		t.Fatal(err)
	}
	tables, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil || len(tables) == 0 {
		t.Fatalf("auth tables %v: %v", tables, err)
	}
	digest := map[string]string{}
	for _, table := range append(tables, "public.users") {
		name := table
		if !strings.Contains(name, ".") {
			name = pgx.Identifier{"auth", table}.Sanitize()
		}
		var sum string
		query := fmt.Sprintf(`SELECT md5(coalesce(string_agg(t::text, ',' ORDER BY t::text), '')) FROM %s t`, name)
		if err := e.admin.QueryRow(ctx, query).Scan(&sum); err != nil {
			t.Fatalf("digest %s: %v", name, err)
		}
		digest[name] = sum
	}
	return digest
}

func expectUnchanged(t *testing.T, before, after map[string]string) {
	t.Helper()
	if reflect.DeepEqual(before, after) {
		return
	}
	for table, sum := range after {
		if before[table] != sum {
			t.Errorf("%s changed during the signer outage", table)
		}
	}
	t.FailNow()
}

func TestEdDSASignerOutageLoginWritesNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newSignerOutageEnv(t, ctx)
	user, _ := e.seed(t, ctx)
	hash, err := passwordhash.Hash("outage-route-password")
	if err != nil {
		t.Fatal(err)
	}
	email := "outage-" + user.String()[:8] + "@example.com"
	if _, err := e.admin.Exec(ctx, `UPDATE users SET password_hash = $2, email = $3 WHERE id = $1`, user, hash, email); err != nil {
		t.Fatal(err)
	}
	header := map[string]string{"Content-Type": "application/json"}
	before := e.stateDigest(t, ctx)
	for _, password := range []string{"outage-route-password", "wrong"} {
		got := e.doWith(t, "/api/v1/auth/login", `{"email":"`+email+`","password":"`+password+`"}`, header)
		if got.status != http.StatusServiceUnavailable || strings.TrimSpace(string(got.body)) != goAPIUnavailable || got.header.Get("Retry-After") != "" {
			t.Fatalf("login (%d chars) = %d %s, Retry-After %q; want go-api's 503", len(password), got.status, got.body, got.header.Get("Retry-After"))
		}
	}
	expectUnchanged(t, before, e.stateDigest(t, ctx))
}

func TestEdDSASignerOutageCompatRefreshKeepsTheCredentialLive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newSignerOutageEnv(t, ctx)
	user, org := e.seed(t, ctx)
	issued := e.start(t, ctx, user, &org)
	before := e.stateDigest(t, ctx)
	got := e.doWith(t, "/api/v1/auth/refresh", `{"refresh_token":"`+issued.Credential.Reveal()+`"}`, map[string]string{"Content-Type": "application/json"})
	if got.status != http.StatusServiceUnavailable || strings.TrimSpace(string(got.body)) != goAPIUnavailable {
		t.Fatalf("refresh = %d %s; want go-api's 503", got.status, got.body)
	}
	expectUnchanged(t, before, e.stateDigest(t, ctx))
	result, err := e.sessions.Exchange(ctx, issued.Credential)
	if err != nil || result.Reason != session.ReasonAccepted {
		t.Fatalf("the presented credential after the outage: %v %v, want it still live", result.Reason, err)
	}
}
