//go:build integration

package authflowvenue_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/providerstub"
)

// The state a refused social sign-up must not change is the CONTENT of the user and session rows: an in-place UPDATE of one
// row, with every count the same, changes the state, and the refusal check then fails.
func TestUserAndSessionStateFailsTheRefusalCheckOnAnInPlaceUpdateIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatalf("postgres: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	conn, err := pgx.Connect(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	// The public tables are the migrated schema, never hand-written DDL (CHAOS-6769); the auth tables are not in the public
	// schema the guard reads and hold only the columns the state query names.
	pgschema.ApplyURI(ctx, t, instance.URI)
	for _, statement := range []string{
		`CREATE SCHEMA auth`,
		`CREATE TABLE auth.users (principal_id uuid PRIMARY KEY, last_login_at timestamptz)`,
		`CREATE TABLE auth.principals (id uuid PRIMARY KEY, display_name text)`,
		`CREATE TABLE auth.refresh_credentials (id uuid PRIMARY KEY, consumed boolean)`,
		`INSERT INTO public.users (id, email, auth_provider) VALUES ('00000000-0000-4000-8000-0000000000b1', 'a@example.com', 'local'), ('00000000-0000-4000-8000-0000000000b2', 'b@example.com', 'local')`,
		`INSERT INTO public.refresh_tokens (id, user_id, token_hash, family_id, expires_at) VALUES ('00000000-0000-4000-8000-0000000000c1', '00000000-0000-4000-8000-0000000000b1', 'hash-1', '00000000-0000-4000-8000-0000000000d1', now() + interval '1 day')`,
		`INSERT INTO auth.users VALUES ('00000000-0000-4000-8000-0000000000b1', NULL)`,
		`INSERT INTO auth.principals VALUES ('00000000-0000-4000-8000-0000000000b1', 'A')`,
		`INSERT INTO auth.refresh_credentials VALUES ('00000000-0000-4000-8000-0000000000e1', false)`,
	} {
		if _, err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("fixture %q: %v", statement, err)
		}
	}
	state := func() string {
		t.Helper()
		var hashes [5]string
		if err := conn.QueryRow(ctx, userAndSessionStateSQL).Scan(&hashes[0], &hashes[1], &hashes[2], &hashes[3], &hashes[4]); err != nil {
			t.Fatalf("the state query: %v", err)
		}
		return strings.Join(hashes[:], " | ")
	}
	user := providerstub.Recorded{Provider: "github", Method: "GET", Path: "/user", Matched: true}
	before := state()
	if again := state(); again != before {
		t.Fatalf("the state is not stable: %q then %q", before, again)
	}
	if err := checkSocialRefusalEffects(before, state(), []providerstub.Recorded{user}); err != nil {
		t.Fatalf("an untouched state failed the check: %v", err)
	}
	for name, update := range map[string]string{
		"a public.users row":              `UPDATE public.users SET email = 'changed@example.com' WHERE id = '00000000-0000-4000-8000-0000000000b2'`,
		"a public.refresh_tokens row":     `UPDATE public.refresh_tokens SET revoked_at = now() WHERE id = '00000000-0000-4000-8000-0000000000c1'`,
		"an auth.users row":               `UPDATE auth.users SET last_login_at = now() WHERE principal_id = '00000000-0000-4000-8000-0000000000b1'`,
		"an auth.principals row":          `UPDATE auth.principals SET display_name = 'B' WHERE id = '00000000-0000-4000-8000-0000000000b1'`,
		"an auth.refresh_credentials row": `UPDATE auth.refresh_credentials SET consumed = true WHERE id = '00000000-0000-4000-8000-0000000000e1'`,
	} {
		if _, err := conn.Exec(ctx, update); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		after := state()
		if after == before {
			t.Errorf("%s: an in-place update left the state the same: %q", name, after)
		}
		if err := checkSocialRefusalEffects(before, after, []providerstub.Recorded{user}); err == nil {
			t.Errorf("%s: the refusal check passed after an in-place update", name)
		}
		before = after
	}
}
