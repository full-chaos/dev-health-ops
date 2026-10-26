//go:build integration

package authflowvenue_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
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
	for _, statement := range []string{
		`CREATE TABLE public.users (id int PRIMARY KEY, email text)`,
		`CREATE TABLE public.refresh_tokens (id int PRIMARY KEY, revoked boolean)`,
		`CREATE SCHEMA auth`,
		`CREATE TABLE auth.users (principal_id int PRIMARY KEY, last_login_at timestamptz)`,
		`CREATE TABLE auth.principals (id int PRIMARY KEY, display_name text)`,
		`CREATE TABLE auth.refresh_credentials (id int PRIMARY KEY, consumed boolean)`,
		`INSERT INTO public.users VALUES (1, 'a@example.com'), (2, 'b@example.com')`,
		`INSERT INTO public.refresh_tokens VALUES (1, false)`,
		`INSERT INTO auth.users VALUES (1, NULL)`,
		`INSERT INTO auth.principals VALUES (1, 'A')`,
		`INSERT INTO auth.refresh_credentials VALUES (1, false)`,
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
		"a public.users row":              `UPDATE public.users SET email = 'changed@example.com' WHERE id = 2`,
		"a public.refresh_tokens row":     `UPDATE public.refresh_tokens SET revoked = true WHERE id = 1`,
		"an auth.users row":               `UPDATE auth.users SET last_login_at = now() WHERE principal_id = 1`,
		"an auth.principals row":          `UPDATE auth.principals SET display_name = 'B' WHERE id = 1`,
		"an auth.refresh_credentials row": `UPDATE auth.refresh_credentials SET consumed = true WHERE id = 1`,
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
