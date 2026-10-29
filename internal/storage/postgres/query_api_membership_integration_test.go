//go:build integration

package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// CHAOS-10985 (acp OPTION P, D6358): as query-api's own login, the membership read is the public one. public.memberships
// (user_id, org_id, role) reads; every other column of it, every write to it, and both auth-schema copies
// (auth.memberships, auth.organizations) are refused with 42501. 4.12's REVOKE targets public.memberships; this test must
// stay green through it.
func TestQueryAPIRoleReadsMembershipsFromThePublicTableOnlyAndNeverFromTheAuthSchemaCopies(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	h := startQueryAPISessionsHarness(t, ctx, true)
	for _, statement := range []string{
		"CREATE TABLE auth.memberships (id uuid PRIMARY KEY, organization_id uuid, principal_id uuid, role text, status text)",
		"CREATE TABLE auth.organizations (id uuid PRIMARY KEY, slug text, name text, status text)",
		"ALTER TABLE public.memberships ADD COLUMN IF NOT EXISTS joined_at timestamptz",
		"INSERT INTO public.memberships (user_id, org_id, role) VALUES ('u1', 'o1', 'admin')",
		"INSERT INTO auth.memberships (id, organization_id, principal_id, role, status) VALUES (gen_random_uuid(), gen_random_uuid(), gen_random_uuid(), 'owner', 'active')",
	} {
		if _, err := h.admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	read := QueryAPISessionReadFor("auth")
	if err := h.applyLeg(ctx, &read); err != nil {
		t.Fatalf("the migrate leg: %v", err)
	}
	pool := connectAs(t, ctx, h.uri, h.role, queryAPISessionsPassword)
	var role string
	if err := pool.QueryRow(ctx, "SELECT role FROM memberships WHERE user_id = $1 AND org_id = $2 LIMIT 1", "u1", "o1").Scan(&role); err != nil || role != "admin" {
		t.Fatalf("the public membership read as query-api's login: role %q, %v", role, err)
	}
	for _, statement := range []string{
		"SELECT joined_at FROM public.memberships",
		"SELECT * FROM auth.memberships",
		"SELECT role FROM auth.memberships",
		"SELECT status FROM auth.memberships",
		"SELECT organization_id FROM auth.memberships",
		"SELECT * FROM auth.organizations",
		"SELECT slug FROM auth.organizations",
		"INSERT INTO public.memberships (user_id, org_id, role) VALUES ('u2', 'o1', 'member')",
		"UPDATE public.memberships SET role = 'owner'",
		"DELETE FROM public.memberships",
		"UPDATE auth.memberships SET role = 'owner'",
	} {
		_, err := pool.Exec(ctx, statement)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
			t.Errorf("%s: %v, want SQLSTATE 42501", statement, err)
		}
	}
}
