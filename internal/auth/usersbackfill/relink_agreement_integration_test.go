//go:build integration

package usersbackfill

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

// TestSSORelinkSQLAndTheBackfillAgreeRowByRow reads every SSO row shape through the backfill's own Postgres reader
// and compares Relinkable with the guard's SQL predicate (identity.SSORelinkableSQL) on the same row. Each shape
// lives in its own savepoint of one rolled-back transaction, so the package database is unchanged.
func TestSSORelinkSQLAndTheBackfillAgreeRowByRow(t *testing.T) {
	env := getReconEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	tx, err := env.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	org := uuid.New()
	if _, err := tx.Exec(ctx, `INSERT INTO public.organizations (id, slug, name) VALUES ($1, $2, 'relink-agreement')`, org, "org-"+org.String()); err != nil {
		t.Fatal(err)
	}
	rows := map[string]uuid.UUID{}
	for _, protocol := range []string{"oidc", "saml", "oauth"} {
		id := uuid.New()
		if _, err := tx.Exec(ctx, `INSERT INTO public.sso_providers (id, org_id, name, protocol, status) VALUES ($1, $2, $3, $4, 'active')`,
			id, org, "idp-"+id.String(), protocol); err != nil {
			t.Fatal(err)
		}
		rows[protocol] = id
	}
	s := func(v string) *string { return &v }
	cases := []struct {
		name              string
		provider, subject *string
		bound             string
		want              bool
	}{
		{"oidc bound to an oidc row", s("oidc"), s("sub-1"), "oidc", true},
		{"saml bound to a saml row (NameID format not stored)", s("saml"), s("nameid-1"), "saml", false},
		{"oidc subject with a leading space", s("oidc"), s(" sub-6"), "oidc", false},
		{"oidc subject with a trailing no-break space", s("oidc"), s("sub-7\u00a0"), "oidc", false},
		{"oidc unbound", s("oidc"), s("sub-2"), "", false},
		{"saml unbound", s("saml"), s("nameid-2"), "", false},
		{"oidc empty subject", s("oidc"), s(""), "oidc", false},
		{"saml NULL subject", s("saml"), nil, "saml", false},
		{"saml bound to an oidc row", s("saml"), s("nameid-3"), "oidc", false},
		{"oidc bound to an oauth row", s("oidc"), s("sub-3"), "oauth", false},
		{"another case of the protocol", s("OIDC"), s("sub-4"), "oidc", false},
		{"github bound to an oidc row", s("github"), s("910001"), "oidc", false},
		{"github unbound", s("github"), s("910002"), "", false},
		{"local", s("local"), nil, "", false},
		{"no provider", nil, s("sub-5"), "oidc", false},
		{"service", s("service"), nil, "", false},
	}
	for _, c := range cases {
		if _, err := tx.Exec(ctx, `SAVEPOINT shape`); err != nil {
			t.Fatal(err)
		}
		id := uuid.New()
		var bound *uuid.UUID
		if c.bound != "" {
			b := rows[c.bound]
			bound = &b
		}
		if _, err := tx.Exec(ctx, `INSERT INTO public.users
	(id, email, auth_provider, auth_provider_id, sso_provider_id, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, true, true, false, 0, now(), now())`, id, id.String()+"@example.com", c.provider, c.subject, bound); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		page, err := pageUsers(ctx, tx, nil, 10000)
		if err != nil {
			t.Fatal(err)
		}
		var row *Row
		for i := range page {
			if page[i].ID == id {
				row = &page[i]
			}
		}
		if row == nil {
			t.Fatalf("%s: the backfill reader did not return the row", c.name)
		}
		var sql *bool
		if err := tx.QueryRow(ctx, `SELECT `+identity.SSORelinkableSQL("u")+` FROM public.users u WHERE u.id = $1`, id).Scan(&sql); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if sql == nil {
			t.Errorf("%s: the guard SQL is NULL", c.name)
		} else if got := Relinkable(*row); got != *sql || got != c.want {
			t.Errorf("%s: backfill Relinkable = %v, guard SQL = %v, want %v", c.name, got, *sql, c.want)
		}
		if _, err := tx.Exec(ctx, `ROLLBACK TO SAVEPOINT shape`); err != nil {
			t.Fatal(err)
		}
	}
	// A deleted provider row unbinds its accounts (ON DELETE SET NULL): no longer re-linkable on either side.
	id := uuid.New()
	if _, err := tx.Exec(ctx, `INSERT INTO public.users
	(id, email, auth_provider, auth_provider_id, sso_provider_id, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, 'oidc', 'sub-deleted', $3, true, true, false, 0, now(), now())`, id, id.String()+"@example.com", rows["oidc"]); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM public.sso_providers WHERE id = $1`, rows["oidc"]); err != nil {
		t.Fatal(err)
	}
	var sql bool
	if err := tx.QueryRow(ctx, `SELECT `+identity.SSORelinkableSQL("u")+` FROM public.users u WHERE u.id = $1`, id).Scan(&sql); err != nil || sql {
		t.Fatalf("an account of a deleted provider row: guard SQL = %v, %v; want false", sql, err)
	}
	page, err := pageUsers(ctx, tx, nil, 10000)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range page {
		if r.ID == id && Relinkable(r) {
			t.Fatal("an account of a deleted provider row is re-linkable on the backfill side")
		}
	}
}
