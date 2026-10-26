//go:build integration

package identity

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestSSORelinkGoAndSQLAgreeOverOneShapeTable runs IsSSORelinkable and SSORelinkableSQL over the one shape table
// (ssoRelinkShapes) on real PostgreSQL rows. Each shape lives in its own savepoint of one rolled-back transaction.
func TestSSORelinkGoAndSQLAgreeOverOneShapeTable(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	tx, err := e.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	org := uuid.New()
	if _, err := tx.Exec(ctx, `INSERT INTO public.organizations (id, slug, name) VALUES ($1, $2, 'relink-parity')`, org, "org-"+org.String()); err != nil {
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
	for _, c := range ssoRelinkShapes() {
		if _, err := tx.Exec(ctx, `SAVEPOINT shape`); err != nil {
			t.Fatal(err)
		}
		var bound *uuid.UUID
		if c.bound != "" {
			b := rows[c.bound]
			bound = &b
		}
		id := uuid.New()
		if _, err := tx.Exec(ctx, `INSERT INTO public.users
	(id, email, auth_provider, auth_provider_id, sso_provider_id, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, true, true, false, 0, now(), now())`, id, id.String()+"@example.com", c.provider, c.subject, bound); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		var relinkable *bool
		if err := tx.QueryRow(ctx, `SELECT `+SSORelinkableSQL("u")+` FROM public.users u WHERE u.id = $1`, id).Scan(&relinkable); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		row := uuid.Nil
		if bound != nil {
			row = *bound
		}
		goRelinkable := IsSSORelinkable(c.goArgs(row))
		switch {
		case relinkable == nil:
			t.Errorf("%s: SSORelinkableSQL is NULL", c.name)
		case *relinkable != goRelinkable || goRelinkable != c.relinkable:
			t.Errorf("%s: SSORelinkableSQL = %v, IsSSORelinkable = %v, want %v", c.name, *relinkable, goRelinkable, c.relinkable)
		}
		if _, err := tx.Exec(ctx, `ROLLBACK TO SAVEPOINT shape`); err != nil {
			t.Fatal(err)
		}
	}
}
