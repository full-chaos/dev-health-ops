//go:build integration

package usersbackfill

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/edgetokenmint"
)

// TestServicePlaceholderSQLAndTheBackfillAgreeRowByRow reads every row shape through the backfill's own Postgres
// reader and compares IsPlaceholder with the guard's SQL predicate on the same row. Each shape lives in its own
// savepoint of one rolled-back transaction, so the package database is unchanged; the proof principal shapes are
// edits of the baseline go-api-prove row, the others are new rows.
func TestServicePlaceholderSQLAndTheBackfillAgreeRowByRow(t *testing.T) {
	env := getReconEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	tx, err := env.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	prove := uuid.MustParse(edgetokenmint.ProvePrincipalID)
	var baseline int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM public.users WHERE id = $1`, prove).Scan(&baseline); err != nil || baseline != 1 {
		t.Fatalf("the baseline go-api-prove row is missing (%d, %v)", baseline, err)
	}
	s := func(v string) *string { return &v }
	hash := "$2b$12$abcdefghijklmnopqrstuuabcdefghijklmnopqrstuvwxyz0123"
	cases := []struct {
		name                  string
		proof                 bool
		provider, hash, subID *string
		want                  bool
	}{
		{"proof principal, NULL hash and id (the go-api-prove shape)", true, s("service"), nil, nil, true},
		{"proof principal, empty hash and id", true, s("service"), s(""), s(""), true},
		{"proof principal with a password hash", true, s("service"), &hash, nil, false},
		{"proof principal with a provider id", true, s("service"), nil, s("sub-1"), false},
		{"proof principal, another case of the literal", true, s("Service"), nil, nil, false},
		{"proof principal, the literal with a trailing space", true, s("service "), nil, nil, false},
		{"proof principal, no provider", true, nil, nil, nil, false},
		{"proof principal, local", true, s("local"), nil, nil, false},
		{"another id, the go-api-prove shape", false, s("service"), nil, nil, false},
		{"another id, no provider", false, nil, nil, nil, false},
		{"another id, github with a subject", false, s("github"), nil, s("910001"), false},
		{"another id, saml", false, s("saml"), nil, nil, false},
	}
	for _, c := range cases {
		if _, err := tx.Exec(ctx, `SAVEPOINT shape`); err != nil {
			t.Fatal(err)
		}
		id := prove
		if c.proof {
			if _, err := tx.Exec(ctx, `UPDATE public.users SET auth_provider = $2, password_hash = $3, auth_provider_id = $4 WHERE id = $1`,
				id, c.provider, c.hash, c.subID); err != nil {
				t.Fatalf("%s: %v", c.name, err)
			}
		} else {
			id = uuid.New()
			if _, err := tx.Exec(ctx, `INSERT INTO public.users
	(id, email, password_hash, auth_provider, auth_provider_id, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, true, true, false, 0, now(), now())`, id, id.String()+"@example.com", c.hash, c.provider, c.subID); err != nil {
				t.Fatalf("%s: %v", c.name, err)
			}
		}
		rows, err := pageUsers(ctx, tx, nil, 10000)
		if err != nil {
			t.Fatal(err)
		}
		var row *Row
		for i := range rows {
			if rows[i].ID == id {
				row = &rows[i]
			}
		}
		if row == nil {
			t.Fatalf("%s: the backfill reader did not return the row", c.name)
		}
		var sql *bool
		if err := tx.QueryRow(ctx, `SELECT `+identity.ServicePlaceholderSQL("u")+` FROM public.users u WHERE u.id = $1`, id).Scan(&sql); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if sql == nil {
			t.Errorf("%s: the guard SQL is NULL", c.name)
		} else if got := IsPlaceholder(*row); got != *sql || got != c.want {
			t.Errorf("%s: backfill IsPlaceholder = %v, guard SQL = %v, want %v", c.name, got, *sql, c.want)
		}
		if _, err := tx.Exec(ctx, `ROLLBACK TO SAVEPOINT shape`); err != nil {
			t.Fatal(err)
		}
	}
}
