//go:build integration

package usersbackfill

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

// TestServicePlaceholderSQLAndTheBackfillAgreeRowByRow reads every row through the backfill's own Postgres reader and
// compares IsPlaceholder with the guard's SQL predicate on the same row. The rows live in one rolled-back
// transaction, so the package database is unchanged. The SQL is NULL, not false, for a NULL provider; the guard
// leaves NULL providers out with its own IS NOT NULL, so NULL counts as "not a placeholder" here and is pinned.
func TestServicePlaceholderSQLAndTheBackfillAgreeRowByRow(t *testing.T) {
	env := getReconEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	tx, err := env.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	s := func(v string) *string { return &v }
	hash := "$2b$12$abcdefghijklmnopqrstuuabcdefghijklmnopqrstuvwxyz0123"
	cases := []struct {
		name                  string
		provider, hash, subID *string
		want, sqlNull         bool
	}{
		{"service, NULL hash and id (the go-api-prove shape)", s("service"), nil, nil, true, false},
		{"service, empty hash and id", s("service"), s(""), s(""), true, false},
		{"service with a password hash", s("service"), &hash, nil, false, false},
		{"service with a provider id", s("service"), nil, s("sub-1"), false, false},
		{"another case of the literal", s("Service"), nil, nil, false, false},
		{"the literal with a trailing space", s("service "), nil, nil, false, false},
		{"no provider", nil, nil, nil, false, true},
		{"local", s("local"), nil, nil, false, false},
		{"github with a subject", s("github"), nil, s("910001"), false, false},
		{"saml", s("saml"), nil, nil, false, false},
	}
	ids := make(map[uuid.UUID]int, len(cases))
	for i, c := range cases {
		id := uuid.New()
		ids[id] = i
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
	seen := 0
	for _, row := range rows {
		i, ok := ids[row.ID]
		if !ok {
			continue
		}
		seen++
		c := cases[i]
		var sql *bool
		if err := tx.QueryRow(ctx, `SELECT `+identity.ServicePlaceholderSQL("u")+` FROM public.users u WHERE u.id = $1`, row.ID).Scan(&sql); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if (sql == nil) != c.sqlNull {
			t.Errorf("%s: guard SQL NULL = %v, want %v", c.name, sql == nil, c.sqlNull)
		}
		sqlTrue := sql != nil && *sql
		if got := IsPlaceholder(row); got != sqlTrue || got != c.want {
			t.Errorf("%s: backfill IsPlaceholder = %v, guard SQL = %v, want %v", c.name, got, sqlTrue, c.want)
		}
	}
	if seen != len(cases) {
		t.Fatalf("the backfill reader returned %d of the %d rows", seen, len(cases))
	}
}
