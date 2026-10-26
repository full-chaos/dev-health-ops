//go:build integration

package authschema

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestMigration0028AddsANullableIssuedTokenVersionColumn: the column is bigint,
// nullable, has no default and no index, and no existing link row gets a value.
func TestMigration0028AddsANullableIssuedTokenVersionColumn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newM10Fixture(t, ctx)

	var dataType, nullable string
	var def *string
	if err := env.admin.QueryRow(ctx, `SELECT data_type, is_nullable, column_default FROM information_schema.columns
		WHERE table_schema = 'auth' AND table_name = 'link_credentials' AND column_name = 'issued_token_version'`).Scan(&dataType, &nullable, &def); err != nil {
		t.Fatalf("link_credentials.issued_token_version: %v", err)
	}
	if dataType != "bigint" || nullable != "YES" || def != nil {
		t.Fatalf("issued_token_version = %s nullable=%s default=%v; want a nullable bigint without a default", dataType, nullable, def)
	}
	var indexes int
	if err := env.admin.QueryRow(ctx, `SELECT count(*) FROM pg_indexes WHERE schemaname = 'auth' AND tablename = 'link_credentials'
		AND indexdef LIKE '%issued_token_version%'`).Scan(&indexes); err != nil || indexes != 0 {
		t.Fatalf("indexes over issued_token_version = %d, %v; want none", indexes, err)
	}
}

// TestMigration0028IssuedTokenVersionIsWriteOnce: the value the issuer's
// INSERT stored never changes, not even in an audit transaction: stamping a
// row that has none, re-stamping and clearing are refused, and a consume of
// the same row still commits.
func TestMigration0028IssuedTokenVersionIsWriteOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newM10Fixture(t, ctx)
	m10Seed(t, ctx, env)

	var bound, unbound string
	if err := env.admin.QueryRow(ctx, `INSERT INTO auth.link_credentials (principal_id, purpose, token_hash, expires_at, issued_token_version)
		VALUES ($1, 'password_reset', $2::bytea, now() + interval '1 hour', 3) RETURNING id::text`, m10Principal, m10Hash("c1")).Scan(&bound); err != nil {
		t.Fatal(err)
	}
	if err := env.admin.QueryRow(ctx, `INSERT INTO auth.link_credentials (principal_id, purpose, token_hash, expires_at)
		VALUES ($1, 'email_verification', $2::bytea, now() + interval '1 hour') RETURNING id::text`, m10Principal, m10Hash("c2")).Scan(&unbound); err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]struct{ sql, id string }{
		"re-stamping":                {`UPDATE auth.link_credentials SET issued_token_version = 4 WHERE id = $1`, bound},
		"clearing":                   {`UPDATE auth.link_credentials SET issued_token_version = NULL WHERE id = $1`, bound},
		"stamping a row without one": {`UPDATE auth.link_credentials SET issued_token_version = 0 WHERE id = $1`, unbound},
	} {
		if err := guardedExec(ctx, env.runtime, c.sql, c.id); err == nil || !strings.Contains(err.Error(), "issued_token_version") {
			t.Errorf("%s in an audit transaction: err = %v, want a refusal naming issued_token_version", name, err)
		}
	}
	if err := guardedExec(ctx, env.runtime, `UPDATE auth.link_credentials SET issued_token_version = 3, consumed_at = now() WHERE id = $1`, bound); err != nil {
		t.Errorf("a consume that writes the same issued_token_version was refused: %v", err)
	}
	var issued *int64
	if err := env.admin.QueryRow(ctx, `SELECT issued_token_version FROM auth.link_credentials WHERE id = $1`, bound).Scan(&issued); err != nil || issued == nil || *issued != 3 {
		t.Errorf("issued_token_version after the refusals = %v, %v; want 3", issued, err)
	}
}
