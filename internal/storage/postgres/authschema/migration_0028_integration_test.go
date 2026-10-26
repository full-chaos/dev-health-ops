//go:build integration

package authschema

import (
	"context"
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
