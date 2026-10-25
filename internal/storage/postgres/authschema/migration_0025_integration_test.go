//go:build integration

package authschema

import (
	"context"
	"testing"
	"time"
)

// TestMigration0025AddsANullableSigningKidsColumn: the column is text[],
// nullable, has no default, and no existing session row gets a value.
func TestMigration0025AddsANullableSigningKidsColumn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newM10Fixture(t, ctx)

	var dataType, nullable string
	var def *string
	if err := env.admin.QueryRow(ctx, `SELECT data_type, is_nullable, column_default FROM information_schema.columns
		WHERE table_schema = 'auth' AND table_name = 'sessions' AND column_name = 'signing_kids'`).Scan(&dataType, &nullable, &def); err != nil {
		t.Fatalf("sessions.signing_kids: %v", err)
	}
	if dataType != "ARRAY" || nullable != "YES" || def != nil {
		t.Fatalf("signing_kids = %s nullable=%s default=%v; want a nullable ARRAY without a default", dataType, nullable, def)
	}
	var indexes int
	if err := env.admin.QueryRow(ctx, `SELECT count(*) FROM pg_indexes WHERE schemaname = 'auth' AND tablename = 'sessions'
		AND indexdef LIKE '%signing_kids%'`).Scan(&indexes); err != nil || indexes != 0 {
		t.Fatalf("indexes over signing_kids = %d, %v; want none", indexes, err)
	}
}
