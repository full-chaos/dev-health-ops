//go:build integration

package authschema

import (
	"context"
	"testing"
	"time"
)

// TestMigration0018UsersGainTokenVersionAndLastLogin: auth.users carries
// legacy_token_version (bigint, not null, default 0) and last_login_at
// (nullable timestamptz).
func TestMigration0018UsersGainTokenVersionAndLastLogin(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newM10Fixture(t, ctx)

	for _, c := range []struct{ column, dataType, nullable string }{
		{"legacy_token_version", "bigint", "NO"},
		{"last_login_at", "timestamp with time zone", "YES"},
	} {
		var dataType, nullable string
		if err := env.admin.QueryRow(ctx, `SELECT data_type, is_nullable FROM information_schema.columns
			WHERE table_schema = 'auth' AND table_name = 'users' AND column_name = $1`, c.column).Scan(&dataType, &nullable); err != nil {
			t.Fatalf("users.%s: %v", c.column, err)
		}
		if dataType != c.dataType || nullable != c.nullable {
			t.Errorf("users.%s is %s nullable=%s, want %s nullable=%s", c.column, dataType, nullable, c.dataType, c.nullable)
		}
	}
	var def string
	if err := env.admin.QueryRow(ctx, `SELECT column_default FROM information_schema.columns
		WHERE table_schema = 'auth' AND table_name = 'users' AND column_name = 'legacy_token_version'`).Scan(&def); err != nil || def != "0" {
		t.Fatalf("legacy_token_version default %q (%v), want 0", def, err)
	}
}
