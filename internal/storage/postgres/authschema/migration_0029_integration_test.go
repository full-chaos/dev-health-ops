//go:build integration

package authschema

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TestMigration0029PinPresentConstraint: 0029 adds the check NOT VALID and it
// refuses a live session row without a pin from any role.
func TestMigration0029PinPresentConstraint(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newM10Fixture(t, ctx)

	migrations, err := Migrations()
	if err != nil || len(migrations) < 29 || migrations[28].Version != 29 || !strings.Contains(migrations[28].SQL, "NOT VALID;") {
		t.Fatalf("migration 0029 is missing or does not add the check NOT VALID: %v", err)
	}
	if _, err := env.admin.Exec(ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ('`+m10Principal+`', 'user', 'm29')`); err != nil {
		t.Fatal(err)
	}
	for name, pool := range map[string]*pgxpool.Pool{"runtime": env.runtime, "admin": env.admin} {
		err := guardedExec(ctx, pool, `INSERT INTO auth.sessions (id, principal_id, token_hash, expires_at)
			VALUES ('`+m10Session+`', '`+m10Principal+`', '\x0a'::bytea, now() + interval '1 hour')`)
		if err == nil || !strings.Contains(err.Error(), "sessions_pin_present") {
			t.Errorf("%s inserting a live session without a pin: err = %v, want sessions_pin_present", name, err)
		}
	}
	if err := guardedExec(ctx, env.admin, `INSERT INTO auth.sessions (id, principal_id, token_hash, expires_at, revoked_at, revoked_reason)
		VALUES ('`+m10Session+`', '`+m10Principal+`', '\x0a'::bytea, now() + interval '1 hour', now(), 'session_revoked')`); err != nil {
		t.Fatalf("inserting a revoked session without a pin: %v", err)
	}
}
