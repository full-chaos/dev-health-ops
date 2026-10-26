//go:build integration

package authschema

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TestMigration0028PinPresentConstraint: the constraint exists, is not yet
// validated, and refuses a live session row without a pin from any role.
func TestMigration0028PinPresentConstraint(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newM10Fixture(t, ctx)

	var validated bool
	if err := env.admin.QueryRow(ctx, `SELECT convalidated FROM pg_constraint
		WHERE conname = 'sessions_pin_present' AND conrelid = 'auth.sessions'::regclass`).Scan(&validated); err != nil {
		t.Fatalf("sessions_pin_present: %v", err)
	}
	if validated {
		t.Fatal("sessions_pin_present is validated before the stored rows were checked")
	}
	if _, err := env.admin.Exec(ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ('`+m10Principal+`', 'user', 'm28')`); err != nil {
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
