//go:build integration

package authschema

import (
	"context"
	"testing"
	"time"
)

// TestMigration0016SessionLegacyTokenVersion: sessions.legacy_token_version is a
// nullable bigint the runtime role writes at insert and nobody changes.
func TestMigration0016SessionLegacyTokenVersion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newM10Fixture(t, ctx)

	var dataType, nullable string
	if err := env.admin.QueryRow(ctx, `SELECT data_type, is_nullable FROM information_schema.columns
		WHERE table_schema = 'auth' AND table_name = 'sessions' AND column_name = 'legacy_token_version'`).Scan(&dataType, &nullable); err != nil {
		t.Fatalf("sessions.legacy_token_version: %v", err)
	}
	if dataType != "bigint" || nullable != "YES" {
		t.Fatalf("legacy_token_version is %s nullable=%s, want bigint nullable", dataType, nullable)
	}

	if _, err := env.admin.Exec(ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ('`+m10Principal+`', 'user', 'm15')`); err != nil {
		t.Fatal(err)
	}
	if _, err := env.runtime.Exec(ctx, `INSERT INTO auth.sessions (id, principal_id, token_hash, expires_at, legacy_token_version)
		VALUES ('`+m10Session+`', '`+m10Principal+`', '\x0a'::bytea, now() + interval '1 hour', 7)`); err != nil {
		t.Fatalf("the runtime role could not insert a pinned session: %v", err)
	}
	for _, who := range []struct {
		name string
		err  error
	}{
		{"runtime, plain", func() error {
			_, err := env.runtime.Exec(ctx, `UPDATE auth.sessions SET legacy_token_version = 8 WHERE id = '`+m10Session+`'`)
			return err
		}()},
		{"runtime, audited", guardedExec(ctx, env.runtime, `UPDATE auth.sessions SET legacy_token_version = 8 WHERE id = '`+m10Session+`'`)},
		{"admin, audited", guardedExec(ctx, env.admin, `UPDATE auth.sessions SET legacy_token_version = NULL WHERE id = '`+m10Session+`'`)},
	} {
		m10Fail(t, who.err, who.name+" changing a session's legacy_token_version")
	}
	var pin int64
	if err := env.admin.QueryRow(ctx, `SELECT legacy_token_version FROM auth.sessions WHERE id = '`+m10Session+`'`).Scan(&pin); err != nil || pin != 7 {
		t.Fatalf("pin %d (%v), want 7 unchanged", pin, err)
	}
	if err := guardedExec(ctx, env.runtime, `UPDATE auth.sessions SET revoked_at = now(), revoked_reason = 'session_revoked' WHERE id = '`+m10Session+`'`); err != nil {
		t.Fatalf("revoking a pinned session: %v", err)
	}
}
