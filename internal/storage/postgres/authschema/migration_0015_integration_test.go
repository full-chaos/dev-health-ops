//go:build integration

package authschema

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestMigration0015AuditUserAgentColumn: security_audit_events.user_agent is a
// nullable text column with no bound, written once by the runtime role and
// never changed.
func TestMigration0015AuditUserAgentColumn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newM10Fixture(t, ctx)

	var dataType, nullable string
	if err := env.admin.QueryRow(ctx, `SELECT data_type, is_nullable FROM information_schema.columns
		WHERE table_schema = 'auth' AND table_name = 'security_audit_events' AND column_name = 'user_agent'`).Scan(&dataType, &nullable); err != nil {
		t.Fatalf("security_audit_events.user_agent: %v", err)
	}
	if dataType != "text" || nullable != "YES" {
		t.Fatalf("user_agent is %s nullable=%s, want text nullable", dataType, nullable)
	}
	var checks int
	if err := env.admin.QueryRow(ctx, `SELECT count(*) FROM pg_constraint c
		JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY (c.conkey)
		WHERE c.conrelid = 'auth.security_audit_events'::regclass AND a.attname = 'user_agent'`).Scan(&checks); err != nil {
		t.Fatal(err)
	}
	if checks != 0 {
		t.Fatalf("user_agent carries %d constraints, want none (no bound)", checks)
	}

	long := strings.Repeat("Mozilla/5.0 ", 4000)
	for _, agent := range []any{"venue-oracle/1", long, nil, "Agenté latin-1"} {
		if err := guardedExec(ctx, env.runtime, `INSERT INTO auth.security_audit_events (event_type, outcome, user_agent)
			VALUES ('cap.probe', 'allowed', $1)`, agent); err != nil {
			t.Fatalf("the runtime role could not insert user_agent %.20v: %v", agent, err)
		}
	}
	for _, agent := range []string{"venue-oracle/1", long, "Agenté latin-1"} {
		var stored int
		if err := env.admin.QueryRow(ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'cap.probe' AND user_agent = $1`, agent).Scan(&stored); err != nil || stored != 1 {
			t.Fatalf("a %d-byte user agent was not stored whole (%d rows, %v)", len(agent), stored, err)
		}
	}
	var nulls int
	if err := env.admin.QueryRow(ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'cap.probe' AND user_agent IS NULL`).Scan(&nulls); err != nil || nulls != 1 {
		t.Fatalf("%d probe rows with a NULL user agent (%v), want 1", nulls, err)
	}
	if err := guardedExec(ctx, env.runtime, `UPDATE auth.security_audit_events SET user_agent = 'changed'`); err == nil {
		t.Fatal("the runtime role changed an audit row's user agent")
	}
}
