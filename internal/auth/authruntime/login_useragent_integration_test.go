//go:build integration

package authruntime

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/passwordhash"
)

// TestLoginAuditRowsCarryTheUserAgent: through the handler configure builds, a
// refused sign-in (no session exists) and an accepted one store the request's
// User-Agent on their audit rows, the same value the session records.
func TestLoginAuditRowsCarryTheUserAgent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newRoutesEnv(t, ctx)
	user, _ := e.seed(t, ctx)
	hash, err := passwordhash.Hash("user-agent-password")
	if err != nil {
		t.Fatal(err)
	}
	var email string
	if err := e.admin.QueryRow(ctx, `UPDATE users SET password_hash = $2 WHERE id = $1 RETURNING email`, user, hash).Scan(&email); err != nil {
		t.Fatal(err)
	}
	// The header carries the UTF-8 bytes of "é"; each byte reads as one latin-1
	// code point, so the stored value is "Ã©".
	const sent, agent = "venue-oracle/1 (é)", "venue-oracle/1 (Ã©)"
	header := map[string]string{"Content-Type": "application/json", "User-Agent": sent}

	agentOf := func(t *testing.T, eventType, requestWire string) *string {
		t.Helper()
		e.auditOf(t, ctx, eventType, requestWire)
		var stored *string
		if err := e.admin.QueryRow(ctx, `SELECT user_agent FROM auth.security_audit_events WHERE event_type = $1 ORDER BY id DESC LIMIT 1`,
			eventType).Scan(&stored); err != nil {
			t.Fatal(err)
		}
		return stored
	}

	got := e.doWith(t, "/api/v1/auth/login", `{"email":"`+email+`","password":"wrong"}`, header)
	if got.status != http.StatusUnauthorized {
		t.Fatalf("wrong password = %d %s", got.status, got.body)
	}
	var sessions int
	if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM auth.sessions WHERE principal_id = $1`, user).Scan(&sessions); err != nil || sessions != 0 {
		t.Fatalf("%d sessions after a refused sign-in (%v), want 0", sessions, err)
	}
	if stored := agentOf(t, "auth.login.failed", got.header.Get("X-Auth-Request-ID")); stored == nil || *stored != agent {
		t.Fatalf("failed-login row user_agent = %v, want %q", stored, agent)
	}

	got = e.doWith(t, "/api/v1/auth/login", `{"email":"`+email+`","password":"user-agent-password"}`, header)
	if got.status != http.StatusOK {
		t.Fatalf("login = %d %s", got.status, got.body)
	}
	stored := agentOf(t, "auth.login.succeeded", got.header.Get("X-Auth-Request-ID"))
	var sessionAgent *string
	if err := e.admin.QueryRow(ctx, `SELECT user_agent FROM auth.sessions WHERE principal_id = $1`, user).Scan(&sessionAgent); err != nil {
		t.Fatal(err)
	}
	if stored == nil || *stored != agent || sessionAgent == nil || *sessionAgent != *stored {
		t.Fatalf("sign-in row user_agent = %v, session user_agent = %v, want both %q", stored, sessionAgent, agent)
	}
}
