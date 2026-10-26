//go:build integration

package authrows

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// the public tables are the migrated schema (pgschema); the auth tables hold only the columns the projections name
const fixtureSchema = `
CREATE SCHEMA auth;
CREATE TABLE auth.users (principal_id uuid PRIMARY KEY, last_login_at timestamptz);
CREATE TABLE auth.sessions (id uuid PRIMARY KEY, principal_id uuid, organization_id uuid, revoked_at timestamptz);
CREATE TABLE auth.refresh_credentials (id uuid PRIMARY KEY, session_id uuid, consumed_at timestamptz, revoked_at timestamptz);
CREATE TABLE auth.security_audit_events (id uuid PRIMARY KEY, event_type text, organization_id uuid, subject_principal_id uuid,
	actor_principal_id uuid, client_ip inet, user_agent text, attributes jsonb, occurred_at timestamptz);
CREATE TABLE auth.link_credentials (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), principal_id uuid NOT NULL, purpose text NOT NULL,
	token_hash bytea NOT NULL, expires_at timestamptz NOT NULL, consumed_at timestamptz, created_at timestamptz NOT NULL DEFAULT now());
INSERT INTO organizations (id, slug, name) VALUES ('00000000-0000-4000-8000-0000000000a1', 'acme', 'Acme');
INSERT INTO users (id, email, auth_provider) VALUES ('00000000-0000-4000-8000-0000000000b1', 'a@example.com', 'local'),
	('00000000-0000-4000-8000-0000000000b2', 'b@example.com', 'local');
`

func fixture(t *testing.T, statements ...string) *pgx.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatalf("postgres: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	pgschema.ApplyURI(ctx, t, instance.URI)
	conn, err := pgx.Connect(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	for _, statement := range append([]string{fixtureSchema}, statements...) {
		if _, err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("fixture: %v\n%s", err, statement)
		}
	}
	return conn
}

func forwardedAll(t *testing.T) Forwarded {
	t.Helper()
	f, err := ParseForwarded("sessions,local_login,social")
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func run(t *testing.T, conn *pgx.Conn, table, public string) []string {
	t.Helper()
	return runWith(t, conn, table, public, forwardedAll(t))
}

func runWith(t *testing.T, conn *pgx.Conn, table, public string, f Forwarded) []string {
	t.Helper()
	query, projected, err := Query(table, public, f)
	if err != nil || !projected {
		t.Fatalf("%s: projected %v err %v", table, projected, err)
	}
	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		t.Fatalf("%s: %v\n%s", table, err, query)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			t.Fatal(err)
		}
		var parts []string
		for _, v := range values {
			parts = append(parts, sprint(v))
		}
		out = append(out, strings.Join(parts, "|"))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func sprint(v any) string {
	if v == nil {
		return "<nil>"
	}
	return strings.TrimSpace(strings.Join(strings.Fields(fmt.Sprint(v)), " "))
}

func TestAuthRowsRefreshProjectionUnionsPublicAndAuthIntegration(t *testing.T) {
	conn := fixture(t, `
INSERT INTO refresh_tokens (id, user_id, org_id, token_hash, family_id, expires_at) VALUES ('00000000-0000-4000-8000-0000000000c1', '00000000-0000-4000-8000-0000000000b1', '00000000-0000-4000-8000-0000000000a1', 'h1', '00000000-0000-4000-8000-0000000000c2', now() + interval '1 day');
INSERT INTO auth.sessions VALUES ('00000000-0000-4000-8000-0000000000d1', '00000000-0000-4000-8000-0000000000b1', '00000000-0000-4000-8000-0000000000a1', NULL),
	('00000000-0000-4000-8000-0000000000d2', '00000000-0000-4000-8000-0000000000b2', NULL, now());
INSERT INTO auth.refresh_credentials VALUES ('00000000-0000-4000-8000-0000000000e1', '00000000-0000-4000-8000-0000000000d1', NULL, NULL),
	('00000000-0000-4000-8000-0000000000e2', '00000000-0000-4000-8000-0000000000d1', now(), NULL),
	('00000000-0000-4000-8000-0000000000e3', '00000000-0000-4000-8000-0000000000d2', NULL, NULL);`)
	got := strings.Join(run(t, conn, TableRefreshTokens, ""), "\n")
	want := strings.Join([]string{"a@example.com|acme|false|2", "a@example.com|acme|true|1", "b@example.com|<null>|true|1"}, "\n")
	if got != want {
		t.Fatalf("refresh rows:\n%s\nwant:\n%s", got, want)
	}
}

func TestAuthRowsAuditProjectionCoversLoginFailedAndSucceededIntegration(t *testing.T) {
	conn := fixture(t, `
INSERT INTO auth.security_audit_events VALUES
 ('00000000-0000-4000-8000-0000000000f1', 'auth.login.succeeded', '00000000-0000-4000-8000-0000000000a1', '00000000-0000-4000-8000-0000000000b1', NULL, '192.0.2.1', 'agent', '{}', '2026-01-01T00:00:00Z'),
 ('00000000-0000-4000-8000-0000000000f2', 'auth.login.failed', NULL, '00000000-0000-4000-8000-0000000000b2', NULL, NULL, NULL, '{"reason_code":"invalid_credentials"}', '2026-01-01T00:00:01Z'),
 ('00000000-0000-4000-8000-0000000000f3', 'auth.session.revoked', NULL, '00000000-0000-4000-8000-0000000000b2', NULL, NULL, NULL, '{}', '2026-01-01T00:00:02Z');`)
	got := run(t, conn, TableAuditLogs, "")
	if len(got) != 2 {
		t.Fatalf("want the 2 sign-in events, got %d: %v", len(got), got)
	}
	if !strings.Contains(got[0], "login") || !strings.Contains(got[0], "User logged in") || !strings.Contains(got[0], "success") {
		t.Errorf("succeeded row: %s", got[0])
	}
	if !strings.Contains(got[1], "login_failed") || !strings.Contains(got[1], "Login failed: invalid credentials") || !strings.Contains(got[1], "Invalid credentials") {
		t.Errorf("failed row: %s", got[1])
	}
}

func TestAuthRowsLastLoginReadsAuthUsersIntegration(t *testing.T) {
	conn := fixture(t, `INSERT INTO auth.users VALUES ('00000000-0000-4000-8000-0000000000b1', now());`)
	got := run(t, conn, TableUsers, `SELECT email, last_login_at IS NOT NULL FROM users WHERE auth_provider IS DISTINCT FROM 'service' ORDER BY email`)
	want := []string{"a@example.com|true", "b@example.com|false"}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("users rows %v, want %v", got, want)
	}
}

func TestAuthRowsRevisionRevokedQuirkIntegration(t *testing.T) {
	conn := fixture(t, `
INSERT INTO auth.sessions VALUES ('00000000-0000-4000-8000-0000000000d1', '00000000-0000-4000-8000-0000000000b1', '00000000-0000-4000-8000-0000000000a1', now());
INSERT INTO auth.refresh_credentials VALUES ('00000000-0000-4000-8000-0000000000e1', '00000000-0000-4000-8000-0000000000d1', NULL, NULL);`)
	got := run(t, conn, TableRefreshTokens, "")
	if len(got) != 1 || got[0] != "a@example.com|acme|true|1" {
		t.Fatalf("a credential of a revoked session reads revoked, got %v", got)
	}
}

func credentialsForwarded(t *testing.T) Forwarded {
	t.Helper()
	f, err := ParseForwarded("sessions,local_login,credentials")
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestAuthRowsLinkTokenProjectionReadsPublicAndAuthServiceRowsIntegration(t *testing.T) {
	conn := fixture(t, `
INSERT INTO email_verification_tokens (id, user_id, token_hash, expires_at, created_at) VALUES ('00000000-0000-4000-8000-0000000000c1', '00000000-0000-4000-8000-0000000000b1', repeat('a', 64), now() - interval '1 hour', now() - interval '25 hours');
INSERT INTO auth.link_credentials (principal_id, purpose, token_hash, expires_at, created_at) VALUES
 ('00000000-0000-4000-8000-0000000000b2', 'email_verification', decode(repeat('ab', 32), 'hex'), now() + interval '1 day', now()),
 ('00000000-0000-4000-8000-0000000000b2', 'password_reset', decode(repeat('cd', 32), 'hex'), now() + interval '1 hour', now());
INSERT INTO auth.link_credentials (principal_id, purpose, token_hash, expires_at, consumed_at, created_at) VALUES
 ('00000000-0000-4000-8000-0000000000b1', 'email_verification', decode(repeat('ef', 32), 'hex'), now() + interval '1 day', now(), now());`)
	f := credentialsForwarded(t)
	got := strings.Join(runWith(t, conn, TableEmailVerificationTokens, "", f), "\n")
	want := "a@example.com|true|false|64|1440\nb@example.com|true|true|64|1440"
	if got != want {
		t.Fatalf("email_verification_tokens rows:\n%s\nwant:\n%s", got, want)
	}
	got = strings.Join(runWith(t, conn, TablePasswordResetTokens, "", f), "\n")
	if want = "b@example.com|true|true|64|60"; got != want {
		t.Fatalf("password_reset_tokens rows:\n%s\nwant:\n%s", got, want)
	}
}

func TestAuthRowsCredentialsAuditProjectionMapsEveryEventIntegration(t *testing.T) {
	var events []string
	i := 0
	for eventType := range CredentialAuditEvents() {
		i++
		events = append(events, fmt.Sprintf(`('00000000-0000-4000-8000-00000000ff%02d', '%s', NULL, '00000000-0000-4000-8000-0000000000b1', NULL, '192.0.2.1', 'agent', '{}', '2026-01-01T00:00:%02dZ')`, i, eventType, i))
	}
	conn := fixture(t, `INSERT INTO auth.security_audit_events VALUES `+strings.Join(events, ",")+`,
 ('00000000-0000-4000-8000-00000000fe01', 'auth.principal.projected', NULL, '00000000-0000-4000-8000-0000000000b1', NULL, NULL, NULL, '{}', '2026-01-02T00:00:00Z'),
 ('00000000-0000-4000-8000-00000000fe02', 'auth.session.revoked', NULL, '00000000-0000-4000-8000-0000000000b1', NULL, NULL, NULL, '{}', '2026-01-02T00:00:01Z');`)
	got := runWith(t, conn, TableAuditLogs, "", credentialsForwarded(t))
	if len(got) != len(CredentialAuditEvents()) {
		t.Fatalf("want one row per mapped event and none for ignored events, got %d: %v", len(got), got)
	}
	for _, row := range got {
		if !strings.HasPrefix(row, "<nil>|a@example.com|") || !strings.Contains(row, "|user:a@example.com|") || !strings.Contains(row, `{"ip_address": "192.0.2.1", "user_agent": "agent"}`) {
			t.Errorf("row %s", row)
		}
	}
}

func TestAuthRowsCredentialsAuditProjectionFailsNamingAnUnmappedEventIntegration(t *testing.T) {
	conn := fixture(t, `INSERT INTO auth.security_audit_events VALUES
 ('00000000-0000-4000-8000-00000000fd01', 'auth.user.unheard_of', NULL, '00000000-0000-4000-8000-0000000000b1', NULL, NULL, NULL, '{}', '2026-01-01T00:00:00Z');`)
	query, _, err := Query(TableAuditLogs, "", credentialsForwarded(t))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := conn.Query(context.Background(), query)
	if err == nil {
		for rows.Next() {
		}
		err = rows.Err()
		rows.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "auth.user.unheard_of") {
		t.Fatalf("an unmapped credentials event must fail the query naming it, got %v", err)
	}
}
