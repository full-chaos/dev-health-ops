//go:build integration

package audit

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// schemaDDL is the slice of the Alembic schema this writer touches, with
// the column types and nullability of models/audit.py AuditLog.
var schemaDDL = []string{
	`CREATE TABLE public.organizations (id uuid PRIMARY KEY, name text NOT NULL)`,
	`CREATE TABLE public.users (id uuid PRIMARY KEY, email text NOT NULL UNIQUE)`,
	`CREATE TABLE public.audit_logs (
		id uuid PRIMARY KEY,
		org_id uuid NOT NULL REFERENCES public.organizations(id) ON DELETE CASCADE,
		user_id uuid REFERENCES public.users(id) ON DELETE SET NULL,
		action text NOT NULL,
		resource_type text NOT NULL,
		resource_id text NOT NULL,
		description text,
		changes json,
		request_metadata json,
		status text NOT NULL DEFAULT 'success',
		error_message text,
		created_at timestamptz NOT NULL)`,
}

func startPool(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	postgres, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = postgres.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, postgres.URI)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)
	for _, stmt := range schemaDDL {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			t.Fatalf("schema: %v\n%s", err, stmt)
		}
	}
	return pool
}

// TestWriteInsertsTheDeclaredColumns is the real-Postgres round trip this
// package's doc comment promises: every Entry field lands in the row a
// caller reads back, with the same JSON encoding pybody/pyjson would give
// the Python plane's changes/request_metadata columns.
func TestWriteInsertsTheDeclaredColumns(t *testing.T) {
	ctx := context.Background()
	pool := startPool(t, ctx)

	orgID := uuid.New()
	userID := uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO organizations (id, name) VALUES ($1, 'acme')`, orgID); err != nil {
		t.Fatalf("seed org: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO users (id, email) VALUES ($1, 'admin@example.com')`, userID); err != nil {
		t.Fatalf("seed user: %v", err)
	}

	fixedNow := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	writer := PGWriter{Now: func() time.Time { return fixedNow }}
	description := "Organization invite created"
	entry := Entry{
		OrgID:           orgID,
		UserID:          &userID,
		Action:          ActionMemberInvited,
		ResourceType:    ResourceMembership,
		ResourceID:      "some-invite-id",
		Description:     &description,
		Changes:         []byte(`{"email":"new@example.com","role":"member","status":"pending"}`),
		RequestMetadata: []byte(`{"ip_address":"203.0.113.9"}`),
	}
	id, err := writer.Write(ctx, pool, entry)
	if err != nil {
		t.Fatalf("write: %v", err)
	}

	var (
		gotOrgID             uuid.UUID
		gotUserID            uuid.UUID
		action, resourceType string
		resourceID           string
		gotDescription       string
		changes, requestMeta []byte
		status               string
		errorMessage         *string
		createdAt            time.Time
	)
	err = pool.QueryRow(ctx, `
SELECT org_id, user_id, action, resource_type, resource_id, description,
       changes, request_metadata, status, error_message, created_at
FROM audit_logs WHERE id = $1`, id,
	).Scan(&gotOrgID, &gotUserID, &action, &resourceType, &resourceID, &gotDescription,
		&changes, &requestMeta, &status, &errorMessage, &createdAt)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}

	if gotOrgID != orgID {
		t.Fatalf("org_id = %s, want %s", gotOrgID, orgID)
	}
	if gotUserID != userID {
		t.Fatalf("user_id = %s, want %s", gotUserID, userID)
	}
	if action != string(ActionMemberInvited) {
		t.Fatalf("action = %q, want %q", action, ActionMemberInvited)
	}
	if resourceType != string(ResourceMembership) {
		t.Fatalf("resource_type = %q, want %q", resourceType, ResourceMembership)
	}
	if resourceID != "some-invite-id" {
		t.Fatalf("resource_id = %q", resourceID)
	}
	if gotDescription != description {
		t.Fatalf("description = %q, want %q", gotDescription, description)
	}
	if string(changes) != string(entry.Changes) {
		t.Fatalf("changes = %s, want %s", changes, entry.Changes)
	}
	if string(requestMeta) != string(entry.RequestMetadata) {
		t.Fatalf("request_metadata = %s, want %s", requestMeta, entry.RequestMetadata)
	}
	if status != "success" {
		t.Fatalf("status = %q, want success (the default, entry.Status was empty)", status)
	}
	if errorMessage != nil {
		t.Fatalf("error_message = %v, want nil", errorMessage)
	}
	if !createdAt.Equal(fixedNow) {
		t.Fatalf("created_at = %s, want %s (the injected clock)", createdAt, fixedNow)
	}
}

// TestWriteWithNilChangesStoresSQLNull proves the nullable columns really
// write NULL, not the four-byte JSON string "null" -- pgx's own default for
// a nil []byte passed to a jsonb parameter is the SQL NULL a caller with no
// changes/metadata needs (matching Python's changes=None).
// TestWriteWithNilChangesStoresEmptyObject pins AuditLog.__init__'s own
// coercion (models/audit.py: "self.changes = changes or {}",
// "self.request_metadata = request_metadata or {}"): both columns are
// nullable in the schema, but every row ever built through AuditLog(...)
// -- create_entry included, which is every row emit_audit_log or
// AuditService.log produces -- stores "{}", never SQL NULL, when the
// caller passed nothing. Proven against the real Python row by
// internal/apiservice/admin's TestOrgInviteAndPasswordChangeAuditMatchThePythonAPI
// (set_user_password's audit row: Python's changes column read back "{}",
// this package's writer wrote SQL NULL, before jsonOrEmptyObject's fix).
// user_id and description stay genuinely nullable -- this Entry leaves both
// unset, and they read back NULL.
func TestWriteWithNilChangesStoresEmptyObject(t *testing.T) {
	ctx := context.Background()
	pool := startPool(t, ctx)

	orgID := uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO organizations (id, name) VALUES ($1, 'acme')`, orgID); err != nil {
		t.Fatalf("seed org: %v", err)
	}

	writer := PGWriter{}
	id, err := writer.Write(ctx, pool, Entry{
		OrgID:        orgID,
		Action:       ActionPasswordChanged,
		ResourceType: ResourceUser,
		ResourceID:   "some-user-id",
	})
	if err != nil {
		t.Fatalf("write: %v", err)
	}

	var changes, requestMeta []byte
	var userID *uuid.UUID
	var description *string
	err = pool.QueryRow(ctx,
		`SELECT changes, request_metadata, user_id, description FROM audit_logs WHERE id = $1`, id,
	).Scan(&changes, &requestMeta, &userID, &description)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if string(changes) != "{}" {
		t.Fatalf("changes = %s, want {}", changes)
	}
	if string(requestMeta) != "{}" {
		t.Fatalf("request_metadata = %s, want {}", requestMeta)
	}
	if userID != nil {
		t.Fatalf("user_id = %s, want SQL NULL", *userID)
	}
	if description != nil {
		t.Fatalf("description = %q, want SQL NULL", *description)
	}
}

// TestWriteRejectsAnUnknownOrg proves the FK actually holds: a writer given
// an org_id no row backs must fail, not silently orphan the audit trail.
func TestWriteRejectsAnUnknownOrg(t *testing.T) {
	ctx := context.Background()
	pool := startPool(t, ctx)

	writer := PGWriter{}
	_, err := writer.Write(ctx, pool, Entry{
		OrgID:        uuid.New(),
		Action:       ActionMemberInvited,
		ResourceType: ResourceMembership,
		ResourceID:   "x",
	})
	if err == nil {
		t.Fatal("expected a foreign-key violation, got nil")
	}
	if pgx.ErrNoRows == err {
		t.Fatalf("unexpected error shape: %v", err)
	}
}
