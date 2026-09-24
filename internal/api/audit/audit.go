// Package audit is the shared audit_logs writer for dho api's ported admin
// routes. It reproduces api/utils/audit.py's emit_audit_log and
// api/services/audit.py's AuditService.log: both are thin builders over the
// same audit_logs row shape (models/audit.py AuditLog), so one Go writer
// serves every caller -- the caller builds the Entry (extracting request
// metadata, merging impersonation context when relevant); this package only
// inserts it.
package audit

import (
	"context"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// Action is models/audit.py AuditAction's value. Only the actions this PR's
// routes emit are declared; the Python enum has many more, but a string
// column accepts any value either side ever writes.
type Action string

const (
	ActionMemberInvited      Action = "member_invited"
	ActionPasswordChanged    Action = "password_changed"
	ActionImpersonationStart Action = "impersonation_start"
	ActionImpersonationStop  Action = "impersonation_stop"
	ActionLogin              Action = "login"
	ActionLogout             Action = "logout"
	ActionLoginFailed        Action = "login_failed"
	ActionCreate             Action = "create"
	ActionPasswordReset      Action = "password_reset"
	ActionPasswordResetAsked Action = "password_reset_requested"
	ActionMemberJoined       Action = "member_joined"
)

// ResourceType is models/audit.py AuditResourceType's value.
type ResourceType string

const (
	ResourceMembership   ResourceType = "membership"
	ResourceUser         ResourceType = "user"
	ResourceSession      ResourceType = "session"
	ResourceOrganization ResourceType = "organization"
)

// Entry is one audit_logs row, in the column order and nullability of
// models/audit.py AuditLog. Changes and RequestMetadata are stored as JSON
// (pyjson.Marshal, so the Python and Go planes render identical JSON for the
// same value); either may be nil, matching a Python None.
type Entry struct {
	OrgID        uuid.UUID
	UserID       *uuid.UUID
	Action       Action
	ResourceType ResourceType
	ResourceID   string
	Description  *string
	// Changes and RequestMetadata are pre-encoded JSON (or nil): the caller
	// builds them with pyjson so map key order and number/string rendering
	// match the Python plane byte for byte, the same discipline pybody uses
	// for response bodies. Both columns are Postgres `json` (alembic
	// 0001_initial_schema.py: sa.JSON()), NOT `jsonb` -- `json` stores the
	// original text verbatim, so key order survives a read-back; `jsonb`
	// parses and re-serializes, silently reordering keys. Getting this
	// wrong is invisible until something diffs the raw text (measured: the
	// integration test below failed on exactly this before the schema was
	// corrected to `json`).
	Changes         []byte
	RequestMetadata []byte
	// Status defaults to "success" (AuditLog.status's own column default)
	// when empty, matching emit_audit_log's status: str = "success" and
	// AuditService.log's identical default.
	Status       string
	ErrorMessage *string
}

// Execer is the subset of *pgxpool.Pool and pgx.Tx that Write needs. A
// caller inside an open transaction passes the tx, so the audit row commits
// or rolls back atomically with whatever else that transaction does --
// Python's own AuditLog is session.add()ed to the SAME SQLAlchemy session
// (and its one implicit request-scoped commit) as every other write the
// route makes, never committed on its own. A caller with no open
// transaction of its own passes the pool directly.
type Execer interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

// Writer inserts audit_logs rows. PGWriter is the production implementation.
type Writer interface {
	Write(ctx context.Context, exec Execer, entry Entry) (uuid.UUID, error)
}

// PGWriter is Writer's production implementation. It carries no pool of its
// own: every call site is inside a transaction it already opened for its
// other writes, or (a pure-audit, no-other-write route, if one is ever
// added) passes the shared pool explicitly.
type PGWriter struct {
	// Now is injectable for tests; nil means time.Now.
	Now func() time.Time
}

func (w PGWriter) now() time.Time {
	if w.Now != nil {
		return w.Now()
	}
	return time.Now()
}

// Write inserts entry through exec and returns its generated id. It never
// assigns created_at itself in SQL (now()): the Go plane's clock, injected
// the same way PGStore's is, keeps a test's expected timestamp exact.
func (w PGWriter) Write(ctx context.Context, exec Execer, entry Entry) (uuid.UUID, error) {
	status := entry.Status
	if status == "" {
		status = "success"
	}
	id := uuid.New()
	_, err := exec.Exec(ctx, `
INSERT INTO audit_logs
	(id, org_id, user_id, action, resource_type, resource_id, description,
	 changes, request_metadata, status, error_message, created_at)
VALUES
	($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
		id, entry.OrgID, entry.UserID, string(entry.Action), string(entry.ResourceType),
		entry.ResourceID, entry.Description, jsonOrEmptyObject(entry.Changes), jsonOrEmptyObject(entry.RequestMetadata),
		status, entry.ErrorMessage, w.now().UTC(),
	)
	if err != nil {
		return uuid.UUID{}, err
	}
	return id, nil
}

// jsonOrEmptyObject is AuditLog.__init__'s own coercion (models/audit.py:
// "self.changes = changes or {}", "self.request_metadata = request_metadata
// or {}") -- BOTH columns are nullable in the schema, but every row ever
// constructed through AuditLog(...) (create_entry included, which is every
// row emit_audit_log or AuditService.log produces) stores "{}", never SQL
// NULL, when the caller passed nothing. An empty/nil Changes or
// RequestMetadata renders "{}"; a non-empty slice passes through for pgx's
// json encoder. Caught by TestOrgInviteAndPasswordChangeAuditMatchThePythonAPI
// (the set_user_password row: Python's changes column read back as an empty
// map, Go's as SQL NULL, before this fix).
func jsonOrEmptyObject(raw []byte) any {
	if len(raw) == 0 {
		return []byte("{}")
	}
	return raw
}

// RequestMetadata is the request_metadata JSON column text emit_audit_log
// stores (api/utils/audit.py extract_request_metadata, then
// AuditLog.create_entry): ip_address (the first X-Forwarded-For hop,
// stripped, when the header is non-empty; else the peer host), user_agent
// and request_id (X-Request-ID), each kept only when non-empty, in that
// order, written as SQLAlchemy's JSON type writes it (json.dumps defaults).
// nil when none is present (create_entry stores None, which AuditLog
// stores as "{}").
func RequestMetadata(r *http.Request) ([]byte, error) {
	metadata := pyjson.NewObject()
	var ip string
	if forwarded := r.Header.Get("X-Forwarded-For"); forwarded != "" {
		first, _, _ := strings.Cut(forwarded, ",")
		ip = pythonparity.Strip(first)
	} else if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		ip = host
	}
	if ip != "" {
		metadata.Set("ip_address", ip)
	}
	if agent := r.Header.Get("User-Agent"); agent != "" {
		metadata.Set("user_agent", agent)
	}
	if requestID := r.Header.Get("X-Request-ID"); requestID != "" {
		metadata.Set("request_id", requestID)
	}
	if metadata.Len() == 0 {
		return nil, nil
	}
	text, err := pyjson.Dumps(metadata)
	if err != nil {
		return nil, err
	}
	return []byte(text), nil
}
