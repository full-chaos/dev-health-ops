package oidc

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity/ssoaudit"
)

// DenialRow is go-api's audit_logs row of an authenticated caller a sign-in
// refused (internal/api/sso recordSSOAuthenticatedDenial): action sso_login,
// resource sso_provider, status failure. ErrorMessage is already sanitized.
type DenialRow struct {
	OrgID        uuid.UUID
	ProviderID   uuid.UUID
	Protocol     string
	Stage        string
	ErrorMessage string
}

// DenialAudit writes a DenialRow. A refusal that must be audited answers only
// after the row is written; a write error is the refusal's 500.
type DenialAudit interface {
	Denied(ctx context.Context, row DenialRow) error
}

// DenialExecer is the subset of *pgxpool.Pool the row needs.
type DenialExecer interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

// PGDenialAudit inserts the row into go-api's public.audit_logs
// (ssoaudit.Insert: internal/api/audit.PGWriter's columns and value types).
type PGDenialAudit struct {
	Exec DenialExecer
	// Now is injectable for tests; nil means time.Now.
	Now func() time.Time
}

// Denied implements DenialAudit.
func (a PGDenialAudit) Denied(ctx context.Context, row DenialRow) error {
	now := time.Now
	if a.Now != nil {
		now = a.Now
	}
	return ssoaudit.Insert(ctx, a.Exec, ssoaudit.Row(row), now())
}
