package oidc

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
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

// PGDenialAudit inserts the row into go-api's public.audit_logs with
// internal/api/audit.PGWriter's columns and value types, so both services
// write the same row for the same refusal.
type PGDenialAudit struct {
	Exec DenialExecer
	// Now is injectable for tests; nil means time.Now.
	Now func() time.Time
}

// Denied implements DenialAudit.
func (a PGDenialAudit) Denied(ctx context.Context, row DenialRow) error {
	meta := pyjson.NewObject()
	meta.Set("protocol", row.Protocol)
	if row.Stage != "" {
		meta.Set("stage", row.Stage)
	}
	metaJSON, err := pyjson.Dumps(meta)
	if err != nil {
		return err
	}
	now := time.Now
	if a.Now != nil {
		now = a.Now
	}
	message := row.ErrorMessage
	var user *uuid.UUID
	var description *string
	_, err = a.Exec.Exec(ctx, `INSERT INTO public.audit_logs
	(id, org_id, user_id, action, resource_type, resource_id, description,
	 changes, request_metadata, status, error_message, created_at)
VALUES
	($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
		uuid.New(), row.OrgID, user, "sso_login", "sso_provider", row.ProviderID.String(), description,
		[]byte("{}"), []byte(metaJSON), "failure", &message, now().UTC())
	return err
}
