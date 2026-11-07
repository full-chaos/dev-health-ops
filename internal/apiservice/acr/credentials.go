package acr

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Credential is what an accepted bearer yields: the verified workload token
// subject, which the entitlement route's audit row carries.
type Credential struct {
	PrincipalID string
}

// CredentialStore is the audit sink of the internal acr routes: one
// internal_service_credential_audits row per entitlement lookup outcome.
type CredentialStore interface {
	// Audit inserts one internal_service_credential_audits row; an empty
	// credentialID is a NULL credential_id (no route reads a credential row
	// any more, so every caller passes it empty), an empty principalID a NULL
	// principal_id (only a verified workload token sets it).
	Audit(ctx context.Context, credentialID, principalID, requestedOrgID, outcome string) error
}

// auditAction is the action every audit row carries
// (api/internal/acr.py: "acr_entitlement_lookup").
const auditAction = "acr_entitlement_lookup"

// PostgresCredentialStore is the CredentialStore over the API database.
type PostgresCredentialStore struct {
	Pool *pgxpool.Pool
	Now  func() time.Time
}

func (s PostgresCredentialStore) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now().UTC()
}

func (s PostgresCredentialStore) Audit(ctx context.Context, credentialID, principalID, requestedOrgID, outcome string) error {
	var credential, principal any
	if credentialID != "" {
		credential = credentialID
	}
	if principalID != "" {
		principal = principalID
	}
	_, err := s.Pool.Exec(ctx, `INSERT INTO internal_service_credential_audits
		(id, credential_id, principal_id, requested_org_id, action, outcome, created_at)
		VALUES ($1, $2::uuid, $3, $4, $5, $6, $7)`,
		uuid.NewString(), credential, principal, requestedOrgID, auditAction, outcome, s.now())
	return err
}
