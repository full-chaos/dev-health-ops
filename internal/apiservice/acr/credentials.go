package acr

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Credential is one internal_service_credentials row, as the Python
// InternalServiceCredential model read it. The token itself is never held.
type Credential struct {
	ID          string
	ServiceName string
	Scopes      []string
	ExpiresAt   *time.Time
	RevokedAt   *time.Time
}

// Valid is InternalServiceCredential.is_valid: not revoked, and not past its
// expiry (a credential is still valid AT its expires_at instant).
func (c Credential) Valid(now time.Time) bool {
	if c.RevokedAt != nil {
		return false
	}
	return c.ExpiresAt == nil || !now.After(*c.ExpiresAt)
}

// Has reports whether the credential holds scope.
func (c Credential) Has(scope string) bool {
	for _, have := range c.Scopes {
		if have == scope {
			return true
		}
	}
	return false
}

// CredentialStore is what the entitlement and health routes need from the
// credential tables: the Python routes' credential lookup by token hash, the
// last_used_at stamp and the InternalServiceCredentialAudit row.
type CredentialStore interface {
	// Find returns the credential whose token hash matches; found is false for
	// an unknown token.
	Find(ctx context.Context, token string) (cred Credential, found bool, err error)
	// MarkUsed stamps last_used_at.
	MarkUsed(ctx context.Context, credentialID string, at time.Time) error
	// Audit inserts one internal_service_credential_audits row; an empty
	// credentialID is a NULL credential_id (the token matched no credential).
	Audit(ctx context.Context, credentialID, requestedOrgID, outcome string) error
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

func hashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func (s PostgresCredentialStore) Find(ctx context.Context, token string) (Credential, bool, error) {
	var cred Credential
	var scopes string
	err := s.Pool.QueryRow(ctx, `SELECT id::text, service_name, scopes::text, expires_at, revoked_at
		FROM internal_service_credentials WHERE token_hash = $1`, hashToken(token)).
		Scan(&cred.ID, &cred.ServiceName, &scopes, &cred.ExpiresAt, &cred.RevokedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Credential{}, false, nil
	}
	if err != nil {
		return Credential{}, false, err
	}
	if err := json.Unmarshal([]byte(scopes), &cred.Scopes); err != nil {
		cred.Scopes = nil
	}
	return cred, true, nil
}

func (s PostgresCredentialStore) MarkUsed(ctx context.Context, credentialID string, at time.Time) error {
	_, err := s.Pool.Exec(ctx, `UPDATE internal_service_credentials SET last_used_at = $2 WHERE id = $1::uuid`,
		credentialID, at)
	return err
}

func (s PostgresCredentialStore) Audit(ctx context.Context, credentialID, requestedOrgID, outcome string) error {
	var credential any
	if credentialID != "" {
		credential = credentialID
	}
	_, err := s.Pool.Exec(ctx, `INSERT INTO internal_service_credential_audits
		(id, credential_id, requested_org_id, action, outcome, created_at)
		VALUES ($1, $2::uuid, $3, $4, $5, $6)`,
		uuid.NewString(), credential, requestedOrgID, auditAction, outcome, s.now())
	return err
}
