package session

import (
	"context"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
)

// InsertIn writes a sign-in's session, its first refresh credential, the last
// login stamp and event inside tx, the caller's audit.Commit: a sign-in that
// joins an organization commits its session with the membership. A missing or
// disabled principal is ErrPrincipalUnavailable.
func (s *Store) InsertIn(ctx context.Context, tx audit.TxOps, principalID uuid.UUID, organizationID *uuid.UUID, amr []string,
	event audit.AuditEvent) (Issued, error) {
	if !validAMR(amr) {
		return Issued{}, ErrInvalidAMR
	}
	if event.RequestID == nil {
		event.RequestID = requestid.Audit(ctx)
	}
	out, err := s.insertSession(ctx, tx, principalID, organizationID, amr, contracts.SecretString{})
	if err := s.stampSignIn(WithSignIn(ctx), tx, out.Session.ID, err); err != nil {
		return Issued{}, err
	}
	if err := tx.AddAudit(event); err != nil {
		return Issued{}, err
	}
	return out, nil
}
