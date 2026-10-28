package identity

import (
	"context"
	"errors"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// ErrMembershipDiverged reports a join of an account that has an
// auth.memberships row in the organization but no public.memberships row.
// Nothing is written.
var ErrMembershipDiverged = errors.New("identity: the account has an auth membership in the organization and no public membership")

// errJoinAlreadyMember reports a join of an account that already is a member
// of the organization in public.memberships. Nothing is written.
var errJoinAlreadyMember = errors.New("identity: already a member of the organization")

// membershipJoin is one active membership a join writes.
type membershipJoin struct {
	ID        uuid.UUID
	Principal uuid.UUID
	OrgID     uuid.UUID
	Role      string
}

// insertJoinMembership writes the active membership of a join inside the
// caller's audit.Commit.
func (s *Store) insertJoinMembership(ctx context.Context, tx audit.TxOps, m membershipJoin) error {
	if !s.ownsUsers {
		return ErrSignupNotAvailable
	}
	_, err := tx.Exec(ctx, `INSERT INTO `+s.q+`.memberships (id, organization_id, principal_id, status, role, joined_at)
VALUES ($1, $2, $3, 'active', $4, clock_timestamp())`, m.ID, m.OrgID, m.Principal, m.Role)
	return err
}
