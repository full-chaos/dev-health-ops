package identity

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

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

// joinPublicMembershipColumns are the public.memberships columns a join
// inserts: exactly authschema.RegisterOrganizationGrants' memberships columns.
var joinPublicMembershipColumns = []string{"id", "user_id", "org_id", "role", "joined_at", "created_at", "updated_at"}

// insertJoinMembership writes the active membership of a join inside the
// caller's audit.Commit: the public.memberships row go-api's readers use, then
// the auth.memberships row, with one id, role and instant. Both rows commit
// together or neither does. It is the only writer of a new auth membership
// apart from the organizations backfill, which copies public rows.
//
// It locks the public.users row as go-api's own JIT join does, so the two
// joins of one account run one after the other. A public row in the
// organization is errJoinAlreadyMember and an auth row without one is
// ErrMembershipDiverged; neither writes. No public.users row, or no
// public.organizations row, is ErrLegacyProjection. A unique violation on
// either table (a join that raced this one) is errJoinAlreadyMember; the
// transaction is then aborted, so the caller returns the error.
func (s *Store) insertJoinMembership(ctx context.Context, tx audit.TxOps, m membershipJoin) error {
	if !s.ownsUsers {
		return ErrSignupNotAvailable
	}
	if !IsOrganizationRole(m.Role) {
		return ErrBackfillRole
	}
	var locked uuid.UUID
	err := tx.QueryRow(ctx, `SELECT id FROM public.users WHERE id = $1 FOR UPDATE`, m.Principal).Scan(&locked)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrLegacyProjection
	}
	if err != nil {
		return err
	}
	var public, auth bool
	if err := tx.QueryRow(ctx, `SELECT
  EXISTS (SELECT 1 FROM public.memberships WHERE user_id = $1 AND org_id = $2),
  EXISTS (SELECT 1 FROM `+s.q+`.memberships WHERE principal_id = $1 AND organization_id = $2)`,
		m.Principal, m.OrgID).Scan(&public, &auth); err != nil {
		return err
	}
	switch {
	case public:
		return errJoinAlreadyMember
	case auth:
		return ErrMembershipDiverged
	}
	var at time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&at); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO public.memberships (`+strings.Join(joinPublicMembershipColumns, ", ")+`)
VALUES ($1, $2, $3, $4, $5, $5, $5)`, m.ID, m.Principal, m.OrgID, m.Role, at); err != nil {
		return joinWriteErr(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO `+s.q+`.memberships (id, organization_id, principal_id, status, role, joined_at)
VALUES ($1, $2, $3, 'active', $4, $5)`, m.ID, m.OrgID, m.Principal, m.Role, at); err != nil {
		return joinWriteErr(err)
	}
	return nil
}

// joinWriteErr maps a membership insert failure: a unique violation is a
// concurrent join; a missing public user or organization is
// ErrLegacyProjection. Anything else stays as it is.
func joinWriteErr(err error) error {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return err
	}
	switch {
	case pgErr.Code == "23505" && pgErr.TableName == "memberships":
		return errJoinAlreadyMember
	case pgErr.Code == "23503" && pgErr.SchemaName == "public" && pgErr.TableName == "memberships":
		return ErrLegacyProjection
	}
	return err
}
