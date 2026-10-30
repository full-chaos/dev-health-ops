package lockorder

import (
	"errors"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/auth/invite"
	"github.com/full-chaos/dev-health-ops/internal/auth/membership"
)

// afterDelete is how an auth verb that raced go-api's organization delete
// ended.
type afterDelete int

const (
	afterDeleteOK afterDelete = iota
	// afterDeleteRefused: a named refusal of a verb that ran after the delete
	// committed (the organization, the membership or the invite is gone, or
	// the verb's plan moved).
	afterDeleteRefused
	// afterDeleteOrgGone: the verb's write into go-api's memberships named the
	// organization row the delete removed (its foreign key check).
	afterDeleteOrgGone
	// afterDeleteOther: anything else, a failure of the race test.
	afterDeleteOther
)

func (a afterDelete) String() string {
	switch a {
	case afterDeleteOK:
		return "ok"
	case afterDeleteRefused:
		return "refused"
	case afterDeleteOrgGone:
		return "org-gone"
	default:
		return "other"
	}
}

func classifyAfterDelete(err error) afterDelete {
	if err == nil {
		return afterDeleteOK
	}
	for _, refusal := range []error{membership.ErrOrgNotFound, membership.ErrNotMember, membership.ErrStateMoved,
		invite.ErrInviteInvalid, invite.ErrOrgNotFound} {
		if errors.Is(err, refusal) {
			return afterDeleteRefused
		}
	}
	var pg *pgconn.PgError
	if errors.As(err, &pg) && pg.Code == "23503" && pg.ConstraintName == "memberships_org_id_fkey" {
		return afterDeleteOrgGone
	}
	return afterDeleteOther
}
