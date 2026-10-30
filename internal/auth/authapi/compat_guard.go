package authapi

import (
	"context"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// GuardedSessions is Sessions plus the exchange that refuses before it rotates
// (*session.Store).
type GuardedSessions interface {
	Sessions
	ExchangeGuarded(ctx context.Context, credential contracts.SecretString, guard session.Guard) (session.Result, error)
}

// userStateOn is a Users that can read on the exchange's own connection.
type userStateOn interface {
	StateOn(ctx context.Context, q session.Queryer, userID uuid.UUID) (UserState, bool, error)
}

// refreshGuard refuses a refresh whose user is missing or inactive, whose
// session pin is missing or stale, or whose organization membership is gone
// (a non-superuser), before the credential is rotated. The
// handler then answers and revokes exactly as it does after an exchange.
func (h compatHandlers) refreshGuard(ctx context.Context, q session.Queryer, record session.Record) error {
	var (
		user  UserState
		found bool
		err   error
	)
	if on, ok := h.Users.(userStateOn); ok {
		user, found, err = on.StateOn(ctx, q, record.PrincipalID)
	} else {
		user, found, err = h.Users.State(ctx, record.PrincipalID)
	}
	if err != nil {
		return err
	}
	if !found || !user.IsActive || checkPin(ctx, h.rec.logger, record, user) != pinCurrent {
		return session.ErrGuardRefused
	}
	if record.OrganizationID != nil && !user.IsSuperuser {
		// A session whose organization membership is gone (D6507) is not
		// rotated; the handler refuses and revokes it.
		_, member, err := h.Users.Role(ctx, record.PrincipalID, *record.OrganizationID)
		if err != nil {
			return err
		}
		if !member {
			return session.ErrGuardRefused
		}
	}
	return nil
}
