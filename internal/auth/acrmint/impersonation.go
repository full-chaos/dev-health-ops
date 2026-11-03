package acrmint

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Queryer is the one pool method PGImpersonation needs; *pgxpool.Pool has it.
type Queryer interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// DelegationReader is the delegated side of the actor check; *session.Store has
// it. It is true when session is delegated, or when principal is the real actor of
// a live delegated session. A session row that does not exist is an error.
type DelegationReader interface {
	ActorInDelegation(ctx context.Context, principal, session uuid.UUID) (bool, error)
}

// PGImpersonation is the one actor check (acp, CHAOS-12251 Q4). Three reads, all
// refuse, fail-closed, for both client classes:
//
//  1. public.impersonation_sessions, the table go-api's policy store reads
//     (internal/api/policy/store.go ActiveImpersonation): admin_user_id =
//     principal, not ended, not expired. Any such row refuses, whether or not the
//     target's users row still exists, so it is stricter than go-api's joined
//     read;
//  2. session is delegated: its principal is the target, so an actor check alone
//     would miss it;
//  3. principal is the real actor of a live delegated session: an actor with a
//     plain web session and a live delegation elsewhere.
//
// The target is not refused under either model. The auth-service role reads
// public.impersonation_sessions through authschema.PublicReadColumns.
type PGImpersonation struct {
	Pool Queryer
	// Sessions is the delegated side, reads 2 and 3. Nil is an error, never a pass.
	Sessions DelegationReader
	// Now is the clock; nil is time.Now.
	Now func() time.Time
}

// ImpersonatingActor implements Impersonation.
func (p PGImpersonation) ImpersonatingActor(ctx context.Context, principal, session uuid.UUID) (bool, error) {
	if p.Sessions == nil {
		return false, errors.New("acrmint: no delegated session reader")
	}
	now := time.Now
	if p.Now != nil {
		now = p.Now
	}
	var active bool
	if err := p.Pool.QueryRow(ctx, `
SELECT EXISTS (
  SELECT 1 FROM impersonation_sessions
  WHERE admin_user_id = $1 AND ended_at IS NULL AND expires_at > $2
)`, principal, now().UTC()).Scan(&active); err != nil {
		return false, fmt.Errorf("acrmint: read impersonation_sessions: %w", err)
	}
	if active {
		return true, nil
	}
	return p.Sessions.ActorInDelegation(ctx, principal, session)
}
