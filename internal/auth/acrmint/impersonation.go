package acrmint

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Queryer is the one pool method PGImpersonation needs; *pgxpool.Pool has it.
type Queryer interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// PGImpersonation reads public.impersonation_sessions, the table go-api's
// policy store reads (internal/api/policy/store.go ActiveImpersonation). It is
// the actor side only: admin_user_id = principal, not ended, not expired. Any
// such row refuses the mint, whether or not the target's users row still
// exists, so it is stricter than go-api's joined read.
//
// No production binary constructs it in O2. O3 wires it and owns the
// auth-service role's SELECT grant on the table; this package adds no grant.
type PGImpersonation struct {
	Pool Queryer
	// Now is the clock; nil is time.Now.
	Now func() time.Time
}

// ImpersonatingActor implements Impersonation.
func (p PGImpersonation) ImpersonatingActor(ctx context.Context, principal uuid.UUID) (bool, error) {
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
	return active, nil
}
