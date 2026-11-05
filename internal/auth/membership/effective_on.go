package membership

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// RowQueryer is a connection or transaction that reads one row.
type RowQueryer interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Effective is the effective membership of one (organization, principal)
// pair. Found is false when public.memberships holds no row for the pair.
type Effective struct {
	Status Status
	Role   string
	Found  bool
}

// EffectiveOn reads the pair's go-api row (public.memberships, unique on
// user_id and org_id) on q, the caller's transaction. public.memberships is the
// authority (D6651): a row with a role is an active member with that role; no
// row, or a row with a NULL role, is no membership. The auth.memberships copy is
// not read.
func EffectiveOn(ctx context.Context, q RowQueryer, orgID, principalID uuid.UUID) (Effective, error) {
	var role *string
	err := q.QueryRow(ctx, `SELECT role FROM public.memberships WHERE user_id = $1 AND org_id = $2`, principalID, orgID).Scan(&role)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return Effective{}, nil
	case err != nil:
		return Effective{}, fmt.Errorf("%w: read public membership: %w", ErrStore, err)
	case role == nil:
		return Effective{}, nil
	}
	return Effective{Status: StatusActive, Role: *role, Found: true}, nil
}
