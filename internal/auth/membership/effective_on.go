package membership

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// RowQueryer is a connection or transaction that reads one row.
type RowQueryer interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Effective is the effective membership of one (organization, principal)
// pair. Found is false when neither table holds a row for the pair.
type Effective struct {
	Status Status
	Role   string
	Found  bool
}

// EffectiveOn reads the pair's go-api row (public.memberships, unique on
// user_id and org_id) and its auth row on q, the caller's transaction, and
// applies effective(). A public row with a NULL role is no public row to
// effective(): the auth row decides, and an active auth row reads as removed.
func EffectiveOn(ctx context.Context, q RowQueryer, schema authschema.ValidatedIdentifier, orgID, principalID uuid.UUID) (Effective, error) {
	var publicRole *string
	err := q.QueryRow(ctx, `SELECT role FROM public.memberships WHERE user_id = $1 AND org_id = $2`, principalID, orgID).Scan(&publicRole)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return Effective{}, fmt.Errorf("%w: read public membership: %w", ErrStore, err)
	}
	var (
		authStatus *Status
		authRole   *string
		status     string
	)
	err = q.QueryRow(ctx, `SELECT status, role FROM `+authschema.Quote(schema)+`.memberships
WHERE organization_id = $1 AND principal_id = $2`, orgID, principalID).Scan(&status, &authRole)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
	case err != nil:
		return Effective{}, fmt.Errorf("%w: read auth membership: %w", ErrStore, err)
	default:
		s := Status(status)
		authStatus = &s
	}
	cur := effective(publicRole, authStatus, authRole)
	return Effective{Status: cur.status, Role: cur.role, Found: cur.status != statusAbsent}, nil
}
