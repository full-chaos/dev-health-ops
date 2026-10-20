package oidc

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PGMemberships reads public.memberships (user_id, org_id, role). It never
// writes: memberships stay a go-api aggregate until Wave 4.
type PGMemberships struct{ Pool *pgxpool.Pool }

func (m PGMemberships) Role(ctx context.Context, principalID, orgID uuid.UUID) (string, bool, error) {
	var role *string
	err := m.Pool.QueryRow(ctx, `SELECT role FROM public.memberships WHERE user_id = $1 AND org_id = $2`,
		principalID, orgID).Scan(&role)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	if role == nil {
		return "member", true, nil
	}
	return *role, true, nil
}
