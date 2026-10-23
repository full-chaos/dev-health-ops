package oidc

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// PGMemberships reads public.memberships (user_id, org_id, role). It never
// writes: memberships stay a go-api aggregate until Wave 4.
type PGMemberships struct {
	Pool *pgxpool.Pool
	// UsersOwned is the users-cutover mode: an active membership in Schema
	// (the auth schema) is read first and wins on role; otherwise only
	// public.memberships is read.
	UsersOwned bool
	Schema     authschema.ValidatedIdentifier
}

func (m PGMemberships) Role(ctx context.Context, principalID, orgID uuid.UUID) (string, bool, error) {
	if m.UsersOwned {
		var role *string
		err := m.Pool.QueryRow(ctx, `SELECT role FROM `+authschema.Quote(m.Schema)+`.memberships
WHERE principal_id = $1 AND organization_id = $2 AND status = 'active'`, principalID, orgID).Scan(&role)
		switch {
		case err == nil && role != nil:
			return *role, true, nil
		case err == nil:
			return "member", true, nil
		case !errors.Is(err, pgx.ErrNoRows):
			return "", false, err
		}
	}
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
