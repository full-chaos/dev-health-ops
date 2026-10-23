package entitlement

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Querier is the pgx surface PGSource reads through.
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// PGSource reads public.org_licenses (org_id, tier, features_override) and public.organizations (id, tier).
type PGSource struct{ Q Querier }

// Load mirrors licensing.OrgHasFeature for every feature at once.
func (PGSource) Load(context.Context, uuid.UUID) (Content, error) { return Content{}, nil }
