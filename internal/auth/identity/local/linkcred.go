package local

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// PGLinks is Links over auth.link_credentials.
type PGLinks struct {
	pool   *pgxpool.Pool
	schema authschema.ValidatedIdentifier
}

// NewPGLinks builds the store.
func NewPGLinks(pool *pgxpool.Pool, schema authschema.ValidatedIdentifier) *PGLinks {
	return &PGLinks{pool: pool, schema: schema}
}

func (l *PGLinks) Issue(ctx context.Context, req IssueRequest) error { return errNotImplemented }

func (l *PGLinks) Redeem(ctx context.Context, req RedeemRequest) (principalID uuid.UUID, ok bool, err error) {
	return principalID, false, errNotImplemented
}
