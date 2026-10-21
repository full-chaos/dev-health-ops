package oidc

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

const protocolOIDC = "oidc"

// Flow is one started login flow, as the flow store keeps it: a hash of the
// sealed state's id, never the state or its secrets.
type Flow struct {
	Hash       [32]byte
	Protocol   string
	ProviderID uuid.UUID
	ExpiresAt  time.Time
}

// FlowStore makes a flow single-use. Begin records a started flow; Consume
// marks it used and reports true exactly once, and only before ExpiresAt and
// for the same protocol and provider.
type FlowStore interface {
	Begin(ctx context.Context, flow Flow) error
	Consume(ctx context.Context, hash [32]byte, protocol string, providerID uuid.UUID, now time.Time) (bool, error)
}

// PGFlowStore keeps flows in the auth schema's sso_flow_states table.
type PGFlowStore struct {
	pool *pgxpool.Pool
	q    string
}

// NewPGFlowStore builds a flow store over schema.
func NewPGFlowStore(pool *pgxpool.Pool, schema authschema.ValidatedIdentifier) (*PGFlowStore, error) {
	if pool == nil {
		return nil, errors.New("oidc: no connection pool")
	}
	if schema.String() == "" {
		return nil, errors.New("oidc: no auth schema")
	}
	return &PGFlowStore{pool: pool, q: authschema.Quote(schema)}, nil
}

func (s *PGFlowStore) Begin(ctx context.Context, flow Flow) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO `+s.q+`.sso_flow_states (flow_hash, protocol, provider_id, expires_at)
VALUES ($1, $2, $3, $4)`, flow.Hash[:], flow.Protocol, flow.ProviderID, flow.ExpiresAt.UTC())
	return err
}

// Consume is one conditional UPDATE: a replay, an expired flow or a flow of
// another provider matches no row.
func (s *PGFlowStore) Consume(ctx context.Context, hash [32]byte, protocol string, providerID uuid.UUID, now time.Time) (bool, error) {
	tag, err := s.pool.Exec(ctx, `UPDATE `+s.q+`.sso_flow_states SET consumed_at = $4
WHERE flow_hash = $1 AND protocol = $2 AND provider_id = $3 AND consumed_at IS NULL AND expires_at > $4`,
		hash[:], protocol, providerID, now.UTC())
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}
