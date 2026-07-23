package jobroute

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresRiverQuiescer struct {
	pool  *pgxpool.Pool
	table string
}

func NewPostgresRiverQuiescer(pool *pgxpool.Pool, schema string) (*PostgresRiverQuiescer, error) {
	if pool == nil || len(schema) == 0 || len(schema) > 63 {
		return nil, ErrInvalidConfiguration
	}
	table := pgx.Identifier{schema, "river_job"}.Sanitize()
	if table == "" {
		return nil, ErrInvalidConfiguration
	}
	return &PostgresRiverQuiescer{pool: pool, table: table}, nil
}

func (quiescer *PostgresRiverQuiescer) Quiesce(ctx context.Context, kind string) error {
	if quiescer == nil || quiescer.pool == nil || kind == "" {
		return ErrInvalidConfiguration
	}
	var count int64
	err := quiescer.pool.QueryRow(ctx, `
		SELECT count(*) FROM `+quiescer.table+`
		WHERE kind = $1 AND state IN ('available', 'pending', 'retryable', 'running', 'scheduled')`,
		kind,
	).Scan(&count)
	if err != nil {
		return ErrUnavailable
	}
	if count != 0 {
		return ErrLiveClaims
	}
	return nil
}

var _ Quiescer = (*PostgresRiverQuiescer)(nil)

func IsPrecondition(err error) bool {
	return errors.Is(err, ErrDrift) || errors.Is(err, ErrPaused) ||
		errors.Is(err, ErrLiveClaims) || errors.Is(err, ErrPendingOutbox) ||
		errors.Is(err, ErrCeleryQuiescenceMissing) || errors.Is(err, ErrUnknownRoute)
}
