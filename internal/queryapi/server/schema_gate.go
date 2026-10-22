package server

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate/schemagate"
)

// queryAPISchemaRecorder reads alembic_version through the registry pool (the query-api role holds SELECT on it); a test swaps it.
var queryAPISchemaRecorder = func(pool *pgxpool.Pool) schemagate.Recorder { return poolRecorder{pool: pool} }

type poolRecorder struct{ pool *pgxpool.Pool }

func (recorder poolRecorder) RecordedRevisions(ctx context.Context) ([]string, error) {
	return schemagate.PoolRecorded(ctx, recorder.pool)
}

// enforceQueryAPISchemaGate refuses a database schema behind this build; the gate logs its own line.
func enforceQueryAPISchemaGate(ctx context.Context, logger *slog.Logger, pool *pgxpool.Pool) error {
	if err := schemagate.Enforce(ctx, logger, "query-api", queryAPISchemaRecorder(pool)); err != nil {
		return fmt.Errorf("query-api: the database schema is behind this build: %w", err)
	}
	return nil
}
