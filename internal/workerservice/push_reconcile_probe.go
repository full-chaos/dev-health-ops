package workerservice

import (
	"context"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"

	"github.com/full-chaos/dev-health-ops/internal/auth/pushreconcile/reconcilejob"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// startPushReconcileProbe starts the External Push projection drift probe (CHAOS-10897) when
// PUSH_RECONCILE_PROBE=on; it is off by default and then registers nothing. The probe reads through the worker's
// domain login: it holds the token table and the projection reader columns, and it writes nothing. Observability
// never stops the worker, so a failure is logged and the worker runs without the probe. It returns the stop func
// of the probe, or nil.
func startPushReconcileProbe(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger) func() error {
	lookup := os.LookupEnv
	if !reconcilejob.Enabled(lookup) || pool == nil {
		return nil
	}
	schema := "auth"
	if v, ok := lookup("AUTH_DATABASE_SCHEMA"); ok && v != "" {
		schema = v
	}
	ident, err := authschema.NewValidatedIdentifier(schema)
	if err != nil {
		logger.Warn("push reconcile probe not started: the auth schema name is not valid")
		return nil
	}
	probe, err := reconcilejob.MaybeStart(ctx, lookup,
		otel.GetMeterProvider().Meter("github.com/full-chaos/dev-health-ops/internal/auth/pushreconcile/reconcilejob"),
		reconcilejob.NewPass(pool, ident, reconcilejob.DefaultBatch, nil), logger)
	if err != nil {
		logger.Warn("push reconcile probe not registered", "error", err)
		return nil
	}
	if probe == nil {
		return nil
	}
	return probe.Stop
}
