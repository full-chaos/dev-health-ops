package dimensionfold

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

// StaleAfter is how old a fold's due time may be before the job is dropped.
// The next occurrence folds the same tables, so a backlog that built up while
// no worker served the queue collapses to its newest runs instead of
// replaying every missed interval.
const StaleAfter = 10 * time.Minute

// Runner is the fold a job executes.
type Runner interface {
	Run(context.Context) ([]TableReport, error)
}

// Handler binds one scheduled fold to the River job runtime.
type Handler struct {
	runner Runner
	logger *slog.Logger
	now    func() time.Time
}

// NewHandler constructs the system.dimension_fold job owner.
func NewHandler(runner Runner, logger *slog.Logger) (*Handler, error) {
	if runner == nil || logger == nil {
		return nil, errors.New("dimension fold runner and logger are required")
	}
	return &Handler{runner: runner, logger: logger, now: time.Now}, nil
}

// Work runs one fold. A table that fails is reported by the fold and does not
// fail the job: the next occurrence retries it, and failing the whole job
// would re-run the tables that succeeded.
func (handler *Handler) Work(
	ctx context.Context,
	execution *jobruntime.Execution[jobruntime.DimensionFoldArgs],
) error {
	if handler == nil || handler.runner == nil || execution == nil {
		return jobruntime.Permanent(errors.New("dimension fold handler is not configured"))
	}
	scheduledFor, err := time.Parse(time.RFC3339, execution.Args.Payload.ScheduledFor)
	if err != nil || scheduledFor.Location() != time.UTC {
		return jobruntime.Permanent(errors.New("dimension fold request is invalid"))
	}
	if age := handler.now().UTC().Sub(scheduledFor); age > StaleAfter {
		handler.logger.InfoContext(ctx, "dimension_fold.run",
			slog.String("outcome", "skipped_stale"),
			slog.String("scheduled_for", scheduledFor.Format(time.RFC3339)),
			slog.Int64("age_seconds", int64(age.Seconds())))
		return nil
	}
	if _, err := handler.runner.Run(ctx); err != nil {
		return jobruntime.Retryable(err)
	}
	return nil
}

// ClickHouseConn adapts a clickhouse-go connection to Conn.
type ClickHouseConn struct{ Conn driver.Conn }

func (conn ClickHouseConn) Exec(ctx context.Context, query string, args ...any) error {
	return conn.Conn.Exec(ctx, query, args...)
}

func (conn ClickHouseConn) QueryRow(ctx context.Context, query string, args ...any) Row {
	return conn.Conn.QueryRow(ctx, query, args...)
}
