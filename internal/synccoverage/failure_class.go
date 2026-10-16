package synccoverage

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
)

// FailureClass names why a configuration's rebuild failed using only static
// words and a PostgreSQL SQLSTATE code. It never returns err's message, which
// can embed stored processor_flags values.
func FailureClass(err error) string {
	var pgErr *pgconn.PgError
	switch {
	case err == nil:
		return "none"
	case errors.Is(err, ErrInvalidProcessorFlags):
		return "invalid_processor_flags"
	case errors.Is(err, ErrConfigNotFound):
		return "config_not_found"
	case errors.Is(err, ErrPoolUnavailable):
		return "pool_unavailable"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, context.Canceled):
		return "context_canceled"
	case errors.As(err, &pgErr):
		return "postgres_" + pgErr.Code
	default:
		return "other"
	}
}
