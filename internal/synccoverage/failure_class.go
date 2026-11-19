package synccoverage

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// FailureClass names why a configuration's rebuild failed using only static
// words and a PostgreSQL SQLSTATE code. It never returns err's message, which
// can embed stored processor_flags values.
func FailureClass(err error) string {
	var pgErr *pgconn.PgError
	var limit *LimitError
	switch {
	case err == nil:
		return "none"
	case errors.Is(err, ErrInvalidProcessorFlags):
		return "invalid_processor_flags"
	case errors.Is(err, ErrConfigNotFound):
		return "config_not_found"
	case errors.Is(err, ErrPoolUnavailable):
		return "pool_unavailable"
	case errors.As(err, &limit):
		return limit.Bound + "_limit"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, context.Canceled):
		return "context_canceled"
	case errors.As(err, &pgErr) && pgErr != nil:
		return "postgres_" + pgErr.Code
	default:
		return "other"
	}
}

// LimitError is a projection larger than one of the projector's bounds (maxCompactWindows and the others). Its text
// holds only the bound's name and two counts, so it is safe to log, unlike the other rebuild errors.
type LimitError struct {
	Bound string
	Count int
	Max   int
}

func newLimitError(bound string, count, max int) *LimitError {
	return &LimitError{Bound: bound, Count: count, Max: max}
}

func (limit *LimitError) Error() string {
	return fmt.Sprintf("sync coverage %s limit exceeded: %d (limit %d)", strings.ReplaceAll(limit.Bound, "_", " "), limit.Count, limit.Max)
}

// FailureDetail is the loggable text of a rebuild failure: a LimitError's text, else "".
func FailureDetail(err error) string {
	var limit *LimitError
	if errors.As(err, &limit) {
		return limit.Error()
	}
	return ""
}
