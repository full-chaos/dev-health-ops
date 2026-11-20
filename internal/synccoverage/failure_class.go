package synccoverage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// FailureClass names why a configuration's rebuild failed using only static
// words and a PostgreSQL SQLSTATE code. It never returns err's message, which
// can embed stored processor_flags values. "other" is left only for an error
// none of the coarse classes below names.
func FailureClass(err error) string {
	var pgErr *pgconn.PgError
	var limit *LimitError
	var connectErr *pgconn.ConnectError
	var netErr net.Error
	var scanErr pgx.ScanArgError
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError
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
	case errors.As(err, &connectErr), errors.As(err, &netErr), errors.Is(err, net.ErrClosed),
		errors.Is(err, pgconn.ErrConnClosed), errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return "connection"
	case errors.Is(err, errBeginRebuild):
		return "pool_acquire"
	case errors.As(err, &scanErr), errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return "decode"
	case errors.Is(err, errEncodeProjection):
		return "encode"
	default:
		return "other"
	}
}

// IsDeterministicFailure reports whether a rebuild failure repeats on every retry: ErrInvalidProcessorFlags,
// ErrConfigNotFound or a *LimitError. An errors.Join (or multi-%w) node counts only when every branch does, so one
// transient cause keeps the failure retryable. nil and every other error are not deterministic.
func IsDeterministicFailure(err error) bool {
	switch err := err.(type) {
	case nil:
		return false
	case *LimitError:
		return err != nil
	}
	if err == ErrInvalidProcessorFlags || err == ErrConfigNotFound {
		return true
	}
	switch node := err.(type) {
	case interface{ Unwrap() []error }:
		branches := node.Unwrap()
		for _, branch := range branches {
			if !IsDeterministicFailure(branch) {
				return false
			}
		}
		return len(branches) > 0
	case interface{ Unwrap() error }:
		return IsDeterministicFailure(node.Unwrap())
	}
	return false
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
