package audit

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgconn"
)

// DBError is the log attribute for an error that may come from the database.
// It never carries error text: a PostgreSQL message can quote an input value
// (22P02 quotes the text that failed to parse) and a driver error can quote a
// bind argument, and no redactor recognises every such value. A PostgreSQL
// error becomes its SQLSTATE, SQLSTATE class and constraint name; any other
// error becomes a kind: the context error, or the Go type of the innermost
// error in the chain.
func DBError(err error) slog.Attr {
	var pgErr *pgconn.PgError
	switch {
	case err == nil:
		return slog.Group("error")
	case errors.As(err, &pgErr):
		attrs := []any{slog.String("sqlstate", pgErr.Code)}
		if len(pgErr.Code) >= 2 {
			attrs = append(attrs, slog.String("class", pgErr.Code[:2]))
		}
		if pgErr.ConstraintName != "" {
			attrs = append(attrs, slog.String("constraint", pgErr.ConstraintName))
		}
		return slog.Group("error", attrs...)
	case errors.Is(err, context.Canceled):
		return slog.Group("error", slog.String("kind", "context_canceled"))
	case errors.Is(err, context.DeadlineExceeded):
		return slog.Group("error", slog.String("kind", "context_deadline_exceeded"))
	}
	return slog.Group("error", slog.String("kind", fmt.Sprintf("%T", innermost(err))))
}

func innermost(err error) error {
	for {
		var next error
		switch u := err.(type) {
		case interface{ Unwrap() error }:
			next = u.Unwrap()
		case interface{ Unwrap() []error }:
			if list := u.Unwrap(); len(list) > 0 {
				next = list[0]
			}
		}
		if next == nil {
			return err
		}
		err = next
	}
}
