// Package schemagate refuses to start a runtime process whose database schema is behind
// the build: the migration head the database records must not lack a revision the
// binary needs. A database ahead of the build (a rollback) starts with a WARN, and a
// read that cannot happen (unreachable, no grant, no table) is skipped with a WARN, so
// the gate adds no failure mode beyond the head compare itself.
package schemagate

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
)

const readTimeout = 10 * time.Second

// Recorder is the optional capability of a process database that can read the revisions
// public.alembic_version records.
type Recorder interface {
	RecordedRevisions(ctx context.Context) ([]string, error)
}

// BehindError is the refusal for a database behind the build. Its text holds revision ids only.
type BehindError struct {
	Recorded   []string
	BuildHeads []string
}

func (e *BehindError) Error() string {
	return fmt.Sprintf("the database schema is behind this build: alembic_version holds %v, the build needs %v", e.Recorded, e.BuildHeads)
}

// ReadError is a failed read of alembic_version, reduced to a class: it never carries the driver text.
type ReadError struct{ Class string }

func (e *ReadError) Error() string { return "read alembic_version: " + e.Class }

// PoolRecorded reads the recorded revisions through pool.
func PoolRecorded(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
	if pool == nil {
		return nil, &ReadError{Class: "no_pool"}
	}
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	rows, err := pool.Query(ctx, "SELECT version_num FROM public.alembic_version")
	if err != nil {
		return nil, &ReadError{Class: readClass(err)}
	}
	revisions, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, &ReadError{Class: readClass(err)}
	}
	sort.Strings(revisions)
	return revisions, nil
}

func readClass(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "42501":
			return "permission_denied"
		case "42P01":
			return "table_missing"
		}
		return "database_error"
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return "timeout"
	}
	return "unreachable"
}

// Enforce applies the gate to a process database. A database that cannot read its
// revisions (a test fake, no Recorder) is not gated. It returns a *BehindError when the
// database is behind this build, and nil in every other case.
func Enforce(ctx context.Context, logger *slog.Logger, process string, database any) error {
	recorder, ok := database.(Recorder)
	if !ok {
		return nil
	}
	if logger == nil {
		logger = slog.Default()
	}
	build, err := pgmigrate.LoadBuildHeads()
	if err != nil {
		logger.WarnContext(ctx, "schema gate skipped", "process", process, "class", "build_heads_unavailable")
		return nil
	}
	return Check(ctx, logger, process, recorder, build)
}

// Check compares the recorded revisions with build.
func Check(ctx context.Context, logger *slog.Logger, process string, recorder Recorder, build pgmigrate.BuildHeads) error {
	recorded, err := recorder.RecordedRevisions(ctx)
	if err != nil {
		class := "unreachable"
		var read *ReadError
		if errors.As(err, &read) {
			class = read.Class
		}
		logger.WarnContext(ctx, "schema gate skipped", "process", process, "class", class)
		return nil
	}
	if len(recorded) == 0 {
		logger.WarnContext(ctx, "schema gate skipped", "process", process, "class", "no_revisions_recorded")
		return nil
	}
	switch build.CompareHeads(recorded) {
	case pgmigrate.HeadsBehind:
		refusal := &BehindError{Recorded: recorded, BuildHeads: build.Heads()}
		logger.ErrorContext(ctx, "schema gate: refusing to start, the database schema is behind this build; run the migration",
			"process", process, "database_heads", refusal.Recorded, "build_heads", refusal.BuildHeads)
		return refusal
	case pgmigrate.HeadsAhead:
		logger.WarnContext(ctx, "schema gate: the database schema is ahead of this build (rollback?); starting",
			"process", process, "database_heads", recorded, "build_heads", build.Heads())
	}
	return nil
}
