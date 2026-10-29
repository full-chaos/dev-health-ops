package restorefence

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// ErrInitRefused reports that init did not write: the server is in recovery, or the
// timeline already has a fence row. Nothing was written.
var ErrInitRefused = errors.New("restorefence: init refused")

// InitHelp is the help text of the init verb.
const InitHelp = "writes the 'initial' fence row of the live timeline: only on a database that is NOT a restore (a restored database is fenced with apply, never init). -if-absent: compose/dev only; never on a restore"

// InitReport is what init wrote: the timeline only.
type InitReport struct {
	Timeline int64 `json:"timeline"`
	// AlreadyFenced is set only with IfAbsent: the timeline already had a row and nothing was written.
	AlreadyFenced bool `json:"already_fenced"`
}

var errInitExists = errors.New("restorefence: the timeline has a fence row")

func initExistsSQL(schema authschema.ValidatedIdentifier) string {
	return `SELECT EXISTS (SELECT 1 FROM ` + authschema.Quote(schema) + `.restore_fence_state WHERE timeline_id = $1)`
}

func initInsertSQL(schema authschema.ValidatedIdentifier) string {
	return `INSERT INTO ` + authschema.Quote(schema) + `.restore_fence_state (timeline_id, manifest_sha256, source) VALUES ($1, 'initial', 'init')`
}

// initDB is what the init transaction needs; an audit.TxOps satisfies it.
type initDB interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// initInTx refuses in recovery first, whatever else is true, then refuses an existing row, then writes.
func initInTx(ctx context.Context, db initDB, schema authschema.ValidatedIdentifier) (int64, error) {
	var recovery bool
	if err := db.QueryRow(ctx, `SELECT pg_is_in_recovery()`).Scan(&recovery); err != nil {
		return 0, fmt.Errorf("reading the recovery state")
	}
	if recovery {
		return 0, fmt.Errorf("%w: the server is in recovery", ErrInitRefused)
	}
	timeline, err := Timeline(ctx, db)
	if err != nil {
		return 0, fmt.Errorf("reading the timeline")
	}
	var exists bool
	if err := db.QueryRow(ctx, initExistsSQL(schema), timeline).Scan(&exists); err != nil {
		return timeline, fmt.Errorf("reading the fence state")
	}
	if exists {
		return timeline, errInitExists
	}
	if _, err := db.Exec(ctx, initInsertSQL(schema), timeline); err != nil {
		return timeline, fmt.Errorf("recording the initial row (sqlstate %s)", sqlState(err))
	}
	return timeline, nil
}

// Init writes the 'initial' fence row for the timeline the server is on, as the
// migration role, in one audit.Commit. It refuses on a server in recovery and when the
// timeline already has a row of any kind. With ifAbsent an existing row is not an error
// (a dev or compose bring-up repeats it); a server in recovery still is.
func Init(ctx context.Context, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier, ifAbsent bool) (InitReport, error) {
	var report InitReport
	err := audit.Commit(ctx, pool, schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			timeline, err := initInTx(ctx, tx, schema)
			report.Timeline = timeline
			return err
		},
		Audit:     audit.AuditEvent{EventType: "auth.restore_fence.initialized", Outcome: audit.OutcomeAllowed},
		OffStream: audit.OffStreamRestoreFence,
	})
	switch {
	case errors.Is(err, errInitExists) && ifAbsent:
		report.AlreadyFenced = true
		return report, nil
	case errors.Is(err, errInitExists):
		return InitReport{}, fmt.Errorf("%w: the timeline already has a fence row", ErrInitRefused)
	case err != nil:
		if errors.Is(err, ErrInitRefused) {
			return InitReport{}, err
		}
		return InitReport{}, fmt.Errorf("%w: %v", ErrApply, err)
	}
	return report, nil
}
