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
const InitHelp = "writes the 'initial' fence row of the live timeline: only on a database that is NOT a restore (a restored database is fenced with apply, never init). On a timeline above 1 it refuses unless -timeline N names the live timeline. -if-absent: compose/dev only; never on a restore; never with -timeline"

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

// InitOptions set one init.
type InitOptions struct {
	// IfAbsent is the dev and compose path: an existing row is not an error.
	IfAbsent bool
	// Timeline is the operator's confirmation of the live timeline (0 = none named). A database on a timeline
	// above 1 has been promoted at least once, which is what a restore looks like: init writes there only when the
	// operator names that exact timeline. IfAbsent never names one.
	Timeline int64
}

func (o InitOptions) validate() error {
	if o.Timeline < 0 {
		return fmt.Errorf("%w: the timeline must be positive", ErrInitRefused)
	}
	if o.IfAbsent && o.Timeline != 0 {
		return fmt.Errorf("%w: -if-absent never names a timeline", ErrInitRefused)
	}
	return nil
}

// initInTx refuses in recovery first, whatever else is true, then a timeline above 1 that the operator did not name,
// then an existing row, then writes.
func initInTx(ctx context.Context, db initDB, schema authschema.ValidatedIdentifier, named int64) (int64, error) {
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
	if named != 0 && named != timeline {
		return timeline, fmt.Errorf("%w: the named timeline is not the live one", ErrInitRefused)
	}
	if timeline > 1 && named != timeline {
		return timeline, fmt.Errorf("%w: the live timeline is %d: a database that was promoted may be a restore; fence it with apply, or name the timeline with -timeline %d only if it is not a restore", ErrInitRefused, timeline, timeline)
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
func Init(ctx context.Context, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier, opts InitOptions) (InitReport, error) {
	if err := opts.validate(); err != nil {
		return InitReport{}, err
	}
	ifAbsent := opts.IfAbsent
	var report InitReport
	err := audit.Commit(ctx, pool, schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			timeline, err := initInTx(ctx, tx, schema, opts.Timeline)
			report.Timeline = timeline
			return err
		},
		Audit:     audit.AuditEvent{EventType: "auth.restore_fence.initialized", Outcome: audit.OutcomeAllowed},
		OffStream: audit.OffStreamRestoreFence,
		Refusals:  initRefusals(),
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

func initRefusals() []error { return []error{errInitExists} }
