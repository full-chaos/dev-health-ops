package pgmigrate

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// lockKey serializes concurrent `dho migrate postgres` runs against one
// database: every run takes this transaction-scoped advisory lock before it
// reads the state it acts on.
const lockKey int64 = 0x64686f5f7067 // "dho_pg"

// Result is what one upgrade did.
type Result struct {
	Action  string   `json:"action"`
	Heads   []string `json:"heads"`
	Applied []string `json:"applied,omitempty"`
}

// Upgrade brings the database behind conn to the head of baseline, then
// applies every chain revision after it.
func Upgrade(ctx context.Context, conn *pgx.Conn, baseline Baseline, chain []ChainFile) (Result, error) {
	return UpgradeLogged(ctx, conn, baseline, chain, slog.New(slog.DiscardHandler))
}

// embeddedKnown is the revisions this build knows: its embedded Alembic walk, its
// baseline heads and its chain.
func embeddedKnown(baseline Baseline, chain []ChainFile) (map[string]bool, error) {
	history, err := LoadHistory()
	if err != nil {
		return nil, err
	}
	return KnownRevisions(history, baseline, chain), nil
}

// UpgradeLogged is Upgrade with a logger for the one event the command's JSON
// result cannot show: a chain step that failed and was recovered because another
// migrator had recorded its revision.
func UpgradeLogged(ctx context.Context, conn *pgx.Conn, baseline Baseline, chain []ChainFile, logger *slog.Logger) (Result, error) {
	known, err := embeddedKnown(baseline, chain)
	if err != nil {
		return Result{Heads: Heads(baseline, chain)}, err
	}
	return upgrade(ctx, conn, baseline, chain, known, logger)
}

// UpgradeWithHistory is UpgradeLogged for a build whose Alembic walk is history
// rather than the embedded one: an older build (an image rolled back) knows fewer
// revisions than this one.
func UpgradeWithHistory(ctx context.Context, conn *pgx.Conn, baseline Baseline, chain []ChainFile, history []HistoryEntry, logger *slog.Logger) (Result, error) {
	return upgrade(ctx, conn, baseline, chain, KnownRevisions(history, baseline, chain), logger)
}

// upgrade brings the database to the head in ONE transaction: the advisory lock,
// the baseline of an empty database and every pending chain revision, each recorded
// in alembic_version, commit together or not at all. That is Python's Alembic walk
// (alembic/env.py: one `begin_transaction()` around `run_migrations()`): a failing
// revision rolls the whole walk back to the head the run started from (CHAOS-7291).
//
// One recovery stays: another migrator that takes no dho lock (the Python Alembic
// upgrade) may commit a revision after this run planned and before this run's SQL
// for it ran, so the SQL fails although the database is where this run wants it. The
// failed walk is rolled back whole; when the revision it was applying is now
// recorded, the walk is planned again under the lock, once.
func upgrade(ctx context.Context, conn *pgx.Conn, baseline Baseline, chain []ChainFile, known map[string]bool, logger *slog.Logger) (Result, error) {
	result := Result{Heads: Heads(baseline, chain)}
	done, err := walkWithRetry(
		func() (walked, error) { return walkOnce(ctx, conn, baseline, chain, known) },
		func(revision string) bool { return revisionRecordedSince(ctx, conn, baseline, chain, known, revision) },
		logger)
	if err != nil {
		return result, err
	}
	result.Action, result.Applied = done.action, done.applied
	return result, nil
}

// walkWithRetry runs one walk and, when it failed while applying a revision another
// migrator has since recorded, plans and runs it once more. Any other failure, and a
// second failure, is the run's.
func walkWithRetry(walk func() (walked, error), recorded func(revision string) bool, logger *slog.Logger) (walked, error) {
	for attempt := 0; ; attempt++ {
		done, err := walk()
		if err == nil {
			return done, nil
		}
		if attempt == 0 && done.attempted != "" && recorded(done.attempted) {
			// Not silent: a run that recovers reports success, so this line is the only
			// trace that another migrator applied the revision under it (and that the two
			// migrators ran together, against the runbook).
			logger.Info("migrate chain walk planned again: another migrator recorded the revision",
				"revision", done.attempted, "sqlstate", sqlState(err))
			continue
		}
		return done, err
	}
}

// walked is what one walk did: the action, the revisions it applied, and the revision
// it was applying when it failed ("" when it failed before it chose one).
type walked struct {
	action    string
	applied   []string
	attempted string
}

// walkOnce is one transaction: plan under the lock, apply the baseline of an empty
// database, apply every pending chain revision in order.
func walkOnce(ctx context.Context, conn *pgx.Conn, baseline Baseline, chain []ChainFile, known map[string]bool) (walked, error) {
	var done walked
	err := inTransaction(ctx, conn, func(tx pgx.Tx) error {
		done = walked{}
		observation, err := observe(ctx, tx)
		if err != nil {
			return err
		}
		plan := Decide(observation, baseline, chain, known)
		switch plan.State {
		case StateAheadOfBuild:
			return AheadOfBuildError{Recorded: observation.Versions, Unknown: plan.Unknown}
		case StateForeign:
			return ForeignDatabaseError{Objects: observation.Objects}
		case StateSchemaMismatch:
			return SchemaMismatchError{Recorded: observation.Versions, MissingTables: plan.MissingTables}
		case StateBelowHead:
			return BelowHeadError{Recorded: observation.Versions, Missing: plan.Missing}
		case StateEmpty:
			if _, err := tx.Exec(ctx, baseline.Schema); err != nil {
				return fmt.Errorf("apply the baseline schema: %w", err)
			}
			if _, err := tx.Exec(ctx, baseline.Data); err != nil {
				return fmt.Errorf("apply the baseline data: %w", err)
			}
			// pg_dump's preamble empties search_path for the whole session;
			// put it back so the chain revisions below run as they were written.
			if _, err := tx.Exec(ctx, "RESET search_path"); err != nil {
				return err
			}
			// The data dump carries alembic_version's rows; check they are
			// exactly the heads, so the database records what was applied.
			after, err := observe(ctx, tx)
			if err != nil {
				return err
			}
			if !sameSet(after.Versions, baseline.Heads) {
				return fmt.Errorf("the baseline recorded alembic_version %v, want %v", after.Versions, baseline.Heads)
			}
			// The chain continues the baseline in this same transaction.
			plan = Decide(after, baseline, chain, known)
			if plan.State != StateAtHead {
				return fmt.Errorf("the database is not at the baseline head after the baseline was applied (state %d, alembic_version %v)", plan.State, after.Versions)
			}
			done.action = "baseline_applied"
		default:
			done.action = "up_to_date"
		}
		previous := plan.ApplicationHead
		for _, file := range plan.Pending {
			done.attempted = file.Revision
			if err := applyChainFile(ctx, tx, file, previous); err != nil {
				return err
			}
			previous = file.Revision
			done.applied = append(done.applied, file.Revision)
		}
		done.attempted = ""
		if done.action == "up_to_date" && len(done.applied) > 0 {
			done.action = "chain_applied"
		}
		return nil
	})
	if err != nil {
		// A rolled-back walk applied nothing.
		done.applied = nil
	}
	return done, err
}

// applyChainFile runs one revision and moves the application head it
// continues to it, in the caller's transaction.
func applyChainFile(ctx context.Context, tx pgx.Tx, file ChainFile, previous string) error {
	if _, err := tx.Exec(ctx, file.SQL); err != nil {
		return fmt.Errorf("%s: %w", file.Name, err)
	}
	tag, err := tx.Exec(ctx, "UPDATE alembic_version SET version_num = $1 WHERE version_num = $2", file.Revision, previous)
	if err != nil {
		return fmt.Errorf("%s: record the revision: %w", file.Name, err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%s: alembic_version does not hold %s, the revision it continues", file.Name, previous)
	}
	return nil
}

// Status is the read-only view of a database.
type Status struct {
	State    string   `json:"state"`
	Heads    []string `json:"heads"`
	Recorded []string `json:"recorded"`
	Missing  []string `json:"missing,omitempty"`
	// Unknown are the recorded revisions this build does not know (ahead_of_build).
	Unknown []string `json:"unknown,omitempty"`
	// MissingTables are the baseline tables a schema_mismatch database lacks.
	MissingTables []string `json:"missing_tables,omitempty"`
	Pending       []string `json:"pending,omitempty"`
}

// ReadStatus reports where the database stands without changing it.
func ReadStatus(ctx context.Context, conn *pgx.Conn, baseline Baseline, chain []ChainFile) (Status, error) {
	known, err := embeddedKnown(baseline, chain)
	if err != nil {
		return Status{Heads: Heads(baseline, chain)}, err
	}
	return readStatus(ctx, conn, baseline, chain, known)
}

// ReadStatusWithHistory is ReadStatus for a build whose Alembic walk is history.
func ReadStatusWithHistory(ctx context.Context, conn *pgx.Conn, baseline Baseline, chain []ChainFile, history []HistoryEntry) (Status, error) {
	return readStatus(ctx, conn, baseline, chain, KnownRevisions(history, baseline, chain))
}

func readStatus(ctx context.Context, conn *pgx.Conn, baseline Baseline, chain []ChainFile, known map[string]bool) (Status, error) {
	status := Status{Heads: Heads(baseline, chain)}
	err := readOnlyTransaction(ctx, conn, func(tx pgx.Tx) error {
		observation, err := observe(ctx, tx)
		if err != nil {
			return err
		}
		status.Recorded = observation.Versions
		plan := Decide(observation, baseline, chain, known)
		status.Missing = plan.Missing
		status.Unknown = plan.Unknown
		status.MissingTables = plan.MissingTables
		for _, file := range plan.Pending {
			status.Pending = append(status.Pending, file.Revision)
		}
		status.State = map[State]string{StateEmpty: "empty", StateAtHead: "at_head", StateBelowHead: "below_head", StateForeign: "foreign", StateSchemaMismatch: "schema_mismatch", StateAheadOfBuild: "ahead_of_build"}[plan.State]
		return nil
	})
	return status, err
}

// userObject is the WHERE clause selecting the rows of catalog (aliased o,
// its namespace aliased n) that are not in a system schema and not owned by
// an extension.
func userObject(catalog string) string {
	return "n.nspname NOT IN ('pg_catalog', 'information_schema') AND n.nspname NOT LIKE 'pg\\_toast%' " +
		"AND n.nspname NOT LIKE 'pg\\_temp\\_%' AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid = '" + catalog +
		"'::regclass AND d.objid = o.oid AND d.deptype = 'e')"
}

// observe reads the state Decide needs.
func observe(ctx context.Context, tx pgx.Tx) (Observation, error) {
	var observation Observation
	if err := tx.QueryRow(ctx, "SELECT to_regclass('public.alembic_version') IS NOT NULL").Scan(&observation.HasVersionTable); err != nil {
		return observation, fmt.Errorf("look for alembic_version: %w", err)
	}
	// Every relation kind (indexes and composite types included), every
	// function or procedure, and every type that is not an array or a
	// relation's row type, in every schema but the system ones -- a River
	// schema holds job rows the Python chain checks, so a database with only
	// River in it is not empty. Objects an extension owns are not counted:
	// they are the extension's, not a schema this migrator would collide with.
	if err := tx.QueryRow(ctx, "SELECT "+
		"(SELECT count(*) FROM pg_class o JOIN pg_namespace n ON n.oid = o.relnamespace WHERE "+userObject("pg_class")+") + "+
		"(SELECT count(*) FROM pg_proc o JOIN pg_namespace n ON n.oid = o.pronamespace WHERE "+userObject("pg_proc")+") + "+
		"(SELECT count(*) FROM pg_type o JOIN pg_namespace n ON n.oid = o.typnamespace "+
		"WHERE "+userObject("pg_type")+" AND o.typrelid = 0 AND o.typcategory <> 'A')").Scan(&observation.Objects); err != nil {
		return observation, fmt.Errorf("count objects: %w", err)
	}
	tables, err := tx.Query(ctx, "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "+
		"WHERE n.nspname = 'public' AND c.relkind IN ('r','p') ORDER BY c.relname")
	if err != nil {
		return observation, fmt.Errorf("list public tables: %w", err)
	}
	if observation.PublicTables, err = pgx.CollectRows(tables, pgx.RowTo[string]); err != nil {
		return observation, fmt.Errorf("list public tables: %w", err)
	}
	if !observation.HasVersionTable {
		return observation, nil
	}
	rows, err := tx.Query(ctx, "SELECT version_num FROM public.alembic_version")
	if err != nil {
		return observation, fmt.Errorf("read alembic_version: %w", err)
	}
	versions, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return observation, fmt.Errorf("read alembic_version: %w", err)
	}
	sort.Strings(versions)
	observation.Versions = versions
	return observation, nil
}

func inTransaction(ctx context.Context, conn *pgx.Conn, fn func(pgx.Tx) error) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", lockKey); err != nil {
		_ = tx.Rollback(ctx)
		return fmt.Errorf("take the migration lock: %w", err)
	}
	if err := fn(tx); err != nil {
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			return errors.Join(err, rollbackErr)
		}
		return err
	}
	return tx.Commit(ctx)
}

// readOnlyTransaction runs fn in one READ ONLY, REPEATABLE READ transaction: every
// query in it reads the same snapshot. Under the default READ COMMITTED each query
// takes its own, so a migrator that commits between two of them tears what fn reads
// (no alembic_version from before, objects and revisions from after) into a state
// that never existed. Nothing is written and no lock beyond ACCESS SHARE is taken.
func readOnlyTransaction(ctx context.Context, conn *pgx.Conn, fn func(pgx.Tx) error) error {
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	return fn(tx)
}

func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	x, y := append([]string(nil), a...), append([]string(nil), b...)
	sort.Strings(x)
	sort.Strings(y)
	for i := range x {
		if x[i] != y[i] {
			return false
		}
	}
	return true
}

// revisionRecordedSince reports whether the database now records the chain revision
// (or a later one): read fresh, after the failed step's transaction is gone.
func revisionRecordedSince(ctx context.Context, conn *pgx.Conn, baseline Baseline, chain []ChainFile, known map[string]bool, revision string) bool {
	var recorded bool
	err := readOnlyTransaction(ctx, conn, func(tx pgx.Tx) error {
		observation, err := observe(ctx, tx)
		if err != nil {
			return err
		}
		current := Decide(observation, baseline, chain, known)
		if current.State != StateAtHead {
			return nil
		}
		for _, file := range current.Pending {
			if file.Revision == revision {
				return nil
			}
		}
		recorded = true
		return nil
	})
	return err == nil && recorded
}

// sqlState is the SQLSTATE of a step's failure, or "" when it is not a server
// error. The message is not logged: a server error can carry row values.
func sqlState(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}
