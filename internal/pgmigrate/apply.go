package pgmigrate

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5"
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
	result := Result{Heads: baseline.Heads}
	var plan Plan
	err := inTransaction(ctx, conn, func(tx pgx.Tx) error {
		observation, err := observe(ctx, tx)
		if err != nil {
			return err
		}
		plan = Decide(observation, baseline, chain)
		switch plan.State {
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
			result.Action = "baseline_applied"
		default:
			result.Action = "up_to_date"
		}
		return nil
	})
	if err != nil {
		return result, err
	}
	previous := plan.ApplicationHead
	for _, file := range plan.Pending {
		if err := inTransaction(ctx, conn, func(tx pgx.Tx) error {
			return applyChainFile(ctx, tx, file, previous)
		}); err != nil {
			return result, err
		}
		result.Applied = append(result.Applied, file.Revision)
		previous = file.Revision
	}
	if result.Action == "up_to_date" && len(result.Applied) > 0 {
		result.Action = "chain_applied"
	}
	return result, nil
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
	// MissingTables are the baseline tables a schema_mismatch database lacks.
	MissingTables []string `json:"missing_tables,omitempty"`
	Pending       []string `json:"pending,omitempty"`
}

// ReadStatus reports where the database stands without changing it.
func ReadStatus(ctx context.Context, conn *pgx.Conn, baseline Baseline, chain []ChainFile) (Status, error) {
	status := Status{Heads: baseline.Heads}
	err := readOnlyTransaction(ctx, conn, func(tx pgx.Tx) error {
		observation, err := observe(ctx, tx)
		if err != nil {
			return err
		}
		status.Recorded = observation.Versions
		plan := Decide(observation, baseline, chain)
		status.Missing = plan.Missing
		status.MissingTables = plan.MissingTables
		for _, file := range plan.Pending {
			status.Pending = append(status.Pending, file.Revision)
		}
		status.State = map[State]string{StateEmpty: "empty", StateAtHead: "at_head", StateBelowHead: "below_head", StateForeign: "foreign", StateSchemaMismatch: "schema_mismatch"}[plan.State]
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

func readOnlyTransaction(ctx context.Context, conn *pgx.Conn, fn func(pgx.Tx) error) error {
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
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
