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
	Variant string   `json:"variant"`
	Action  string   `json:"action"`
	Heads   []string `json:"heads"`
	Applied []string `json:"applied,omitempty"`
}

// Upgrade brings the database behind conn to the head of baseline, then
// applies every chain revision after it.
func Upgrade(ctx context.Context, conn *pgx.Conn, baseline Baseline, chain []ChainFile) (Result, error) {
	result := Result{Variant: Variant(baseline.Cutover), Heads: baseline.Heads}
	var plan Plan
	err := inTransaction(ctx, conn, func(tx pgx.Tx) error {
		observation, err := observe(ctx, tx)
		if err != nil {
			return err
		}
		plan = Decide(observation, baseline, chain)
		switch plan.State {
		case StateForeign:
			return ForeignDatabaseError{Relations: observation.PublicRelations}
		case StateBelowHead:
			return BelowHeadError{Cutover: baseline.Cutover, Recorded: observation.Versions, Missing: plan.Missing}
		case StateEmpty:
			if _, err := tx.Exec(ctx, baseline.Schema); err != nil {
				return fmt.Errorf("apply the %s baseline schema: %w", Variant(baseline.Cutover), err)
			}
			if _, err := tx.Exec(ctx, baseline.Data); err != nil {
				return fmt.Errorf("apply the %s baseline data: %w", Variant(baseline.Cutover), err)
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
				return fmt.Errorf("the %s baseline recorded alembic_version %v, want %v", Variant(baseline.Cutover), after.Versions, baseline.Heads)
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
	Variant  string   `json:"variant"`
	State    string   `json:"state"`
	Heads    []string `json:"heads"`
	Recorded []string `json:"recorded"`
	Missing  []string `json:"missing,omitempty"`
	Pending  []string `json:"pending,omitempty"`
}

// ReadStatus reports where the database stands without changing it.
func ReadStatus(ctx context.Context, conn *pgx.Conn, baseline Baseline, chain []ChainFile) (Status, error) {
	status := Status{Variant: Variant(baseline.Cutover), Heads: baseline.Heads}
	err := readOnlyTransaction(ctx, conn, func(tx pgx.Tx) error {
		observation, err := observe(ctx, tx)
		if err != nil {
			return err
		}
		status.Recorded = observation.Versions
		plan := Decide(observation, baseline, chain)
		status.Missing = plan.Missing
		for _, file := range plan.Pending {
			status.Pending = append(status.Pending, file.Revision)
		}
		status.State = map[State]string{StateEmpty: "empty", StateAtHead: "at_head", StateBelowHead: "below_head", StateForeign: "foreign"}[plan.State]
		return nil
	})
	return status, err
}

// observe reads the state Decide needs.
func observe(ctx context.Context, tx pgx.Tx) (Observation, error) {
	var observation Observation
	if err := tx.QueryRow(ctx, "SELECT to_regclass('public.alembic_version') IS NOT NULL").Scan(&observation.HasVersionTable); err != nil {
		return observation, fmt.Errorf("look for alembic_version: %w", err)
	}
	if err := tx.QueryRow(ctx, "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "+
		"WHERE n.nspname = 'public' AND c.relkind IN ('r','p','v','m','S','f')").Scan(&observation.PublicRelations); err != nil {
		return observation, fmt.Errorf("count public relations: %w", err)
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
