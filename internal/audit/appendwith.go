package audit

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// Registered here, not in the appendOnlyEventTypes literal, so the line anchors
// that contracts/auth/v1 pins in append.go stay where they are.
func init() { appendOnlyEventTypes[EventPasswordResetRequested] = true }

// EventPasswordResetRequested is a password-reset link the service issued (or
// refused to issue for an unknown address): the request changes no guarded
// state.
const EventPasswordResetRequested = "auth.password.reset_requested"

// ErrGuardedWriteInAppend reports that the apply function of AppendWith wrote
// a guarded table. Nothing is written.
var ErrGuardedWriteInAppend = fmt.Errorf("%w: a guarded write belongs in Commit", ErrMutationFailed)

// AppendWith is Append with the caller's unguarded writes in the same
// transaction: apply runs, then the audit row is inserted, and they commit
// together or not at all. Only append-only event types are accepted; an apply
// that writes a guarded table is rolled back with ErrGuardedWriteInAppend
// (that write belongs in Commit).
func AppendWith(ctx context.Context, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier, event AuditEvent, apply func(ctx context.Context, tx TxOps) error) (err error) {
	defer func() {
		if err != nil {
			slog.ErrorContext(ctx, "audit: AppendWith failed; nothing was written",
				"audit_event_type", event.EventType, logging.ErrorAttr(err))
		}
	}()
	if err := event.validate(); err != nil {
		return err
	}
	if !appendOnlyEventTypes[event.EventType] {
		return ErrNotAppendOnly
	}
	if apply == nil {
		return fmt.Errorf("%w: no apply function", ErrMutationFailed)
	}
	if pool == nil {
		return fmt.Errorf("%w: no connection pool", ErrMutationFailed)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("%w: beginning the transaction", ErrMutationFailed)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	return appendWithTx(ctx, tx, schema, event, apply)
}

// txStatusOf is the connection's real transaction state; a test replaces it.
var txStatusOf = func(tx pgx.Tx) byte { return tx.Conn().PgConn().TxStatus() }

func appendWithTx(ctx context.Context, tx pgx.Tx, schema authschema.ValidatedIdentifier, event AuditEvent, apply func(ctx context.Context, tx TxOps) error) error {
	if _, err := tx.Exec(ctx, `SELECT set_config('`+commitGuardSetting+`', pg_current_xact_id()::text, true),
		set_config('`+changedSetting+`', pg_current_xact_id()::text || ':0', true)`); err != nil {
		return fmt.Errorf("%w: arming the commit guard: %w", ErrMutationFailed, err)
	}
	if err := apply(ctx, TxOps{tx: tx}); err != nil {
		return fmt.Errorf("%w: applying the writes: %w", ErrMutationFailed, err)
	}
	var changed int64
	if err := tx.QueryRow(ctx,
		`SELECT CASE WHEN current_setting('`+changedSetting+`', true) LIKE pg_current_xact_id()::text || ':%'
		THEN split_part(current_setting('`+changedSetting+`', true), ':', 2)::bigint ELSE 0 END`).Scan(&changed); err != nil {
		return fmt.Errorf("%w: reading the changed-row count: %w", ErrMutationFailed, err)
	}
	if changed > 0 {
		return ErrGuardedWriteInAppend
	}
	if status := txStatusOf(tx); status != 'T' {
		return fmt.Errorf("%w: the writes ended the transaction itself (status %q)", ErrMutationFailed, string(status))
	}
	if err := insertAuditEvent(ctx, tx, schema, event); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: committing", ErrMutationFailed)
	}
	return nil
}
