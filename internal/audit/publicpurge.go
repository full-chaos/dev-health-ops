package audit

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// EventLegacyLinkTokensPurged records one run of the legacy public link-token
// purge (CHAOS-11506): the counts and the run id, nothing else.
const EventLegacyLinkTokensPurged = "auth.legacy_link_tokens.purged"

// publicPurgeEventTypes are the events whose state change is a delete in the
// ops (public) schema. Commit cannot write them: its changed-row count only
// sees guarded auth tables, so it would refuse the mutation as a no-op. A type
// is added here, in review, with the code that writes it.
var publicPurgeEventTypes = map[string]bool{EventLegacyLinkTokensPurged: true}

// ErrNotPublicPurge reports a CommitPublicPurge of an event type outside
// publicPurgeEventTypes. Nothing is written.
var ErrNotPublicPurge = fmt.Errorf("%w: the audit event type is not a public purge", ErrMutationFailed)

// CommitPublicPurge runs apply and writes one audit row of eventType in one
// transaction, or neither. apply returns the row's attributes (they hold counts
// that exist only after it ran); they pass the same allowlist as every audit
// row. It writes no outbox event: the purge changes no provisioning aggregate.
func CommitPublicPurge(ctx context.Context, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier, eventType string, apply func(ctx context.Context, tx TxOps) (map[string]any, error)) (err error) {
	defer func() {
		if err != nil {
			slog.ErrorContext(ctx, "audit: CommitPublicPurge failed; its own transaction rolled back",
				"audit_event_type", eventType, logging.ErrorAttr(err))
		}
	}()
	if !publicPurgeEventTypes[eventType] {
		return ErrNotPublicPurge
	}
	if apply == nil {
		return fmt.Errorf("%w: no state mutation", ErrMutationFailed)
	}
	if pool == nil {
		return fmt.Errorf("%w: no connection pool", ErrMutationFailed)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("%w: beginning the transaction", ErrMutationFailed)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := armTx(ctx, tx); err != nil {
		return err
	}
	attributes, applyErr := apply(ctx, TxOps{tx: tx})
	if applyErr != nil {
		return fmt.Errorf("%w: applying the state mutation: %w", ErrMutationFailed, applyErr)
	}
	if status := tx.Conn().PgConn().TxStatus(); status != 'T' {
		return fmt.Errorf("%w: the mutation ended the transaction itself (status %q)", ErrMutationFailed, string(status))
	}
	event := AuditEvent{EventType: eventType, Outcome: OutcomeAllowed, Attributes: attributes}
	if err := event.validate(); err != nil {
		return err
	}
	if err := insertAuditEvent(ctx, tx, schema, event); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: committing", ErrMutationFailed)
	}
	return nil
}
