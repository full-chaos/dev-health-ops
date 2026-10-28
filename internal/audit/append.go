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

// appendOnlyEventTypes are the security events that record an attempt with
// no state change, so Commit (which requires one) cannot write them. Append
// writes only these, and Commit refuses them. A type is added here, in
// review, with the code that writes it.
var appendOnlyEventTypes = map[string]bool{
	EventLoginFailed:         true,
	EventSigningKeyRefused:   true,
	EventWorkloadTokenIssued: true,
}

// EventLoginFailed is a refused sign-in: wrong or unknown credentials, a
// locked, disabled or unverified account, or an organization the principal
// is not a member of.
const EventLoginFailed = "auth.login.failed"

// EventSigningKeyRefused is a signing-key rotation verb the state machine
// refused: nothing changed.
const EventSigningKeyRefused = "auth.signing_key.refused"

// EventWorkloadTokenIssued is a workload token the exchange issued: it
// records which binding and registry revision the token's jti came from. The
// token itself is stateless, so no security state changes.
const EventWorkloadTokenIssued = "auth.workload.token_issued"

// ErrNotAppendOnly reports an Append of an event type outside
// appendOnlyEventTypes. Nothing is written.
var ErrNotAppendOnly = fmt.Errorf("%w: the audit event type is not append-only", ErrMutationFailed)

// Append writes one audit row of an append-only event type in its own
// transaction, under the same validation as Commit's audit row. It changes no
// security state and writes no outbox event: an event that records a state
// change goes through Commit with that change.
func Append(ctx context.Context, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier, event AuditEvent) (err error) {
	defer func() {
		if err != nil {
			slog.ErrorContext(ctx, "audit: Append failed; nothing was written",
				"audit_event_type", event.EventType, logging.ErrorAttr(err))
		}
	}()
	if err := event.validate(); err != nil {
		return err
	}
	if !appendOnlyEventTypes[event.EventType] {
		return ErrNotAppendOnly
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
	if err := insertAuditEvent(ctx, tx, schema, event); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: committing", ErrMutationFailed)
	}
	return nil
}

// EventPasswordResetRequested is a password-reset link the service issued (or
// refused to issue for an unknown address): the request changes no guarded
// state.
const EventPasswordResetRequested = "auth.password.reset_requested"

// ErrGuardedWriteInAppend reports that the apply function of AppendWith wrote
// a guarded table. Nothing is written.
var ErrGuardedWriteInAppend = fmt.Errorf("%w: a guarded write belongs in Commit", ErrMutationFailed)

// AppendWith is Append with the caller's unguarded writes in the same
// transaction.
func AppendWith(ctx context.Context, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier, event AuditEvent, apply func(ctx context.Context, tx TxOps) error) error {
	return fmt.Errorf("%w: not implemented", ErrMutationFailed)
}

func appendWithTx(ctx context.Context, tx pgx.Tx, schema authschema.ValidatedIdentifier, event AuditEvent, apply func(ctx context.Context, tx TxOps) error) error {
	return fmt.Errorf("%w: not implemented", ErrMutationFailed)
}
