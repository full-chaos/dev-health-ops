package audit

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// EventUserProfileUpdated records an admin change of a user's profile columns that live only in go-api's public.users
// (username, avatar_url) or in no guarded auth column (full_name, the principal's display name).
const EventUserProfileUpdated = "auth.user.profile_updated"

// EventUserTouched records an admin PATCH that changes no profile column: go-api bumps public.users.updated_at on every
// PATCH that finds the user, also with no field change or with a platform role change only. No reader outside
// auth.security_audit_events serves it.
const EventUserTouched = "auth.user.touched"

// OffStreamPublicProfile marks a change of profile columns no consumer of the provisioning stream reads: no
// provisioning event names them (CHAOS-13376 consumer census: the one stream consumer, the revision feed, invalidates
// by aggregate and reads no profile column).
const OffStreamPublicProfile = "public_profile"

func init() { offStreamReasons[OffStreamPublicProfile] = true }

// publicProjectionEventTypes are the events whose state change is an update of public (ops schema) columns and of
// unguarded auth columns only. Commit cannot write them: its changed-row count only sees guarded auth tables, so it
// would refuse the change as a no-op. A type is added here, in review, with the code that writes it.
var publicProjectionEventTypes = map[string]bool{EventUserProfileUpdated: true, EventUserTouched: true}

// commitAdmitsPublicProjection is the one exception to Commit's refusal of a public projection type: a platform role
// change with the touch and no profile column is one Commit (the role rows are guarded), its own audit row
// EventUserTouched, off the stream (OffStreamPublicProfile). Every other public projection type, and EventUserTouched
// with any other off-stream reason or none, stays refused.
func commitAdmitsPublicProjection(eventType, offStream string) bool {
	return eventType == EventUserTouched && offStream == OffStreamPublicProfile
}

// ErrNotPublicProjection reports a CommitPublicProjection of an event type outside publicProjectionEventTypes.
// Nothing is written.
var ErrNotPublicProjection = fmt.Errorf("%w: the audit event type is not a public projection change", ErrMutationFailed)

// CommitPublicProjection runs apply and writes the one audit row event in one transaction, or neither. apply returns
// the number of public rows it changed: zero is ErrNoStateChange, and nothing is written. It writes no outbox event
// (OffStreamPublicProfile): the change moves no provisioning aggregate.
func CommitPublicProjection(ctx context.Context, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier, event AuditEvent,
	apply func(ctx context.Context, tx TxOps) (int64, error)) (err error) {
	defer func() {
		if err != nil && !isNoStateChange(err) {
			slog.ErrorContext(ctx, "audit: CommitPublicProjection failed; its own transaction rolled back",
				"audit_event_type", event.EventType, logging.ErrorAttr(err))
		}
	}()
	if !publicProjectionEventTypes[event.EventType] {
		return ErrNotPublicProjection
	}
	if apply == nil {
		return fmt.Errorf("%w: no state mutation", ErrMutationFailed)
	}
	if err := event.validate(); err != nil {
		return err
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
	changed, applyErr := apply(ctx, TxOps{tx: tx})
	if applyErr != nil {
		return fmt.Errorf("%w: applying the state mutation: %w", ErrMutationFailed, applyErr)
	}
	if changed <= 0 {
		return ErrNoStateChange
	}
	if status := tx.Conn().PgConn().TxStatus(); status != 'T' {
		return fmt.Errorf("%w: the mutation ended the transaction itself (status %q)", ErrMutationFailed, string(status))
	}
	if err := insertAuditEvent(ctx, tx, schema, event); err != nil {
		return err
	}
	if Rehearsing(ctx) {
		return nil
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: committing", ErrMutationFailed)
	}
	return nil
}

func isNoStateChange(err error) bool { return errors.Is(err, ErrNoStateChange) }
