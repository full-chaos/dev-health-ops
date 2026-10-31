package audit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/externalpush"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// ErrPushLifecycle wraps ErrMutationFailed: AppendPushLifecycle refused the
// events before it wrote anything, or failed part way. The caller owns the
// transaction and must roll it back on any error.
var ErrPushLifecycle = fmt.Errorf("%w: external push lifecycle events", ErrMutationFailed)

// PushLifecycleEvent is one external-push-lifecycle-event.v1 event a credential
// domain appends in its own mutation transaction. The writer owns the revision,
// the idempotency key and the clock (the outbox row's created_at); the caller
// names the verb, the ids and the allowlisted change.
type PushLifecycleEvent struct {
	EventType      string
	AggregateID    string
	OrganizationID string
	SourceID       string
	Change         externalpush.Change
	// Snapshot is the credential's state as of this write, in the created form
	// (status active, fingerprint, scopes, optional expires_at). It is required
	// on a scoped, revoked or expired event and used only when the credential
	// has no event yet (it predates the stream): the writer then emits created
	// at revision 1 from it, and this event at revision 2.
	Snapshot *externalpush.Change
}

// AppendPushLifecycle writes external push lifecycle events to the auth outbox
// inside the caller's transaction tx, so the credential row change and its
// events commit or roll back together. The outbox and revision tables are
// guarded by migration 0006; the guard is armed for the writes below and
// disarmed before this function returns, on every path, so a caller cannot use
// it to write any other guarded table. It accepts only this one contract.
//
// Events are written in aggregate-id order (one lock order for every writer);
// events of one aggregate keep the caller's order. A created or rotated event
// must be the aggregate's first (revision 1). The caller must hold the
// credential's row lock, so two writers cannot both find the aggregate new.
func AppendPushLifecycle(ctx context.Context, tx pgx.Tx, schema authschema.ValidatedIdentifier, events []PushLifecycleEvent) (err error) {
	if tx == nil || len(events) == 0 {
		return fmt.Errorf("%w: a transaction and at least one event are required", ErrPushLifecycle)
	}
	for _, e := range events {
		if err := checkPushLifecycle(e); err != nil {
			return fmt.Errorf("%w: %w", ErrPushLifecycle, err)
		}
	}
	ordered := append([]PushLifecycleEvent(nil), events...)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].AggregateID < ordered[j].AggregateID })

	var armed bool
	if err := tx.QueryRow(ctx, `SELECT current_setting('`+commitGuardSetting+`', true) IS NOT DISTINCT FROM pg_current_xact_id()::text`).Scan(&armed); err != nil {
		return fmt.Errorf("%w: reading the commit guard: %w", ErrPushLifecycle, err)
	}
	if armed {
		return fmt.Errorf("%w: the transaction is already armed for a guarded write", ErrPushLifecycle)
	}
	if err := armTx(ctx, tx); err != nil {
		return err
	}
	defer func() {
		if _, disarmErr := tx.Exec(ctx, `SELECT set_config('`+commitGuardSetting+`', '', true)`); disarmErr != nil && err == nil {
			err = fmt.Errorf("%w: disarming the commit guard: %w", ErrPushLifecycle, disarmErr)
		}
	}()

	q := authschema.Quote(schema)
	for _, e := range ordered {
		if err := appendPushLifecycleEvent(ctx, tx, q, e); err != nil {
			return err
		}
	}
	return nil
}

func checkPushLifecycle(e PushLifecycleEvent) error {
	if err := externalpush.Check(e.EventType, e.AggregateID, e.OrganizationID, e.SourceID, e.Change); err != nil {
		return err
	}
	switch e.EventType {
	case externalpush.Scoped, externalpush.Revoked, externalpush.Expired:
		if e.Snapshot == nil {
			return errors.New("a scoped, revoked or expired event needs the credential's snapshot")
		}
		return externalpush.Check(externalpush.Created, e.AggregateID, e.OrganizationID, e.SourceID, *e.Snapshot)
	default:
		if e.Snapshot != nil {
			return errors.New("only a scoped, revoked or expired event takes a snapshot")
		}
	}
	return nil
}

func appendPushLifecycleEvent(ctx context.Context, tx pgx.Tx, q string, e PushLifecycleEvent) error {
	revision, err := bumpRevision(ctx, tx, q, externalpush.Aggregate, e.AggregateID)
	if err != nil {
		return err
	}
	first := e.EventType == externalpush.Created || e.EventType == externalpush.Rotated
	switch {
	case first && revision != 1:
		return fmt.Errorf("%w: a %s event needs a credential with no earlier event, found revision %d", ErrPushLifecycle, e.EventType, revision)
	case !first && revision == 1:
		if err := insertPushLifecycleRow(ctx, tx, q, externalpush.Created, e, *e.Snapshot, 1); err != nil {
			return err
		}
		if revision, err = bumpRevision(ctx, tx, q, externalpush.Aggregate, e.AggregateID); err != nil {
			return err
		}
	}
	if err := insertPushLifecycleRow(ctx, tx, q, e.EventType, e, e.Change, revision); err != nil {
		return err
	}
	slog.DebugContext(ctx, "audit: external push lifecycle event written", "event_type", e.EventType, "revision", revision)
	return nil
}

func insertPushLifecycleRow(ctx context.Context, tx pgx.Tx, q, eventType string, e PushLifecycleEvent, change externalpush.Change, revision int64) error {
	payload, err := json.Marshal(struct {
		Revision       int64               `json:"revision"`
		OrganizationID string              `json:"organization_id"`
		SourceID       string              `json:"source_id"`
		Change         externalpush.Change `json:"change"`
	}{revision, e.OrganizationID, e.SourceID, change})
	if err != nil {
		return fmt.Errorf("%w: encoding the outbox payload", ErrPushLifecycle)
	}
	return insertOutboxRow(ctx, tx, q, externalpush.Aggregate, e.AggregateID, eventType, revision, payload)
}
