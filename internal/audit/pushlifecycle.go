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

// AppendPushLifecycle appends external push lifecycle events to the auth
// outbox inside the caller's transaction tx, so the credential row change and
// its events commit or roll back together. It accepts only this one contract.
//
// It writes through the auth schema's append_external_push_lifecycle_event
// function (migration 0040), not through the tables: the caller's login holds
// EXECUTE on that function and no table privilege. The function fixes the
// aggregate type, the event list and the payload members, assigns the revision
// and the idempotency key, and arms and disarms the commit guard itself, so
// this package holds no INSERT for this path. Go validates every event against
// the contract first, so a refusal here writes nothing.
//
// Events are sent in aggregate-id order (one lock order for every writer);
// events of one aggregate keep the caller's order. A created or rotated event
// must be the aggregate's first (revision 1). The caller must hold the
// credential's row lock, so two writers cannot both find the aggregate new.
func AppendPushLifecycle(ctx context.Context, tx pgx.Tx, schema authschema.ValidatedIdentifier, events []PushLifecycleEvent) error {
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

	call := `SELECT ` + authschema.Quote(schema) + `.append_external_push_lifecycle_event($1, $2, $3::jsonb, $4::jsonb)`
	for _, e := range ordered {
		payload, snapshot, err := pushLifecycleArguments(e)
		if err != nil {
			return err
		}
		var revision int64
		if err := tx.QueryRow(ctx, call, e.EventType, e.AggregateID, payload, snapshot).Scan(&revision); err != nil {
			return fmt.Errorf("%w: appending %s: %w", ErrPushLifecycle, e.EventType, err)
		}
		slog.DebugContext(ctx, "audit: external push lifecycle event written", "event_type", e.EventType, "revision", revision)
	}
	return nil
}

// pushLifecycleArguments are the function's payload and snapshot arguments:
// the payload members the caller chooses, and the snapshot (nil when absent).
func pushLifecycleArguments(e PushLifecycleEvent) (payload string, snapshot any, err error) {
	encoded, err := json.Marshal(struct {
		OrganizationID string              `json:"organization_id"`
		SourceID       string              `json:"source_id"`
		Change         externalpush.Change `json:"change"`
	}{e.OrganizationID, e.SourceID, e.Change})
	if err != nil {
		return "", nil, fmt.Errorf("%w: encoding the payload", ErrPushLifecycle)
	}
	if e.Snapshot == nil {
		return string(encoded), nil, nil
	}
	raw, err := json.Marshal(e.Snapshot)
	if err != nil {
		return "", nil, fmt.Errorf("%w: encoding the snapshot", ErrPushLifecycle)
	}
	return string(encoded), string(raw), nil
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
