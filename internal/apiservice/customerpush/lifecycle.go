package customerpush

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/externalpush"
	"github.com/full-chaos/dev-health-ops/internal/auth/pushevent"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// lifecycle writes External Push credential lifecycle events (the
// external-push-lifecycle-event.v1 contract) to the auth outbox in the
// credential mutation's own transaction. Off (the zero value), it writes
// nothing and the routes behave exactly as before it existed.
type lifecycle struct {
	on     bool
	schema authschema.ValidatedIdentifier
	err    error
}

func newLifecycle(on bool, schema string) lifecycle {
	if !on {
		return lifecycle{}
	}
	if schema == "" {
		schema = "auth"
	}
	ident, err := authschema.NewValidatedIdentifier(schema)
	return lifecycle{on: true, schema: ident, err: err}
}

// created, rotated and revoked append the events of one mutation in tx, which
// must hold the credential rows' locks. They do nothing while the events are
// off. A source-less token is organization-wide: its events carry binding
// organization and no source_id.
func (l lifecycle) created(ctx context.Context, tx pgx.Tx, token newToken) error {
	if !l.on {
		return nil
	}
	events, err := createdEvents(token)
	if err != nil {
		return err
	}
	return l.append(ctx, tx, events)
}

func (l lifecycle) rotated(ctx context.Context, tx pgx.Tx, old tokenRow, next newToken) error {
	if !l.on {
		return nil
	}
	events, err := rotatedEvents(old, next)
	if err != nil {
		return err
	}
	return l.append(ctx, tx, events)
}

func (l lifecycle) revoked(ctx context.Context, tx pgx.Tx, token tokenRow) error {
	if !l.on {
		return nil
	}
	event, err := revokedEvent(token)
	if err != nil {
		return err
	}
	return l.append(ctx, tx, []audit.PushLifecycleEvent{event})
}

func (l lifecycle) append(ctx context.Context, tx pgx.Tx, events []audit.PushLifecycleEvent) error {
	if !l.on || len(events) == 0 {
		return nil
	}
	if l.err != nil {
		return l.err
	}
	return audit.AppendPushLifecycle(ctx, tx, l.schema, events)
}

// createdEvents is the event of a new token.
func createdEvents(t newToken) ([]audit.PushLifecycleEvent, error) {
	organization, binding, source, err := pushevent.WireIDs(t.OrgID, t.SourceID)
	if err != nil {
		return nil, err
	}
	var expires *time.Time
	if t.ExpiresAt != nil {
		at := t.ExpiresAt.Time.UTC()
		expires = &at
	}
	return []audit.PushLifecycleEvent{{EventType: externalpush.Created, AggregateID: t.ID.String(),
		OrganizationID: organization, Binding: binding, SourceID: source, Change: pushevent.CreatedChange(t.ID, t.Scopes, expires)}}, nil
}

// revokedEvent is the retirement of a stored token. The snapshot is the token
// as stored, for a token that predates the stream.
func revokedEvent(old tokenRow) (audit.PushLifecycleEvent, error) {
	organization, binding, source, err := pushevent.WireIDs(old.OrgID, old.SourceID)
	if err != nil {
		return audit.PushLifecycleEvent{}, err
	}
	names, err := scopeNames(old.Scopes)
	if err != nil {
		return audit.PushLifecycleEvent{}, err
	}
	snapshot := pushevent.CreatedChange(old.ID, names, old.ExpiresAt)
	return audit.PushLifecycleEvent{EventType: externalpush.Revoked, AggregateID: old.ID.String(),
		OrganizationID: organization, Binding: binding, SourceID: source,
		Change: externalpush.Change{Status: "revoked"}, Snapshot: &snapshot}, nil
}

// rotatedEvents is a rotation: the new token's rotated event and the old
// token's revoked event, appended together.
func rotatedEvents(old tokenRow, next newToken) ([]audit.PushLifecycleEvent, error) {
	revoked, err := revokedEvent(old)
	if err != nil {
		return nil, err
	}
	created, err := createdEvents(next)
	if err != nil {
		return nil, err
	}
	rotated := created[0]
	rotated.EventType = externalpush.Rotated
	rotated.Change.RotationParentID = old.ID.String()
	return []audit.PushLifecycleEvent{rotated, revoked}, nil
}

// credentialFingerprint is the shared fingerprint (internal/auth/pushevent).
func credentialFingerprint(id uuid.UUID) string { return pushevent.Fingerprint(id) }

// scopeNames is the shared scope-name reader (internal/auth/pushevent).
func scopeNames(raw []byte) ([]string, error) { return pushevent.ScopeNames(raw) }
