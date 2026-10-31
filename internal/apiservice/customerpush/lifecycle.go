package customerpush

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/externalpush"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
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
// off. A source-less (organization-wide) token has no event: the contract
// binds a credential to a source.
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
	event, ok, err := revokedEvent(token)
	if err != nil || !ok {
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

// credentialFingerprint is the public fingerprint of a token: the first 16 hex
// characters of the SHA-256 of its id. It identifies the credential, is
// derived from neither the token nor its hash, and cannot authenticate.
func credentialFingerprint(id uuid.UUID) string {
	sum := sha256.Sum256([]byte(externalpush.Aggregate + ":" + id.String()))
	return hex.EncodeToString(sum[:8])
}

// credentialScopes is the contract's scope set: a sorted set, never nil.
func credentialScopes(scopes []string) []string {
	out := slices.Clone(scopes)
	slices.Sort(out)
	return append([]string{}, slices.Compact(out)...)
}

func credentialExpiry(at *time.Time) *time.Time {
	if at == nil {
		return nil
	}
	utc := at.UTC()
	return &utc
}

// createdChange is the credential's state in the created form.
func createdChange(id uuid.UUID, scopes []string, expiresAt *time.Time) externalpush.Change {
	return externalpush.Change{Status: "active", Fingerprint: credentialFingerprint(id),
		Scopes: credentialScopes(scopes), ExpiresAt: credentialExpiry(expiresAt)}
}

func wireIDs(orgID string, sourceID *uuid.UUID) (organization, source string, err error) {
	if sourceID == nil {
		return "", "", nil
	}
	organization = provisioning.WireID("organization", orgID)
	if organization == "" {
		return "", "", errors.New("customerpush: the organization id cannot be carried on the lifecycle wire")
	}
	return organization, sourceID.String(), nil
}

// createdEvents is the event of a new token.
func createdEvents(t newToken) ([]audit.PushLifecycleEvent, error) {
	organization, source, err := wireIDs(t.OrgID, t.SourceID)
	if err != nil || source == "" {
		return nil, err
	}
	var expires *time.Time
	if t.ExpiresAt != nil {
		at := t.ExpiresAt.Time.UTC()
		expires = &at
	}
	return []audit.PushLifecycleEvent{{EventType: externalpush.Created, AggregateID: t.ID.String(),
		OrganizationID: organization, SourceID: source, Change: createdChange(t.ID, t.Scopes, expires)}}, nil
}

// revokedEvent is the retirement of a stored token. The snapshot is the token
// as stored, for a token that predates the stream.
func revokedEvent(old tokenRow) (audit.PushLifecycleEvent, bool, error) {
	organization, source, err := wireIDs(old.OrgID, old.SourceID)
	if err != nil || source == "" {
		return audit.PushLifecycleEvent{}, false, err
	}
	names, err := scopeNames(old.Scopes)
	if err != nil {
		return audit.PushLifecycleEvent{}, false, err
	}
	snapshot := createdChange(old.ID, names, old.ExpiresAt)
	return audit.PushLifecycleEvent{EventType: externalpush.Revoked, AggregateID: old.ID.String(),
		OrganizationID: organization, SourceID: source,
		Change: externalpush.Change{Status: "revoked"}, Snapshot: &snapshot}, true, nil
}

// rotatedEvents is a rotation: the new token's rotated event and the old
// token's revoked event, appended together.
func rotatedEvents(old tokenRow, next newToken) ([]audit.PushLifecycleEvent, error) {
	revoked, ok, err := revokedEvent(old)
	if err != nil || !ok {
		return nil, err
	}
	created, err := createdEvents(next)
	if err != nil || len(created) == 0 {
		return nil, err
	}
	rotated := created[0]
	rotated.EventType = externalpush.Rotated
	rotated.Change.RotationParentID = old.ID.String()
	return []audit.PushLifecycleEvent{rotated, revoked}, nil
}

func scopeNames(raw []byte) ([]string, error) {
	value, err := scopesValue(raw)
	if err != nil {
		return nil, err
	}
	list := value.([]pyjson.Value)
	names := make([]string, len(list))
	for index, scope := range list {
		names[index] = scope.(string)
	}
	return names, nil
}
