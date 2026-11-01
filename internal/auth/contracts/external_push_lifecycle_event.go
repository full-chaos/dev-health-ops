package contracts

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/externalpush"
)

// ExternalPushLifecycleEventSurface names the external-push-lifecycle-event.v1
// wire contract.
const ExternalPushLifecycleEventSurface = externalpush.Surface

// ExternalPushCredentialAggregate is the one aggregate type of the surface.
const ExternalPushCredentialAggregate = externalpush.Aggregate

// External Push credential bindings.
const (
	ExternalPushBindingSource       = externalpush.BindingSource
	ExternalPushBindingOrganization = externalpush.BindingOrganization
)

// External Push lifecycle event types, in the schema's closed order.
const (
	ExternalPushCreated = externalpush.Created
	ExternalPushScoped  = externalpush.Scoped
	ExternalPushRotated = externalpush.Rotated
	ExternalPushRevoked = externalpush.Revoked
	ExternalPushExpired = externalpush.Expired
)

// ExternalPushLifecycleChange is the allowlisted description of what changed.
// Every member the schema admits has a field here; none of them can hold a
// secret, a verifier or a token.
type ExternalPushLifecycleChange struct {
	Status           string     `json:"status,omitempty"`
	Fingerprint      string     `json:"fingerprint,omitempty"`
	Scopes           []string   `json:"scopes,omitempty"`
	RotationParentID string     `json:"rotation_parent_id,omitempty"`
	ExpiresAt        *Timestamp `json:"expires_at,omitempty"`
}

// Wire is the change as the producer-side vocabulary carries it.
func (c ExternalPushLifecycleChange) Wire() externalpush.Change {
	wire := externalpush.Change{Status: c.Status, Fingerprint: c.Fingerprint, Scopes: c.Scopes, RotationParentID: c.RotationParentID}
	if c.ExpiresAt != nil {
		at := c.ExpiresAt.Time
		wire.ExpiresAt = &at
	}
	return wire
}

// MarshalJSON is the producer-side marshal: a non-nil empty Scopes stays
// "scopes": [] and a nil Scopes is omitted.
func (c ExternalPushLifecycleChange) MarshalJSON() ([]byte, error) { return c.Wire().MarshalJSON() }

// ExternalPushLifecyclePayload is the event-specific half of the envelope.
type ExternalPushLifecyclePayload struct {
	Revision         Revision                    `json:"revision"`
	OrganizationID   string                      `json:"organization_id"`
	Binding          string                      `json:"binding"`
	SourceID         string                      `json:"source_id,omitempty"`
	ActorPrincipalID string                      `json:"actor_principal_id,omitempty"`
	RequestID        string                      `json:"request_id,omitempty"`
	Change           ExternalPushLifecycleChange `json:"change"`
}

// ExternalPushLifecycleEvent is one lifecycle event, validated and parsed.
type ExternalPushLifecycleEvent struct {
	SchemaVersion  string                       `json:"schema_version"`
	AggregateType  string                       `json:"aggregate_type"`
	AggregateID    string                       `json:"aggregate_id"`
	EventType      string                       `json:"event_type"`
	IdempotencyKey string                       `json:"idempotency_key"`
	CreatedAt      Timestamp                    `json:"created_at"`
	Payload        ExternalPushLifecyclePayload `json:"payload"`
}

// DedupeKey is the value idempotency_key must equal.
func (e ExternalPushLifecycleEvent) DedupeKey() string {
	return e.AggregateType + ":" + e.AggregateID + ":" + strconv.FormatInt(int64(e.Payload.Revision), 10)
}

// ParseExternalPushLifecycleEvent validates raw against the surface and parses
// it. Duplicate members and trailing content are refused first, a decimal- or
// exponent-form revision is refused on the raw token, and the idempotency key
// must equal aggregate_type:aggregate_id:revision.
func ParseExternalPushLifecycleEvent(root string, raw []byte) (ExternalPushLifecycleEvent, error) {
	if err := validateRawDocument(root, ExternalPushLifecycleEventSurface, raw); err != nil {
		return ExternalPushLifecycleEvent{}, err
	}
	var event ExternalPushLifecycleEvent
	if err := json.Unmarshal(raw, &event); err != nil {
		return ExternalPushLifecycleEvent{}, fmt.Errorf("%s: decoding validated document: %w", ExternalPushLifecycleEventSurface, err)
	}
	if err := checkInstantInRange(ExternalPushLifecycleEventSurface, "created_at", event.CreatedAt.Time); err != nil {
		return ExternalPushLifecycleEvent{}, err
	}
	if expires := event.Payload.Change.ExpiresAt; expires != nil {
		if err := checkInstantInRange(ExternalPushLifecycleEventSurface, "payload/change/expires_at", expires.Time); err != nil {
			return ExternalPushLifecycleEvent{}, err
		}
	}
	if want := event.DedupeKey(); event.IdempotencyKey != want {
		return ExternalPushLifecycleEvent{}, fmt.Errorf(
			"%s: idempotency_key %q does not equal %q (aggregate_type:aggregate_id:payload.revision)",
			ExternalPushLifecycleEventSurface, event.IdempotencyKey, want)
	}
	return event, nil
}
