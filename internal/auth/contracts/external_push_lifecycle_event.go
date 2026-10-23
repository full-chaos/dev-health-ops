package contracts

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// ExternalPushLifecycleEventSurface names the external-push-lifecycle-event.v1
// wire contract.
const ExternalPushLifecycleEventSurface = "external-push-lifecycle-event.v1"

// ExternalPushCredentialAggregate is the one aggregate type of the surface.
const ExternalPushCredentialAggregate = "external_push_credential"

// External Push lifecycle event types, in the schema's closed order.
const (
	ExternalPushCreated = ExternalPushCredentialAggregate + ".created"
	ExternalPushScoped  = ExternalPushCredentialAggregate + ".scoped"
	ExternalPushRotated = ExternalPushCredentialAggregate + ".rotated"
	ExternalPushRevoked = ExternalPushCredentialAggregate + ".revoked"
	ExternalPushExpired = ExternalPushCredentialAggregate + ".expired"
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

// ExternalPushLifecyclePayload is the event-specific half of the envelope.
type ExternalPushLifecyclePayload struct {
	Revision         Revision                    `json:"revision"`
	OrganizationID   string                      `json:"organization_id"`
	SourceID         string                      `json:"source_id"`
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
