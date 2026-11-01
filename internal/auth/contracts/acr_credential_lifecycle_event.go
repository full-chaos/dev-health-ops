package contracts

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// AcrCredentialLifecycleEventSurface names the acr-credential-lifecycle-event.v1
// wire contract.
const AcrCredentialLifecycleEventSurface = "acr-credential-lifecycle-event.v1"

// AcrCredentialAggregate is the one aggregate type of the surface.
const AcrCredentialAggregate = "acr_credential"

// ACR credential lifecycle event types, in the schema's closed order.
const (
	AcrCredentialIssued         = AcrCredentialAggregate + ".issued"
	AcrCredentialDeviceRedeemed = AcrCredentialAggregate + ".device_redeemed"
	AcrCredentialRotated        = AcrCredentialAggregate + ".rotated"
	AcrCredentialRevoked        = AcrCredentialAggregate + ".revoked"
	AcrCredentialExpired        = AcrCredentialAggregate + ".expired"
)

// ACR audit actor types, in the schema's closed order.
const (
	AcrActorUser       = "user"
	AcrActorOperator   = "operator"
	AcrActorCredential = "credential"
	AcrActorSystem     = "system"
)

// AcrClasses is the closed acr issuance-class vocabulary, in the schema's order.
var AcrClasses = []string{"browser", "oauth_user", "native_device", "org_bearer", "operator", "workload"}

// AcrCredentialLifecycleChange is the allowlisted description of what changed.
// Every member the schema admits has a field here; none of them can hold a
// secret, a verifier, a token, a token prefix or a fingerprint.
type AcrCredentialLifecycleChange struct {
	Status           string     `json:"status,omitempty"`
	AcrClass         string     `json:"acr_class,omitempty"`
	Scopes           []string   `json:"scopes,omitempty"`
	OwnerSubject     string     `json:"owner_subject,omitempty"`
	RotationParentID string     `json:"rotation_parent_id,omitempty"`
	ExpiresAt        *Timestamp `json:"expires_at,omitempty"`
}

// AcrCredentialLifecyclePayload is the event-specific half of the envelope.
type AcrCredentialLifecyclePayload struct {
	Revision       Revision                     `json:"revision"`
	OrganizationID string                       `json:"organization_id"`
	ActorType      string                       `json:"actor_type"`
	ActorRef       string                       `json:"actor_ref,omitempty"`
	RequestID      string                       `json:"request_id,omitempty"`
	Change         AcrCredentialLifecycleChange `json:"change"`
}

// AcrCredentialLifecycleEvent is one lifecycle event, validated and parsed.
type AcrCredentialLifecycleEvent struct {
	SchemaVersion  string                        `json:"schema_version"`
	AggregateType  string                        `json:"aggregate_type"`
	AggregateID    string                        `json:"aggregate_id"`
	EventType      string                        `json:"event_type"`
	IdempotencyKey string                        `json:"idempotency_key"`
	CreatedAt      Timestamp                     `json:"created_at"`
	Payload        AcrCredentialLifecyclePayload `json:"payload"`
}

// DedupeKey is the value idempotency_key must equal.
func (e AcrCredentialLifecycleEvent) DedupeKey() string {
	return e.AggregateType + ":" + e.AggregateID + ":" + strconv.FormatInt(int64(e.Payload.Revision), 10)
}

// ParseAcrCredentialLifecycleEvent validates raw against the surface and parses
// it. Duplicate members and trailing content are refused first, a decimal- or
// exponent-form revision is refused on the raw token, the idempotency key must
// equal aggregate_type:aggregate_id:revision, and a credential acting on itself
// must name itself: on revoked its actor_ref is the aggregate id, on rotated it
// is the credential being replaced.
func ParseAcrCredentialLifecycleEvent(root string, raw []byte) (AcrCredentialLifecycleEvent, error) {
	if err := validateRawDocument(root, AcrCredentialLifecycleEventSurface, raw); err != nil {
		return AcrCredentialLifecycleEvent{}, err
	}
	var event AcrCredentialLifecycleEvent
	if err := json.Unmarshal(raw, &event); err != nil {
		return AcrCredentialLifecycleEvent{}, fmt.Errorf("%s: decoding validated document: %w", AcrCredentialLifecycleEventSurface, err)
	}
	if err := checkInstantInRange(AcrCredentialLifecycleEventSurface, "created_at", event.CreatedAt.Time); err != nil {
		return AcrCredentialLifecycleEvent{}, err
	}
	if expires := event.Payload.Change.ExpiresAt; expires != nil {
		if err := checkInstantInRange(AcrCredentialLifecycleEventSurface, "payload/change/expires_at", expires.Time); err != nil {
			return AcrCredentialLifecycleEvent{}, err
		}
	}
	if want := event.DedupeKey(); event.IdempotencyKey != want {
		return AcrCredentialLifecycleEvent{}, fmt.Errorf(
			"%s: idempotency_key %q does not equal %q (aggregate_type:aggregate_id:payload.revision)",
			AcrCredentialLifecycleEventSurface, event.IdempotencyKey, want)
	}
	if event.Payload.ActorType == AcrActorCredential {
		var self string
		switch event.EventType {
		case AcrCredentialRevoked:
			self = event.AggregateID
		case AcrCredentialRotated:
			self = event.Payload.Change.RotationParentID
		}
		if self != "" && event.Payload.ActorRef != self {
			return AcrCredentialLifecycleEvent{}, fmt.Errorf(
				"%s: actor_ref %q does not equal %q (the credential a credential actor acts on)",
				AcrCredentialLifecycleEventSurface, event.Payload.ActorRef, self)
		}
	}
	return event, nil
}
