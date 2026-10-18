package contracts

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// ProvisioningEventSurface names the provisioning-event.v1 wire contract.
const ProvisioningEventSurface = "provisioning-event.v1"

// AuditEventSurface names the audit-event.v1 wire contract.
const AuditEventSurface = "audit-event.v1"

// ProvisioningPayload is the outbox row's payload (jsonb): everything
// event-specific. Revision is the package's Revision type, whose decoder
// refuses any decimal- or exponent-form token, so 1.0 and 9007199254740990.1
// never become a revision (see Revision.UnmarshalJSON).
type ProvisioningPayload struct {
	Revision         Revision                   `json:"revision"`
	OrganizationID   string                     `json:"organization_id,omitempty"`
	ActorPrincipalID string                     `json:"actor_principal_id,omitempty"`
	RequestID        string                     `json:"request_id,omitempty"`
	Change           map[string]json.RawMessage `json:"change"`
}

// ProvisioningEvent is one outbox row as a consumer receives it, validated and
// parsed. Its members are the columns of auth_outbox_events that cross a
// service boundary.
type ProvisioningEvent struct {
	SchemaVersion  string              `json:"schema_version"`
	AggregateType  string              `json:"aggregate_type"`
	AggregateID    string              `json:"aggregate_id"`
	EventType      string              `json:"event_type"`
	IdempotencyKey string              `json:"idempotency_key"`
	CreatedAt      Timestamp           `json:"created_at"`
	Payload        ProvisioningPayload `json:"payload"`
}

// DedupeKey is the value idempotency_key must equal: aggregate type,
// aggregate id and revision. Revisions are per aggregate, so the key names the
// aggregate as well as the revision.
func (e ProvisioningEvent) DedupeKey() string {
	return e.AggregateType + ":" + e.AggregateID + ":" + strconv.FormatInt(int64(e.Payload.Revision), 10)
}

// ParseProvisioningEvent validates raw against provisioning-event.v1 and
// parses it. Duplicate members and trailing content are refused before
// decoding (a decode destroys that evidence).
func ParseProvisioningEvent(root string, raw []byte) (ProvisioningEvent, error) {
	if err := validateRawDocument(root, ProvisioningEventSurface, raw); err != nil {
		return ProvisioningEvent{}, err
	}
	var event ProvisioningEvent
	if err := json.Unmarshal(raw, &event); err != nil {
		return ProvisioningEvent{}, fmt.Errorf("%s: decoding validated document: %w", ProvisioningEventSurface, err)
	}
	// The pattern bounds timestamp components, not the calendar: 2026-02-30 is
	// lexically valid. Timestamp parses the envelope's; the change object's
	// timestamp members are parsed here, every one, not only the one known today.
	for name, value := range event.Payload.Change {
		if name != "expires_at" {
			continue
		}
		var parsed Timestamp
		if err := json.Unmarshal(value, &parsed); err != nil {
			return ProvisioningEvent{}, fmt.Errorf("%s: payload.change.%s: %w", ProvisioningEventSurface, name, err)
		}
	}
	// A pattern cannot compare two members. A key that disagrees with the
	// payload would let two revisions collide in the UNIQUE index, or one
	// revision be delivered under two keys.
	if want := event.DedupeKey(); event.IdempotencyKey != want {
		return ProvisioningEvent{}, fmt.Errorf(
			"%s: idempotency_key %q does not equal %q (aggregate_type:aggregate_id:payload.revision)",
			ProvisioningEventSurface, event.IdempotencyKey, want)
	}
	return event, nil
}

// Disposition is what a consumer does with an event.
type Disposition string

const (
	// Apply: the revision is the next one for its aggregate.
	Apply Disposition = "apply"
	// Ignore: the revision is at or below the last applied one, a duplicate or
	// stale delivery.
	Ignore Disposition = "ignore"
	// Gap: the revision skips ahead. It is not applied and the consumer must
	// resynchronise; the cursor does not move.
	Gap Disposition = "gap"
)

// ConsumerCursor tracks the last applied revision per aggregate. It orders
// nothing across aggregates: revisions are per aggregate.
type ConsumerCursor struct{ last map[string]int64 }

// NewConsumerCursor returns an empty cursor.
func NewConsumerCursor() *ConsumerCursor { return &ConsumerCursor{last: map[string]int64{}} }

// Observe classifies e and advances the cursor only on Apply.
func (c *ConsumerCursor) Observe(e ProvisioningEvent) Disposition {
	key := e.AggregateType + ":" + e.AggregateID
	last := c.last[key] // 0 for an aggregate never seen
	rev := int64(e.Payload.Revision)
	switch {
	case rev <= last:
		return Ignore
	case rev == last+1:
		c.last[key] = rev
		return Apply
	default:
		return Gap
	}
}

// AuditAttributes are the allowlisted audit attributes; every schema member is
// carried, so a parser never silently drops data the schema admits.
type AuditAttributes struct {
	ReasonCode        string    `json:"reason_code,omitempty"`
	Action            string    `json:"action,omitempty"`
	ResourceType      string    `json:"resource_type,omitempty"`
	AggregateType     string    `json:"aggregate_type,omitempty"`
	AggregateRevision *Revision `json:"aggregate_revision,omitempty"`
	CredentialClass   string    `json:"credential_class,omitempty"`
	DecisionID        string    `json:"decision_id,omitempty"`
}

// AuditEvent is one audit event, validated and parsed. Every schema member is
// carried (TestParsedTypesCarryEverySchemaMember asserts it both ways).
type AuditEvent struct {
	SchemaVersion      string          `json:"schema_version"`
	EventType          string          `json:"event_type"`
	OccurredAt         Timestamp       `json:"occurred_at"`
	Outcome            string          `json:"outcome"`
	ActorPrincipalID   string          `json:"actor_principal_id,omitempty"`
	SubjectPrincipalID string          `json:"subject_principal_id,omitempty"`
	OrganizationID     string          `json:"organization_id,omitempty"`
	PolicyRevision     *Revision       `json:"policy_revision,omitempty"`
	RequestID          string          `json:"request_id,omitempty"`
	Attributes         AuditAttributes `json:"attributes"`
}

// ParseAuditEvent validates raw against audit-event.v1 and parses it, refusing
// decimal-form revisions on the raw token.
func ParseAuditEvent(root string, raw []byte) (AuditEvent, error) {
	if err := validateRawDocument(root, AuditEventSurface, raw); err != nil {
		return AuditEvent{}, err
	}
	var event AuditEvent
	if err := json.Unmarshal(raw, &event); err != nil {
		return AuditEvent{}, fmt.Errorf("%s: decoding validated document: %w", AuditEventSurface, err)
	}
	return event, nil
}

// validateRawDocument is the shared front half of every raw-bytes parser:
// duplicate members and trailing content first, then the schema.
func validateRawDocument(root, surface string, raw []byte) error {
	if err := refuseDuplicateMembers(raw); err != nil {
		return fmt.Errorf("%s: %w", surface, err)
	}
	var document any
	if err := json.Unmarshal(raw, &document); err != nil {
		return fmt.Errorf("%s: document is not valid JSON: %w", surface, err)
	}
	return Validate(root, surface, document)
}
