package contracts

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// RepositoryIdentityEventSurface names the repository-identity-event.v1 wire contract.
const RepositoryIdentityEventSurface = "repository-identity-event.v1"

// RepositoryIdentityAggregate is the one aggregate type of the surface.
const RepositoryIdentityAggregate = "repository_identity"

// RepositoryIdentityObserved is the one event type of the surface.
const RepositoryIdentityObserved = RepositoryIdentityAggregate + ".observed"

// RepositoryIdentityChange is the full current state of one repository.
type RepositoryIdentityChange struct {
	Provider       string    `json:"provider"`
	ProviderRepoID string    `json:"provider_repo_id"`
	Path           string    `json:"path"`
	ObservedAt     Timestamp `json:"observed_at"`
}

// RepositoryIdentityPayload is the event-specific half of the envelope.
type RepositoryIdentityPayload struct {
	Revision       Revision                 `json:"revision"`
	OrganizationID string                   `json:"organization_id"`
	Change         RepositoryIdentityChange `json:"change"`
}

// AggregateID is the value aggregate_id must equal: "<organization>/<provider>/<provider id>".
func (p RepositoryIdentityPayload) AggregateID() string {
	return p.OrganizationID + "/" + p.Change.Provider + "/" + p.Change.ProviderRepoID
}

// RepositoryIdentityEvent is one event, validated and parsed.
type RepositoryIdentityEvent struct {
	SchemaVersion  string                    `json:"schema_version"`
	AggregateType  string                    `json:"aggregate_type"`
	AggregateID    string                    `json:"aggregate_id"`
	EventType      string                    `json:"event_type"`
	IdempotencyKey string                    `json:"idempotency_key"`
	CreatedAt      Timestamp                 `json:"created_at"`
	Payload        RepositoryIdentityPayload `json:"payload"`
}

// DedupeKey is the value idempotency_key must equal.
func (e RepositoryIdentityEvent) DedupeKey() string {
	return e.AggregateType + ":" + e.AggregateID + ":" + strconv.FormatInt(int64(e.Payload.Revision), 10)
}

// ParseRepositoryIdentityEvent validates raw against the surface and parses it. Duplicate members and trailing
// content are refused first, a decimal- or exponent-form revision is refused on the raw token, both instants must
// be real dates, the aggregate id must name the payload's repository and the idempotency key must equal
// aggregate_type:aggregate_id:revision.
func ParseRepositoryIdentityEvent(root string, raw []byte) (RepositoryIdentityEvent, error) {
	if err := validateRawDocument(root, RepositoryIdentityEventSurface, raw); err != nil {
		return RepositoryIdentityEvent{}, err
	}
	var event RepositoryIdentityEvent
	if err := json.Unmarshal(raw, &event); err != nil {
		return RepositoryIdentityEvent{}, fmt.Errorf("%s: decoding validated document: %w", RepositoryIdentityEventSurface, err)
	}
	if err := checkInstantInRange(RepositoryIdentityEventSurface, "created_at", event.CreatedAt.Time); err != nil {
		return RepositoryIdentityEvent{}, err
	}
	if err := checkInstantInRange(RepositoryIdentityEventSurface, "payload/change/observed_at", event.Payload.Change.ObservedAt.Time); err != nil {
		return RepositoryIdentityEvent{}, err
	}
	if want := event.Payload.AggregateID(); event.AggregateID != want {
		return RepositoryIdentityEvent{}, fmt.Errorf(
			"%s: aggregate_id %q does not equal %q (organization_id/provider/provider_repo_id)",
			RepositoryIdentityEventSurface, event.AggregateID, want)
	}
	if want := event.DedupeKey(); event.IdempotencyKey != want {
		return RepositoryIdentityEvent{}, fmt.Errorf(
			"%s: idempotency_key %q does not equal %q (aggregate_type:aggregate_id:payload.revision)",
			RepositoryIdentityEventSurface, event.IdempotencyKey, want)
	}
	return event, nil
}
