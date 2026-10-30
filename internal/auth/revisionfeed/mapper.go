package revisionfeed

import (
	"encoding/json"
	"errors"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

var (
	// ErrKeyDisagrees: the row's idempotency key is not aggregate_type:aggregate_id:revision. Two revisions could collide
	// on one key, or one revision travel under two keys.
	ErrKeyDisagrees = errors.New("revisionfeed: the idempotency key disagrees with the aggregate and revision")
	// ErrNotAProvisioningEvent: the row's payload carries no valid revision.
	ErrNotAProvisioningEvent = errors.New("revisionfeed: the outbox row is not a provisioning event")
)

// FromOutbox maps an outbox row (audit.Event, what a Sink receives) to the wire type consumers parse
// (contracts.ProvisioningEvent). It lives here, not in contracts: audit imports contracts. The error texts are fixed:
// they never quote the payload or the aggregate id.
func FromOutbox(e audit.Event) (contracts.ProvisioningEvent, error) {
	var payload contracts.ProvisioningPayload
	if err := json.Unmarshal(e.Payload, &payload); err != nil || payload.Revision < 1 {
		return contracts.ProvisioningEvent{}, ErrNotAProvisioningEvent
	}
	mapped := contracts.ProvisioningEvent{
		SchemaVersion:  contracts.ProvisioningEventSurface,
		AggregateType:  e.AggregateType,
		AggregateID:    e.AggregateID,
		EventType:      e.EventType,
		IdempotencyKey: e.IdempotencyKey,
		CreatedAt:      contracts.Timestamp{Time: e.CreatedAt.UTC()},
		Payload:        payload,
	}
	if mapped.IdempotencyKey != mapped.DedupeKey() {
		return contracts.ProvisioningEvent{}, ErrKeyDisagrees
	}
	return mapped, nil
}
