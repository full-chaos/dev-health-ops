package revisionfeed

import (
	"encoding/json"
	"errors"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
)

var (
	// ErrKeyDisagrees: the row's idempotency key is not aggregate_type:aggregate_id:revision. Two revisions could collide
	// on one key, or one revision travel under two keys.
	ErrKeyDisagrees = errors.New("revisionfeed: the idempotency key disagrees with the aggregate and revision")
	// ErrNotAProvisioningEvent: the row's payload carries no valid revision.
	ErrNotAProvisioningEvent = errors.New("revisionfeed: the outbox row is not a provisioning event")
	// ErrOneShotRevision: a one-shot aggregate (a session revoke) has exactly one revision, 1.
	ErrOneShotRevision = errors.New("revisionfeed: a one-shot aggregate event must be revision 1")
	// ErrSigningKeyID: a signing_key event whose aggregate id is not the derived id of its kid.
	ErrSigningKeyID = errors.New("revisionfeed: the signing_key aggregate id is not the derived id of its kid")
)

// FromOutbox maps an outbox row (audit.Event, what a Sink receives) to the wire type consumers parse
// (contracts.ProvisioningEvent). It lives here, not in contracts: audit imports contracts. The error texts are fixed:
// they never quote the payload or the aggregate id.
func FromOutbox(e audit.Event) (contracts.ProvisioningEvent, error) {
	var payload contracts.ProvisioningPayload
	if err := json.Unmarshal(e.Payload, &payload); err != nil || payload.Revision < 1 {
		return contracts.ProvisioningEvent{}, ErrNotAProvisioningEvent
	}
	if provisioning.IsOneShot(e.AggregateType) && payload.Revision != 1 {
		return contracts.ProvisioningEvent{}, ErrOneShotRevision
	}
	if e.AggregateType == "signing_key" {
		var kid string
		if raw, ok := payload.Change["kid"]; !ok || json.Unmarshal(raw, &kid) != nil || e.AggregateID != provisioning.SigningKeyID(kid) {
			return contracts.ProvisioningEvent{}, ErrSigningKeyID
		}
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
