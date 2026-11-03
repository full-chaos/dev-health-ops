package pushlifecycle

import (
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// AcrSourceID is the one source of acr credentials: one external_source
// principal per organization. owner_subject is projected data, never a principal.
const AcrSourceID = "acr"

var acrEventTypes = map[string]string{
	contracts.AcrCredentialIssued:         contracts.ExternalPushCreated,
	contracts.AcrCredentialDeviceRedeemed: contracts.ExternalPushCreated,
	contracts.AcrCredentialRotated:        contracts.ExternalPushRotated,
	contracts.AcrCredentialRevoked:        contracts.ExternalPushRevoked,
	contracts.AcrCredentialExpired:        contracts.ExternalPushExpired,
}

// FromAcr maps a parsed acr event onto the event Apply takes, so the ordering,
// terminal and no-widen rules are the push rules. A rotation stays a new
// aggregate that names its parent. The fingerprint is left empty: the engine
// derives it from the aggregate id.
func FromAcr(e contracts.AcrCredentialLifecycleEvent) contracts.ExternalPushLifecycleEvent {
	change := contracts.ExternalPushLifecycleChange{
		Status: e.Payload.Change.Status, Scopes: e.Payload.Change.Scopes,
		RotationParentID: e.Payload.Change.RotationParentID, ExpiresAt: e.Payload.Change.ExpiresAt,
	}
	return contracts.ExternalPushLifecycleEvent{
		SchemaVersion: contracts.ExternalPushLifecycleEventSurface, AggregateType: contracts.ExternalPushCredentialAggregate,
		AggregateID: e.AggregateID, EventType: acrEventTypes[e.EventType], IdempotencyKey: e.IdempotencyKey, CreatedAt: e.CreatedAt,
		Payload: contracts.ExternalPushLifecyclePayload{
			Revision: e.Payload.Revision, OrganizationID: e.Payload.OrganizationID, SourceID: AcrSourceID, Change: change,
		},
	}
}
