package pushlifecycle

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// AcrSourceID is the one source of acr credentials: one external_source
// principal per organization. owner_subject is projected data, never a principal.
const AcrSourceID = "acr"

// AcrFingerprint is the public fingerprint of an acr credential: the first 8
// bytes of the SHA-256 of its cred_ id, in hex. The event carries none, by
// contract, and this value proves nothing about the secret.
func AcrFingerprint(credentialID string) string {
	sum := sha256.Sum256([]byte(credentialID))
	return hex.EncodeToString(sum[:8])
}

var acrEventTypes = map[string]string{
	contracts.AcrCredentialIssued:         contracts.ExternalPushCreated,
	contracts.AcrCredentialDeviceRedeemed: contracts.ExternalPushCreated,
	contracts.AcrCredentialRotated:        contracts.ExternalPushRotated,
	contracts.AcrCredentialRevoked:        contracts.ExternalPushRevoked,
	contracts.AcrCredentialExpired:        contracts.ExternalPushExpired,
}

// FromAcr maps a parsed acr event onto the event Apply takes, so the ordering,
// terminal and no-widen rules are the push rules. A rotation stays a new
// aggregate that names its parent.
func FromAcr(e contracts.AcrCredentialLifecycleEvent) contracts.ExternalPushLifecycleEvent {
	change := contracts.ExternalPushLifecycleChange{
		Status: e.Payload.Change.Status, Scopes: e.Payload.Change.Scopes,
		RotationParentID: e.Payload.Change.RotationParentID, ExpiresAt: e.Payload.Change.ExpiresAt,
	}
	if e.Payload.Revision == 1 {
		change.Fingerprint = AcrFingerprint(e.AggregateID)
	}
	return contracts.ExternalPushLifecycleEvent{
		SchemaVersion: contracts.ExternalPushLifecycleEventSurface, AggregateType: contracts.ExternalPushCredentialAggregate,
		AggregateID: e.AggregateID, EventType: acrEventTypes[e.EventType], IdempotencyKey: e.IdempotencyKey, CreatedAt: e.CreatedAt,
		Payload: contracts.ExternalPushLifecyclePayload{
			Revision: e.Payload.Revision, OrganizationID: e.Payload.OrganizationID, SourceID: AcrSourceID, Change: change,
		},
	}
}
