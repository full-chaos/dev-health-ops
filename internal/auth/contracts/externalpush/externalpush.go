// Package externalpush is the producer-side vocabulary of the
// external-push-lifecycle-event.v1 contract: the event names, the allowlisted
// change a producer may carry, and the runtime check of it. It depends on
// nothing in this module, so a writer (internal/audit) can use it without
// importing the contracts package, which the writer's own tests import.
package externalpush

import (
	"encoding/json"
	"fmt"
	"regexp"
	"time"
)

// Surface names the wire contract.
const Surface = "external-push-lifecycle-event.v1"

// Aggregate is the one aggregate type of the surface.
const Aggregate = "external_push_credential"

// Event types, in the schema's closed order.
const (
	Created = Aggregate + ".created"
	Scoped  = Aggregate + ".scoped"
	Rotated = Aggregate + ".rotated"
	Revoked = Aggregate + ".revoked"
	Expired = Aggregate + ".expired"
)

var (
	idPattern          = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
	orgIDPattern       = regexp.MustCompile(`^org_[A-Za-z0-9_-]{1,64}$`)
	fingerprintPattern = regexp.MustCompile(`^[0-9a-f]{16}$`)
	scopePattern       = regexp.MustCompile(`^[a-z][a-z0-9_]*(:[a-z][a-z0-9_]*)+$`)
)

// Change is the allowlisted description of what changed. Every member the
// schema admits has a field; none of them can hold a secret, a verifier or a
// token.
type Change struct {
	Status           string
	Fingerprint      string
	Scopes           []string
	RotationParentID string
	ExpiresAt        *time.Time
}

// MarshalJSON writes the schema's members. A non-nil Scopes, even empty, is
// written as a list (a scoped event that narrows a credential to nothing
// requires the member); a nil Scopes is omitted. expires_at is UTC with at most
// six fractional digits.
func (c Change) MarshalJSON() ([]byte, error) {
	wire := struct {
		Status           string    `json:"status,omitempty"`
		Fingerprint      string    `json:"fingerprint,omitempty"`
		Scopes           *[]string `json:"scopes,omitempty"`
		RotationParentID string    `json:"rotation_parent_id,omitempty"`
		ExpiresAt        string    `json:"expires_at,omitempty"`
	}{Status: c.Status, Fingerprint: c.Fingerprint, RotationParentID: c.RotationParentID}
	if c.Scopes != nil {
		wire.Scopes = &c.Scopes
	}
	if c.ExpiresAt != nil {
		wire.ExpiresAt = c.ExpiresAt.UTC().Truncate(time.Microsecond).Format(time.RFC3339Nano)
	}
	return json.Marshal(wire)
}

// Check refuses an event the external-push-lifecycle-event.v1
// schema would refuse, from the members a producer chooses (the revision, the
// key, the envelope and the clock are the writer's): the event type, the ids
// and the change. It is the runtime twin of the schema, which a running
// service cannot read from disk, and the corpus differential test keeps the two
// equal. A change member the schema forbids has no field on the type, so it
// cannot be carried at all.
func Check(eventType, aggregateID, organizationID, sourceID string, change Change) error {
	surface := Surface
	if !idPattern.MatchString(aggregateID) {
		return fmt.Errorf("%s: aggregate_id is not 1..64 of [A-Za-z0-9_-]", surface)
	}
	if !orgIDPattern.MatchString(organizationID) {
		return fmt.Errorf("%s: organization_id is not org_ followed by 1..64 of [A-Za-z0-9_-]", surface)
	}
	if !idPattern.MatchString(sourceID) {
		return fmt.Errorf("%s: source_id is not 1..64 of [A-Za-z0-9_-]", surface)
	}
	var (
		wantStatus      string
		needFingerprint bool
		needScopes      bool
		minScopes       int
		needParent      bool
		needExpiry      bool
	)
	switch eventType {
	case Created:
		wantStatus, needFingerprint, needScopes, minScopes = "active", true, true, 1
	case Rotated:
		wantStatus, needFingerprint, needScopes, minScopes, needParent = "active", true, true, 1, true
	case Scoped:
		needScopes = true
	case Revoked:
		wantStatus = "revoked"
	case Expired:
		wantStatus, needExpiry = "expired", true
	default:
		return fmt.Errorf("%s: event_type %q is not an external push lifecycle event", surface, eventType)
	}
	if change.Status != wantStatus {
		return fmt.Errorf("%s: %s must carry change.status %q", surface, eventType, wantStatus)
	}
	if needFingerprint != (change.Fingerprint != "") ||
		(needFingerprint && !fingerprintPattern.MatchString(change.Fingerprint)) {
		return fmt.Errorf("%s: %s change.fingerprint is missing, not allowed, or not 16 lowercase hex characters", surface, eventType)
	}
	if needScopes != (change.Scopes != nil) {
		return fmt.Errorf("%s: %s change.scopes is missing or not allowed", surface, eventType)
	}
	if needScopes {
		if len(change.Scopes) < minScopes || len(change.Scopes) > 64 {
			return fmt.Errorf("%s: %s change.scopes holds %d scopes, want %d..64", surface, eventType, len(change.Scopes), minScopes)
		}
		seen := map[string]bool{}
		for _, scope := range change.Scopes {
			if len(scope) > 64 || !scopePattern.MatchString(scope) {
				return fmt.Errorf("%s: %s change.scopes holds a scope of the wrong shape", surface, eventType)
			}
			if seen[scope] {
				return fmt.Errorf("%s: %s change.scopes holds a scope twice", surface, eventType)
			}
			seen[scope] = true
		}
	}
	if needParent != (change.RotationParentID != "") ||
		(needParent && !idPattern.MatchString(change.RotationParentID)) {
		return fmt.Errorf("%s: %s change.rotation_parent_id is missing, not allowed, or not 1..64 of [A-Za-z0-9_-]", surface, eventType)
	}
	switch {
	case needExpiry && change.ExpiresAt == nil:
		return fmt.Errorf("%s: %s must carry change.expires_at", surface, eventType)
	case change.ExpiresAt != nil && eventType != Created && eventType != Rotated && eventType != Expired:
		return fmt.Errorf("%s: %s may not carry change.expires_at", surface, eventType)
	case change.ExpiresAt != nil:
		return checkInstant(change.ExpiresAt.UTC())
	}
	return nil
}

func checkInstant(moment time.Time) error {
	if year := moment.Year(); year < 1 || year > 9999 {
		return fmt.Errorf("%s: payload/change/expires_at normalises to UTC year %d, outside 0001-01-01..9999-12-31", Surface, year)
	}
	return nil
}
