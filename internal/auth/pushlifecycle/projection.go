// Package pushlifecycle is the shared projection core for External Push
// credential lifecycle events (external-push-lifecycle-event.v1). It folds one
// event into the projected state of one credential and decides, by a closed
// outcome, whether the event applied. It is pure: no storage, no clock, no
// logging, so a producer, a consumer and a reconciler can all use the same
// rule. It never validates a credential and holds no secret.
package pushlifecycle

import (
	"slices"
	"sort"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// Status is a projected credential's lifecycle state.
type Status string

const (
	StatusUnknown Status = ""
	StatusActive  Status = "active"
	StatusRevoked Status = "revoked"
	StatusExpired Status = "expired"
)

// Terminal reports whether no later event may change the credential.
func (s Status) Terminal() bool { return s == StatusRevoked || s == StatusExpired }

// Outcome is the closed result of Apply; its values are safe as metric labels.
type Outcome string

const (
	// Applied: the event was the next revision and was folded in.
	Applied Outcome = "applied"
	// Duplicate: the revision equals the last applied one.
	Duplicate Outcome = "duplicate"
	// Stale: the revision is below the last applied one.
	Stale Outcome = "stale"
	// Gap: the revision skips ahead; nothing is applied and the caller must
	// resynchronise before retrying.
	Gap Outcome = "gap"
	// RefusedTerminal: the credential is revoked or expired.
	RefusedTerminal Outcome = "refused_terminal"
	// RefusedWiden: the event would widen scopes.
	RefusedWiden Outcome = "refused_widen"
	// RefusedTransition: the event is not valid for the credential's state or
	// changes its identity.
	RefusedTransition Outcome = "refused_transition"
	// RefusedParent: a rotation names no parent the caller could supply.
	RefusedParent Outcome = "refused_parent"
)

// Outcomes is every Outcome, for exhaustive label sets and tests.
var Outcomes = []Outcome{Applied, Duplicate, Stale, Gap, RefusedTerminal, RefusedWiden, RefusedTransition, RefusedParent}

// State is one credential's projection. The zero value is a credential never
// seen. Revision is the last applied revision.
type State struct {
	AggregateID      string
	OrganizationID   string
	SourceID         string
	Revision         int64
	Status           Status
	Fingerprint      string
	Scopes           []string
	RotationParentID string
	ExpiresAt        string
}

// Parent is what a rotation is compared with: the scopes of the credential it
// replaces. A nil Parent for a rotated event is RefusedParent, never "no
// parent, so apply".
type Parent struct{ Scopes []string }

// Apply folds event into state. The returned state is state itself unless the
// outcome is Applied. Apply never panics and never returns Applied for a
// terminal credential.
func Apply(state State, event contracts.ExternalPushLifecycleEvent, parent *Parent) (State, Outcome) {
	revision := int64(event.Payload.Revision)
	switch {
	case revision < 1 || event.AggregateType != contracts.ExternalPushCredentialAggregate || event.AggregateID == "":
		return state, RefusedTransition
	case state.Revision > 0 && event.AggregateID != state.AggregateID:
		return state, RefusedTransition
	case revision == state.Revision:
		return state, Duplicate
	case revision < state.Revision:
		return state, Stale
	case revision > state.Revision+1:
		return state, Gap
	}
	if state.Status.Terminal() {
		return state, RefusedTerminal
	}
	change := event.Payload.Change
	next := state
	next.AggregateID = event.AggregateID
	next.Revision = revision
	switch event.EventType {
	case contracts.ExternalPushCreated, contracts.ExternalPushRotated:
		if state.Revision != 0 || state.Status != StatusUnknown || change.Fingerprint == "" || len(change.Scopes) == 0 {
			return state, RefusedTransition
		}
		if event.EventType == contracts.ExternalPushRotated {
			if change.RotationParentID == "" || change.RotationParentID == event.AggregateID {
				return state, RefusedTransition
			}
			if parent == nil {
				return state, RefusedParent
			}
			if !subset(change.Scopes, parent.Scopes) {
				return state, RefusedWiden
			}
			next.RotationParentID = change.RotationParentID
		}
		next.OrganizationID = event.Payload.OrganizationID
		next.SourceID = event.Payload.SourceID
		next.Status = StatusActive
		next.Fingerprint = change.Fingerprint
		next.Scopes = normalise(change.Scopes)
		next.ExpiresAt = expiry(change)
	case contracts.ExternalPushScoped:
		if state.Status != StatusActive || !sameIdentity(state, event) {
			return state, RefusedTransition
		}
		if !subset(change.Scopes, state.Scopes) {
			return state, RefusedWiden
		}
		next.Scopes = normalise(change.Scopes)
	case contracts.ExternalPushRevoked:
		if state.Status != StatusActive || !sameIdentity(state, event) {
			return state, RefusedTransition
		}
		next.Status = StatusRevoked
	case contracts.ExternalPushExpired:
		if state.Status != StatusActive || !sameIdentity(state, event) {
			return state, RefusedTransition
		}
		next.Status = StatusExpired
		next.ExpiresAt = expiry(change)
	default:
		return state, RefusedTransition
	}
	return next, Applied
}

func sameIdentity(state State, event contracts.ExternalPushLifecycleEvent) bool {
	return state.OrganizationID == event.Payload.OrganizationID && state.SourceID == event.Payload.SourceID
}

func expiry(change contracts.ExternalPushLifecycleChange) string {
	if change.ExpiresAt == nil {
		return ""
	}
	return change.ExpiresAt.UTC().Format("2006-01-02T15:04:05.000000Z")
}

func normalise(scopes []string) []string {
	out := slices.Clone(scopes)
	sort.Strings(out)
	return slices.Compact(out)
}

func subset(narrow, wide []string) bool {
	for _, scope := range narrow {
		if !slices.Contains(wide, scope) {
			return false
		}
	}
	return true
}
