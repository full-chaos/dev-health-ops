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
	"time"

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
	// TerminalAt is the created_at of the revoked or expired event that ended
	// the credential; zero while it is active.
	TerminalAt time.Time
}

// Apply folds event into state. For a rotated event, parent is the projected
// State of the credential named by rotation_parent_id; the caller loads it from
// storage keyed by that id inside the event's organization, and Apply trusts
// only what it can check: the parent's id, organization and source must equal
// the event's, or the outcome is RefusedParent (so does a nil parent). A parent
// that was already retired before the rotation's created_at is RefusedParent,
// where retired means the earliest of its terminal event's created_at and its
// expires_at; one retired at or after it, in any arrival order, is accepted,
// because a rotation commits together with the retirement of its parent. An
// unknown status, or a terminal parent without a TerminalAt, is RefusedParent.
// Apply trusts the producer's created_at: a rotation stamped before its
// parent's retirement applies, so the producer must stamp from one clock. A
// The producer writes a rotated event and its parent's retirement in one
// transaction, each on its own aggregate; between the two events a consumer may
// see both credentials live, and that window is accepted.
// A parent that has not been seen yet is the caller's nil: retry later. The
// returned state is state itself unless the outcome is Applied. Apply never panics and never returns Applied for a
// terminal credential.
func Apply(state State, event contracts.ExternalPushLifecycleEvent, parent *State) (State, Outcome) {
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
			if parent == nil || parent.AggregateID == "" || parent.AggregateID != change.RotationParentID ||
				parent.OrganizationID != event.Payload.OrganizationID || parent.SourceID != event.Payload.SourceID ||
				parent.Revision < 1 {
				return state, RefusedParent
			}
			if !parentUsable(parent, event.CreatedAt.Time) {
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
		next.TerminalAt = event.CreatedAt.Time
	case contracts.ExternalPushExpired:
		if state.Status != StatusActive || !sameIdentity(state, event) {
			return state, RefusedTransition
		}
		next.Status = StatusExpired
		next.TerminalAt = event.CreatedAt.Time
		next.ExpiresAt = earlierExpiry(state.ExpiresAt, expiry(change))
	default:
		return state, RefusedTransition
	}
	return next, Applied
}

// Live reports whether the projected credential may be treated as live at now.
// A past expires_at ends liveness without waiting for the expired event, so a
// late event can never extend a credential. It fails closed: a credential never
// seen, a terminal or unknown status, or an unparseable ExpiresAt is not live.
// The caller supplies now; the core holds no clock.
func Live(state State, now time.Time) bool {
	if state.Revision < 1 || state.Status != StatusActive {
		return false
	}
	if state.ExpiresAt == "" {
		return true
	}
	expires, err := time.Parse(time.RFC3339Nano, state.ExpiresAt)
	if err != nil {
		return false
	}
	return now.Before(expires)
}

// Permits reports whether the credential is live at now and holds scope. A
// credential with no scopes permits nothing: empty means no scope, never all.
func Permits(state State, scope string, now time.Time) bool {
	return scope != "" && Live(state, now) && slices.Contains(state.Scopes, scope)
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

// parentUsable reports whether a rotation created at rotatedAt may build on
// parent. It fails closed: an unknown status, a terminal parent with no
// TerminalAt, or an unparseable ExpiresAt is not usable.
func parentUsable(parent *State, rotatedAt time.Time) bool {
	switch parent.Status {
	case StatusActive:
	case StatusRevoked, StatusExpired:
		if parent.TerminalAt.IsZero() {
			return false
		}
	default:
		return false
	}
	var retired []time.Time
	if !parent.TerminalAt.IsZero() {
		retired = append(retired, parent.TerminalAt)
	}
	if parent.ExpiresAt != "" {
		expires, err := time.Parse(time.RFC3339Nano, parent.ExpiresAt)
		if err != nil {
			return false
		}
		retired = append(retired, expires)
	}
	for _, at := range retired {
		if at.Before(rotatedAt) {
			return false
		}
	}
	return true
}

// earlierExpiry keeps the earlier of two expires_at values in the form
// expiry() writes, so the result does not depend on which event arrived first.
func earlierExpiry(stored, incoming string) string {
	if stored == "" {
		return incoming
	}
	if incoming == "" {
		return stored
	}
	a, errA := time.Parse(time.RFC3339Nano, stored)
	b, errB := time.Parse(time.RFC3339Nano, incoming)
	if errA != nil || errB != nil || !b.Before(a) {
		return stored
	}
	return incoming
}
