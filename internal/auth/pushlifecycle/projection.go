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
	// RefusedParent: the parent the caller supplied is wrong (another
	// credential, organization or source, retired before the rotation, or in an
	// unknown state). It is final: a retry cannot change it.
	RefusedParent Outcome = "refused_parent"
	// ParentPending: a rotation's parent is not seen yet (the caller's nil).
	// Like Gap it is a retry: nothing is applied and the caller parks the event.
	ParentPending Outcome = "parent_pending"
)

// Outcomes is every Outcome, for exhaustive label sets and tests.
var Outcomes = []Outcome{Applied, Duplicate, Stale, Gap, RefusedTerminal, RefusedWiden, RefusedTransition, RefusedParent, ParentPending}

// Retry reports whether the event may apply later: a Gap fills when the missing
// revisions arrive, and a pending parent when the parent does. Every other
// outcome is final for this delivery.
func (o Outcome) Retry() bool { return o == Gap || o == ParentPending }

// State is one credential's projection. The zero value is a credential never
// seen. Revision is the last applied revision.
type State struct {
	AggregateID    string
	OrganizationID string
	Binding        string
	SourceID       string
	Revision       int64
	// Decided is the last revision above Revision that a final refusal decided,
	// with every revision between them decided too; a value at or below Revision
	// means none. The next revision is then Decided+1, judged against the last
	// applied state.
	Decided          int64
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
// only what it can check: the parent's id, organization, binding and source must equal
// the event's, or the outcome is RefusedParent; a nil parent is ParentPending. A parent
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
// A parent that has not been seen yet is the caller's nil: ParentPending, retry later.
//
// Order: the next revision is max(Revision, Decided)+1. A revoked or expired
// event that skips revisions applies to an active credential and is final (the
// revision jumps to its own; a skipped revision is then Stale); every other
// event that skips revisions, and a terminal one that would be refused, is a
// Gap. A final refusal of the next revision decides it (Decided moves, nothing
// else), so a refused revision never parks the later ones. The returned state is
// state itself unless the outcome is Applied or that refusal. Apply never panics
// and never returns Applied for a terminal credential.
func Apply(state State, event contracts.ExternalPushLifecycleEvent, parent *State) (State, Outcome) {
	revision := int64(event.Payload.Revision)
	frontier := max(state.Revision, state.Decided)
	switch {
	case revision < 1 || event.AggregateType != contracts.ExternalPushCredentialAggregate || event.AggregateID == "":
		return state, RefusedTransition
	case state.AggregateID != "" && event.AggregateID != state.AggregateID:
		return state, RefusedTransition
	case revision == state.Revision:
		return state, Duplicate
	case revision < state.Revision:
		return state, Stale
	case revision <= state.Decided:
		return state, Duplicate
	}
	next := revision == frontier+1
	switch {
	case state.Status.Terminal():
		return decide(state, event, next, RefusedTerminal)
	case state.Revision == 0 && state.Decided > 0:
		// The first revision was refused: the credential never existed, so no later event can apply.
		return decide(state, event, next, RefusedTransition)
	case !next && event.EventType != contracts.ExternalPushRevoked && event.EventType != contracts.ExternalPushExpired:
		return state, Gap
	}
	applied, outcome := transition(state, event, parent)
	switch {
	case outcome == Applied:
		return applied, Applied
	case !next:
		return state, Gap
	case outcome.Retry():
		return state, outcome
	}
	return decide(state, event, true, outcome)
}

// decide returns a final refusal; a refusal of the next revision moves Decided and nothing else.
func decide(state State, event contracts.ExternalPushLifecycleEvent, next bool, outcome Outcome) (State, Outcome) {
	if next {
		state.Decided = int64(event.Payload.Revision)
	}
	return state, outcome
}

// transition folds one event of the next revision, or a terminal event over a gap, into an active or unseen state.
func transition(state State, event contracts.ExternalPushLifecycleEvent, parent *State) (State, Outcome) {
	revision := int64(event.Payload.Revision)
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
				return state, ParentPending
			}
			if parent.AggregateID == "" || parent.AggregateID != change.RotationParentID ||
				parent.OrganizationID != event.Payload.OrganizationID || parent.Binding != event.Payload.Binding ||
				parent.SourceID != event.Payload.SourceID ||
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
		next.Binding = event.Payload.Binding
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
	return state.OrganizationID == event.Payload.OrganizationID && state.Binding == event.Payload.Binding &&
		state.SourceID == event.Payload.SourceID
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
