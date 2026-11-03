// Package pushreconcile is the pure core of the reconciliation of the External
// Push credential projection against the domain store (CHAOS-10897). It does no
// I/O, reads no clock and writes nothing: it compares two sets and names the
// drift, so the job around it can never change a credential or a projection.
//
// It compares effective state (organization, source, status, scopes, expiry),
// never revisions: after CHAOS-12699 a terminal event applies over a revision
// gap and a refused revision only moves State.Decided, so the projected
// revision legitimately differs from the domain's aggregate revision.
package pushreconcile

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/pushlifecycle"
)

// Class is a closed drift class; its values are safe as metric labels.
type Class string

const (
	// ClassMissing: the domain holds the credential and the projection holds none.
	ClassMissing Class = "missing"
	// ClassExtra: the projection holds a non-terminal credential the domain does not.
	ClassExtra Class = "extra"
	// ClassStatus: the projection reads live what the domain ended, or ends what the domain holds live.
	ClassStatus Class = "status"
	// ClassScopes: the scope sets differ, in either direction.
	ClassScopes Class = "scopes"
	// ClassExpiry: the expiry instants differ.
	ClassExpiry Class = "expiry"
	// ClassBinding: the organization or the source differs.
	ClassBinding Class = "binding"
	// ClassUnwireable: the domain row cannot be carried on the lifecycle wire.
	ClassUnwireable Class = "unwireable"
)

// Classes is every Class, for label sets and tests.
var Classes = []Class{ClassMissing, ClassExtra, ClassStatus, ClassScopes, ClassExpiry, ClassBinding, ClassUnwireable}

// Token is the expected projected state of one domain token. Organization is
// the canonical lower-case organization UUID; SourceID is empty for an
// organization-wide token. Unwireable marks a row the producer cannot carry on
// the wire; the other fields are then not compared.
type Token struct {
	ID           string
	Organization string
	SourceID     string
	Scopes       []string
	ExpiresAt    *time.Time
	Revoked      bool
	Unwireable   bool
}

// Projected is one credential_projection row of the external_push domain.
type Projected struct {
	ID           string
	Organization string
	SourceID     string
	Status       pushlifecycle.Status
	Scopes       []string
	ExpiresAt    *time.Time
}

// Finding is one drift of one credential.
type Finding struct {
	Class Class
	ID    string
}

var (
	// ErrDuplicate reports one id twice in one input: a broken walk, not drift.
	ErrDuplicate = errors.New("pushreconcile: duplicate credential id")
	// ErrUnknownStatus reports a status outside the closed set: a wrong shape, not "no drift".
	ErrUnknownStatus = errors.New("pushreconcile: unknown status")
)

func knownStatus(s pushlifecycle.Status) bool {
	return s == pushlifecycle.StatusActive || s == pushlifecycle.StatusRevoked || s == pushlifecycle.StatusExpired
}

// Diff compares the domain tokens with the projection at now. Not drift: a
// terminal projected credential the domain no longer holds; a projected
// "active" credential past its expiry (Live already fails it closed); both
// sides terminal, whichever kind; any revision difference. Findings are sorted
// by id, then class.
func Diff(domain []Token, projected []Projected, now time.Time) ([]Finding, error) {
	return nil, nil
}

// statusDrifts compares liveness. A revoked domain token with a live
// projection, and a live domain token with a revoked projection or with an
// expired one before its expiry, drift. Both terminal never drifts (Apply
// refuses the second terminal event). A live projection past expiry never
// drifts: Live ends it without the event.
func statusDrifts(t Token, r Projected, now time.Time) bool {
	if t.Revoked {
		return r.Status == pushlifecycle.StatusActive
	}
	switch r.Status {
	case pushlifecycle.StatusRevoked:
		return true
	case pushlifecycle.StatusExpired:
		return t.ExpiresAt == nil || now.Before(*t.ExpiresAt)
	}
	return false
}

func sameScopes(a, b []string) bool {
	return slices.Equal(normalise(a), normalise(b))
}

func normalise(s []string) []string {
	out := slices.Clone(s)
	sort.Strings(out)
	return slices.Compact(out)
}

// sameInstant compares at microsecond precision, the precision the projection stores.
func sameInstant(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.UTC().Truncate(time.Microsecond).Equal(b.UTC().Truncate(time.Microsecond))
}

// Confirm keeps the findings of the first pass that the second pass, run after
// a delay, reports again with the same class. Only those are alerted: an event
// that landed between the passes clears a finding.
func Confirm(first, second []Finding) []Finding { return nil }

var (
	_ = fmt.Sprint
	_ = strings.Compare
)
