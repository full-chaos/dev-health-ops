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
// terminal projected credential the domain no longer holds; any pair of
// credentials that are both non-live at now, whatever the kind; any revision
// difference. One finding per credential at most (see classify). Findings are
// sorted by id.
func Diff(domain []Token, projected []Projected, now time.Time) ([]Finding, error) {
	d := make(map[string]Token, len(domain))
	for _, t := range domain {
		if _, dup := d[t.ID]; dup {
			return nil, fmt.Errorf("%w (domain)", ErrDuplicate)
		}
		d[t.ID] = t
	}
	p := make(map[string]Projected, len(projected))
	for _, r := range projected {
		if !knownStatus(r.Status) {
			return nil, fmt.Errorf("%w: %q (projection)", ErrUnknownStatus, r.Status)
		}
		if _, dup := p[r.ID]; dup {
			return nil, fmt.Errorf("%w (projection)", ErrDuplicate)
		}
		p[r.ID] = r
	}
	var out []Finding
	for id, t := range d {
		if t.Unwireable {
			out = append(out, Finding{ClassUnwireable, id})
			continue
		}
		r, ok := p[id]
		if !ok {
			out = append(out, Finding{ClassMissing, id})
			continue
		}
		if class := classify(t, r, now); class != "" {
			out = append(out, Finding{class, id})
		}
	}
	for id, r := range p {
		if _, ok := d[id]; !ok && !r.Status.Terminal() {
			out = append(out, Finding{ClassExtra, id})
		}
	}
	slices.SortFunc(out, func(a, b Finding) int {
		if c := strings.Compare(a.ID, b.ID); c != 0 {
			return c
		}
		return strings.Compare(string(a.Class), string(b.Class))
	})
	return out, nil
}

// classify returns the one drift class of a credential present on both sides,
// or "" for none. Precedence, highest first: status, scopes, binding, expiry.
// A credential never gets two findings: the class of the most serious
// difference is the one an operator must act on first, and the next pass names
// the rest once it is fixed. missing, extra and unwireable are decided before
// this call and are exclusive.
func classify(t Token, r Projected, now time.Time) Class {
	switch {
	case domainLive(t, now) != projectedLive(r, now):
		return ClassStatus
	case !sameScopes(t.Scopes, r.Scopes):
		return ClassScopes
	case t.Organization != r.Organization || t.SourceID != r.SourceID:
		return ClassBinding
	case !sameInstant(t.ExpiresAt, r.ExpiresAt):
		return ClassExpiry
	}
	return ""
}

// domainLive and projectedLive are each side's effective liveness at now:
// not revoked or expired by state, and not past its own expiry. A projected
// "active" credential past its expiry is not live (Live fails it closed), so it
// agrees with a domain token that is revoked or past expiry. A difference of
// the expiry instant itself stays in ClassExpiry.
func domainLive(t Token, now time.Time) bool {
	return !t.Revoked && (t.ExpiresAt == nil || now.Before(*t.ExpiresAt))
}

func projectedLive(r Projected, now time.Time) bool {
	return r.Status == pushlifecycle.StatusActive && (r.ExpiresAt == nil || now.Before(*r.ExpiresAt))
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
func Confirm(first, second []Finding) []Finding {
	var out []Finding
	for _, f := range first {
		if slices.Contains(second, f) {
			out = append(out, f)
		}
	}
	return out
}
