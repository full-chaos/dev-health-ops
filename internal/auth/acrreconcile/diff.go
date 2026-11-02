// Package acrreconcile is the pure core of the daily reconciliation of the ACP
// acr credential projection against the acr listing (CHAOS-11199). It does no
// I/O, reads no clock and writes nothing: it compares two sets and names the
// drift, so the job around it can never change a credential or a projection.
package acrreconcile

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/pushlifecycle"
)

// Class is a closed drift class; its values are safe as metric labels.
type Class string

const (
	// ClassMissing: acr lists the credential and the projection holds none.
	ClassMissing Class = "missing"
	// ClassExtra: the projection holds an active credential acr does not list.
	ClassExtra Class = "extra"
	// ClassStale: the revisions differ, in either direction.
	ClassStale Class = "stale"
	// ClassStatus: the revisions are equal and the statuses differ.
	ClassStatus Class = "status"
)

// Listed is one row of the acr listing (credential_lifecycle_list.v1).
type Listed struct {
	ID       string
	Revision int64
	Status   pushlifecycle.Status
}

// Projected is one credential_projection row of the acr domain.
type Projected struct {
	ID        string
	Revision  int64
	Status    pushlifecycle.Status
	ExpiresAt *time.Time
}

// Finding is one drift of one credential.
type Finding struct {
	Class Class
	ID    string
}

var (
	// ErrDuplicate reports one id twice in one input: a broken walk, not drift.
	ErrDuplicate = errors.New("acrreconcile: duplicate credential id")
	// ErrUnknownStatus reports a status outside the closed set: a wrong shape, not "no drift".
	ErrUnknownStatus = errors.New("acrreconcile: unknown status")
)

func knownStatus(s pushlifecycle.Status) bool {
	return s == pushlifecycle.StatusActive || s == pushlifecycle.StatusRevoked || s == pushlifecycle.StatusExpired
}

// Diff compares the listing with the projection. Not drift: a projected
// terminal credential acr no longer lists (acr reaps its rows), and a
// projection that holds "expired" while acr still reads "active" within one
// lifecycle tick of the expiry (acr writes the expired event on its next tick).
// Findings are sorted by id.
func Diff(listed []Listed, projected []Projected, now time.Time, tick time.Duration) ([]Finding, error) {
	l := make(map[string]Listed, len(listed))
	for _, r := range listed {
		if !knownStatus(r.Status) {
			return nil, fmt.Errorf("%w: %q", ErrUnknownStatus, r.Status)
		}
		if _, dup := l[r.ID]; dup {
			return nil, fmt.Errorf("%w (listing)", ErrDuplicate)
		}
		l[r.ID] = r
	}
	p := make(map[string]Projected, len(projected))
	for _, r := range projected {
		if _, dup := p[r.ID]; dup {
			return nil, fmt.Errorf("%w (projection)", ErrDuplicate)
		}
		p[r.ID] = r
	}
	var out []Finding
	for id, lr := range l {
		pr, ok := p[id]
		switch {
		case !ok:
			out = append(out, Finding{ClassMissing, id})
		case lr.Revision != pr.Revision:
			out = append(out, Finding{ClassStale, id})
		case lr.Status != pr.Status && !withinExpiryTick(lr, pr, now, tick):
			out = append(out, Finding{ClassStatus, id})
		}
	}
	for id, pr := range p {
		if _, ok := l[id]; !ok && !pr.Status.Terminal() {
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

func withinExpiryTick(l Listed, p Projected, now time.Time, tick time.Duration) bool {
	return l.Status == pushlifecycle.StatusActive && p.Status == pushlifecycle.StatusExpired &&
		p.ExpiresAt != nil && now.Sub(*p.ExpiresAt) <= tick
}

// Confirm keeps the findings of the first pass that the second pass, run after
// a delay, reports again with the same class. Only those are alerted: an event
// or a first-touch row that landed between the passes clears a finding.
func Confirm(first, second []Finding) []Finding {
	var out []Finding
	for _, f := range first {
		if slices.Contains(second, f) {
			out = append(out, f)
		}
	}
	return out
}
