// Package orgactivity holds the one rule for the organization a login lands in
// when the user asks for none and holds several memberships: the requested
// organization, else the one with metrics data, then the newest metrics, then
// the earliest join. It is go-api's rule (internal/api/session selectActiveMembership);
// the auth plane applies it to the Postgres projection public.org_activity that
// the metrics finalize job writes, so the auth plane never reads ClickHouse.
package orgactivity

import (
	"time"

	"github.com/google/uuid"
)

// Activity is what is known of one organization's metrics.
type Activity struct {
	// HasData is true when any of the daily metrics tables holds a measurement row of the organization.
	HasData bool
	// Last is the newest computed_at over those rows; nil when there is none.
	Last *time.Time
	// Unknown means the activity could not be established (no projection row, a failed read): it ranks nobody.
	Unknown bool
}

// Membership is the part of a membership the choice reads.
type Membership struct {
	OrgID     uuid.UUID
	JoinedAt  *time.Time
	CreatedAt time.Time
}

var minTime = time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)

// AnyUnknown reports whether the activity of any membership is unknown.
func AnyUnknown(memberships []Membership, activities map[uuid.UUID]Activity) bool {
	for _, m := range memberships {
		if a, ok := activities[m.OrgID]; !ok || a.Unknown {
			return true
		}
	}
	return false
}

// Select returns the index of the chosen membership, or -1 for none.
//
// With a requested organization the answer is that organization's membership or
// none. Otherwise the memberships are ranked by (has data first, newest
// computed_at first, earliest join first) and the first of equal keys wins. When
// the activity of ANY membership is unknown, activity ranks nobody and the
// earliest join decides, so a missing fact can never send a login to another
// organization (go-api's rule, identical).
func Select(memberships []Membership, requested *uuid.UUID, activities map[uuid.UUID]Activity) int {
	if requested != nil {
		for index := range memberships {
			if memberships[index].OrgID == *requested {
				return index
			}
		}
		return -1
	}
	if len(memberships) == 0 {
		return -1
	}
	fallback := AnyUnknown(memberships, activities)
	type key struct {
		noData bool
		last   time.Time
		joined time.Time
	}
	keyOf := func(m Membership) key {
		a := activities[m.OrgID]
		last := minTime
		if a.Last != nil {
			last = *a.Last
		}
		joined := m.CreatedAt
		if m.JoinedAt != nil {
			joined = *m.JoinedAt
		}
		if fallback {
			return key{last: minTime, joined: joined.UTC()}
		}
		return key{noData: !a.HasData, last: last, joined: joined.UTC()}
	}
	less := func(a, b key) bool {
		if a.noData != b.noData {
			return !a.noData
		}
		if !a.last.Equal(b.last) {
			return a.last.After(b.last)
		}
		return a.joined.Before(b.joined)
	}
	best := 0
	bestKey := keyOf(memberships[0])
	for index := 1; index < len(memberships); index++ {
		if candidate := keyOf(memberships[index]); less(candidate, bestKey) {
			best, bestKey = index, candidate
		}
	}
	return best
}
