package audit

import (
	"context"
	"errors"
	"maps"
	"slices"
	"testing"
)

const userTouched = "auth.user.touched"

// Commit refuses a public projection type as its own audit row with one exception: auth.user.touched off the stream
// as OffStreamPublicProfile (an admin PATCH that changes the platform role and no profile column). Each clause of
// the exception has a cell that fails without it.
func TestCommitAdmitsTheTouchOnlyWithThePublicProfileOffStream(t *testing.T) {
	const id = "0b6f1c3e-8e4f-4b7a-9d2a-3c5e7f9a1b2d"
	mutation := func(eventType, offStream string) Mutation {
		m := Mutation{
			Apply: func(context.Context, TxOps) error { return nil },
			Audit: AuditEvent{EventType: eventType, Outcome: OutcomeAllowed},
		}
		if offStream == "" {
			m.Events = []OutboxEvent{{AggregateType: "principal", AggregateID: "prn_" + id, EventType: "principal.created",
				Change: map[string]any{"status": "active"}}}
		}
		m.OffStream = offStream
		return m
	}
	for _, c := range []struct {
		name, eventType, offStream string
		admitted                   bool
	}{
		{"touched, public profile off-stream", userTouched, OffStreamPublicProfile, true},
		{"touched, another off-stream", userTouched, OffStreamSessionLifecycle, false},
		{"touched, on the stream", userTouched, "", false},
		{"profile_updated, public profile off-stream", EventUserProfileUpdated, OffStreamPublicProfile, false},
	} {
		err := mutation(c.eventType, c.offStream).validate()
		switch {
		case c.admitted && err != nil:
			t.Errorf("%s: validate = %v, want it admitted", c.name, err)
		case !c.admitted && !errors.Is(err, ErrMutationFailed):
			t.Errorf("%s: validate = %v, want ErrMutationFailed", c.name, err)
		}
	}
	also := mutation(userTouched, OffStreamPublicProfile)
	also.Audit = AuditEvent{EventType: "auth.user.updated", Outcome: OutcomeAllowed}
	also.Also = []AuditEvent{{EventType: userTouched, Outcome: OutcomeAllowed}}
	if err := also.validate(); !errors.Is(err, ErrMutationFailed) {
		t.Errorf("touched as an Also row: validate = %v, want ErrMutationFailed (only the mutation's own row is admitted)", err)
	}
}

// The public projection event types are the reviewed set: a type is added here only in review, with its writer.
func TestPublicProjectionEventTypesAreTheReviewedSet(t *testing.T) {
	got := slices.Sorted(maps.Keys(publicProjectionEventTypes))
	want := []string{EventUserProfileUpdated, userTouched}
	if !slices.Equal(got, want) {
		t.Errorf("publicProjectionEventTypes = %v, want %v", got, want)
	}
}
