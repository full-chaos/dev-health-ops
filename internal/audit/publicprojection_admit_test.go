package audit

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"maps"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
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

// The public projection event types are the reviewed set, each with its off-stream reason: a type is added here only in
// review, with its writer.
func TestPublicProjectionEventTypesAreTheReviewedSet(t *testing.T) {
	want := map[string]string{
		EventUserProfileUpdated: OffStreamPublicProfile, userTouched: OffStreamPublicProfile,
		EventSSOProviderCreated: OffStreamSSOProviderConfig, EventSSOProviderUpdated: OffStreamSSOProviderConfig,
		EventSSOProviderDeleted: OffStreamSSOProviderConfig, EventSSOProviderActivated: OffStreamSSOProviderConfig,
		EventSSOProviderDeactivated: OffStreamSSOProviderConfig,
	}
	if !maps.Equal(publicProjectionEventTypes, want) {
		t.Errorf("publicProjectionEventTypes = %v, want %v", publicProjectionEventTypes, want)
	}
	for typ, reason := range publicProjectionEventTypes {
		if !offStreamReasons[reason] {
			t.Errorf("%s: off-stream reason %q is not in offStreamReasons", typ, reason)
		}
	}
}

// The existing callers pass no options: a failure is still one ERROR line, as before the options existed. With the
// failure named in Refusals the same failure is one WARN line instead.
func TestCommitPublicProjectionLogsAFailureAtErrorAndARefusalAtWarn(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	event := AuditEvent{EventType: userTouched, Outcome: OutcomeAllowed}
	apply := func(context.Context, TxOps) (int64, error) { return 1, nil }
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	err = CommitPublicProjection(context.Background(), nil, schema, event, apply)
	if !errors.Is(err, ErrMutationFailed) || !strings.Contains(logs.String(), "level=ERROR") || strings.Contains(logs.String(), "level=WARN") {
		t.Fatalf("no options: err %v, logs %q, want one ERROR line", err, logs.String())
	}
	logs.Reset()
	err = CommitPublicProjectionWith(context.Background(), nil, schema, event, apply, PublicProjectionOptions{Refusals: []error{ErrMutationFailed}})
	if !errors.Is(err, ErrMutationFailed) || !strings.Contains(logs.String(), "level=WARN") || strings.Contains(logs.String(), "level=ERROR") {
		t.Fatalf("with the failure as a refusal: err %v, logs %q, want one WARN line", err, logs.String())
	}
}
