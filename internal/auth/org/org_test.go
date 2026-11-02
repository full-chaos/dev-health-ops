package org

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

func TestCreateRefusesBadSlugBeforeAnyWrite(t *testing.T) {
	for _, slug := range []string{"", "a", "-ab", "Ab", "a_b", "über", strings.Repeat("a", 64), "a b"} {
		if err := ValidateSlug(slug); !errors.Is(err, ErrSlugInvalid) {
			t.Errorf("ValidateSlug(%q) = %v, want ErrSlugInvalid", slug, err)
		}
	}
	for _, slug := range []string{"ab", "acme", "a-b-1", strings.Repeat("a", 63)} {
		if err := ValidateSlug(slug); err != nil {
			t.Errorf("ValidateSlug(%q) = %v", slug, err)
		}
	}
	called := false
	s := newTestStore(func(context.Context, audit.Mutation) error { called = true; return nil })
	if _, err := s.Create(context.Background(), CreateInput{Name: "Acme", Slug: "Bad Slug"}, Actor{}); !errors.Is(err, ErrSlugInvalid) {
		t.Fatalf("Create = %v, want ErrSlugInvalid", err)
	}
	if _, err := s.Create(context.Background(), CreateInput{Name: "  ", Slug: "acme"}, Actor{}); !errors.Is(err, ErrNameInvalid) {
		t.Fatalf("Create = %v, want ErrNameInvalid", err)
	}
	if called {
		t.Fatal("a refused input reached the committer")
	}
}

func TestCreateNameIsStrippedAndBoundedTo100Runes(t *testing.T) {
	got, err := NormalizeName("  Acme Inc \n")
	if err != nil || got != "Acme Inc" {
		t.Fatalf("NormalizeName = %q, %v", got, err)
	}
	if _, err := NormalizeName(strings.Repeat("é", 100)); err != nil {
		t.Fatalf("100 runes refused: %v", err)
	}
	if _, err := NormalizeName(strings.Repeat("é", 101)); !errors.Is(err, ErrNameInvalid) {
		t.Fatalf("101 runes = %v, want ErrNameInvalid", err)
	}
	if _, err := NormalizeName(" \t "); !errors.Is(err, ErrNameInvalid) {
		t.Fatalf("blank = %v, want ErrNameInvalid", err)
	}
}

func TestRenameOnSuspendedOrgIsRefused(t *testing.T) {
	_, err := decide(verbRename, state{name: "A", active: false}, "B")
	if !errors.Is(err, ErrSuspended) {
		t.Fatalf("decide = %v, want ErrSuspended", err)
	}
	d, err := decide(verbRename, state{name: "A", active: true}, "A")
	if err != nil || !d.noop {
		t.Fatalf("same-name rename = %+v, %v; want noop", d, err)
	}
	d, err = decide(verbRename, state{name: "A", active: true}, "B")
	if err != nil || d.noop || d.next.name != "B" || !d.next.active {
		t.Fatalf("rename = %+v, %v", d, err)
	}
}

func TestTransitionTableAllowsOnlyActiveSuspendedPairs(t *testing.T) {
	cases := []struct {
		v          verb
		from       bool
		noop       bool
		nextActive bool
	}{
		{verbSuspend, true, false, false},
		{verbSuspend, false, true, false},
		{verbReactivate, false, false, true},
		{verbReactivate, true, true, true},
	}
	for _, c := range cases {
		d, err := decide(c.v, state{name: "A", active: c.from}, "")
		if err != nil || d.noop != c.noop || d.next.active != c.nextActive || d.next.name != "A" {
			t.Errorf("decide(%s, active=%v) = %+v, %v", c.v, c.from, d, err)
		}
	}
}

func TestRepeatedSuspendIsNoopWithoutEventOrRevisionBump(t *testing.T) {
	s := newTestStore(func(context.Context, audit.Mutation) error { return audit.ErrNoStateChange })
	if err := s.Suspend(context.Background(), uuid.New(), nil, Actor{}); err != nil {
		t.Fatalf("Suspend on a no-change commit = %v, want nil", err)
	}
	if got := outcomeLabel(audit.ErrNoStateChange); got != "noop" {
		t.Fatalf("outcomeLabel(ErrNoStateChange) = %q", got)
	}
}

func TestMetricLabelsAreClosedSetsWithoutIDs(t *testing.T) {
	closed := map[string]bool{"ok": true, "noop": true, "not_found": true, "slug_taken": true, "conflict": true,
		"invalid": true, "suspended": true, "denied": true, "error": true}
	for _, err := range []error{nil, audit.ErrNoStateChange, ErrNotFound, ErrSlugTaken, ErrRevisionConflict,
		ErrSlugInvalid, ErrNameInvalid, ErrSuspended, ErrDenied, ErrStore, errors.New("uuid " + uuid.NewString())} {
		got := outcomeLabel(err)
		if !closed[got] {
			t.Errorf("outcomeLabel(%v) = %q, outside the closed set", err, got)
		}
	}
	if outcomeLabel(ErrDenied) != "denied" {
		t.Error("outcomeLabel(ErrDenied) must be denied")
	}
	if outcomeLabel(nil) != "ok" || outcomeLabel(ErrSlugTaken) != "slug_taken" || outcomeLabel(errors.New("x")) != "error" {
		t.Error("outcomeLabel mapping is wrong")
	}
}

func TestEventsCarryNoNameAndOnlyAllowlistedChange(t *testing.T) {
	id := uuid.New()
	for _, v := range []verb{verbCreate, verbRename, verbSuspend, verbReactivate} {
		ev := eventFor(v, id, "acme")
		if ev.AggregateType != "organization" || ev.AggregateID != provisioning.WireID("organization", id.String()) {
			t.Errorf("%s: aggregate = %s/%s", v, ev.AggregateType, ev.AggregateID)
		}
		if err := provisioning.CheckEvent(ev.AggregateType, ev.AggregateID, ev.EventType, ev.Change); err != nil {
			t.Errorf("%s: %v", v, err)
		}
		if _, ok := ev.Change["name"]; ok {
			t.Errorf("%s: the event carries the name", v)
		}
		if (v == verbCreate) != (ev.Change["slug"] != nil) {
			t.Errorf("%s: slug presence = %v", v, ev.Change["slug"])
		}
	}
	want := map[verb]string{verbCreate: "organization.created", verbRename: "organization.updated",
		verbSuspend: "organization.suspended", verbReactivate: "organization.reactivated"}
	for v, w := range want {
		if got := eventFor(v, id, "acme").EventType; got != w {
			t.Errorf("%s: event type %q, want %q", v, got, w)
		}
	}
}

func TestAuditRowsPassTheClosedAttributeRules(t *testing.T) {
	id := uuid.New()
	actor := uuid.New()
	for _, v := range []verb{verbCreate, verbRename, verbSuspend, verbReactivate} {
		a := auditFor(v, id, Actor{PrincipalID: &actor}, StatusActive, StatusSuspended)
		if err := audit.ValidateAttributes(a.Attributes); err != nil {
			t.Errorf("%s: %v", v, err)
		}
		if !strings.HasPrefix(a.EventType, "auth.organization.") || a.OrganizationID == nil || *a.OrganizationID != id.String() {
			t.Errorf("%s: audit = %+v", v, a)
		}
	}
}

func TestStaleIfRevisionIsRefusedAndWritesNothing(t *testing.T) {
	two, three := int64(2), int64(3)
	if err := checkRevision(3, &two); !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("stale = %v", err)
	}
	if err := checkRevision(3, &three); err != nil {
		t.Fatalf("equal = %v", err)
	}
	if err := checkRevision(3, nil); err != nil {
		t.Fatalf("absent = %v", err)
	}
}

func allowAll(context.Context, audit.TxOps, uuid.UUID) error { return nil }

// A nil Authorize refuses before the commit is built: no row is read and no
// write is planned.
func TestReactivateAuthorizedWithoutAuthorizeIsDeniedBeforeAnyCommit(t *testing.T) {
	committed := false
	s := newTestStore(func(context.Context, audit.Mutation) error { committed = true; return nil })
	changed, err := s.ReactivateAuthorized(context.Background(), uuid.New(), nil, Actor{}, nil)
	if !errors.Is(err, ErrDenied) || changed || committed {
		t.Fatalf("nil Authorize: changed=%v err=%v committed=%v, want denied and no commit", changed, err, committed)
	}
}

func TestReactivateAuthorizedReportsChangedAndNoop(t *testing.T) {
	for name, tc := range map[string]struct {
		commitErr error
		changed   bool
	}{"a change": {nil, true}, "an already active org": {audit.ErrNoStateChange, false}} {
		s := newTestStore(func(context.Context, audit.Mutation) error { return tc.commitErr })
		changed, err := s.ReactivateAuthorized(context.Background(), uuid.New(), nil, Actor{}, allowAll)
		if err != nil || changed != tc.changed {
			t.Fatalf("%s: changed=%v err=%v, want changed=%v and no error", name, changed, err, tc.changed)
		}
	}
}

func TestReactivateAuthorizedAnswersDenialAndMissingOrgAsTheyAre(t *testing.T) {
	for _, want := range []error{ErrDenied, ErrNotFound, ErrRevisionConflict} {
		s := newTestStore(func(context.Context, audit.Mutation) error { return want })
		_, err := s.ReactivateAuthorized(context.Background(), uuid.New(), nil, Actor{}, allowAll)
		if !errors.Is(err, want) || errors.Is(err, ErrStore) {
			t.Fatalf("commit error %v came back as %v", want, err)
		}
	}
	s := newTestStore(func(context.Context, audit.Mutation) error { return errors.New("down") })
	if _, err := s.ReactivateAuthorized(context.Background(), uuid.New(), nil, Actor{}, allowAll); !errors.Is(err, ErrStore) {
		t.Fatalf("an unknown commit error = %v, want ErrStore", err)
	}
}

// The mutation is the one Reactivate plans: the same audit row and event, with
// the actor on the audit row.
func TestReactivateAuthorizedPlansTheReactivateAuditAndEvent(t *testing.T) {
	var got audit.Mutation
	s := newTestStore(func(_ context.Context, m audit.Mutation) error { got = m; return nil })
	org, actor := uuid.New(), uuid.New()
	if _, err := s.ReactivateAuthorized(context.Background(), org, nil, Actor{PrincipalID: &actor}, allowAll); err != nil {
		t.Fatal(err)
	}
	if got.Audit.EventType != "auth.organization.reactivated" || got.Audit.ActorPrincipalID == nil || *got.Audit.ActorPrincipalID != actor.String() ||
		got.Audit.Attributes["reason_code"] != "organization_updated" || got.Audit.Attributes["previous_status"] != "suspended" ||
		got.Audit.Attributes["new_status"] != "active" {
		t.Fatalf("audit = %+v", got.Audit)
	}
	if len(got.Events) != 1 || got.Events[0].EventType != "organization.reactivated" || got.Events[0].Change["status"] != "active" {
		t.Fatalf("events = %+v", got.Events)
	}
}

// A nil Authorize refuses before the commit is built: no row is read and no write is planned.
func TestSuspendAuthorizedWithoutAuthorizeIsDeniedBeforeAnyCommit(t *testing.T) {
	committed := false
	s := newTestStore(func(context.Context, audit.Mutation) error { committed = true; return nil })
	changed, err := s.SuspendAuthorized(context.Background(), uuid.New(), nil, Actor{}, nil)
	if !errors.Is(err, ErrDenied) || changed || committed {
		t.Fatalf("nil Authorize: changed=%v err=%v committed=%v, want denied and no commit", changed, err, committed)
	}
}

func TestSuspendAuthorizedReportsChangedAndNoop(t *testing.T) {
	for name, tc := range map[string]struct {
		commitErr error
		changed   bool
	}{"a change": {nil, true}, "an already suspended org": {audit.ErrNoStateChange, false}} {
		s := newTestStore(func(context.Context, audit.Mutation) error { return tc.commitErr })
		changed, err := s.SuspendAuthorized(context.Background(), uuid.New(), nil, Actor{}, allowAll)
		if err != nil || changed != tc.changed {
			t.Fatalf("%s: changed=%v err=%v, want changed=%v and no error", name, changed, err, tc.changed)
		}
	}
}

func TestSuspendAuthorizedAnswersDenialAndMissingOrgAsTheyAre(t *testing.T) {
	for _, want := range []error{ErrDenied, ErrNotFound, ErrRevisionConflict} {
		s := newTestStore(func(context.Context, audit.Mutation) error { return want })
		_, err := s.SuspendAuthorized(context.Background(), uuid.New(), nil, Actor{}, allowAll)
		if !errors.Is(err, want) || errors.Is(err, ErrStore) {
			t.Fatalf("commit error %v came back as %v", want, err)
		}
	}
	s := newTestStore(func(context.Context, audit.Mutation) error { return errors.New("down") })
	if _, err := s.SuspendAuthorized(context.Background(), uuid.New(), nil, Actor{}, allowAll); !errors.Is(err, ErrStore) {
		t.Fatalf("an unknown commit error = %v, want ErrStore", err)
	}
}

// The mutation is the one Suspend plans: the same audit row and event, with the actor on the audit row.
func TestSuspendAuthorizedPlansTheSuspendAuditAndEvent(t *testing.T) {
	var got audit.Mutation
	s := newTestStore(func(_ context.Context, m audit.Mutation) error { got = m; return nil })
	org, actor := uuid.New(), uuid.New()
	if _, err := s.SuspendAuthorized(context.Background(), org, nil, Actor{PrincipalID: &actor}, allowAll); err != nil {
		t.Fatal(err)
	}
	if got.Audit.EventType != "auth.organization.suspended" || got.Audit.ActorPrincipalID == nil || *got.Audit.ActorPrincipalID != actor.String() ||
		got.Audit.Attributes["reason_code"] != "organization_updated" || got.Audit.Attributes["previous_status"] != "active" ||
		got.Audit.Attributes["new_status"] != "suspended" {
		t.Fatalf("audit = %+v", got.Audit)
	}
	if len(got.Events) != 1 || got.Events[0].EventType != "organization.suspended" || got.Events[0].Change["status"] != "suspended" {
		t.Fatalf("events = %+v", got.Events)
	}
}

// A suspend that finds a session busy runs again, a bounded number of times, then answers ErrStore.
func TestSuspendRetriesBusySessionsThenAnswersErrStore(t *testing.T) {
	attempts := 0
	s := newTestStore(func(context.Context, audit.Mutation) error { attempts++; return errSessionsBusy })
	changed, err := s.SuspendAuthorized(context.Background(), uuid.New(), nil, Actor{}, allowAll)
	if !errors.Is(err, ErrStore) || changed || attempts != session.BusyAttempts {
		t.Fatalf("changed=%v err=%v attempts=%d, want ErrStore after %d attempts", changed, err, attempts, session.BusyAttempts)
	}
	attempts = 0
	s = newTestStore(func(context.Context, audit.Mutation) error {
		attempts++
		if attempts < session.BusyAttempts {
			return errSessionsBusy
		}
		return nil
	})
	if changed, err := s.SuspendAuthorized(context.Background(), uuid.New(), nil, Actor{}, allowAll); err != nil || !changed || attempts != session.BusyAttempts {
		t.Fatalf("changed=%v err=%v attempts=%d, want the last attempt to win", changed, err, attempts)
	}
}

func TestABusySuspendIsADomainRefusalOfItsCommit(t *testing.T) {
	var got audit.Mutation
	s := newTestStore(func(_ context.Context, m audit.Mutation) error { got = m; return nil })
	if _, err := s.SuspendAuthorized(context.Background(), uuid.New(), nil, Actor{}, allowAll); err != nil {
		t.Fatal(err)
	}
	if len(got.Refusals) != 1 || !errors.Is(got.Refusals[0], errSessionsBusy) {
		t.Fatalf("refusals %v", got.Refusals)
	}
}

// A nil Authorize or a bad name refuses before the commit is built.
func TestRenameAuthorizedRefusesBeforeAnyCommit(t *testing.T) {
	committed := false
	s := newTestStore(func(context.Context, audit.Mutation) error { committed = true; return nil })
	if changed, err := s.RenameAuthorized(context.Background(), uuid.New(), "Name", nil, Actor{}, nil); !errors.Is(err, ErrDenied) || changed {
		t.Fatalf("nil Authorize: changed=%v err=%v, want denied", changed, err)
	}
	for _, name := range []string{"   ", strings.Repeat("x", 101)} {
		if _, err := s.RenameAuthorized(context.Background(), uuid.New(), name, nil, Actor{}, allowAll); !errors.Is(err, ErrNameInvalid) {
			t.Fatalf("name %q: err=%v, want ErrNameInvalid", name, err)
		}
	}
	if committed {
		t.Fatal("a refused rename reached the commit")
	}
}

// The mutation is the one Rename plans: the renamed audit row with the actor, and organization.updated with no name.
func TestRenameAuthorizedPlansTheRenameAuditAndEvent(t *testing.T) {
	var got audit.Mutation
	s := newTestStore(func(_ context.Context, m audit.Mutation) error { got = m; return nil })
	actor := uuid.New()
	changed, err := s.RenameAuthorized(context.Background(), uuid.New(), "New", nil, Actor{PrincipalID: &actor}, allowAll)
	if err != nil || !changed {
		t.Fatalf("rename: changed=%v err=%v", changed, err)
	}
	if got.Audit.EventType != "auth.organization.renamed" || got.Audit.ActorPrincipalID == nil || *got.Audit.ActorPrincipalID != actor.String() {
		t.Fatalf("audit = %+v", got.Audit)
	}
	if len(got.Events) != 1 || got.Events[0].EventType != "organization.updated" || len(got.Events[0].Change) != 0 {
		t.Fatalf("events = %+v", got.Events)
	}
}
