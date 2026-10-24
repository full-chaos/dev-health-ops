package org

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
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
		"invalid": true, "suspended": true, "error": true}
	for _, err := range []error{nil, audit.ErrNoStateChange, ErrNotFound, ErrSlugTaken, ErrRevisionConflict,
		ErrSlugInvalid, ErrNameInvalid, ErrSuspended, ErrStore, errors.New("uuid " + uuid.NewString())} {
		got := outcomeLabel(err)
		if !closed[got] {
			t.Errorf("outcomeLabel(%v) = %q, outside the closed set", err, got)
		}
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
