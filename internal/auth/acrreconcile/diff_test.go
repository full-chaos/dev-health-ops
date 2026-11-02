package acrreconcile

import (
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/pushlifecycle"
)

var (
	now  = time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	tick = 5 * time.Minute
)

func listed(id string, rev int64, st pushlifecycle.Status) Listed {
	return Listed{ID: id, Revision: rev, Status: st}
}

func projected(id string, rev int64, st pushlifecycle.Status) Projected {
	return Projected{ID: id, Revision: rev, Status: st}
}

func diff(t *testing.T, l []Listed, p []Projected) []Finding {
	t.Helper()
	got, err := Diff(l, p, now, tick)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	return got
}

func want(t *testing.T, got []Finding, w ...Finding) {
	t.Helper()
	if len(got) != len(w) {
		t.Fatalf("findings = %v, want %v", got, w)
	}
	for i := range w {
		if got[i] != w[i] {
			t.Fatalf("finding %d = %v, want %v", i, got[i], w[i])
		}
	}
}

func TestNoDrift(t *testing.T) {
	got := diff(t,
		[]Listed{listed("a", 3, pushlifecycle.StatusActive), listed("b", 2, pushlifecycle.StatusRevoked)},
		[]Projected{projected("a", 3, pushlifecycle.StatusActive), projected("b", 2, pushlifecycle.StatusRevoked)})
	want(t, got)
}

func TestMissing(t *testing.T) {
	got := diff(t, []Listed{listed("a", 1, pushlifecycle.StatusActive)}, nil)
	want(t, got, Finding{Class: ClassMissing, ID: "a"})
}

func TestMissingTerminalStillMissing(t *testing.T) {
	got := diff(t, []Listed{listed("a", 2, pushlifecycle.StatusRevoked)}, nil)
	want(t, got, Finding{Class: ClassMissing, ID: "a"})
}

func TestExtraActive(t *testing.T) {
	got := diff(t, nil, []Projected{projected("a", 1, pushlifecycle.StatusActive)})
	want(t, got, Finding{Class: ClassExtra, ID: "a"})
}

func TestExtraTerminalIsReapedNotDrift(t *testing.T) {
	got := diff(t, nil, []Projected{
		projected("a", 2, pushlifecycle.StatusRevoked),
		projected("b", 3, pushlifecycle.StatusExpired),
	})
	want(t, got)
}

func TestStaleRevisionProjectionBehind(t *testing.T) {
	got := diff(t, []Listed{listed("a", 4, pushlifecycle.StatusActive)}, []Projected{projected("a", 2, pushlifecycle.StatusActive)})
	want(t, got, Finding{Class: ClassStale, ID: "a"})
}

func TestStaleRevisionProjectionAhead(t *testing.T) {
	got := diff(t, []Listed{listed("a", 2, pushlifecycle.StatusActive)}, []Projected{projected("a", 4, pushlifecycle.StatusActive)})
	want(t, got, Finding{Class: ClassStale, ID: "a"})
}

func TestStaleWinsOverStatus(t *testing.T) {
	got := diff(t, []Listed{listed("a", 4, pushlifecycle.StatusRevoked)}, []Projected{projected("a", 2, pushlifecycle.StatusActive)})
	want(t, got, Finding{Class: ClassStale, ID: "a"})
}

func TestStatusMismatchSameRevision(t *testing.T) {
	got := diff(t, []Listed{listed("a", 2, pushlifecycle.StatusRevoked)}, []Projected{projected("a", 2, pushlifecycle.StatusActive)})
	want(t, got, Finding{Class: ClassStatus, ID: "a"})
}

func TestActiveListedPastExpiryInsideOneTickIsNotDrift(t *testing.T) {
	exp := now.Add(-tick / 2)
	p := projected("a", 2, pushlifecycle.StatusExpired)
	p.ExpiresAt = &exp
	got := diff(t, []Listed{listed("a", 2, pushlifecycle.StatusActive)}, []Projected{p})
	want(t, got)
}

func TestActiveListedPastExpiryBeyondOneTickIsDrift(t *testing.T) {
	exp := now.Add(-2 * tick)
	p := projected("a", 2, pushlifecycle.StatusExpired)
	p.ExpiresAt = &exp
	got := diff(t, []Listed{listed("a", 2, pushlifecycle.StatusActive)}, []Projected{p})
	want(t, got, Finding{Class: ClassStatus, ID: "a"})
}

func TestActivePastExpiryBothActiveIsNotDrift(t *testing.T) {
	exp := now.Add(-time.Hour)
	p := projected("a", 1, pushlifecycle.StatusActive)
	p.ExpiresAt = &exp
	got := diff(t, []Listed{listed("a", 1, pushlifecycle.StatusActive)}, []Projected{p})
	want(t, got)
}

func TestFindingsSortedByIDThenClass(t *testing.T) {
	got := diff(t,
		[]Listed{listed("c", 1, pushlifecycle.StatusActive), listed("a", 1, pushlifecycle.StatusActive)},
		[]Projected{projected("b", 1, pushlifecycle.StatusActive)})
	want(t, got,
		Finding{Class: ClassMissing, ID: "a"},
		Finding{Class: ClassExtra, ID: "b"},
		Finding{Class: ClassMissing, ID: "c"})
}

func TestDuplicateIDIsAnError(t *testing.T) {
	_, err := Diff([]Listed{listed("a", 1, pushlifecycle.StatusActive), listed("a", 1, pushlifecycle.StatusActive)}, nil, now, tick)
	if !errors.Is(err, ErrDuplicate) {
		t.Fatalf("err = %v, want ErrDuplicate", err)
	}
	_, err = Diff(nil, []Projected{projected("a", 1, pushlifecycle.StatusActive), projected("a", 1, pushlifecycle.StatusActive)}, now, tick)
	if !errors.Is(err, ErrDuplicate) {
		t.Fatalf("err = %v, want ErrDuplicate", err)
	}
}

func TestUnknownListedStatusIsAnErrorNotNoDrift(t *testing.T) {
	_, err := Diff([]Listed{listed("a", 1, "frozen")}, []Projected{projected("a", 1, pushlifecycle.StatusActive)}, now, tick)
	if !errors.Is(err, ErrUnknownStatus) {
		t.Fatalf("err = %v, want ErrUnknownStatus", err)
	}
}

func TestConfirmKeepsOnlyFindingsInBothPasses(t *testing.T) {
	p1 := []Finding{{ClassMissing, "a"}, {ClassExtra, "b"}, {ClassStale, "c"}}
	p2 := []Finding{{ClassExtra, "b"}, {ClassStale, "c"}, {ClassStatus, "d"}}
	want(t, Confirm(p1, p2), Finding{ClassExtra, "b"}, Finding{ClassStale, "c"})
}

func TestConfirmMidWalkInsertDoesNotAlert(t *testing.T) {
	// pass 1 saw X listed and not yet projected (missing); the event landed before pass 2.
	pass1 := diff(t, []Listed{listed("x", 1, pushlifecycle.StatusActive)}, nil)
	pass2 := diff(t, []Listed{listed("x", 1, pushlifecycle.StatusActive)}, []Projected{projected("x", 1, pushlifecycle.StatusActive)})
	want(t, Confirm(pass1, pass2))
}

func TestConfirmClassMustMatch(t *testing.T) {
	want(t, Confirm([]Finding{{ClassMissing, "a"}}, []Finding{{ClassStale, "a"}}))
}

func TestConfirmEmptyPassTwoClearsAll(t *testing.T) {
	want(t, Confirm([]Finding{{ClassMissing, "a"}}, nil))
	want(t, Confirm(nil, []Finding{{ClassMissing, "a"}}))
}

func TestConfirmKeepsOrder(t *testing.T) {
	p := []Finding{{ClassMissing, "a"}, {ClassMissing, "b"}}
	want(t, Confirm(p, p), p...)
}

func TestClassNamesAreClosedMetricLabels(t *testing.T) {
	for c, name := range map[Class]string{ClassMissing: "missing", ClassExtra: "extra", ClassStale: "stale", ClassStatus: "status"} {
		if string(c) != name {
			t.Fatalf("class %q, want %q", c, name)
		}
	}
}

func TestExpiryToleranceHoldsOnlyForAnExpiredProjection(t *testing.T) {
	exp := now.Add(-tick / 2)
	p := projected("a", 2, pushlifecycle.StatusRevoked)
	p.ExpiresAt = &exp
	got := diff(t, []Listed{listed("a", 2, pushlifecycle.StatusActive)}, []Projected{p})
	want(t, got, Finding{Class: ClassStatus, ID: "a"})
}

func TestExpiryToleranceHoldsOnlyForAnActiveListing(t *testing.T) {
	exp := now.Add(-tick / 2)
	p := projected("a", 2, pushlifecycle.StatusExpired)
	p.ExpiresAt = &exp
	got := diff(t, []Listed{listed("a", 2, pushlifecycle.StatusRevoked)}, []Projected{p})
	want(t, got, Finding{Class: ClassStatus, ID: "a"})
}

func TestExpiryToleranceNeedsAnExpiry(t *testing.T) {
	got := diff(t, []Listed{listed("a", 2, pushlifecycle.StatusActive)}, []Projected{projected("a", 2, pushlifecycle.StatusExpired)})
	want(t, got, Finding{Class: ClassStatus, ID: "a"})
}
