package sync

import (
	"testing"
	"time"
)

func TestInitialBackfillWindow(t *testing.T) {
	created := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	intPtr := func(v int) *int { return &v }
	cases := []struct {
		name           string
		depth, horizon int
		tierCap        *int
		wantOK         bool
		wantSinceDays  int
		wantBeforeDays int
	}{
		{"default horizon", 30, 365, nil, true, 365, 30},
		{"tier cap lowers the horizon", 30, 365, intPtr(90), true, 90, 30},
		{"cap equals depth", 30, 365, intPtr(30), false, 0, 0},
		{"cap below depth", 30, 365, intPtr(7), false, 0, 0},
		{"horizon equals depth", 30, 30, nil, false, 0, 0},
	}
	for _, tc := range cases {
		since, before, ok := initialBackfillWindow(created, tc.depth, tc.horizon, tc.tierCap)
		if ok != tc.wantOK {
			t.Errorf("%s: ok = %t, want %t", tc.name, ok, tc.wantOK)
			continue
		}
		if !ok {
			continue
		}
		if want := created.AddDate(0, 0, -tc.wantSinceDays); !since.Equal(want) {
			t.Errorf("%s: since = %s, want %s", tc.name, since, want)
		}
		if want := created.AddDate(0, 0, -tc.wantBeforeDays); !before.Equal(want) {
			t.Errorf("%s: before = %s, want %s", tc.name, before, want)
		}
	}
}

func TestInitialBackfillHorizonDaysEnv(t *testing.T) {
	t.Setenv(initialBackfillHorizonEnv, "")
	if got := initialBackfillHorizonDays(); got != initialBackfillDefaultHorizon {
		t.Errorf("empty = %d", got)
	}
	t.Setenv(initialBackfillHorizonEnv, "90")
	if got := initialBackfillHorizonDays(); got != 90 {
		t.Errorf("90 = %d", got)
	}
	t.Setenv(initialBackfillHorizonEnv, "-3")
	if got := initialBackfillHorizonDays(); got != initialBackfillDefaultHorizon {
		t.Errorf("-3 = %d", got)
	}
}

func TestInitialBackfillSkipCountedOncePerFirstSuccess(t *testing.T) {
	enqueuer := newInitialBackfillEnqueuer(nil)
	candidate := initialBackfillCandidate{configID: "c1", completedAt: time.Unix(100, 0)}
	if !enqueuer.countSkipOnce(candidate, InitialBackfillSkipCapNotAboveDepth) {
		t.Fatal("first skip not counted")
	}
	if enqueuer.countSkipOnce(candidate, InitialBackfillSkipCapNotAboveDepth) {
		t.Fatal("repeat skip counted twice")
	}
	if enqueuer.outcomes[InitialBackfillSkipCapNotAboveDepth] != 1 {
		t.Fatalf("outcomes = %v", enqueuer.outcomes)
	}
	enqueuer.pruneSeen(nil)
	if len(enqueuer.seen) != 0 {
		t.Fatalf("seen not pruned: %v", enqueuer.seen)
	}
}
