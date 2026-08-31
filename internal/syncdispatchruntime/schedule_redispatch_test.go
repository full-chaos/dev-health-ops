package syncdispatchruntime

import (
	"testing"
	"time"
)

// TestDueNowRearmAtNeverDelaysAnEarlierPendingWakeup pins the rule
// codex round 1 caught regressing on a68ca42d6 (P2): scheduleRedispatch writes
// ONE wakeup and unconditionally overwrites a still-pending row for its kind,
// so a pass that both left units outside the guard snapshot AND has a budget
// deferral pending must arm the EARLIER of the two. Arming the countdown
// unconditionally pushed an existing now+5s wakeup out to now+60s.
//
// Table-driven over the three shapes, because the failure mode is a single
// branch collapsing back to one side.
func TestDueNowRearmAtNeverDelaysAnEarlierPendingWakeup(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	countdown := redispatchCountdown()
	if countdown <= 0 {
		t.Fatalf("redispatchCountdown()=%s want a positive default", countdown)
	}
	countdownAt := now.Add(countdown)
	earlier := now.Add(countdown / 2)
	later := now.Add(countdown * 2)

	for _, testCase := range []struct {
		name           string
		nextDeferredAt *time.Time
		want           time.Time
		why            string
	}{
		{
			name: "no deferral pending", nextDeferredAt: nil, want: countdownAt,
			why: "nothing else is scheduled, so the snapshot-missed units set the wakeup",
		},
		{
			name: "deferral is earlier than the countdown", nextDeferredAt: &earlier, want: earlier,
			why: "arming the countdown here would OVERWRITE the earlier wakeup and delay it -- the regression",
		},
		{
			name: "deferral is later than the countdown", nextDeferredAt: &later, want: countdownAt,
			why: "the snapshot-missed units are due now and must not wait out a long budget backoff",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			got := dueNowRearmAt(testCase.nextDeferredAt, now)
			if got == nil {
				t.Fatalf("got nil; want %s -- nil means scheduleRedispatch falls back to the countdown implicitly, which is exactly the branch that must be explicit here", testCase.want)
			}
			if !got.Equal(testCase.want) {
				t.Fatalf("got=%s want=%s (%s)", got, testCase.want, testCase.why)
			}
		})
	}
}

// TestDueNowRearmAtDoesNotMutateItsInput pins that the caller's
// nextDeferredAt (shared with the budget result and read again by nothing
// downstream, but pointer-shared all the same) is never written through.
func TestDueNowRearmAtDoesNotMutateItsInput(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	deferral := now.Add(time.Second)
	original := deferral
	if got := dueNowRearmAt(&deferral, now); got == nil || !got.Equal(original) {
		t.Fatalf("got=%v want=%s", got, original)
	}
	if !deferral.Equal(original) {
		t.Fatalf("input mutated: %s -> %s", original, deferral)
	}
}
