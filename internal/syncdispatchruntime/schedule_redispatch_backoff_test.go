package syncdispatchruntime

import (
	"testing"
	"time"
)

const backoffTestRun = "3c9fa3a1-ebe6-56c1-9cd2-578728198674"

func TestBackoffCountdownGrowsWithIdleAndStaysInBounds(t *testing.T) {
	base, limit := 10*time.Second, 300*time.Second
	for _, idle := range []time.Duration{0, 9 * time.Second, 10 * time.Second, 30 * time.Second, 70 * time.Second, 150 * time.Second, 600 * time.Second, 48 * time.Hour} {
		got, level := backoffCountdown(base, limit, idle, backoffTestRun)
		if got < base || got > limit {
			t.Fatalf("idle=%s got=%s outside [%s,%s]", idle, got, base, limit)
		}
		if idle < base && (level != 0 || got != base) {
			t.Fatalf("idle=%s got=%s level=%d want base and level 0", idle, got, level)
		}
		if idle >= base && level == 0 {
			t.Fatalf("idle=%s level=0 want a back-off", idle)
		}
		if idle == 48*time.Hour && got < limit*8/10 {
			t.Fatalf("a long idle run got %s want near the %s limit", got, limit)
		}
	}
	if got, _ := backoffCountdown(base, limit, 150*time.Second, backoffTestRun); got <= 2*base {
		t.Fatalf("idle 150s got %s want far above the base", got)
	}
}

func TestBackoffCountdownIsDeterministicPerRunAndSpreadsRuns(t *testing.T) {
	base, limit := 10*time.Second, 300*time.Second
	a1, _ := backoffCountdown(base, limit, 100*time.Second, backoffTestRun)
	a2, _ := backoffCountdown(base, limit, 100*time.Second, backoffTestRun)
	if a1 != a2 {
		t.Fatalf("same run, same level: %s vs %s", a1, a2)
	}
	seen := map[time.Duration]bool{}
	for _, run := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		got, _ := backoffCountdown(base, limit, 100*time.Second, run)
		if got < base || got > limit {
			t.Fatalf("run %s got %s outside bounds", run, got)
		}
		seen[got] = true
	}
	if len(seen) < 3 {
		t.Fatalf("8 runs gave %d distinct delays; want a spread", len(seen))
	}
}

func TestBackoffMaxBelowBaseClampsToBase(t *testing.T) {
	t.Setenv("SYNC_DISPATCH_REDISPATCH_COUNTDOWN", "60")
	t.Setenv("SYNC_DISPATCH_REDISPATCH_COUNTDOWN_MAX", "20")
	base := redispatchCountdown()
	limit := redispatchCountdownMax(base)
	if limit != base {
		t.Fatalf("limit=%s want base %s", limit, base)
	}
	got, _ := countdownForIdle(base, limit, 24*time.Hour, backoffTestRun)
	if got != base {
		t.Fatalf("got %s want exactly the base %s when max < base", got, base)
	}
}

func TestBackoffMaxDefaultsAndUsesConfiguredBase(t *testing.T) {
	t.Setenv("SYNC_DISPATCH_REDISPATCH_COUNTDOWN", "10")
	t.Setenv("SYNC_DISPATCH_REDISPATCH_COUNTDOWN_MAX", "")
	base := redispatchCountdown()
	if base != 10*time.Second {
		t.Fatalf("base=%s want the configured 10s", base)
	}
	if limit := redispatchCountdownMax(base); limit != 300*time.Second {
		t.Fatalf("default limit=%s want 300s", limit)
	}
	t.Setenv("SYNC_DISPATCH_REDISPATCH_COUNTDOWN_MAX", "not-a-number")
	if limit := redispatchCountdownMax(base); limit != 300*time.Second {
		t.Fatalf("bad value limit=%s want the 300s default", limit)
	}
	t.Setenv("SYNC_DISPATCH_REDISPATCH_COUNTDOWN", "")
	if base := redispatchCountdown(); base != 60*time.Second {
		t.Fatalf("code default base=%s want 60s", base)
	}
}

func TestUnitEndResetsTheBackoffOnTheNextArm(t *testing.T) {
	base, limit := 10*time.Second, 300*time.Second
	now := time.Date(2026, 10, 21, 14, 0, 0, 0, time.UTC)
	stalled, stalledAttrs := countdownForIdle(base, limit, now.Sub(now.Add(-2*time.Hour)), backoffTestRun)
	if stalled <= base || len(stalledAttrs) == 0 {
		t.Fatalf("stalled run got %s attrs=%d want a back-off with log fields", stalled, len(stalledAttrs))
	}
	afterEnd, endAttrs := countdownForIdle(base, limit, now.Sub(now), backoffTestRun)
	if afterEnd != base || endAttrs != nil {
		t.Fatalf("a unit just ended: got %s attrs=%v want base and no back-off fields", afterEnd, endAttrs)
	}
}

func TestBackoffNeverDelaysAnEarlierDeferral(t *testing.T) {
	now := time.Date(2026, 10, 21, 14, 0, 0, 0, time.UTC)
	backoff := 300 * time.Second
	earlier := now.Add(5 * time.Second)
	later := now.Add(time.Hour)
	if got := dueNowRearmAtAfter(&earlier, now, backoff); got == nil || !got.Equal(earlier) {
		t.Fatalf("got %v want the earlier deferral %s", got, earlier)
	}
	if got := dueNowRearmAtAfter(&later, now, backoff); got == nil || !got.Equal(now.Add(backoff)) {
		t.Fatalf("got %v want now+backoff %s", got, now.Add(backoff))
	}
	if got := dueNowRearmAtAfter(nil, now, backoff); got == nil || !got.Equal(now.Add(backoff)) {
		t.Fatalf("got %v want now+backoff", got)
	}
}
