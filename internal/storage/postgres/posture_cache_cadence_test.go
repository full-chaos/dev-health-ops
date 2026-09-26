package postgres

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// CHAOS-6937. Prod ran this check every ~10 s per replica, 0.9-3 s each (14 s
// at worst), which held 10-30% of a 4-connection domain pool and made other
// stages wait for a connection until their budgets ran out. A healthy role is
// re-proven every five minutes, with per-pass jitter so replicas do not
// refresh together.

func TestPostureCadenceDefaultsAreFiveMinutesWithJitterAndThreeTTLsOfSlack(t *testing.T) {
	if defaultPostureTTL != 300*time.Second {
		t.Fatalf("defaultPostureTTL = %s, want 300 s (CHAOS-6937)", defaultPostureTTL)
	}
	if defaultPostureMaxStale < 2*defaultPostureTTL {
		t.Fatalf("defaultPostureMaxStale = %s, want room for two unanswered refreshes past the %s TTL",
			defaultPostureMaxStale, defaultPostureTTL)
	}
	check := newCachedPostureCheck("r", func(context.Context) error { return nil }, PostureCheckOptions{})
	if check.ttl != defaultPostureTTL || check.maxStale != defaultPostureMaxStale || check.jitter != defaultPostureJitter {
		t.Fatalf("zero options did not take the defaults: ttl=%s maxStale=%s jitter=%v", check.ttl, check.maxStale, check.jitter)
	}
}

// The refresh moves inside [ttl*(1-jitter), ttl]: never later than the TTL (it
// is the bound on staleness), and never earlier than the jitter allows.
func TestPostureRefreshIsJitteredWithinItsBoundsAndNeverLaterThanTheTTL(t *testing.T) {
	const ttl = 100 * time.Second
	for _, tc := range []struct {
		name   string
		draw   float64
		option float64
		want   time.Duration // the pass's freshness window
	}{
		{"no shortening", 0, 0.2, 100 * time.Second},
		{"largest shortening", 0.999999, 0.2, 80 * time.Second},
		{"middle", 0.5, 0.2, 90 * time.Second},
		{"disabled", 0.9, -1, 100 * time.Second},
		{"clamped above one", 0.5, 7, 50 * time.Second},
		{"a draw outside [0,1) is ignored", 1.5, 0.2, 100 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var runs atomic.Int32
			check, clock := newTestCachedPostureCheck(func(context.Context) error {
				runs.Add(1)
				return nil
			}, PostureCheckOptions{TTL: ttl, MaxStale: 10 * ttl, Jitter: tc.option, Rand: func() float64 { return tc.draw }})
			if err := check.Check(context.Background()); err != nil {
				t.Fatal(err)
			}
			// One tick before the window: served fresh, no refresh started.
			clock.Advance(tc.want - time.Second)
			if err := check.Check(context.Background()); err != nil {
				t.Fatal(err)
			}
			time.Sleep(20 * time.Millisecond)
			if got := runs.Load(); got != 1 {
				t.Fatalf("runs = %d before the %s window closed, want 1", got, tc.want)
			}
			// One tick after: still a pass, and exactly one refresh starts.
			clock.Advance(2 * time.Second)
			if err := check.Check(context.Background()); err != nil {
				t.Fatal(err)
			}
			waitFor(t, "the refresh once the window closed", func() bool { return runs.Load() == 2 })
		})
	}
}

// The window is drawn per pass, not once: two passes that draw differently get
// different windows, so replicas that started together drift apart.
func TestPostureJitterIsDrawnAfreshForEveryPass(t *testing.T) {
	draws := []float64{0, 0.999999}
	var next atomic.Int32
	check, clock := newTestCachedPostureCheck(func(context.Context) error { return nil },
		PostureCheckOptions{TTL: 100 * time.Second, MaxStale: time.Hour, Jitter: 0.2,
			Rand: func() float64 { return draws[int(next.Add(1)-1)%len(draws)] }})
	if err := check.Check(context.Background()); err != nil {
		t.Fatal(err)
	}
	check.mu.Lock()
	first := check.passTTL
	check.mu.Unlock()
	clock.Advance(101 * time.Second)
	if err := check.Check(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the second pass", func() bool {
		check.mu.Lock()
		defer check.mu.Unlock()
		return check.flight == nil && check.passTTL != first
	})
	check.mu.Lock()
	second := check.passTTL
	check.mu.Unlock()
	if first != 100*time.Second || second < 80*time.Second || second > 80*time.Second+time.Millisecond {
		t.Fatalf("pass windows = %s then %s, want 100s then ~80s from the two draws", first, second)
	}
}

// CHAOS-6937 r1 P1: the 300 s cadence is the ROLE-POSTURE cadence (provisioned grants
// change at provisioning time). A cached check that is not a role-posture statement -- the
// River schema, the queued contract versions -- can change after a pass, so it keeps the
// generic 30 s / 5 min freshness unless its caller opts into the posture cadence.
func TestGenericRunChecksKeepThirtySecondFreshnessAndOnlyRolePostureRunsGetTheFiveMinuteCadence(t *testing.T) {
	generic := NewCachedRunCheck("river_schema", func(context.Context) error { return nil }, PostureCheckOptions{})
	if generic.ttl != 30*time.Second || generic.maxStale != 5*time.Minute {
		t.Fatalf("a generic run check has ttl=%s maxStale=%s, want 30s/5m", generic.ttl, generic.maxStale)
	}
	posture := NewCachedRunCheck("queue_postgres", func(context.Context) error { return nil }, AsRolePosture(PostureCheckOptions{}))
	if posture.ttl != defaultPostureTTL || posture.maxStale != defaultPostureMaxStale {
		t.Fatalf("a role-posture run check has ttl=%s maxStale=%s, want the posture defaults", posture.ttl, posture.maxStale)
	}
	// An explicit choice always wins.
	explicit := NewCachedRunCheck("x", func(context.Context) error { return nil },
		AsRolePosture(PostureCheckOptions{TTL: 7 * time.Second, MaxStale: 9 * time.Second}))
	if explicit.ttl != 7*time.Second || explicit.maxStale != 9*time.Second {
		t.Fatalf("explicit options were overridden: ttl=%s maxStale=%s", explicit.ttl, explicit.maxStale)
	}
}

// The reviewer's scenario, executed: a generic readiness result that changes after a pass
// (an unsupported contract version appears) is seen once its 30 s window closes, not 5 minutes
// later.
func TestAGenericRunCheckSeesAChangedResultAfterThirtySecondsNotFiveMinutes(t *testing.T) {
	var changed atomic.Bool
	var runs atomic.Int32
	check := NewCachedRunCheck("queued_contract_versions", func(context.Context) error {
		runs.Add(1)
		if changed.Load() {
			return errors.Join(ErrPostureRefused, errors.New("unsupported contract"))
		}
		return nil
	}, PostureCheckOptions{Jitter: -1})
	clock := &postureClock{now: time.Unix(1_700_000_000, 0)}
	check.now = clock.Now
	if err := check.Check(context.Background()); err != nil {
		t.Fatal(err)
	}
	changed.Store(true)
	clock.Advance(31 * time.Second)
	_ = check.CheckNoWait() // stale pass served, refresh starts
	waitFor(t, "the refresh after the 30 s window", func() bool { return runs.Load() == 2 })
	waitFor(t, "the refusal", func() bool { return errors.Is(check.CheckNoWait(), ErrPostureRefused) })
}
