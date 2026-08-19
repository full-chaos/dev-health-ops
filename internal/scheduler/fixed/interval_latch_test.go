package fixed

import (
	"testing"
	"time"
)

func intervalSkipSchedule() Schedule {
	return Schedule{
		ID: "sync_coverage_refresh", Native: true,
		Cadence: EveryInterval(300 * time.Second), Timezone: "UTC",
		CatchUp: CatchUpSkip, UniquenessWindow: time.Hour,
		TargetKind: "system.sync_coverage_refresh", ProducerID: "sync_coverage_refresh",
		MaxAttempts: 3, AlertThreshold: 30 * time.Minute,
		Rationale: "regression fixture for CHAOS-3914",
	}
}

// An interval schedule with CatchUpSkip must keep producing work.
//
// CHAOS-3914: counting grid boundaries to decide staleness made this latch on
// the cold-start baseline forever. The interval guard defers the first run to
// the boundary after anchor.ObservedAt+period -- a process almost never starts
// exactly on a grid point -- so scheduledFor lands two boundaries past the
// anchor and a boundary count calls that "stale" on a scheduler that missed
// nothing. In production sync_coverage_refresh recorded 314 baselines and 1
// materialized occurrence, and the coverage projector never ran.
func TestIntervalSkipScheduleKeepsMaterializing(t *testing.T) {
	t.Parallel()
	schedule := intervalSkipSchedule()
	// Start 7s after a grid point: an ordinary process start, not a contrived one.
	start := time.Date(2026, 8, 18, 12, 0, 7, 0, time.UTC)

	var anchor *Anchor
	materialized, baselines := 0, 0
	for i := 0; i < 4*60*4; i++ { // 4 hours of 15s polls, the loop's default
		now := start.Add(time.Duration(i) * 15 * time.Second)
		decision, err := DueOccurrence(schedule, now, anchor)
		if err != nil {
			t.Fatal(err)
		}
		if decision.Occurrence == nil {
			continue
		}
		if decision.ColdStart || decision.SkippedStale {
			baselines++
		} else {
			materialized++
		}
		// The engine records every emitted occurrence, baseline or not.
		anchor = &Anchor{
			ScheduledFor: decision.Occurrence.ScheduledFor,
			ObservedAt:   decision.Occurrence.ObservedAt,
		}
	}

	if materialized == 0 {
		t.Fatalf("interval schedule never materialized in 4h: baselines=%d", baselines)
	}
	// One cold-start baseline is expected; everything after it is real work.
	if baselines != 1 {
		t.Errorf("baselines = %d, want exactly 1 (the cold start)", baselines)
	}

	// 4h at a 300s cadence spans 48 boundaries, so ~47 runs would be exact
	// pacing. We get about half that, and the shortfall is a SEPARATE defect
	// this fix deliberately does not change: the guard above measures the next
	// eligible instant from the anchor's OBSERVED time, which lands a poll
	// jitter after the boundary, so `earliest` falls just past the following
	// grid point and the schedule waits for the one after that. Effective rate
	// is therefore half the nominal cadence -- a 300s schedule runs every 600s.
	//
	// Correcting that means measuring from the anchor's scheduled boundary
	// instead, which breaks TestIntervalScheduleWaitsAFullPeriodAfterItsBaseline:
	// after a 10:02 cold start the next grid point is 10:05, and firing three
	// minutes after activation is the regression that test exists to prevent.
	// Distinguishing the cold-start anchor from a steady-state one needs the
	// ledger to expose whether the last occurrence was a baseline, which is a
	// contract change rather than a bug fix. Asserted here so the dilation is
	// recorded rather than mistaken for correct pacing.
	const boundariesIn4h = 48
	if materialized < boundariesIn4h/2-2 {
		t.Errorf("materialized = %d in 4h, want at least %d (every other boundary)",
			materialized, boundariesIn4h/2-2)
	}
	if materialized > boundariesIn4h-4 {
		t.Logf("materialized = %d: pacing improved beyond every-other-boundary; "+
			"if the observation-anchored guard was fixed, tighten this bound", materialized)
	}
}

// The staleness skip must still fire after a REAL outage, otherwise this fix
// would trade a latch for a backdated run.
func TestIntervalSkipScheduleStillSkipsAfterOutage(t *testing.T) {
	t.Parallel()
	schedule := intervalSkipSchedule()
	anchorAt := time.Date(2026, 8, 18, 12, 0, 7, 0, time.UTC)
	anchor := &Anchor{
		ScheduledFor: time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC),
		ObservedAt:   anchorAt,
	}

	// The first boundary the guard releases (12:10, since `earliest` is
	// 12:05:07 and 12:05 falls before it) must produce work, not a stale
	// re-baseline. This is the case that latched before CHAOS-3914.
	decision, err := DueOccurrence(schedule, mustUTC("2026-08-18T12:10:05Z"), anchor)
	if err != nil {
		t.Fatal(err)
	}
	if decision.Occurrence == nil || decision.SkippedStale {
		t.Fatalf("first released boundary: occurrence=%v skippedStale=%v, want work",
			decision.Occurrence != nil, decision.SkippedStale)
	}

	// An hour of downtime is more than two periods: resume without claiming
	// the missed buckets ran.
	decision, err = DueOccurrence(schedule, anchorAt.Add(time.Hour), anchor)
	if err != nil {
		t.Fatal(err)
	}
	if decision.Occurrence == nil || !decision.SkippedStale {
		t.Fatalf("after an hour gap: occurrence=%v skippedStale=%v, want a stale re-baseline",
			decision.Occurrence != nil, decision.SkippedStale)
	}
}

func mustUTC(value string) time.Time {
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		panic(err)
	}
	return parsed.UTC()
}
