package joboutbox

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

// scriptedStrandRepair returns one scripted result per call and records when
// it was called, so a test can assert both THAT a pass ran and WHEN.
type scriptedStrandRepair struct {
	next  func(call int) (StrandRepairResult, error)
	calls []time.Time
}

func (repair *scriptedStrandRepair) Step(_ context.Context, now time.Time, _ int) (StrandRepairResult, error) {
	repair.calls = append(repair.calls, now)
	if repair.next == nil {
		return StrandRepairResult{}, nil
	}
	return repair.next(len(repair.calls))
}

func newTestIdleBackoff(t *testing.T, inner StrandRepairStepper) *IdleBackoffStrandRepair {
	t.Helper()
	backoff, err := NewIdleBackoffStrandRepair(inner, DefaultStrandRepairIdleCeiling)
	if err != nil {
		t.Fatal(err)
	}
	return backoff
}

// workResults is one StrandRepairResult per field that counts as work, each
// with ONLY that field set. It is derived from the result type, so a field
// added later appears here without anyone remembering to add it.
func workResults(t *testing.T) map[string]StrandRepairResult {
	t.Helper()
	results := map[string]StrandRepairResult{}
	resultType := reflect.TypeOf(StrandRepairResult{})
	for index := 0; index < resultType.NumField(); index++ {
		field := resultType.Field(index)
		if strandPassLevelFields[field.Name] {
			continue
		}
		value := reflect.New(resultType).Elem()
		switch field.Type.Kind() {
		case reflect.Int:
			value.Field(index).SetInt(1)
		case reflect.Bool:
			value.Field(index).SetBool(true)
		case reflect.Slice:
			value.Field(index).Set(reflect.MakeSlice(field.Type, 1, 1))
		default:
			t.Fatalf("StrandRepairResult.%s has kind %s, which this test cannot set; teach "+
				"workResults and strandPassSawNothing about it together", field.Name, field.Type.Kind())
		}
		results[field.Name] = value.Interface().(StrandRepairResult)
	}
	if len(results) == 0 {
		t.Fatal("no work fields were derived from StrandRepairResult; every case below would be vacuous")
	}
	return results
}

func TestNewIdleBackoffStrandRepairRejectsUnusableConfiguration(t *testing.T) {
	inner := &scriptedStrandRepair{}
	for name, build := range map[string]func() (*IdleBackoffStrandRepair, error){
		"nil stepper": func() (*IdleBackoffStrandRepair, error) {
			return NewIdleBackoffStrandRepair(nil, DefaultStrandRepairIdleCeiling)
		},
		"ceiling below the floor": func() (*IdleBackoffStrandRepair, error) {
			return NewIdleBackoffStrandRepair(inner, strandRepairIdleFloor-time.Nanosecond)
		},
		"ceiling past the maximum": func() (*IdleBackoffStrandRepair, error) {
			return NewIdleBackoffStrandRepair(inner, maxStrandRepairIdleCeiling+time.Nanosecond)
		},
	} {
		if _, err := build(); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("%s: error = %v, want ErrInvalidConfiguration", name, err)
		}
	}
	for _, ceiling := range []time.Duration{strandRepairIdleFloor, maxStrandRepairIdleCeiling} {
		if _, err := NewIdleBackoffStrandRepair(inner, ceiling); err != nil {
			t.Fatalf("ceiling %s rejected: %v", ceiling, err)
		}
	}
	var missing *IdleBackoffStrandRepair
	if _, err := missing.Step(context.Background(), time.Now(), 1); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("nil backoff Step error = %v, want ErrInvalidConfiguration", err)
	}
}

// TestDefaultStrandRepairIdleCeilingIsThirtySeconds pins the one number the
// repair-latency argument in the constant's own comment rests on. Raising it
// is a design change, not a tuning change.
func TestDefaultStrandRepairIdleCeilingIsThirtySeconds(t *testing.T) {
	if DefaultStrandRepairIdleCeiling != 30*time.Second {
		t.Fatalf("DefaultStrandRepairIdleCeiling = %s, want 30s: this is the whole latency the "+
			"idle backoff may add to a strand repair", DefaultStrandRepairIdleCeiling)
	}
	if strandRepairIdleFloor != 2*time.Second {
		t.Fatalf("strandRepairIdleFloor = %s, want 2s", strandRepairIdleFloor)
	}
}

// TestIdleBackoffSpacesOutIdlePassesAndSaturatesAtTheCeiling drives one tick a
// second for three minutes over a repair that never finds anything, and
// asserts the exact instants a survey ran. Asserting the instants rather than
// a call count is what pins all three properties at once: the wait doubles, it
// starts at the floor, and it never exceeds the ceiling.
func TestIdleBackoffSpacesOutIdlePassesAndSaturatesAtTheCeiling(t *testing.T) {
	inner := &scriptedStrandRepair{}
	backoff := newTestIdleBackoff(t, inner)
	start := time.Date(2026, time.October, 3, 12, 0, 0, 0, time.UTC)
	skipped := 0
	for second := 0; second <= 180; second++ {
		result, err := backoff.Step(context.Background(), start.Add(time.Duration(second)*time.Second), 100)
		if err != nil {
			t.Fatal(err)
		}
		if result.PassSkippedIdle {
			skipped++
		}
	}
	// Waits of 2, 4, 8, 16, then 30 (32 saturated), 30, 30, ...
	wantSeconds := []int{0, 2, 6, 14, 30, 60, 90, 120, 150, 180}
	if len(inner.calls) != len(wantSeconds) {
		t.Fatalf("the repair surveyed %d times in 181 ticks, want %d: %v",
			len(inner.calls), len(wantSeconds), inner.calls)
	}
	for index, want := range wantSeconds {
		if got := int(inner.calls[index].Sub(start) / time.Second); got != want {
			t.Fatalf("survey %d ran at +%ds, want +%ds (all: %v)", index, got, want, inner.calls)
		}
	}
	if skipped != 181-len(wantSeconds) {
		t.Fatalf("%d passes reported PassSkippedIdle, want %d: a held-back pass must say so",
			skipped, 181-len(wantSeconds))
	}
	for index := 1; index < len(inner.calls); index++ {
		if gap := inner.calls[index].Sub(inner.calls[index-1]); gap > DefaultStrandRepairIdleCeiling {
			t.Fatalf("surveys %d and %d are %s apart, past the %s ceiling",
				index-1, index, gap, DefaultStrandRepairIdleCeiling)
		}
	}
}

// TestIdleBackoffNeverHoldsBackAPassAfterAnyFinding is the half that keeps the
// backoff from delaying a real repair: whatever a pass found, the very next
// tick surveys again. One case per result field, derived from the type.
func TestIdleBackoffNeverHoldsBackAPassAfterAnyFinding(t *testing.T) {
	for name, found := range workResults(t) {
		t.Run(name, func(t *testing.T) {
			inner := &scriptedStrandRepair{next: func(int) (StrandRepairResult, error) { return found, nil }}
			backoff := newTestIdleBackoff(t, inner)
			start := time.Date(2026, time.October, 3, 12, 0, 0, 0, time.UTC)
			for second := 0; second < 5; second++ {
				result, err := backoff.Step(context.Background(), start.Add(time.Duration(second)*time.Second), 100)
				if err != nil {
					t.Fatal(err)
				}
				if result.PassSkippedIdle {
					t.Fatalf("tick %d was held back although every pass reported %s", second, name)
				}
				if !reflect.DeepEqual(result, found) {
					t.Fatalf("tick %d result = %+v, want the wrapped result %+v unchanged", second, result, found)
				}
			}
			if len(inner.calls) != 5 {
				t.Fatalf("the repair surveyed %d times in 5 ticks, want 5", len(inner.calls))
			}
		})
	}
}

// TestIdleBackoffResetsTheMomentAPassFindsSomething lets the wait grow to its
// ceiling, then has one pass find a strand, and requires the following tick to
// survey -- not the tick after the remaining wait.
func TestIdleBackoffResetsTheMomentAPassFindsSomething(t *testing.T) {
	for name, found := range workResults(t) {
		t.Run(name, func(t *testing.T) {
			inner := &scriptedStrandRepair{}
			backoff := newTestIdleBackoff(t, inner)
			start := time.Date(2026, time.October, 3, 12, 0, 0, 0, time.UTC)
			at := func(second int) time.Time { return start.Add(time.Duration(second) * time.Second) }
			for second := 0; second < 60; second++ {
				if _, err := backoff.Step(context.Background(), at(second), 100); err != nil {
					t.Fatal(err)
				}
			}
			// The survey at +60 is the next one due; make it find something.
			inner.next = func(int) (StrandRepairResult, error) { return found, nil }
			before := len(inner.calls)
			if result, err := backoff.Step(context.Background(), at(60), 100); err != nil || result.PassSkippedIdle {
				t.Fatalf("the pass due at +60s did not run: %+v, %v", result, err)
			}
			inner.next = nil
			if result, err := backoff.Step(context.Background(), at(61), 100); err != nil || result.PassSkippedIdle {
				t.Fatalf("the tick after a finding was held back (%+v, %v); a finding must reset the wait", result, err)
			}
			if len(inner.calls) != before+2 {
				t.Fatalf("the repair surveyed %d times across the finding and the next tick, want 2",
					len(inner.calls)-before)
			}
			// And the wait starts again from the floor, not from where it was.
			if result, err := backoff.Step(context.Background(), at(62), 100); err != nil || !result.PassSkippedIdle {
				t.Fatalf("tick +62s = %+v, %v; want held back by the floor wait", result, err)
			}
			if result, err := backoff.Step(context.Background(), at(63), 100); err != nil || result.PassSkippedIdle {
				t.Fatalf("tick +63s = %+v, %v; want a survey: the wait restarts at the floor", result, err)
			}
		})
	}
}

// TestIdleBackoffResetsOnError: a failed pass proves nothing about whether
// there was work, and the loop's failure streak expects the next tick to try.
func TestIdleBackoffResetsOnError(t *testing.T) {
	inner := &scriptedStrandRepair{}
	backoff := newTestIdleBackoff(t, inner)
	start := time.Date(2026, time.October, 3, 12, 0, 0, 0, time.UTC)
	at := func(second int) time.Time { return start.Add(time.Duration(second) * time.Second) }
	for second := 0; second < 30; second++ {
		if _, err := backoff.Step(context.Background(), at(second), 100); err != nil {
			t.Fatal(err)
		}
	}
	partial := StrandRepairResult{Rearmed: 2}
	inner.next = func(int) (StrandRepairResult, error) { return partial, ErrUnavailable }
	result, err := backoff.Step(context.Background(), at(30), 100)
	if !errors.Is(err, ErrUnavailable) || !reflect.DeepEqual(result, partial) {
		t.Fatalf("Step() = %+v, %v; want the wrapped error and its partial result passed through", result, err)
	}
	inner.next = nil
	before := len(inner.calls)
	if result, err := backoff.Step(context.Background(), at(31), 100); err != nil || result.PassSkippedIdle {
		t.Fatalf("the tick after a failed pass was held back: %+v, %v", result, err)
	}
	if len(inner.calls) != before+1 {
		t.Fatal("the tick after a failed pass did not survey")
	}
}

// TestIdleBackoffRepeatsTheBlockedLevelWhileHeldBack: undelivered_blocked is a
// gauge the loop sets from every successful step. A held-back pass reporting
// zero would make it fall to zero between surveys and read as a drained
// backlog.
func TestIdleBackoffRepeatsTheBlockedLevelWhileHeldBack(t *testing.T) {
	level := 7
	inner := &scriptedStrandRepair{next: func(int) (StrandRepairResult, error) {
		return StrandRepairResult{UndeliveredBlocked: level}, nil
	}}
	backoff := newTestIdleBackoff(t, inner)
	start := time.Date(2026, time.October, 3, 12, 0, 0, 0, time.UTC)
	at := func(second int) time.Time { return start.Add(time.Duration(second) * time.Second) }

	first, err := backoff.Step(context.Background(), at(0), 100)
	if err != nil || first.PassSkippedIdle || first.UndeliveredBlocked != 7 {
		t.Fatalf("first pass = %+v, %v", first, err)
	}
	held, err := backoff.Step(context.Background(), at(1), 100)
	if err != nil {
		t.Fatal(err)
	}
	// A blocked backlog alone is a level, not a finding: the pass IS held back.
	if !held.PassSkippedIdle {
		t.Fatalf("a pass that found only a blocked level was not treated as idle: %+v", held)
	}
	if held.UndeliveredBlocked != 7 {
		t.Fatalf("held-back pass reported undelivered_blocked %d, want the last surveyed level 7",
			held.UndeliveredBlocked)
	}
	// The level follows the next survey, not the first one forever.
	level = 3
	if _, err := backoff.Step(context.Background(), at(2), 100); err != nil {
		t.Fatal(err)
	}
	held, err = backoff.Step(context.Background(), at(3), 100)
	if err != nil || !held.PassSkippedIdle || held.UndeliveredBlocked != 3 {
		t.Fatalf("held-back pass after a new survey = %+v, %v; want level 3", held, err)
	}
	// Everything else in a held-back result is zero: it reports no work it did
	// not do.
	held.PassSkippedIdle, held.UndeliveredBlocked = false, 0
	if !reflect.DeepEqual(held, StrandRepairResult{}) {
		t.Fatalf("a held-back pass reported work it did not do: %+v", held)
	}
}

// TestIdleBackoffIsBoundedAgainstAClockThatStepsBack: `now` is the caller's
// clock. Without the bound a backwards step of an hour would hold the repair
// off for an hour.
func TestIdleBackoffIsBoundedAgainstAClockThatStepsBack(t *testing.T) {
	inner := &scriptedStrandRepair{}
	backoff := newTestIdleBackoff(t, inner)
	start := time.Date(2026, time.October, 3, 12, 0, 0, 0, time.UTC)
	for second := 0; second <= 60; second++ {
		if _, err := backoff.Step(context.Background(), start.Add(time.Duration(second)*time.Second), 100); err != nil {
			t.Fatal(err)
		}
	}
	// The last survey ran at +60s and the next is due at +90s. Step the clock
	// back one hour: the wait now looks 91 minutes long.
	before := len(inner.calls)
	result, err := backoff.Step(context.Background(), start.Add(-time.Hour), 100)
	if err != nil || result.PassSkippedIdle {
		t.Fatalf("a pass was held back by a wait longer than the ceiling: %+v, %v", result, err)
	}
	if len(inner.calls) != before+1 {
		t.Fatal("the pass after a backwards clock step did not survey")
	}
}

// TestStrandPassSawNothingTreatsEveryFieldButTheLevelsAsWork is the guard on
// the idle definition itself. A field that signals work but is not seen by
// strandPassSawNothing would let the backoff slow the very repair that should
// act on it.
func TestStrandPassSawNothingTreatsEveryFieldButTheLevelsAsWork(t *testing.T) {
	if !strandPassSawNothing(StrandRepairResult{}) {
		t.Fatal("a zero result is not idle; the backoff could never engage")
	}
	for name, found := range workResults(t) {
		if strandPassSawNothing(found) {
			t.Fatalf("a result with only %s set is treated as idle", name)
		}
	}
	// The levels, and nothing else, are ignored -- and they exist on the type.
	levels := StrandRepairResult{UndeliveredBlocked: 9, PassSkippedIdle: true}
	if !strandPassSawNothing(levels) {
		t.Fatal("a result carrying only the blocked level and the skip flag is treated as work")
	}
	resultType := reflect.TypeOf(StrandRepairResult{})
	for name := range strandPassLevelFields {
		if _, ok := resultType.FieldByName(name); !ok {
			t.Fatalf("strandPassLevelFields names %s, which StrandRepairResult no longer has", name)
		}
	}
	if len(strandPassLevelFields) != 2 {
		t.Fatalf("strandPassLevelFields has %d entries, want exactly UndeliveredBlocked and "+
			"PassSkippedIdle: every other field is something a pass found", len(strandPassLevelFields))
	}
	// An empty, non-nil slice is still nothing.
	empty := StrandRepairResult{
		RetiredKindObservations: []RetiredKindObservation{},
		ProviderUnitRearms:      []ProviderUnitRearm{},
		UndeliveredResolutions:  []UndeliveredResolution{},
	}
	if !strandPassSawNothing(empty) {
		t.Fatal("empty non-nil slices are treated as work; the backoff would silently never engage")
	}
}

// TestRelayStepCarriesAHeldBackStrandPassIntoTheResult keeps the skip visible
// one layer up, where the loop counts it.
func TestRelayStepCarriesAHeldBackStrandPassIntoTheResult(t *testing.T) {
	for _, held := range []bool{true, false} {
		strand := &fakeStrandRepair{result: StrandRepairResult{PassSkippedIdle: held, UndeliveredBlocked: 4}}
		relay := &Relay{repair: fakeTerminalRepair{}, strandRepair: strand}
		result, err := relay.stepRecovery(context.Background(), time.Now(), 1)
		if err != nil {
			t.Fatal(err)
		}
		if result.StrandPassSkippedIdle != held || result.UndeliveredBlocked != 4 {
			t.Fatalf("held=%t: result = %+v", held, result)
		}
	}
}

// TestReconcilerLoopCountsHeldBackStrandPasses: a pass that did not run must
// not read as one that ran and found nothing.
func TestReconcilerLoopCountsHeldBackStrandPasses(t *testing.T) {
	clock := &testReconcilerClock{now: time.Date(2026, time.October, 3, 12, 0, 0, 0, time.UTC)}
	results := []StepResult{
		{UndeliveredBlocked: 5},
		{StrandPassSkippedIdle: true, UndeliveredBlocked: 5},
		{StrandPassSkippedIdle: true, UndeliveredBlocked: 5},
		{UndeliveredBlocked: 5},
	}
	loop, _ := newTestReconcilerLoop(t, loopStepFunc(func(context.Context, time.Time, int) (StepResult, error) {
		result := results[0]
		results = results[1:]
		return result, nil
	}), clock)
	for range 4 {
		if err := loop.step(context.Background(), clock.Now()); err != nil {
			t.Fatal(err)
		}
	}
	var metrics bytes.Buffer
	if err := loop.WritePrometheus(&metrics); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"# TYPE worker_outbox_reconciler_strand_passes_skipped_idle_total counter",
		"worker_outbox_reconciler_strand_passes_skipped_idle_total 2",
		"worker_outbox_reconciler_undelivered_blocked 5",
	} {
		if !strings.Contains(metrics.String(), want+"\n") {
			t.Fatalf("metrics missing %q:\n%s", want, metrics.String())
		}
	}
}

// TestIdleBackoffBehindTheRelayKeepsTheBlockedGaugeLevel runs the real chain
// the reconciler builds -- backoff, relay recovery, loop -- over a repair that
// reports a standing blocked backlog and nothing else, and reads the exported
// gauge after a held-back pass.
func TestIdleBackoffBehindTheRelayKeepsTheBlockedGaugeLevel(t *testing.T) {
	inner := &scriptedStrandRepair{next: func(int) (StrandRepairResult, error) {
		return StrandRepairResult{UndeliveredBlocked: 11}, nil
	}}
	relay := &Relay{repair: fakeTerminalRepair{}, strandRepair: newTestIdleBackoff(t, inner)}
	clock := &testReconcilerClock{now: time.Date(2026, time.October, 3, 12, 0, 0, 0, time.UTC)}
	tick := 0
	loop, _ := newTestReconcilerLoop(t, loopStepFunc(func(ctx context.Context, _ time.Time, limit int) (StepResult, error) {
		now := clock.Now().Add(time.Duration(tick) * time.Second)
		tick++
		return relay.stepRecovery(ctx, now, limit)
	}), clock)
	for range 2 {
		if err := loop.step(context.Background(), clock.Now()); err != nil {
			t.Fatal(err)
		}
	}
	if len(inner.calls) != 1 {
		t.Fatalf("the repair surveyed %d times in two ticks, want 1: the second is inside the floor wait",
			len(inner.calls))
	}
	var metrics bytes.Buffer
	if err := loop.WritePrometheus(&metrics); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"worker_outbox_reconciler_strand_passes_skipped_idle_total 1",
		"worker_outbox_reconciler_undelivered_blocked 11",
	} {
		if !strings.Contains(metrics.String(), want+"\n") {
			t.Fatalf("metrics missing %q after a held-back pass:\n%s", want, metrics.String())
		}
	}
}
