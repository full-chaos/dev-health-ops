package syncreconciler

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// runawayLogPipeline builds a MutationPipeline whose materializer reports the
// SAME runaway wakeup on every pass -- the live shape CHAOS-5470 is about: a
// permanently stuck row that the reconciler re-reports on every tick because
// nothing ever clears it.
func runawayLogPipeline(t *testing.T, wakeups func(int) []RunawayDispatchWakeup) *MutationPipeline {
	t.Helper()
	pass := 0
	publish := AtLeastOncePublisher(func(context.Context, pgx.Tx, TransportClaim) (string, error) {
		return "", nil
	})
	postSync := PostSyncHandoff(func(context.Context, TransportClaim) error { return nil })
	pipeline, err := NewMutationPipeline(
		pipelineLeaseRepairFunc(func(context.Context, time.Time, int) (LeaseRepairResult, error) {
			return LeaseRepairResult{}, nil
		}),
		pipelineTerminalDeliveryRepairFunc(func(context.Context, time.Time, int) (TerminalDeliveryRepairResult, error) {
			return TerminalDeliveryRepairResult{}, nil
		}),
		pipelineMaterializerFunc(func(context.Context, time.Time, time.Time, int) (MaterializerResult, error) {
			pass++
			report := wakeups(pass)
			return MaterializerResult{
				Runaway:      report,
				RunawayTotal: int64(len(report)),
			}, nil
		}),
		pipelineKernelFunc(func(
			context.Context, time.Time, int, time.Duration,
			AtLeastOncePublisher, PostSyncHandoff,
		) (KernelResult, error) {
			return KernelResult{}, nil
		}),
		pipelineObserverFunc(func(context.Context, time.Time, int) (Observation, error) {
			return Observation{}, nil
		}),
		publish,
		postSync,
		nil,
		noopTerminalOutboxClose(),
		noopOrphanedUnitRepair(),
		DefaultMutationPipelineConfig(),
	)
	if err != nil {
		t.Fatal(err)
	}
	return pipeline
}

// runawayLogTicks drives `ticks` passes `apart` apart and returns every JSON
// log record the pipeline emitted, in order.
func runawayLogTicks(
	t *testing.T,
	pipeline *MutationPipeline,
	start time.Time,
	ticks int,
	apart time.Duration,
) []map[string]any {
	t.Helper()
	var buf bytes.Buffer
	original := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	for tick := 0; tick < ticks; tick++ {
		if _, err := pipeline.Step(context.Background(), start.Add(time.Duration(tick)*apart), 17); err != nil {
			slog.SetDefault(original)
			t.Fatalf("tick %d: %v", tick, err)
		}
	}
	slog.SetDefault(original)

	var records []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		record := map[string]any{}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("log line is not JSON: %s (%v)", line, err)
		}
		records = append(records, record)
	}
	return records
}

func runawayLogCount(records []map[string]any, msg string) int {
	count := 0
	for _, record := range records {
		if record["msg"] == msg {
			count++
		}
	}
	return count
}

func runawayLogRecords(records []map[string]any, msg string) []map[string]any {
	var found []map[string]any
	for _, record := range records {
		if record["msg"] == msg {
			found = append(found, record)
		}
	}
	return found
}

// CHAOS-5470: sync_run 1410329c sat at attempts=1344 against a threshold of
// 1000 and the reconciler logged dispatch_wakeup_attempts_exceeded at ERROR
// roughly once a second, continuously, for days -- one line per stuck row per
// tick, forever, because the report is a pure read of a durable column and
// nothing about emitting it changes the row. An operator's only signal that a
// run is looping is a signal that never stops, which is the same as no signal.
//
// The report itself must NOT get quieter: the count still has to be visible on
// every pass. What changes is the ERROR stream -- one line when a row ENTERS
// the over-threshold set, then re-emission on a growing interval, with a
// per-pass INFO line carrying total/emitted/suppressed so a suppressed row is
// never a silent one.
func TestRunawayDispatchWakeupsAreLoggedOnTransitionNotEveryTick(t *testing.T) {
	start := time.Date(2026, time.September, 9, 12, 0, 0, 0, time.UTC)
	stuck := []RunawayDispatchWakeup{{SyncRunID: "1410329c-51aa-5895-89c7-b36442da361e", Attempts: 1344}}

	t.Run("a row stuck across ten one-second ticks is logged ONCE at ERROR", func(t *testing.T) {
		pipeline := runawayLogPipeline(t, func(int) []RunawayDispatchWakeup { return stuck })
		records := runawayLogTicks(t, pipeline, start, 10, time.Second)

		errors := runawayLogCount(records, "syncreconciler.dispatch_wakeup_attempts_exceeded")
		if errors != 1 {
			t.Fatalf("dispatch_wakeup_attempts_exceeded emitted %d times across 10 ticks, want 1 "+
				"(one per tick is CHAOS-5470's ~1/s log storm)", errors)
		}

		// The report must not go dark: every tick still states the count.
		passes := runawayLogRecords(records, "syncreconciler.runaway_dispatch_pass")
		if len(passes) != 10 {
			t.Fatalf("runaway_dispatch_pass emitted %d times across 10 ticks, want 10 "+
				"(the per-pass count is what keeps a suppressed row visible)", len(passes))
		}
		for tick, pass := range passes {
			if pass["total"] != float64(1) {
				t.Fatalf("tick %d runaway_dispatch_pass total = %#v, want 1", tick, pass["total"])
			}
		}
		if passes[0]["emitted"] != float64(1) || passes[0]["suppressed"] != float64(0) {
			t.Fatalf("first pass emitted/suppressed = %#v/%#v, want 1/0",
				passes[0]["emitted"], passes[0]["suppressed"])
		}
		for tick, pass := range passes[1:] {
			if pass["emitted"] != float64(0) || pass["suppressed"] != float64(1) {
				t.Fatalf("tick %d runaway_dispatch_pass emitted/suppressed = %#v/%#v, want 0/1",
					tick+1, pass["emitted"], pass["suppressed"])
			}
		}
	})

	t.Run("the ERROR line names the transition and the state", func(t *testing.T) {
		pipeline := runawayLogPipeline(t, func(int) []RunawayDispatchWakeup { return stuck })
		records := runawayLogTicks(t, pipeline, start, 3, time.Second)
		found := runawayLogRecords(records, "syncreconciler.dispatch_wakeup_attempts_exceeded")
		if len(found) != 1 {
			t.Fatalf("want exactly 1 ERROR line, got %d", len(found))
		}
		for key, want := range map[string]any{
			"sync_run_id": "1410329c-51aa-5895-89c7-b36442da361e",
			"attempts":    float64(1344),
			"threshold":   float64(runawayDispatchAttempts),
			"reason":      runawayLogReasonFirstSeen,
			"state":       runawayLogStateRetrying,
		} {
			got, ok := found[0][key]
			if !ok {
				t.Fatalf("ERROR line missing field %q: %#v", key, found[0])
			}
			if got != want {
				t.Fatalf("ERROR line %q = %#v, want %#v", key, got, want)
			}
		}
	})

	t.Run("re-emission happens on the growing interval, not the tick", func(t *testing.T) {
		pipeline := runawayLogPipeline(t, func(int) []RunawayDispatchWakeup { return stuck })
		// Ten ticks a minute apart. The first interval is one minute, and it
		// doubles on each re-emission, so within ten minutes the row is
		// reported at t=0, t=1m, t=3m, t=7m -- four lines, not ten.
		records := runawayLogTicks(t, pipeline, start, 10, time.Minute)
		found := runawayLogRecords(records, "syncreconciler.dispatch_wakeup_attempts_exceeded")
		if len(found) != 4 {
			t.Fatalf("want 4 ERROR lines across ten one-minute ticks (t=0,1m,3m,7m), got %d", len(found))
		}
		if found[1]["reason"] != runawayLogReasonInterval {
			t.Fatalf("second line reason = %#v, want %q", found[1]["reason"], runawayLogReasonInterval)
		}
	})

	t.Run("a materially advanced attempt count re-emits immediately", func(t *testing.T) {
		pipeline := runawayLogPipeline(t, func(pass int) []RunawayDispatchWakeup {
			// Pass 3 jumps by the material step; passes 2 and 4 do not.
			attempts := int64(1344)
			if pass >= 3 {
				attempts = 1344 + runawayLogAttemptsStep
			}
			return []RunawayDispatchWakeup{{SyncRunID: "1410329c-51aa-5895-89c7-b36442da361e", Attempts: attempts}}
		})
		records := runawayLogTicks(t, pipeline, start, 4, time.Second)
		found := runawayLogRecords(records, "syncreconciler.dispatch_wakeup_attempts_exceeded")
		if len(found) != 2 {
			t.Fatalf("want 2 ERROR lines (first-seen, then the advance), got %d", len(found))
		}
		if found[1]["reason"] != runawayLogReasonAdvanced {
			t.Fatalf("second line reason = %#v, want %q", found[1]["reason"], runawayLogReasonAdvanced)
		}
	})

	t.Run("a row that clears and later returns is reported again", func(t *testing.T) {
		pipeline := runawayLogPipeline(t, func(pass int) []RunawayDispatchWakeup {
			if pass == 2 {
				return nil
			}
			return stuck
		})
		records := runawayLogTicks(t, pipeline, start, 3, time.Second)
		found := runawayLogRecords(records, "syncreconciler.dispatch_wakeup_attempts_exceeded")
		if len(found) != 2 {
			t.Fatalf("want 2 ERROR lines (first sighting, then the return), got %d", len(found))
		}
		if found[1]["reason"] != runawayLogReasonFirstSeen {
			t.Fatalf("return line reason = %#v, want %q", found[1]["reason"], runawayLogReasonFirstSeen)
		}
	})

}

// The pipeline-level test above cannot reach these: they need a report that is
// truncated, a report that did not deliver, a map at its cap, and a clock
// hours ahead. Each one exists because the naive version of this fix
// reintroduces the storm through a different door.
func TestRunawayLogStateForgetsOnlyWhatItActuallyObserved(t *testing.T) {
	now := time.Date(2026, time.September, 9, 12, 0, 0, 0, time.UTC)
	stuck := []RunawayDispatchWakeup{{SyncRunID: "run-a", Attempts: 1344}}

	t.Run("an absent row is forgotten only when the report actually delivered", func(t *testing.T) {
		state := newRunawayLogState()
		if emissions, _ := state.decide(now, stuck, false, true); len(emissions) != 1 {
			t.Fatalf("first sighting emitted %d lines, want 1", len(emissions))
		}
		// An aborted tick or a faulted statement produces an empty report.
		// Forgetting on that would re-emit every tracked row as first_seen on
		// the next healthy pass -- the storm, rebuilt one outage at a time.
		if _, outcome := state.decide(now.Add(time.Second), nil, false, false); outcome.Tracked != 1 {
			t.Fatalf("an undelivered report forgot the row: tracked=%d, want 1", outcome.Tracked)
		}
		emissions, _ := state.decide(now.Add(2*time.Second), stuck, false, true)
		if len(emissions) != 0 {
			t.Fatalf("row re-emitted after an undelivered report: %#v", emissions)
		}
		// A delivered, untruncated report that omits the row IS evidence it
		// cleared.
		if _, outcome := state.decide(now.Add(3*time.Second), nil, false, true); outcome.Tracked != 0 {
			t.Fatalf("a delivered report did not forget the cleared row: tracked=%d, want 0",
				outcome.Tracked)
		}
	})

	t.Run("a truncated report never forgets a row it could not see", func(t *testing.T) {
		state := newRunawayLogState()
		state.decide(now, stuck, false, true)
		// The report is capped at runawayDispatchScan rows. An id past the cap
		// is UNOBSERVED, not absent -- treating the two the same would drop
		// and re-report rows in rotation on any incident wider than the cap.
		other := []RunawayDispatchWakeup{{SyncRunID: "run-b", Attempts: 9000}}
		if _, outcome := state.decide(now.Add(time.Second), other, true, true); outcome.Tracked != 2 {
			t.Fatalf("a truncated report forgot an unobserved row: tracked=%d, want 2", outcome.Tracked)
		}
		emissions, _ := state.decide(now.Add(2*time.Second), stuck, true, true)
		if len(emissions) != 0 {
			t.Fatalf("row re-emitted after a truncated report: %#v", emissions)
		}
	})

	t.Run("a row nothing has reported for the TTL is dropped", func(t *testing.T) {
		state := newRunawayLogState()
		state.decide(now, stuck, false, true)
		// Every later pass is truncated, so prune-on-absence never fires --
		// the TTL is the only bound left, and it is what stops the map
		// growing without limit while the report is degraded.
		if _, outcome := state.decide(now.Add(runawayLogForgetAfter/2), nil, true, true); outcome.Tracked != 1 {
			t.Fatalf("row dropped before the TTL: tracked=%d, want 1", outcome.Tracked)
		}
		if _, outcome := state.decide(now.Add(runawayLogForgetAfter+time.Second), nil, true, true); outcome.Tracked != 0 {
			t.Fatalf("row survived the TTL: tracked=%d, want 0", outcome.Tracked)
		}
	})

	t.Run("past the tracking cap a row is emitted, never swallowed", func(t *testing.T) {
		state := newRunawayLogState()
		full := make([]RunawayDispatchWakeup, 0, runawayLogMaxTracked)
		for index := 0; index < runawayLogMaxTracked; index++ {
			full = append(full, RunawayDispatchWakeup{
				SyncRunID: "run-" + strconv.Itoa(index), Attempts: 1344,
			})
		}
		if emissions, outcome := state.decide(now, full, true, true); len(emissions) != runawayLogMaxTracked ||
			outcome.Tracked != runawayLogMaxTracked {
			t.Fatalf("filling the map emitted %d / tracked %d, want %d / %d",
				len(emissions), outcome.Tracked, runawayLogMaxTracked, runawayLogMaxTracked)
		}
		// The map is full. The next NEW row cannot be deduplicated, so it must
		// be reported rather than dropped: a row this code cannot account for
		// must never be one it silently swallows.
		overflow := append(append([]RunawayDispatchWakeup(nil), full...),
			RunawayDispatchWakeup{SyncRunID: "run-overflow", Attempts: 5000})
		emissions, outcome := state.decide(now.Add(time.Second), overflow, true, true)
		if len(emissions) != 1 || emissions[0].Reason != runawayLogReasonUntracked ||
			emissions[0].Wakeup.SyncRunID != "run-overflow" {
			t.Fatalf("overflow row was not emitted as untracked: %#v", emissions)
		}
		if outcome.Suppressed != runawayLogMaxTracked {
			t.Fatalf("suppressed = %d, want %d (every already-tracked row)",
				outcome.Suppressed, runawayLogMaxTracked)
		}
	})

	t.Run("the interval grows on an attempts-advance too", func(t *testing.T) {
		// Growing only on the interval branch would let a fast-climbing row
		// re-emit through the attempts branch every tick and restore the storm
		// through the other door. Each advance must cost the same doubling an
		// interval re-emission does.
		state := newRunawayLogState()
		at := now
		attempts := int64(1344)
		state.decide(at, []RunawayDispatchWakeup{{SyncRunID: "run-a", Attempts: attempts}}, false, true)
		emitted := 0
		for tick := 0; tick < 60; tick++ {
			at = at.Add(time.Second)
			attempts += runawayLogAttemptsStep
			emissions, _ := state.decide(at,
				[]RunawayDispatchWakeup{{SyncRunID: "run-a", Attempts: attempts}}, false, true)
			emitted += len(emissions)
		}
		if emitted > 8 {
			t.Fatalf("a row advancing by the material step every second emitted %d lines "+
				"in 60 ticks; the interval is not growing on the advance branch", emitted)
		}
	})
}
