package syncreconciler

import (
	"sync"
	"time"
)

// CHAOS-5470. sync_run 1410329c sat at attempts=1344 against a threshold of
// 1000 and the reconciler logged syncreconciler.dispatch_wakeup_attempts_exceeded
// at ERROR roughly once a second, continuously, for days. The report is a pure
// READ of a durable column -- emitting it changes nothing about the row -- so a
// permanently stuck run produces one ERROR line per tick forever. A signal that
// never stops is the same as no signal, and it buries every other ERROR the
// reconciler emits.
//
// What this file does NOT do, deliberately: it does not suppress, delay or
// otherwise change any WRITE. runawayDispatchAttempts is documented at
// materializer.go's own declaration as deciding NOTHING -- crossing it changes
// no predicate, no window and no write -- because a ceiling that suppressed
// re-arming would turn a visibility gap into a stalled run the first time a
// legitimately long-lived run crossed it. That decision is untouched here.
// Backoff on the re-arm itself, and a terminal quarantine state, are CHAOS-5470
// phase 2 and are not in this change; runawayLogStateQuarantined below exists
// so the log's shape does not have to change when they land, and is
// unreachable today.
//
// The report also does not get QUIETER. Every pass still states the count, on
// syncreconciler.runaway_dispatch_pass, including the zero pass -- same
// contract as ready_finalize_pass and orphaned_unit_pass. Only the ERROR
// stream is deduplicated, and every ERROR it withholds is accounted for as
// `suppressed` on that line, so a suppressed row is never a silent one.
const (
	// runawayLogFirstInterval is the delay before the SECOND report of a row
	// that is still stuck. One minute is short enough that a genuinely new
	// incident is confirmed almost immediately and long enough that a
	// one-second tick cannot storm.
	runawayLogFirstInterval = time.Minute
	// runawayLogMaxInterval caps the growth. An hour keeps a permanently
	// stuck run present in the log -- it must never vanish entirely, or the
	// fix would trade a storm for a silence, which is the failure the
	// original report existed to end.
	runawayLogMaxInterval = time.Hour
	// runawayLogAttemptsStep is what makes an attempt count "materially"
	// changed, and it is DERIVED from the threshold rather than chosen: a
	// tenth of the ceiling that defines a runaway in the first place. A row
	// climbing this fast is escalating, and escalation is worth a line
	// before the interval is up.
	runawayLogAttemptsStep = runawayDispatchAttempts / 10
	// runawayLogForgetAfter drops an entry nothing has reported for this
	// long. It is the backstop for the case prune-on-absence cannot cover --
	// a sample that stayed truncated across the row's disappearance -- and it
	// is what bounds the map when the report itself is failing.
	runawayLogForgetAfter = 6 * time.Hour
	// runawayLogMaxAttemptsStep caps the attempts bar the way
	// runawayLogMaxInterval caps the time bar, and it exists because the
	// UNCAPPED version was a P1 found by round r1 and reproduced here: step
	// doubles on every emission, and a permanently stuck row emits hourly
	// forever, so after ~57 doublings the int64 multiply WRAPS. Measured on
	// the unfixed tip: after 70 emissions `step=0`, which makes
	// `wakeup.Attempts - entry.attempts >= entry.step` true for a completely
	// STATIC row -- ten lines in ten ticks with the attempt count not moving
	// at all. The bug turned the anti-storm bar into a guarantee of the
	// storm. A hundred times the runaway threshold is a bar no real row
	// clears by accident, and it can never wrap.
	runawayLogMaxAttemptsStep = int64(runawayDispatchAttempts) * 100
	// runawayLogMaxTracked bounds the map absolutely. The report is capped at
	// runawayDispatchScan rows per pass, so four times that is generous;
	// CHAOS-4093's worst hour held 83 stuck runs. Past the cap a row is
	// EMITTED rather than tracked (see runawayLogReasonUntracked): a row this
	// code cannot account for must never be one it silently swallows.
	runawayLogMaxTracked = 4 * runawayDispatchScan
)

// The reason a line was emitted. It is on the line because "why am I seeing
// this now" is the first thing an operator asks of a deduplicated stream, and
// because the three reasons want different responses: a new incident, an
// escalating one, and a still-unresolved one.
const (
	runawayLogReasonFirstSeen = "first_seen"
	runawayLogReasonInterval  = "interval_elapsed"
	runawayLogReasonAdvanced  = "attempts_advanced"
	// runawayLogReasonUntracked means the tracking map was full, so this row
	// was reported without being deduplicated. It is deliberately loud: the
	// alternative is dropping a row nothing is keeping count of.
	runawayLogReasonUntracked = "untracked"
)

// The row's disposition, as distinct from why the line was emitted.
// Only runawayLogStateRetrying is reachable today -- nothing quarantines a
// dispatch wakeup yet. The field exists now so that when CHAOS-5470 phase 2
// adds a terminal state, an operator's existing queries and dashboards do not
// have to change shape to tell the two apart.
const (
	runawayLogStateRetrying    = "retrying"
	runawayLogStateQuarantined = "quarantined"
)

// growRunawayInterval and growRunawayStep double a bar and stop at its
// ceiling, saturating rather than wrapping.
//
// The bug round r1 found was an ABSENT ceiling, not a late one: `step *= 2`
// had no ceiling at all, so it wrapped int64 to zero after ~57 doublings, and
// a zero bar is cleared by every row on every tick -- including one whose
// attempt count is not moving.
//
// Honest note on the interval, recorded because a mutation SURVIVED here and a
// surviving mutant corrects the claim, not the mutant: for the INTERVAL,
// clamping after the multiply is behaviourally identical, because the ceiling
// is one hour and 2h cannot overflow an int64 nanosecond duration. Testing
// before the multiply is therefore defensive uniformity on that bar, not a
// fix, and no test can distinguish the two forms. It is written this way so
// both bars read the same and neither can regress into the step's shape if a
// ceiling is ever raised.
func growRunawayInterval(current time.Duration) time.Duration {
	if current >= runawayLogMaxInterval/2 {
		return runawayLogMaxInterval
	}
	return current * 2
}

func growRunawayStep(current int64) int64 {
	if current >= runawayLogMaxAttemptsStep/2 {
		return runawayLogMaxAttemptsStep
	}
	return current * 2
}

// runawayLogEmission is one ERROR line this pass will emit.
type runawayLogEmission struct {
	Wakeup RunawayDispatchWakeup
	Reason string
	State  string
	// UntrackedRows is non-zero ONLY on a runawayLogReasonUntracked line, and
	// it is how many rows this pass could not track -- the line names one of
	// them as an example and counts the rest, rather than printing one line
	// per row. Zero on every other reason.
	UntrackedRows int
}

// runawayLogOutcome is what the per-pass line reports. Every field is emitted
// on every pass, zeros included: a counter that only appears when it is
// non-zero cannot answer "is this still happening", which is the whole point.
type runawayLogOutcome struct {
	Emitted    int
	Suppressed int
	Tracked    int
	// Untracked is how many rows this pass could not fit in the map. It goes
	// on the per-pass line whether or not an ERROR was emitted for them, so
	// rate-limiting the untracked ERROR cannot make an untracked row silent.
	Untracked int
}

type runawayLogEntry struct {
	// attempts is the value at the LAST emission, not the last sighting --
	// comparing against the last sighting would let a row creep past the
	// material step one attempt at a time without ever re-reporting.
	attempts int64
	// step is the advance this row must make to earn its next line, and it
	// DOUBLES alongside interval on every emission. A fixed step does not
	// bound anything: a row climbing by one step per tick satisfies it every
	// tick, so the storm comes back through the attempts door while the
	// interval sits there growing and never being consulted. Measured, by the
	// test that found this: a fixed step emitted 60 lines in 60 ticks. Making
	// the bar geometric keeps "escalation is worth a line before the interval
	// is up" true while bounding the total logarithmically.
	step     int64
	lastEmit time.Time
	interval time.Duration
	lastSeen time.Time
}

// runawayLogState is per-MutationPipeline, and it is mutable state shared
// across ticks, so it carries its own mutex for the same reason
// rollupBumpCounts and stageTelemetry do.
type runawayLogState struct {
	mu      sync.Mutex
	entries map[string]*runawayLogEntry
	// overflow rate-limits the AT-CAP condition itself, as one thing, rather
	// than per row. Round r1's second P1: once the map is full and the
	// truncated sample ROTATES, every tick presents a different id the map
	// has no room for, each one emitted as untracked -- reproduced at ten
	// ERROR lines in ten ticks, the exact storm this file exists to end,
	// rebuilt through the fail-loud door. The condition is what an operator
	// needs to see, and it is one condition however many rows rotate through
	// it, so it gets one growing-interval entry of its own. nil means the
	// condition is not currently active.
	overflow *runawayLogEntry
}

func newRunawayLogState() runawayLogState {
	return runawayLogState{entries: make(map[string]*runawayLogEntry)}
}

// decide is the whole policy, in one place, so it can be tested without a
// pipeline and so there is exactly one definition of "should this line be
// emitted".
//
// reportDelivered gates PRUNING, not emission. A row missing from this pass's
// report is only evidence it cleared when the report actually ran and was not
// truncated: an aborted tick and a faulted statement both produce an empty
// report, and pruning on those would forget every tracked row and re-emit the
// lot as first_seen on the next healthy pass -- rebuilding the storm one
// outage at a time. A truncated sample is the same problem in miniature: an id
// past runawayDispatchScan is unobserved, not absent.
func (state *runawayLogState) decide(
	now time.Time,
	wakeups []RunawayDispatchWakeup,
	truncated bool,
	reportDelivered bool,
) ([]runawayLogEmission, runawayLogOutcome) {
	state.mu.Lock()
	defer state.mu.Unlock()

	emissions := make([]runawayLogEmission, 0, len(wakeups))
	outcome := runawayLogOutcome{}
	seen := make(map[string]struct{}, len(wakeups))
	untrackedExample := RunawayDispatchWakeup{}

	for _, wakeup := range wakeups {
		seen[wakeup.SyncRunID] = struct{}{}
		entry, tracked := state.entries[wakeup.SyncRunID]
		switch {
		case !tracked && len(state.entries) >= runawayLogMaxTracked:
			// Cannot dedupe this row. Counted here and decided once, below,
			// for the whole at-cap condition -- emitting per row is what
			// round r1's F2 reproduced as a per-tick storm.
			outcome.Untracked++
			if untrackedExample.SyncRunID == "" {
				untrackedExample = wakeup
			}
		case !tracked:
			state.entries[wakeup.SyncRunID] = &runawayLogEntry{
				attempts: wakeup.Attempts,
				step:     runawayLogAttemptsStep,
				lastEmit: now,
				interval: runawayLogFirstInterval,
				lastSeen: now,
			}
			emissions = append(emissions, runawayLogEmission{
				Wakeup: wakeup,
				Reason: runawayLogReasonFirstSeen,
				State:  runawayLogStateRetrying,
			})
		default:
			entry.lastSeen = now
			reason := ""
			switch {
			case wakeup.Attempts-entry.attempts >= entry.step:
				reason = runawayLogReasonAdvanced
			case !now.Before(entry.lastEmit.Add(entry.interval)):
				reason = runawayLogReasonInterval
			}
			if reason == "" {
				outcome.Suppressed++
				continue
			}
			// BOTH bars grow on EVERY re-emission, whatever its reason.
			// Growing only the interval is not enough and was a real defect
			// caught by its own test: the attempts branch never consults the
			// interval, so a row advancing by one step per tick re-emitted
			// every tick with the interval quietly doubling beside it.
			entry.attempts = wakeup.Attempts
			entry.lastEmit = now
			entry.interval = growRunawayInterval(entry.interval)
			entry.step = growRunawayStep(entry.step)
			emissions = append(emissions, runawayLogEmission{
				Wakeup: wakeup,
				Reason: reason,
				State:  runawayLogStateRetrying,
			})
		}
	}

	// The at-cap condition, decided ONCE for the pass on its own growing
	// interval -- never once per row, and never once per tick. Whether or not
	// a line comes out of this, outcome.Untracked is on the per-pass line, so
	// an untracked row is counted on every single pass even while its ERROR
	// is rate-limited. That is what keeps "fail loud rather than swallow"
	// true without handing the storm back.
	if outcome.Untracked > 0 {
		emit := false
		switch {
		case state.overflow == nil:
			state.overflow = &runawayLogEntry{lastEmit: now, interval: runawayLogFirstInterval}
			emit = true
		case !now.Before(state.overflow.lastEmit.Add(state.overflow.interval)):
			state.overflow.lastEmit = now
			state.overflow.interval = growRunawayInterval(state.overflow.interval)
			emit = true
		}
		if emit {
			emissions = append(emissions, runawayLogEmission{
				Wakeup:        untrackedExample,
				Reason:        runawayLogReasonUntracked,
				State:         runawayLogStateRetrying,
				UntrackedRows: outcome.Untracked,
			})
		} else {
			outcome.Suppressed += outcome.Untracked
		}
	} else {
		// The condition cleared. Forget it, so its return is reported as a
		// new condition rather than waiting out a stale interval.
		state.overflow = nil
	}

	if reportDelivered && !truncated {
		for id := range state.entries {
			if _, present := seen[id]; !present {
				delete(state.entries, id)
			}
		}
	}
	// The TTL runs unconditionally, because it is the only bound that holds
	// when the report is failing or permanently truncated.
	forgetBefore := now.Add(-runawayLogForgetAfter)
	for id, entry := range state.entries {
		if entry.lastSeen.Before(forgetBefore) {
			delete(state.entries, id)
		}
	}

	outcome.Emitted = len(emissions)
	outcome.Tracked = len(state.entries)
	return emissions, outcome
}
