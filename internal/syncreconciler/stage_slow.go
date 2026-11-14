package syncreconciler

import (
	"log/slog"
	"sync"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/dbphase"
)

// A successful pass that used at least 3/4 of its budget is logged as
// syncreconciler.stage_slow (CHAOS-13743). stage_failed is cut at the budget, so
// only successful passes near it show how much time a stage really needs.
const (
	stageSlowNumerator   = 3
	stageSlowDenominator = 4
	// stageSlowInterval bounds the line to one per stage per interval; the
	// passes in between are counted and their slowest is carried on the next line.
	stageSlowInterval = 10 * time.Second
)

func stageIsSlow(elapsed, budget time.Duration) bool {
	return elapsed*stageSlowDenominator >= budget*stageSlowNumerator
}

type stageSlowWindow struct {
	emitted     bool
	lastEmitted time.Time
	count       int64
	maxElapsed  time.Duration
	maxPhases   string
}

// stageSlowLog is per-pipeline state shared between Step calls.
type stageSlowLog struct {
	mu      sync.Mutex
	windows map[StageName]*stageSlowWindow
}

type stageSlowLine struct {
	count      int64
	maxElapsed time.Duration
	maxPhases  string
}

// observe counts one slow pass and returns the line to write now, if one is
// due: the first slow pass of a stage, then the first one at least
// stageSlowInterval after the stage's previous line. count and the max cover
// every slow pass since that previous line, this one included. Passes after
// the last line of a burst are reported on the stage's next line.
func (log *stageSlowLog) observe(stage StageName, now time.Time, elapsed time.Duration, phases string) (stageSlowLine, bool) {
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.windows == nil {
		log.windows = make(map[StageName]*stageSlowWindow, len(orderedStages))
	}
	window := log.windows[stage]
	if window == nil {
		window = &stageSlowWindow{}
		log.windows[stage] = window
	}
	window.count++
	if window.count == 1 || elapsed > window.maxElapsed {
		window.maxElapsed = elapsed
		window.maxPhases = phases
	}
	if window.emitted && now.Sub(window.lastEmitted) < stageSlowInterval {
		return stageSlowLine{}, false
	}
	line := stageSlowLine{count: window.count, maxElapsed: window.maxElapsed, maxPhases: window.maxPhases}
	*window = stageSlowWindow{emitted: true, lastEmitted: now}
	return line, true
}

func (pipeline *MutationPipeline) logSlowStage(stage StageName, budget, elapsed time.Duration, trace *dbphase.Trace) {
	line, due := pipeline.slowLog.observe(stage, time.Now(), elapsed, trace.Summary())
	if !due {
		return
	}
	slog.Info(
		"syncreconciler.stage_slow",
		append([]any{
			"stage", string(stage),
			"budget_ms", budget.Milliseconds(),
			"elapsed_ms", elapsed.Milliseconds(),
			"count", line.count,
			"max_elapsed_ms", line.maxElapsed.Milliseconds(),
			"max_phases", line.maxPhases,
		}, stagePhaseAttrs(trace)...)...,
	)
}
