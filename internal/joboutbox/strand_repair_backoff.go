package joboutbox

import (
	"context"
	"reflect"
	"sync"
	"time"
)

const (
	// DefaultStrandRepairIdleCeiling is the longest a strand-repair pass is
	// held back while the passes before it saw nothing.
	//
	// It is the only latency this adds to a repair, and it is small against
	// what a strand already waits for: a row becomes a candidate only after a
	// domain lease has expired AND River has made its delivery terminal, both
	// of which are measured in minutes, and the undelivered sweep's own
	// ceiling is 72 hours. Nothing in the design states a repair latency in
	// seconds; what the relay does promise -- a row rearmed in a pass is
	// claimed in that same pass (NewRelayWithRoutesRecoveryAndStrandRepair) --
	// is untouched, because a pass that runs still runs before the claim.
	DefaultStrandRepairIdleCeiling = 30 * time.Second

	// strandRepairIdleFloor is the first wait after an idle pass. It is two
	// ticks of the reconciler's default one-second poll, the smallest wait
	// that skips anything at all.
	strandRepairIdleFloor = 2 * time.Second

	// maxStrandRepairIdleCeiling bounds the configuration, not the default: a
	// ceiling past this would turn a cost control into a repair outage.
	maxStrandRepairIdleCeiling = 5 * time.Minute
)

// IdleBackoffStrandRepair spaces out strand-repair passes while they find
// nothing (CHAOS-8421).
//
// The reconciler loop ticks once a second because the RELAY needs to: that is
// the latency between a handoff committing and its River job existing. The
// strand repair rides the same tick but has no such need -- it looks for rows
// that became stranded minutes ago -- and every pass costs six statements (four shape surveys, the undelivered
// survey and the retired-kind observation) whether or not there is anything
// to repair. On a quiet stack that is most of the reconciler's database load.
//
// So a pass that saw NOTHING doubles the wait before the next one, from
// strandRepairIdleFloor up to the ceiling, and anything at all resets it: a
// rearm, a refusal of any kind, a retired-kind observation, an undelivered
// resolution, or an error. Refusals reset it on purpose. A candidate that was
// refused this pass (its River job still live, its claim still held) is the
// one most likely to become repairable on the next, and the refusal counters
// are read as rates -- "skipped_live climbing while nothing is rearmed" is the
// documented signature of a stopped River rescuer -- so they keep the cadence
// they have always had.
//
// It is a decorator rather than a field on StrandRepair so that StrandRepair
// stays a pure function of the database: every existing caller and test that
// steps it twice in a row still gets two surveys.
type IdleBackoffStrandRepair struct {
	inner   StrandRepairStepper
	ceiling time.Duration

	mu sync.Mutex
	// wait is the current backoff; zero means the last pass was not idle.
	wait time.Duration
	// resume is the first instant a pass may run again.
	resume time.Time
	// blocked is the UndeliveredBlocked level of the last pass that ran. It is
	// a LEVEL, exported as a gauge from every successful step, so a held-back
	// pass repeats it; reporting zero instead would make the gauge fall to
	// zero between surveys and read as a drained backlog.
	blocked int
}

// NewIdleBackoffStrandRepair wraps inner. ceiling is the longest wait between
// two passes; DefaultStrandRepairIdleCeiling is the production value.
func NewIdleBackoffStrandRepair(
	inner StrandRepairStepper,
	ceiling time.Duration,
) (*IdleBackoffStrandRepair, error) {
	if inner == nil || ceiling < strandRepairIdleFloor || ceiling > maxStrandRepairIdleCeiling {
		return nil, ErrInvalidConfiguration
	}
	return &IdleBackoffStrandRepair{inner: inner, ceiling: ceiling}, nil
}

// Step runs the wrapped pass, or reports PassSkippedIdle without touching the
// database when the passes before it were idle and the wait has not elapsed.
func (backoff *IdleBackoffStrandRepair) Step(
	ctx context.Context,
	now time.Time,
	limit int,
) (StrandRepairResult, error) {
	if backoff == nil || backoff.inner == nil {
		return StrandRepairResult{}, ErrInvalidConfiguration
	}
	backoff.mu.Lock()
	resume, blocked := backoff.resume, backoff.blocked
	backoff.mu.Unlock()
	// The wait is honoured only while it is no longer than the ceiling. `now`
	// is the caller's clock, and a clock that steps backwards would otherwise
	// leave `resume` arbitrarily far in the future and hold the repair off for
	// as long as the step was large. Bounded this way, the worst a clock step
	// can cost is one ceiling.
	if now.Before(resume) && resume.Sub(now) <= backoff.ceiling {
		return StrandRepairResult{PassSkippedIdle: true, UndeliveredBlocked: blocked}, nil
	}

	result, err := backoff.inner.Step(ctx, now, limit)

	backoff.mu.Lock()
	defer backoff.mu.Unlock()
	if err != nil || !strandPassSawNothing(result) {
		// An error resets too: a failed pass proves nothing about whether
		// there was work, and the loop's own failure accounting expects the
		// next tick to try again.
		backoff.wait = 0
		backoff.resume = time.Time{}
		if err == nil {
			backoff.blocked = result.UndeliveredBlocked
		}
		return result, err
	}
	backoff.blocked = result.UndeliveredBlocked
	backoff.wait = nextStrandRepairIdleWait(backoff.wait, backoff.ceiling)
	backoff.resume = now.Add(backoff.wait)
	return result, nil
}

// nextStrandRepairIdleWait doubles the wait and saturates at the ceiling. It
// cannot overflow: the value it doubles is never larger than the ceiling.
func nextStrandRepairIdleWait(current, ceiling time.Duration) time.Duration {
	next := strandRepairIdleFloor
	if current > 0 {
		next = current * 2
	}
	if next > ceiling {
		next = ceiling
	}
	return next
}

// strandPassLevelFields are the StrandRepairResult fields that describe a
// standing level or the backoff itself rather than something a pass FOUND.
// Every other field counts as work.
//
// UndeliveredBlocked is a level: rows inside the 72-hour undelivered ceiling,
// which no pass acts on. A standing blocked backlog is the normal state of a
// stack with post-sync chains in flight, and treating it as work would keep
// the repair at full cadence for as long as any chain existed.
var strandPassLevelFields = map[string]bool{
	"UndeliveredBlocked": true,
	"PassSkippedIdle":    true,
}

// strandPassSawNothing reports whether a pass found no candidate, no refusal
// and no observation.
//
// It walks the result TYPE instead of naming fields. A field added to
// StrandRepairResult later is therefore treated as work until someone decides
// otherwise in strandPassLevelFields -- the safe direction, because the
// failure it avoids is a new kind of finding that silently slows the repair
// that should be acting on it. A field of a kind this function does not know
// is treated as work for the same reason.
func strandPassSawNothing(result StrandRepairResult) bool {
	value := reflect.ValueOf(result)
	for index := 0; index < value.NumField(); index++ {
		if strandPassLevelFields[value.Type().Field(index).Name] {
			continue
		}
		field := value.Field(index)
		switch field.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if field.Int() != 0 {
				return false
			}
		case reflect.Bool:
			if field.Bool() {
				return false
			}
		case reflect.Slice:
			if field.Len() != 0 {
				return false
			}
		default:
			return false
		}
	}
	return true
}
