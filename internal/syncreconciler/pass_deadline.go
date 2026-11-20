package syncreconciler

import (
	"context"
	"time"
)

// passDeadline lets a per-item repair pass stop BEFORE its stage deadline and
// commit the items it finished, instead of running into the deadline and
// rolling all of them back (CHAOS-14238). An item may start only while the
// time left is at least twice the longest item so far -- the rule
// chunk_sidecar_reclaim.go applies per statement. An item's cost is the drop
// in time left across it, which under a fixed deadline is its elapsed time.
type passDeadline struct {
	ctx      context.Context
	previous time.Duration
	longest  time.Duration
	started  bool
}

func newPassDeadline(ctx context.Context) *passDeadline {
	return &passDeadline{ctx: ctx}
}

// next reports whether one more item fits. The first item always runs.
func (pass *passDeadline) next() bool {
	deadline, ok := pass.ctx.Deadline()
	if !ok {
		return true
	}
	left := time.Until(deadline)
	if pass.started {
		if cost := pass.previous - left; cost > pass.longest {
			pass.longest = cost
		}
		if left < 2*pass.longest {
			return false
		}
	}
	pass.started = true
	pass.previous = left
	return true
}
