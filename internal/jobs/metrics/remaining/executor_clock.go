package remaining

import (
	"errors"
	"fmt"
	"time"
)

// errExecutorClockUnset reports an executor whose injected clock is nil.
//
// # WHY THIS REFUSES RATHER THAN FALLING BACK TO time.Now()
//
// The obvious alternative is a nil-safe accessor that substitutes
// `time.Now().UTC()` when the field is unset. That is the right answer for a
// kind whose zero value is a legitimate production state. It is the WRONG
// answer for these two, and the difference is worth stating because the two
// shapes look identical at the call site.
//
// NewDORAExecutor and NewCapacityExecutor both set nowUTC unconditionally on
// the ONLY path that returns a non-nil executor; every other path returns
// (nil, err). So in production the field cannot be nil, and a fallback would be
// dead code there. Its only reachable effect is in tests, where it would
// convert "this test forgot to inject a clock" from a loud failure into a real
// wall clock -- a test that silently depends on the day it runs. That is the
// one direction a clock guard must not fail in, because the resulting
// flakiness appears on unrelated changes, long after the test was written.
//
// Refusing costs nothing where the value is unreachable and buys determinism
// where it is not. The refusal is an ERROR rather than a panic so callers can
// assert it by identity with errors.Is; all three call sites already return an
// error, so no signature changed to accommodate it.
//
// This decision is per kind, not a house rule. A kind whose `now` is a
// CALLER-SUPPLIED PARAMETER, where the field is a genuine fallback rather than
// the sole source, may take a nil-safe accessor instead -- there the fallback
// is reachable and substituting a wall clock is the lesser failure. Copying
// either choice across kinds mechanically is how the wrong one spreads.
//
// An earlier version of this paragraph named a specific executor as the
// contrasting case. That type lives on an unmerged branch and appears nowhere
// in this tree, so the contrast could not be traced from here -- a comment
// citing something the reader cannot open is worse than one that states the
// principle, because it looks checkable and is not.
var errExecutorClockUnset = errors.New("executor clock is unset")

// clockOrRefuse returns the injected instant, or refuses if none was injected.
//
// It deliberately does NOT re-apply .UTC(): both constructors already inject a
// UTC clock, and normalising here would mask an injected clock that is not UTC
// rather than letting its own tests catch it.
func clockOrRefuse(kind string, nowUTC func() time.Time) (time.Time, error) {
	if nowUTC == nil {
		return time.Time{}, fmt.Errorf(
			"%w: %s was built as a literal without one; use its constructor",
			errExecutorClockUnset, kind)
	}
	return nowUTC(), nil
}

// nowOrRefuse yields this executor's instant, refusing an uninjected clock.
func (executor *DORAExecutor) nowOrRefuse() (time.Time, error) {
	return clockOrRefuse("DORAExecutor", executor.nowUTC)
}

// nowOrRefuse yields this executor's instant, refusing an uninjected clock.
func (executor *CapacityExecutor) nowOrRefuse() (time.Time, error) {
	return clockOrRefuse("CapacityExecutor", executor.nowUTC)
}

// nowOrRefuse yields this executor's instant, refusing an uninjected clock.
//
// CHAOS-4954/CHAOS-4935 merge conflict, fixed: this used to be a nil-safe
// `wallClock()` accessor that fell back to `time.Now().UTC()` -- the SAME
// shape this file's own doc comment names as wrong for a kind whose nowUTC
// is the sole source. NewRecommendationsExecutor sets nowUTC unconditionally
// on its only non-nil-returning path (recommendations_native.go), same as
// DORA/Capacity, so a nil clock here is exactly as unreachable in production
// as it is for them -- only reachable via a zero-valued literal, which is
// what a test writes when it does not care about time. Refusing loudly
// there is the correct failure mode, not a wall-clock fallback that makes
// the test's result depend on the day it runs.
func (executor *RecommendationsExecutor) nowOrRefuse() (time.Time, error) {
	return clockOrRefuse("RecommendationsExecutor", executor.nowUTC)
}
