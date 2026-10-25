package synchandoff

import "fmt"

// ScaleRefusal is a window and scope the scheduler would refuse over its unit
// cap. Routes answer it 422 with Error() as the detail.
type ScaleRefusal struct{ floor, unitCap int64 }

func (r *ScaleRefusal) Error() string {
	return fmt.Sprintf("Backfill is too large: at least %d work units for this window and scope, over the limit of %d. Narrow the window or the scope.", r.floor, r.unitCap)
}

// CheckBackfillScale refuses a window and scope the scheduler would refuse over
// SYNC_RUN_MAX_UNITS (schedsync.UnitCapFromEnv; materializer.go: a plan over the
// cap is quarantined). It is a floor, never an estimate above the truth: every
// source plans at least one unit per window of the widest planner chunk (14
// days), so only a run over that floor is refused. No cap (0) or no known
// sources refuses nothing. The text names only the caller's own numbers.
func CheckBackfillScale(days, sources, unitCap int64) error {
	if unitCap <= 0 || sources <= 0 {
		return nil
	}
	const widestChunkDays = 14
	chunks := (days + widestChunkDays - 1) / widestChunkDays
	if chunks < 1 {
		chunks = 1
	}
	if floor := sources * chunks; floor > unitCap {
		return &ScaleRefusal{floor: floor, unitCap: unitCap}
	}
	return nil
}
