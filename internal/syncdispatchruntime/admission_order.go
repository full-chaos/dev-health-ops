package syncdispatchruntime

import (
	"context"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
)

// runOrder is the admission position of a run (CHAOS-8880): runs of one bucket are admitted oldest first on the
// total order (createdAt, id). backfill marks a run that never holds back a non-backfill run.
type runOrder struct {
	id        string
	createdAt time.Time
	backfill  bool
}

// olderRun is one open run of a bucket, older than the deciding run, that still has units waiting for a slot.
// progressAt is the newest sign that its dispatch makes progress; waiting is how many of its units wait in the bucket.
type olderRun struct {
	runOrder
	progressAt time.Time
	waiting    int
}

// admissionLivenessBound is how long an older run may show no progress and still hold slots back.
func admissionLivenessBound() time.Duration {
	return 2 * redispatchCountdownMax(redispatchCountdown())
}

// holdBackFor returns how many of freeSlots the deciding run leaves to older live runs of the bucket, and the
// oldest run it held slots for (empty when it held none).
func holdBackFor(self runOrder, older []olderRun, freeSlots int, now time.Time, bound time.Duration) (int, string) {
	return 0, ""
}

// wakeHeldBackRuns arms one dispatch wakeup per distinct run the pass held slots back for, after the pass committed.
func wakeHeldBackRuns(ctx context.Context, db syncrunrollup.Execer, runIDs []string, now time.Time) int {
	return 0
}
