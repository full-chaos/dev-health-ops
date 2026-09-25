package workerservice

import (
	"log/slog"

	"github.com/full-chaos/dev-health-ops/internal/platform/busyprobe"
)

// CHAOS-6771: readiness answers "can this replica do its work", and a work
// pool fully acquired by progressing jobs is BUSY, not BROKEN. The rule and its
// bounds live in internal/platform/busyprobe (shared with the scheduler and the
// reconciler); the worker's progress evidence is claim liveness (a real job was
// claimed recently, or the queues are empty or at capacity).

// newReadinessBusy builds the worker's busy counter:
// worker_readiness_busy_total{check}.
func newReadinessBusy(logger *slog.Logger) *busyprobe.Counter {
	return busyprobe.NewCounter("worker_readiness_busy_total",
		[]string{"idempotency_backend", "execution_liveness"}, logger)
}
