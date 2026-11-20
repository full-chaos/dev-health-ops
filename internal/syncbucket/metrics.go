package syncbucket

import (
	"context"
	"slices"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// LockWaitMetricName is the histogram of the bucket-lock wait on a unit writer's commit path (acp lock-1 rule 4). It
// observes only: a long wait never fails the commit.
const LockWaitMetricName = "sync_bucket_lock_wait_seconds"

// Commit-path writers whose bucket-lock wait is observed.
const (
	LockWaitPathProviderUnit = "provider_unit"
)

var lockWaitPaths = []string{LockWaitPathProviderUnit}

type lockWaitRecorder struct {
	mu     sync.Mutex
	totals map[string]uint64
}

var lockWaits = &lockWaitRecorder{totals: map[string]uint64{}}

func (recorder *lockWaitRecorder) observe(ctx context.Context, path string, wait time.Duration) {
	if !slices.Contains(lockWaitPaths, path) {
		path = "other"
	}
	recorder.mu.Lock()
	recorder.totals[path]++
	recorder.mu.Unlock()
	histogram, err := otel.GetMeterProvider().Meter("github.com/full-chaos/dev-health-ops/internal/syncbucket").Float64Histogram(
		LockWaitMetricName,
		metric.WithUnit("s"),
		metric.WithDescription("wait for the sync dispatch bucket advisory lock on a unit writer's commit path, by writer (observed, never enforced)"))
	if err != nil {
		return
	}
	if path == LockWaitPathProviderUnit {
		histogram.Record(ctx, wait.Seconds(), metric.WithAttributes(attribute.String("writer", LockWaitPathProviderUnit)))
		return
	}
	histogram.Record(ctx, wait.Seconds(), metric.WithAttributes(attribute.String("writer", "other")))
}

// LockWaitCount is how many commit-path waits were observed for path, for tests in other packages.
func LockWaitCount(path string) uint64 {
	lockWaits.mu.Lock()
	defer lockWaits.mu.Unlock()
	return lockWaits.totals[path]
}
