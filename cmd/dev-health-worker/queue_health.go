package main

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
)

// The Celery monitor-queue-depths Beat task ran every 60 seconds, logged one
// queue_depth record per non-empty queue, and escalated to queue_backlog when a
// queue exceeded 200 pending messages or its oldest message exceeded 10
// minutes. It wrote no ClickHouse rows and exported no metrics: the logs were
// the alert surface.
//
// This is the River-native replacement. It samples the same queue-control
// telemetry the readiness and Prometheus paths already use, so depth and age
// come from the authoritative River tables rather than a Redis LLEN probe that
// silently returned zero on a non-Redis transport. The thresholds are carried
// over unchanged so existing alert rules keep firing on the same conditions.
const (
	queueHealthInterval       = time.Minute
	queueDepthWarningRows     = 200
	queueAgeWarningSeconds    = 600
	queueSaturationWarnRatio  = 0.9
	queueHealthSampleTimeout  = 15 * time.Second
	queueHealthFailureLogging = "queue_health_sample_failed"
)

type queueHealthMonitor struct {
	sampler  queueTelemetrySampler
	logger   *slog.Logger
	profile  string
	interval time.Duration
	start    sync.Once
	stop     sync.Once
	running  atomic.Bool
	done     chan struct{}
	stopped  chan struct{}
}

func newQueueHealthMonitor(
	sampler queueTelemetrySampler,
	logger *slog.Logger,
	profile string,
) *queueHealthMonitor {
	if sampler == nil || logger == nil || profile == "" {
		return nil
	}
	return &queueHealthMonitor{
		sampler:  sampler,
		logger:   logger,
		profile:  profile,
		interval: queueHealthInterval,
		done:     make(chan struct{}),
		stopped:  make(chan struct{}),
	}
}

func (monitor *queueHealthMonitor) Name() string { return "queue-health-monitor" }

// Start never blocks the runtime on telemetry. A queue-control outage already
// closes readiness through queued_contract_versions; the monitor must not also
// prevent the process from serving what it can still serve.
func (monitor *queueHealthMonitor) Start(context.Context) error {
	monitor.start.Do(func() {
		monitor.running.Store(true)
		go monitor.loop()
	})
	return nil
}

// Shutdown is safe to call more than once and safe to call on a monitor that
// never started. The runtime only stops components it started, but a lifecycle
// component that can panic or hang when that assumption is violated turns an
// unrelated startup failure into a stuck or crashing shutdown.
func (monitor *queueHealthMonitor) Shutdown(ctx context.Context) error {
	if !monitor.running.Load() {
		return nil
	}
	monitor.stop.Do(func() { close(monitor.done) })
	select {
	case <-monitor.stopped:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (monitor *queueHealthMonitor) loop() {
	defer close(monitor.stopped)
	ticker := time.NewTicker(monitor.interval)
	defer ticker.Stop()
	for {
		monitor.sample()
		select {
		case <-monitor.done:
			return
		case <-ticker.C:
		}
	}
}

func (monitor *queueHealthMonitor) sample() {
	ctx, cancel := context.WithTimeout(context.Background(), queueHealthSampleTimeout)
	defer cancel()
	snapshot, err := monitor.sampler.Snapshot(ctx)
	if err != nil {
		// The underlying error may have originated at a DSN boundary, so only
		// the bounded category is logged.
		monitor.logger.WarnContext(ctx, queueHealthFailureLogging, "profile", monitor.profile)
		return
	}
	monitor.report(ctx, snapshot)
}

// report emits one record per queue that currently holds work. Empty queues are
// skipped, exactly as the Celery task did, so a quiet fleet does not drown the
// records that matter.
func (monitor *queueHealthMonitor) report(
	ctx context.Context,
	snapshot riverstore.QueueTelemetrySnapshot,
) {
	depths := make(map[string]int64, len(snapshot.Jobs))
	for _, job := range snapshot.Jobs {
		depths[job.Queue] += job.Available
	}
	ages := make(map[string]time.Duration, len(snapshot.Queues))
	for _, queue := range snapshot.Queues {
		ages[queue.Queue] = queue.OldestAvailableAge
	}
	queues := make([]string, 0, len(depths))
	for queue := range depths {
		queues = append(queues, queue)
	}
	sort.Strings(queues)

	for _, queue := range queues {
		depth := depths[queue]
		if depth <= 0 {
			continue
		}
		age := ages[queue]
		attributes := []any{
			"profile", monitor.profile,
			"queue", queue,
			"depth", depth,
			"oldest_age_seconds", age.Seconds(),
		}
		monitor.logger.InfoContext(ctx, "queue_depth", attributes...)
		if depth > queueDepthWarningRows || age.Seconds() > queueAgeWarningSeconds {
			monitor.logger.WarnContext(ctx, "queue_backlog", attributes...)
		}
	}
	// Saturation has no Celery equivalent: Celery could not see how much of a
	// worker's capacity was already committed. It is the earliest signal that a
	// queue is about to build a backlog, so it is reported independently of
	// depth.
	if snapshot.ExecutionSaturation > queueSaturationWarnRatio {
		monitor.logger.WarnContext(ctx, "queue_saturation",
			"profile", monitor.profile,
			"execution_saturation", snapshot.ExecutionSaturation,
		)
	}
}
