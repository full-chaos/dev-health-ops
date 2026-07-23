package streamrunner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

var errNotReady = errors.New("stream runner has not completed a successful stream window")
var errTransientWrite = errors.New("transient stream durable-write failure")

// Runner owns a long-lived XREADGROUP loop. It has one in-flight message per
// configured stream lane, so shutdown can either finish that message or leave
// it pending; it never ACKs a message merely because the process is stopping.
type Runner struct {
	transport Transport
	handler   Handler
	config    Config
	registry  *health.Registry

	ready atomic.Bool

	mu       sync.Mutex
	started  bool
	stopping bool
	cancel   context.CancelFunc
	done     chan struct{}

	processed   uint64
	quarantined uint64
	reclaimed   uint64
	retries     uint64
	failures    uint64
	lastSuccess time.Time
	up          bool
	lastStats   map[string]StreamStats
	streams     []string
	readCursor  int
}

func New(transport Transport, handler Handler, config Config, registry *health.Registry) (*Runner, error) {
	if transport == nil || handler == nil || registry == nil || config.validate() != nil {
		return nil, ErrInvalidConfig
	}
	runner := &Runner{
		transport: transport,
		handler:   handler,
		config:    config,
		registry:  registry,
		lastStats: make(map[string]StreamStats, len(config.Streams)),
	}
	if err := registry.RegisterRequired(config.Name+"_loop", runner.readiness); err != nil {
		return nil, fmt.Errorf("register stream readiness: %w", err)
	}
	if err := registry.RegisterMetrics(config.Name, runner); err != nil {
		return nil, fmt.Errorf("register stream metrics: %w", err)
	}
	return runner, nil
}

func (r *Runner) Name() string { return "stream-" + r.config.Name }

func (r *Runner) Start(parent context.Context) error {
	if parent == nil || parent.Err() != nil {
		return context.Canceled
	}
	ctx, cancel := context.WithCancel(parent)
	r.mu.Lock()
	if r.started {
		r.mu.Unlock()
		cancel()
		return ErrAlreadyStarted
	}
	r.started, r.cancel, r.done = true, cancel, make(chan struct{})
	done := r.done
	r.mu.Unlock()

	if err := r.refreshStreams(ctx); err != nil {
		cancel()
		close(done)
		return err
	}
	go r.run(ctx, done)
	return nil
}

func (r *Runner) run(ctx context.Context, done chan struct{}) {
	defer close(done)
	nextMaintenance := time.Time{}
	for {
		maintain := nextMaintenance.IsZero() || !time.Now().Before(nextMaintenance)
		if maintain {
			nextMaintenance = time.Now().Add(r.config.ReclaimEvery)
		}
		if err := r.cycle(ctx, maintain); err != nil {
			r.recordFailure()
			select {
			case <-ctx.Done():
				return
			case <-time.After(minDuration(r.config.ReclaimEvery, time.Second)):
			}
		}
	}
}

func (r *Runner) window(ctx context.Context) error {
	return r.cycle(ctx, true)
}

func (r *Runner) cycle(ctx context.Context, maintain bool) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if maintain {
		if err := r.refreshStreams(ctx); err != nil {
			return err
		}
	}
	r.mu.Lock()
	streams := append([]string(nil), r.streams...)
	r.mu.Unlock()

	var failures []error
	for _, stream := range streams {
		if maintain {
			if err := r.reclaim(ctx, stream); err != nil {
				failures = append(failures, err)
			}
		}
	}
	readStreams, perStreamCount := r.nextReadLanes(streams)
	if len(readStreams) > 0 {
		messages, err := r.transport.ReadNew(ctx, readStreams, r.config.ConsumerGroup, r.config.ConsumerName, perStreamCount, r.config.Block)
		if err != nil {
			failures = append(failures, fmt.Errorf("read streams: %w", err))
		} else {
			for _, message := range messages {
				if err := r.process(ctx, message); err != nil {
					failures = append(failures, err)
				}
			}
		}
	}
	for _, stream := range streams {
		stats, err := r.transport.Stats(ctx, stream, r.config.ConsumerGroup)
		if err != nil {
			failures = append(failures, fmt.Errorf("inspect stream %q: %w", stream, err))
			continue
		}
		r.mu.Lock()
		r.lastStats[stream] = stats
		r.mu.Unlock()
	}
	if err := errors.Join(failures...); err != nil {
		r.ready.Store(false)
		r.mu.Lock()
		r.up = false
		r.mu.Unlock()
		return err
	}
	r.mu.Lock()
	r.lastSuccess, r.up = time.Now().UTC(), true
	r.mu.Unlock()
	r.ready.Store(true)
	return nil
}

// nextReadLanes keeps one XREADGROUP response bounded by BatchSize even when
// discovery finds more streams than fit in one command. The cursor advances
// deterministically so a hot early lane cannot starve later lanes.
func (r *Runner) nextReadLanes(streams []string) ([]string, int) {
	if len(streams) == 0 {
		return nil, 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	laneCount := min(len(streams), r.config.BatchSize)
	start := r.readCursor % len(streams)
	selected := make([]string, 0, laneCount)
	for offset := range laneCount {
		selected = append(selected, streams[(start+offset)%len(streams)])
	}
	r.readCursor = (start + laneCount) % len(streams)
	return selected, max(1, r.config.BatchSize/laneCount)
}

func (r *Runner) refreshStreams(ctx context.Context) error {
	streams := append([]string(nil), r.config.Streams...)
	if len(r.config.Patterns) > 0 {
		discoverer, ok := r.transport.(Discoverer)
		if !ok {
			return fmt.Errorf("stream discovery unavailable")
		}
		found, err := discoverer.Discover(ctx, r.config.Patterns, r.config.DiscoveryLimit)
		if err != nil {
			return fmt.Errorf("discover streams: %w", err)
		}
		streams = append(streams, found...)
	}
	streams = sortedUnique(streams)
	for _, stream := range streams {
		if err := r.transport.EnsureGroup(ctx, stream, r.config.ConsumerGroup); err != nil {
			return fmt.Errorf("ensure group for stream %q: %w", stream, err)
		}
	}
	r.mu.Lock()
	r.streams = streams
	if len(streams) == 0 {
		r.readCursor = 0
	} else {
		r.readCursor %= len(streams)
	}
	active := make(map[string]struct{}, len(streams))
	for _, stream := range streams {
		active[stream] = struct{}{}
	}
	for stream := range r.lastStats {
		if _, ok := active[stream]; !ok {
			delete(r.lastStats, stream)
		}
	}
	r.mu.Unlock()
	return nil
}

func (r *Runner) reclaim(ctx context.Context, stream string) error {
	pending, err := r.transport.Pending(ctx, stream, r.config.ConsumerGroup, r.config.BatchSize, r.config.ReclaimIdle)
	if err != nil {
		return fmt.Errorf("inspect pending: %w", err)
	}
	claim := make([]string, 0, len(pending))
	for _, item := range pending {
		if item.TimesDelivered >= r.config.MaxDeliveries {
			message := Message{Stream: stream, ID: item.MessageID}
			if err := r.transport.Quarantine(ctx, message, "max_deliveries_exceeded"); err != nil {
				return fmt.Errorf("quarantine poison pending message: %w", err)
			}
			if err := r.transport.Ack(ctx, stream, r.config.ConsumerGroup, item.MessageID); err != nil {
				return fmt.Errorf("ack quarantined pending message: %w", err)
			}
			r.mu.Lock()
			r.quarantined++
			r.mu.Unlock()
			continue
		}
		claim = append(claim, item.MessageID)
	}
	if len(claim) == 0 {
		return nil
	}
	claimed, err := r.transport.Claim(ctx, stream, r.config.ConsumerGroup, r.config.ConsumerName, claim, r.config.ReclaimIdle)
	if err != nil {
		return fmt.Errorf("claim pending: %w", err)
	}
	var failures []error
	for _, message := range claimed {
		r.mu.Lock()
		r.reclaimed++
		r.mu.Unlock()
		if err := r.process(ctx, message); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func sortedUnique(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	slices.Sort(values)
	return slices.Compact(values)
}

func (r *Runner) process(ctx context.Context, message Message) error {
	if err := r.handler.Handle(ctx, message); err != nil {
		if IsPermanent(err) {
			reason := "invalid_message"
			var permanent *PermanentError
			if errors.As(err, &permanent) && permanent.Reason != "" {
				reason = permanent.Reason
			}
			if err := r.transport.Quarantine(ctx, message, reason); err != nil {
				return fmt.Errorf("quarantine permanent message: %w", err)
			}
			if err := r.transport.Ack(ctx, message.Stream, r.config.ConsumerGroup, message.ID); err != nil {
				return fmt.Errorf("ack quarantined message: %w", err)
			}
			r.mu.Lock()
			r.quarantined++
			r.mu.Unlock()
			return nil
		}
		// A transient durable-write failure must leave the entry in the PEL.
		r.mu.Lock()
		r.retries++
		r.mu.Unlock()
		return fmt.Errorf("%w: %w", errTransientWrite, err)
	}
	if err := r.transport.Ack(ctx, message.Stream, r.config.ConsumerGroup, message.ID); err != nil {
		// The durable write committed but the ACK did not. Redelivery is safe only
		// because handlers are required to be idempotent at their authoritative
		// sink boundary; never turn this into an implicit success.
		return fmt.Errorf("ack durable message: %w", err)
	}
	r.mu.Lock()
	r.processed++
	r.mu.Unlock()
	return nil
}

func (r *Runner) recordFailure() {
	r.ready.Store(false)
	r.mu.Lock()
	r.failures++
	r.up = false
	r.mu.Unlock()
}

func (r *Runner) readiness(context.Context) error {
	if r != nil && r.ready.Load() {
		return nil
	}
	return errNotReady
}

func (r *Runner) Shutdown(ctx context.Context) error {
	if r == nil || ctx == nil {
		return ErrInvalidConfig
	}
	r.ready.Store(false)
	r.mu.Lock()
	r.stopping = true
	cancel, done := r.cancel, r.done
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done == nil {
		r.transport.Close()
		return nil
	}
	drainCtx, drainCancel := context.WithTimeout(ctx, r.config.ShutdownDrain)
	defer drainCancel()
	select {
	case <-done:
		r.transport.Close()
		return nil
	case <-drainCtx.Done():
		// The open message remains pending. Close wakes a blocking transport and
		// lets a later process reclaim it; no ACK is attempted during shutdown.
		r.transport.Close()
		return drainCtx.Err()
	}
}

func (r *Runner) WritePrometheus(out io.Writer) error {
	if r == nil || out == nil {
		return errors.New("Prometheus output is required")
	}
	r.mu.Lock()
	processed, quarantined, reclaimed, retries, failures := r.processed, r.quarantined, r.reclaimed, r.retries, r.failures
	lastSuccess, up := r.lastSuccess, r.up
	stats := make(map[string]StreamStats, len(r.lastStats))
	for stream, snapshot := range r.lastStats {
		stats[stream] = snapshot
	}
	r.mu.Unlock()
	var text strings.Builder
	writeCounter(&text, "worker_stream_processed_total", "Durably processed stream messages.", processed)
	writeCounter(&text, "worker_stream_quarantined_total", "Quarantined poison stream messages.", quarantined)
	writeCounter(&text, "worker_stream_reclaimed_total", "Pending stream messages reclaimed for retry.", reclaimed)
	writeCounter(&text, "worker_stream_retries_total", "Transient durable-write failures left pending.", retries)
	writeCounter(&text, "worker_stream_failures_total", "Failed stream windows.", failures)
	var lag, pending int64
	var oldest time.Duration
	for _, snapshot := range stats {
		lag += snapshot.Lag
		pending += snapshot.Pending
		if snapshot.OldestPending > oldest {
			oldest = snapshot.OldestPending
		}
	}
	fmt.Fprintf(&text, "# HELP worker_stream_lag Current stream backlog, aggregated without tenant labels.\n# TYPE worker_stream_lag gauge\nworker_stream_lag %d\n", lag)
	fmt.Fprintf(&text, "# HELP worker_stream_pending Pending consumer-group entries, aggregated without tenant labels.\n# TYPE worker_stream_pending gauge\nworker_stream_pending %d\n", pending)
	fmt.Fprintf(&text, "# HELP worker_stream_oldest_pending_seconds Age of the oldest pending entry.\n# TYPE worker_stream_oldest_pending_seconds gauge\nworker_stream_oldest_pending_seconds %s\n", strconv.FormatFloat(oldest.Seconds(), 'g', -1, 64))
	age := 0.0
	if !lastSuccess.IsZero() {
		age = time.Since(lastSuccess).Seconds()
	}
	fmt.Fprintf(&text, "# HELP worker_stream_last_success_age_seconds Age of the most recent completed stream window.\n# TYPE worker_stream_last_success_age_seconds gauge\nworker_stream_last_success_age_seconds %s\n", strconv.FormatFloat(age, 'g', -1, 64))
	if up {
		text.WriteString("# HELP worker_stream_up Whether a current stream window completed successfully.\n# TYPE worker_stream_up gauge\nworker_stream_up 1\n")
	} else {
		text.WriteString("# HELP worker_stream_up Whether a current stream window completed successfully.\n# TYPE worker_stream_up gauge\nworker_stream_up 0\n")
	}
	_, err := io.WriteString(out, text.String())
	return err
}

func writeCounter(text *strings.Builder, name, help string, value uint64) {
	fmt.Fprintf(text, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", name, help, name, name, value)
}

func minDuration(left, right time.Duration) time.Duration {
	if left < right {
		return left
	}
	return right
}
