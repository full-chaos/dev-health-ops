package streamrunner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/tracing"
	"go.opentelemetry.io/otel/attribute"
)

var errNotReady = errors.New("stream runner has not completed a successful stream window")
var errTransientWrite = errors.New("transient stream durable-write failure")

const initialIdleDelay = 10 * time.Millisecond

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

	processed     uint64
	quarantined   uint64
	quarantinedBy map[quarantineKey]uint64
	reclaimed     uint64
	retries       uint64
	failures      uint64
	lastSuccess   time.Time
	up            bool
	lastStats     map[string]StreamStats
	streams       []string
	readCursor    int
	waitIdle      func(context.Context, time.Duration) bool
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
		waitIdle:  waitForContext,
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
	idleDelay := initialIdleDelay
	maxIdleDelay := minDuration(r.config.Block, time.Second)
	for {
		maintain := nextMaintenance.IsZero() || !time.Now().Before(nextMaintenance)
		if maintain {
			nextMaintenance = time.Now().Add(r.config.ReclaimEvery)
		}
		active, err := r.cycle(ctx, maintain)
		if err != nil {
			r.recordFailure()
			r.logger().ErrorContext(ctx, "stream runner cycle failed",
				"stream", r.config.Name,
				"error", err.Error(),
			)
			idleDelay = initialIdleDelay
			select {
			case <-ctx.Done():
				return
			case <-time.After(minDuration(r.config.ReclaimEvery, time.Second)):
			}
			continue
		}
		if active {
			idleDelay = initialIdleDelay
			continue
		}
		if !r.waitIdle(ctx, idleDelay) {
			return
		}
		idleDelay = minDuration(idleDelay*2, maxIdleDelay)
	}
}

func (r *Runner) window(ctx context.Context) error {
	_, err := r.cycle(ctx, true)
	return err
}

func (r *Runner) cycle(ctx context.Context, maintain bool) (bool, error) {
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	if maintain {
		if err := r.refreshStreams(ctx); err != nil {
			return false, err
		}
	}
	r.mu.Lock()
	streams := append([]string(nil), r.streams...)
	r.mu.Unlock()

	active := false
	var failures []error
	for _, stream := range streams {
		if maintain {
			reclaimed, err := r.reclaim(ctx, stream)
			active = active || reclaimed
			if err != nil {
				failures = append(failures, err)
			}
		}
	}
	readStreams, perStreamCount := r.nextReadLanes(streams)
	if len(readStreams) > 0 {
		messages, err := r.transport.ReadNew(ctx, readStreams, r.config.ConsumerGroup, r.config.ConsumerName, perStreamCount, r.config.Block)
		if err != nil {
			failures = append(failures, fmt.Errorf("read streams: %w", err))
			// A failed read is work that went wrong, not an idle cycle.
			_, readSpan := tracing.StartWorkSpan(ctx, runnerTracerName, "dev_health.stream.read_failed",
				attribute.String("dev_health.stream.runner", r.config.Name),
				attribute.Int("dev_health.stream.lanes", len(readStreams)),
			)
			tracing.EndWorkSpan(readSpan, err)
		} else {
			active = active || len(messages) > 0
			if len(messages) > 0 {
				// One span per NON-EMPTY read batch: an idle cycle (no messages)
				// repeats about once a second and would be noise.
				batchCtx, batch := tracing.StartWorkSpan(ctx, runnerTracerName, "dev_health.stream.batch",
					attribute.String("dev_health.stream.runner", r.config.Name),
					attribute.Int("dev_health.stream.messages", len(messages)),
					attribute.Int("dev_health.stream.lanes", len(readStreams)),
				)
				var batchFailures []error
				for _, message := range messages {
					if err := r.process(batchCtx, message); err != nil {
						failures = append(failures, err)
						batchFailures = append(batchFailures, err)
					}
				}
				batch.SetAttributes(attribute.Int("dev_health.stream.failed", len(batchFailures)))
				tracing.EndWorkSpan(batch, errors.Join(batchFailures...))
			}
		}
	}
	if maintain {
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
	}
	if err := errors.Join(failures...); err != nil {
		r.ready.Store(false)
		r.mu.Lock()
		r.up = false
		r.mu.Unlock()
		return active, err
	}
	// A cycle that was already in flight when Shutdown began finishes here
	// with no failure. Shutdown cleared readiness before it cancelled this
	// cycle's context, so an unconditional ready.Store(true) would land after
	// that clear and report a stopped runner ready. Both writers order through
	// r.mu, and Shutdown sets stopping under it, so either this runs first
	// (Shutdown's clear then wins) or it sees stopping and leaves the state
	// Shutdown left.
	r.mu.Lock()
	if !r.stopping {
		r.lastSuccess, r.up = time.Now().UTC(), true
		r.ready.Store(true)
	}
	r.mu.Unlock()
	return active, nil
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
	// A runner must never consume a dead-letter stream it writes. Redis globs
	// cannot exclude a separator, so the PagerDuty pattern
	// "pagerduty-webhooks:*" necessarily also matches its own
	// "pagerduty-webhooks:<binding>:dlq" key, and consuming that would replay
	// quarantined entries forever. Existing lanes are unaffected: their DLQ keys
	// are not discovered by their own patterns.
	streams = slices.DeleteFunc(streams, func(stream string) bool {
		return strings.HasSuffix(stream, ":dlq")
	})
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

func (r *Runner) reclaim(ctx context.Context, stream string) (bool, error) {
	pending, err := r.transport.Pending(ctx, stream, r.config.ConsumerGroup, r.config.BatchSize, r.config.ReclaimIdle)
	if err != nil {
		return false, fmt.Errorf("inspect pending: %w", err)
	}
	claim := make([]string, 0, len(pending))
	poison := make(map[string]struct{})
	for _, item := range pending {
		if item.TimesDelivered >= r.config.MaxDeliveries {
			poison[item.MessageID] = struct{}{}
		}
		claim = append(claim, item.MessageID)
	}
	if len(claim) == 0 {
		return false, nil
	}
	claimed, err := r.transport.Claim(ctx, stream, r.config.ConsumerGroup, r.config.ConsumerName, claim, r.config.ReclaimIdle)
	if err != nil {
		return false, fmt.Errorf("claim pending: %w", err)
	}
	claimedByID := make(map[string]Message, len(claimed))
	for _, message := range claimed {
		claimedByID[message.ID] = message
	}
	var failures []error
	for _, item := range pending {
		message, found := claimedByID[item.MessageID]
		if !found {
			if _, isPoison := poison[item.MessageID]; !isPoison {
				continue
			}
			// Streams can trim an entry while its ID remains in the PEL. With
			// no fields left to recover, preserve a tombstone DLQ row and ACK
			// the orphan; the external orphan reconciler owns any status row.
			message = Message{Stream: stream, ID: item.MessageID}
		}
		r.mu.Lock()
		r.reclaimed++
		r.mu.Unlock()
		if _, isPoison := poison[item.MessageID]; isPoison {
			if err := r.quarantine(ctx, message, "max_deliveries_exceeded"); err != nil {
				failures = append(failures, err)
			}
			continue
		}
		if err := r.process(ctx, message); err != nil {
			failures = append(failures, err)
		}
	}
	return len(claimed) > 0, errors.Join(failures...)
}

func sortedUnique(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	slices.Sort(values)
	return slices.Compact(values)
}

// process handles one message under one dev_health.stream.handle span. The
// span carries the runner name and a fixed outcome word; never the stream key,
// the message id or any payload (CHAOS-7879).
func (r *Runner) process(ctx context.Context, message Message) error {
	ctx, span := tracing.StartWorkSpan(ctx, runnerTracerName, "dev_health.stream.handle",
		attribute.String("dev_health.stream.runner", r.config.Name),
	)
	outcome, err := r.handle(ctx, message)
	span.SetAttributes(attribute.String("dev_health.stream.outcome", outcome))
	tracing.EndWorkSpan(span, err)
	return err
}

// runnerTracerName scopes the stream runner's spans by package.
const runnerTracerName = "github.com/full-chaos/dev-health-ops/internal/streamrunner"

// handle is process's body; it returns a fixed outcome word with the error.
func (r *Runner) handle(ctx context.Context, message Message) (string, error) {
	if err := r.handler.Handle(ctx, message); err != nil {
		if IsPermanent(err) {
			reason := "invalid_message"
			var permanent *PermanentError
			if errors.As(err, &permanent) && permanent.Reason != "" {
				reason = permanent.Reason
			}
			if err := r.quarantine(ctx, message, reason); err != nil {
				return "quarantine_error", err
			}
			return "quarantined", nil
		}
		// A transient durable-write failure must leave the entry in the PEL.
		r.mu.Lock()
		r.retries++
		r.mu.Unlock()
		return "transient_error", fmt.Errorf("%w: %w", errTransientWrite, err)
	}
	if err := r.transport.Ack(ctx, message.Stream, r.config.ConsumerGroup, message.ID); err != nil {
		// The durable write committed but the ACK did not. Redelivery is safe only
		// because handlers are required to be idempotent at their authoritative
		// sink boundary; never turn this into an implicit success.
		return "ack_error", fmt.Errorf("ack durable message: %w", err)
	}
	r.mu.Lock()
	r.processed++
	r.mu.Unlock()
	return "acked", nil
}

func (r *Runner) quarantine(ctx context.Context, message Message, reason string) error {
	if err := r.transport.Quarantine(ctx, message, reason); err != nil {
		return fmt.Errorf("quarantine permanent message: %w", err)
	}
	if finalizer, ok := r.handler.(PermanentFinalizer); ok {
		if err := finalizer.FinalizePermanent(ctx, message, reason); err != nil {
			return fmt.Errorf("finalize permanent message: %w", err)
		}
	}
	// The dead-letter row is durable from here: count and log it now, so a quarantine whose ACK then
	// fails (the entry is redelivered and written again) is still visible, once per row written.
	r.recordQuarantineRow(ctx, message, reason)
	if err := r.transport.Ack(ctx, message.Stream, r.config.ConsumerGroup, message.ID); err != nil {
		return fmt.Errorf("ack quarantined message: %w", err)
	}
	r.mu.Lock()
	r.quarantined++
	r.mu.Unlock()
	return nil
}

func (r *Runner) recordQuarantineRow(ctx context.Context, message Message, reason string) {
	r.mu.Lock()
	if r.quarantinedBy == nil {
		r.quarantinedBy = make(map[quarantineKey]uint64)
	}
	r.quarantinedBy[newQuarantineKey(message.Stream, reason)]++
	r.mu.Unlock()
	retention := retentionFor(message, reason)
	// Sizes and counts only: a payload never reaches a log line.
	r.logger().WarnContext(ctx, "stream message quarantined",
		"reason", reason, "stream", message.Stream, "entry_id", message.ID,
		"fields", len(message.Fields), "events_retention", retention.Mode,
		"events_bytes", retention.Bytes, "events_count", retention.EventsCount,
		"row_bytes", deadLetterRowBytes(message, reason))
}

// quarantineKey is the bounded label set of the per-reason counter: the stream
// family (the first segment of the stream key, never a per-org key) and the
// reason when it is a plain lower-case token.
type quarantineKey struct{ stream, reason string }

func newQuarantineKey(stream, reason string) quarantineKey {
	family, _, _ := strings.Cut(stream, ":")
	switch family {
	case "product-telemetry", "external-ingest", "ingest", "pagerduty-webhooks":
	default:
		family = "other"
	}
	valid := reason != "" && len(reason) <= 64
	for _, c := range reason {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_') {
			valid = false
		}
	}
	if !valid {
		reason = "other"
	}
	return quarantineKey{stream: family, reason: reason}
}

func (r *Runner) recordFailure() {
	r.ready.Store(false)
	r.mu.Lock()
	r.failures++
	r.up = false
	r.mu.Unlock()
}

// logger is nil-safe: an unset Config.Logger discards rather than panicking,
// and never falls back to slog.Default(), so an embedder cannot be surprised
// by stream-runner output appearing on a logger it did not choose.
func (r *Runner) logger() *slog.Logger {
	if r == nil || r.config.Logger == nil {
		return slog.New(slog.DiscardHandler)
	}
	return r.config.Logger
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
	r.mu.Lock()
	r.stopping = true
	r.ready.Store(false)
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
	quarantinedBy := make(map[quarantineKey]uint64, len(r.quarantinedBy))
	for key, count := range r.quarantinedBy {
		quarantinedBy[key] = count
	}
	stats := make(map[string]StreamStats, len(r.lastStats))
	for stream, snapshot := range r.lastStats {
		stats[stream] = snapshot
	}
	r.mu.Unlock()
	var text strings.Builder
	writeCounter(&text, "worker_stream_processed_total", "Durably processed stream messages.", processed)
	writeCounter(&text, "worker_stream_quarantined_total", "Quarantined poison stream messages.", quarantined)
	text.WriteString("# HELP worker_stream_quarantined_by_reason_total Quarantined poison stream messages by stream family and reason.\n# TYPE worker_stream_quarantined_by_reason_total counter\n")
	keys := make([]quarantineKey, 0, len(quarantinedBy))
	for key := range quarantinedBy {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, func(a, b quarantineKey) int {
		return strings.Compare(a.stream+"\x00"+a.reason, b.stream+"\x00"+b.reason)
	})
	for _, key := range keys {
		fmt.Fprintf(&text, "worker_stream_quarantined_by_reason_total{stream=%q,reason=%q} %d\n", key.stream, key.reason, quarantinedBy[key])
	}
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

func waitForContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// deadLetterRowBytes is the size of the field names and values the dead-letter row carries, with
// moved_at counted at its longest RFC3339Nano form.
func deadLetterRowBytes(message Message, reason string) int {
	total := 0
	for key, value := range DeadLetterFields(message, reason, "0000-00-00T00:00:00.000000000Z") {
		total += len(key) + len(value)
	}
	return total
}
