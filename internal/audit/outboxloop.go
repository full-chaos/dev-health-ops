package audit

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// ErrInvalidLoopConfig reports an unusable LoopConfig.
var ErrInvalidLoopConfig = errors.New("audit: invalid outbox loop configuration")

const (
	// CheckOutboxLoop is the readiness check name (a metric label value).
	CheckOutboxLoop = "outbox_loop"

	// degradeAfter consecutive failed steps close readiness; one success
	// reopens it. A single transient failure must not flap readiness.
	degradeAfter = 3

	minPollInterval = 10 * time.Millisecond
	maxPollInterval = 15 * time.Minute
)

// LoopConfig bounds the polling work of OutboxLoop.
type LoopConfig struct {
	Schema authschema.ValidatedIdentifier
	// PollInterval is the time between steps.
	PollInterval time.Duration
	// PublishLimit bounds the events one step delivers (1..100).
	PublishLimit int
	// PublishedRetention is how long a published event is kept before the
	// reaper may delete it.
	PublishedRetention time.Duration
	// ReapLimit bounds the rows one sweep deletes.
	ReapLimit int
	Registry  *health.Registry
	Logger    *slog.Logger
	// Meter registers the loop's instruments; nil uses the process meter.
	Meter metric.Meter
}

// DefaultLoopConfig returns the production cadence.
func DefaultLoopConfig(schema authschema.ValidatedIdentifier, registry *health.Registry) LoopConfig {
	return LoopConfig{
		Schema: schema, PollInterval: time.Second, PublishLimit: 50,
		PublishedRetention: 7 * 24 * time.Hour, ReapLimit: 1000, Registry: registry,
	}
}

func (c LoopConfig) validate() error {
	if c.Registry == nil ||
		c.PollInterval < minPollInterval || c.PollInterval > maxPollInterval ||
		c.PublishLimit < 1 || c.PublishLimit > maxStepLimit ||
		c.PublishedRetention <= 0 || c.ReapLimit < 1 || c.ReapLimit > maxReapLimit {
		return ErrInvalidLoopConfig
	}
	return nil
}

// stepper is the seam the loop drives, so its lifecycle is testable apart from
// a database.
type stepper interface {
	// Step publishes (when a publisher exists) and reaps.
	Step(ctx context.Context, now time.Time) error
}

// lagTracker holds the age of the oldest due, unpublished event as of the last
// step. It belongs to the loop, not the publisher, so the backlog is observable
// when publishing is disabled (no sink), which is exactly when it grows.
type lagTracker struct {
	pool   *pgxpool.Pool
	schema authschema.ValidatedIdentifier
	logger *slog.Logger
	age    atomic.Int64
}

func (l *lagTracker) observe(ctx context.Context, now time.Time) {
	if l == nil {
		return
	}
	var age int64
	err := l.pool.QueryRow(ctx, `
		SELECT coalesce(extract(epoch FROM ($1::timestamptz - min(created_at)))::bigint, 0)
		FROM `+authschema.Quote(l.schema)+`.auth_outbox_events
		WHERE published_at IS NULL AND available_at <= $1`, now.UTC()).Scan(&age)
	if err != nil {
		// A gauge that silently stops moving reads as a healthy outbox.
		l.logger.WarnContext(ctx, "outbox lag read failed", "error", err)
		return
	}
	if age < 0 {
		age = 0
	}
	l.age.Store(age)
}

// outboxStepper is the production stepper: record lag, publish due events,
// then reap.
type outboxStepper struct {
	pool      *pgxpool.Pool
	publisher *Publisher // nil: publishing disabled, reaping continues
	config    LoopConfig
	lag       *lagTracker
}

func (s outboxStepper) Step(ctx context.Context, now time.Time) error {
	s.lag.observe(ctx, now)
	var errs []error
	if s.publisher != nil {
		if _, err := s.publisher.Step(ctx, now, s.config.PublishLimit); err != nil {
			errs = append(errs, err)
		}
	}
	// The reaper runs whether or not the publisher did: it deletes only rows
	// already marked published, and a failed publish must not stop it.
	if _, err := Reap(ctx, s.pool, s.config.Schema, now.Add(-s.config.PublishedRetention), s.config.ReapLimit); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// OutboxLoop owns one goroutine that publishes and reaps the outbox. It is a
// lifecycle.Component.
type OutboxLoop struct {
	stepper stepper
	config  LoopConfig
	logger  *slog.Logger

	ready    atomic.Bool
	failures atomic.Int64

	mu      sync.Mutex
	started bool
	cancel  context.CancelFunc
	done    chan struct{}

	// tick supplies the polling channel; tests replace it.
	tick func(time.Duration) (<-chan time.Time, func())
	// stepDone is called after each step, for tests.
	stepDone func()
}

// NewOutboxLoop builds the production loop. publisher may be nil, which turns
// publishing off (no consumer is configured) while the reaper keeps running;
// the loop then says so once at start.
func NewOutboxLoop(pool *pgxpool.Pool, publisher *Publisher, config LoopConfig) (*OutboxLoop, error) {
	if pool == nil {
		return nil, ErrInvalidLoopConfig
	}
	logger := config.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	lag := &lagTracker{pool: pool, schema: config.Schema, logger: logger}
	return newOutboxLoop(outboxStepper{pool: pool, publisher: publisher, config: config, lag: lag}, config, publisher != nil, lag)
}

func newOutboxLoop(s stepper, config LoopConfig, publishing bool, lag *lagTracker) (*OutboxLoop, error) {
	if s == nil || config.validate() != nil {
		return nil, ErrInvalidLoopConfig
	}
	logger := config.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	if !publishing {
		logger = logger.With("publishing", "disabled")
	}
	l := &OutboxLoop{
		stepper: s, config: config, logger: logger,
		tick: func(d time.Duration) (<-chan time.Time, func()) {
			t := time.NewTicker(d)
			return t.C, t.Stop
		},
	}
	if err := config.Registry.RegisterRequired(CheckOutboxLoop, l.readiness); err != nil {
		return nil, fmt.Errorf("register outbox loop readiness: %w", err)
	}
	enabled := int64(0)
	if publishing {
		enabled = 1
	}
	meter := config.Meter
	if meter == nil {
		meter = otel.Meter("github.com/full-chaos/dev-health-ops/internal/audit")
	}
	if _, err := meter.Int64ObservableGauge(
		"devhealth_auth_outbox_publisher_enabled",
		metric.WithDescription("1 when an outbox sink is configured and events are delivered; 0 when publishing is disabled (no sink), so zero deliveries is not read as healthy."),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(enabled)
			return nil
		})); err != nil {
		return nil, fmt.Errorf("register publisher-enabled gauge: %w", err)
	}
	if lag != nil {
		if _, err := meter.Int64ObservableGauge(
			"devhealth_auth_outbox_oldest_pending_age_seconds",
			metric.WithDescription("Age of the oldest due unpublished outbox event at the last step; 0 when none. Exported with or without a sink."),
			metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
				o.Observe(lag.age.Load())
				return nil
			})); err != nil {
			return nil, fmt.Errorf("register pending-age gauge: %w", err)
		}
	}
	if !publishing {
		logger.Info("auth outbox delivery disabled: no sink configured; the reaper still runs")
	}
	return l, nil
}

func (l *OutboxLoop) readiness(context.Context) error {
	if !l.ready.Load() {
		return errors.New("outbox loop has not completed a successful step")
	}
	return nil
}

// Name identifies the component to the lifecycle runtime.
func (*OutboxLoop) Name() string { return "auth-outbox-loop" }

// Start launches the polling goroutine and returns at once: an unreachable
// database must leave the process running with readiness closed, not make
// Start fail (see authruntime.configure). Readiness opens after the first
// successful step.
func (l *OutboxLoop) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.started {
		return errors.New("audit: outbox loop already started")
	}
	l.started = true
	loopCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	l.cancel = cancel
	l.done = make(chan struct{})
	ticks, stop := l.tick(l.config.PollInterval)
	go l.run(loopCtx, ticks, stop, l.done)
	return nil
}

func (l *OutboxLoop) run(ctx context.Context, ticks <-chan time.Time, stop func(), done chan struct{}) {
	defer close(done)
	defer stop()
	l.stepOnce(ctx, time.Now())
	for {
		select {
		case <-ctx.Done():
			return
		case now, open := <-ticks:
			if !open {
				return
			}
			l.stepOnce(ctx, now)
		}
	}
}

func (l *OutboxLoop) stepOnce(ctx context.Context, now time.Time) {
	err := l.stepper.Step(ctx, now)
	switch {
	case err == nil:
		l.failures.Store(0)
		l.ready.Store(true)
	case ctx.Err() != nil:
		// Shutdown in flight: not an operational failure.
	default:
		n := l.failures.Add(1)
		if n >= degradeAfter {
			l.ready.Store(false)
		}
		l.logger.ErrorContext(ctx, "outbox step failed", "consecutive_failures", n, "error", err)
	}
	if l.stepDone != nil {
		l.stepDone()
	}
}

// Shutdown stops the goroutine and waits for the step in flight, within ctx.
func (l *OutboxLoop) Shutdown(ctx context.Context) error {
	l.mu.Lock()
	cancel, done := l.cancel, l.done
	l.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("audit: outbox loop did not stop: %w", ctx.Err())
	}
}
