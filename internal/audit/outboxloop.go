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

// outboxStepper is the production stepper: publish due events, then reap.
type outboxStepper struct {
	pool      *pgxpool.Pool
	publisher *Publisher // nil: publishing disabled, reaping continues
	config    LoopConfig
}

func (s outboxStepper) Step(ctx context.Context, now time.Time) error {
	var errs []error
	if s.publisher != nil {
		if _, err := s.publisher.Step(ctx, now, s.config.PublishLimit); err != nil {
			errs = append(errs, err)
		}
	}
	// The reaper runs whether or not the publisher did: it deletes only rows
	// already marked published, and a failed publish must not stop it.
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
	return newOutboxLoop(outboxStepper{pool: pool, publisher: publisher, config: config}, config, publisher != nil)
}

func newOutboxLoop(s stepper, config LoopConfig, publishing bool) (*OutboxLoop, error) {
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
