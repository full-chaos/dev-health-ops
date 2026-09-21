package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/streamhandlers"
	"github.com/full-chaos/dev-health-ops/internal/streamrunner"
)

var errStreamConsumersAlreadyStarted = errors.New("stream consumers already started")

type streamConsumerSpec struct {
	kind   streamHandlerKind
	config streamrunner.Config
}

// streamStartupRetry bounds how often the supervisor retries a start-up that
// failed on a dependency. The cap keeps a long outage from producing a log
// line every second while still recovering within half a minute of the
// dependency coming back.
type streamStartupRetry struct {
	initial time.Duration
	max     time.Duration
}

var defaultStreamStartupRetry = streamStartupRetry{initial: time.Second, max: 30 * time.Second}

type streamStorageRef struct{ storage streamStorage }

// streamConsumerSupervisor owns everything in the stream runner that needs a
// live dependency: the storage connections, the start-up probes and the
// consumers built on them. It starts them in the background and retries with
// backoff until every start-up probe passes, so a dependency that is down
// while the process starts delays readiness instead of latching it shut.
//
// The readiness checks registered in configure delegate to whatever storage
// the supervisor currently holds, so /readyz always reports the live state of
// each dependency rather than the outcome of the first start-up attempt.
//
// Consumers are built and started exactly once, after the first attempt on
// which every probe passes; a retry never builds a second set. Once built, the
// consumers ride out later outages on their own: the drivers reconnect and a
// runner's loop retries its cycle.
type streamConsumerSupervisor struct {
	cfg           config.Config
	registry      *health.Registry
	open          func(context.Context, config.Config, *slog.Logger) (streamStorage, error)
	logger        *slog.Logger
	specs         []streamConsumerSpec
	observer      streamhandlers.ExternalIngestObserver
	startupChecks []streamReadinessCheck
	retry         streamStartupRetry

	storage   atomic.Pointer[streamStorageRef]
	consuming atomic.Bool
	failures  chan error

	mu      sync.Mutex
	started bool
	cancel  context.CancelFunc
	done    chan struct{}
	running []lifecycle.Component
}

func newStreamConsumerSupervisor(
	cfg config.Config,
	registry *health.Registry,
	open func(context.Context, config.Config, *slog.Logger) (streamStorage, error),
	logger *slog.Logger,
	specs []streamConsumerSpec,
	observer streamhandlers.ExternalIngestObserver,
) *streamConsumerSupervisor {
	return &streamConsumerSupervisor{
		cfg: cfg, registry: registry, open: open, logger: logger,
		specs: specs, observer: observer, retry: defaultStreamStartupRetry,
		failures: make(chan error, 1),
	}
}

func (*streamConsumerSupervisor) Name() string { return "stream-consumers" }

// Errors reports a failure no retry can fix, found after Start returned: a
// consumer configuration the runner refuses, or a consumer that would not
// start. The runtime stops the process on it.
func (supervisor *streamConsumerSupervisor) Errors() <-chan error { return supervisor.failures }

// ShutdownBudget reserves each consumer's own drain window, which the
// runtime would otherwise have granted per consumer component.
func (supervisor *streamConsumerSupervisor) ShutdownBudget() time.Duration {
	var budget time.Duration
	for _, spec := range supervisor.specs {
		budget += spec.config.ShutdownDrain
	}
	return budget
}

func (supervisor *streamConsumerSupervisor) Start(parent context.Context) error {
	if parent == nil || parent.Err() != nil {
		return context.Canceled
	}
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	if supervisor.started {
		return errStreamConsumersAlreadyStarted
	}
	ctx, cancel := context.WithCancel(parent)
	supervisor.started, supervisor.cancel, supervisor.done = true, cancel, make(chan struct{})
	go supervisor.run(ctx, supervisor.done)
	return nil
}

func (supervisor *streamConsumerSupervisor) run(ctx context.Context, done chan<- struct{}) {
	defer close(done)
	storage, ok := supervisor.awaitDependencies(ctx)
	if !ok {
		return
	}
	components, err := supervisor.build(storage)
	if err != nil {
		if errors.Is(err, streamrunner.ErrInvalidConfig) {
			supervisor.fail(err)
			return
		}
		// A consumer that cannot be constructed over healthy storage keeps
		// the process live and stream_consumer unready, as before; the
		// storage stays open so the other checks keep reporting live state.
		supervisor.log().ErrorContext(ctx, "stream consumers could not be built",
			"error_category", "stream_consumer_unavailable",
			"profile", supervisor.cfg.Profile,
		)
		return
	}
	for _, component := range components {
		if ctx.Err() != nil {
			return
		}
		if err := component.Start(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			supervisor.fail(fmt.Errorf("start component %s: %w", component.Name(), err))
			return
		}
		supervisor.mu.Lock()
		supervisor.running = append(supervisor.running, component)
		supervisor.mu.Unlock()
	}
	supervisor.consuming.Store(true)
	supervisor.log().InfoContext(ctx, "stream consumers started",
		"profile", supervisor.cfg.Profile,
		"consumers", len(components),
	)
}

// awaitDependencies opens storage and runs the start-up probes until every
// probe passes, backing off between attempts. It returns false when the
// context ends first or when the failure is a configuration no retry can fix.
func (supervisor *streamConsumerSupervisor) awaitDependencies(ctx context.Context) (streamStorage, bool) {
	delay := supervisor.retry.initial
	for attempt := 1; ; attempt++ {
		storage, err := supervisor.attempt(ctx)
		if err == nil {
			if attempt > 1 {
				supervisor.log().InfoContext(ctx, "stream runner dependencies recovered",
					"profile", supervisor.cfg.Profile,
					"attempts", attempt,
				)
			}
			return storage, true
		}
		if ctx.Err() != nil {
			return nil, false
		}
		attributes := []any{
			"error_category", "dependency_unavailable",
			"profile", supervisor.cfg.Profile,
			"attempt", attempt,
		}
		var failure dependencyFailure
		var probes startupProbeFailure
		switch {
		case errors.As(err, &failure):
			attributes = append(attributes, "reason", failure.reason)
		case errors.As(err, &probes):
			attributes = append(attributes, "failed_checks", probes.checks)
		default:
			attributes = append(attributes, "reason", "stream_storage_open_failed")
		}
		if !startupRetryable(err) {
			// Retrying a rejected configuration cannot succeed. The process
			// stays live and unready with every check named, which is what
			// an operator scrapes to find the missing setting.
			supervisor.log().ErrorContext(ctx, "stream runner start-up stopped: configuration rejected", attributes...)
			return nil, false
		}
		attributes = append(attributes, "retry_in", delay.String())
		supervisor.log().WarnContext(ctx, "stream runner start-up attempt failed", attributes...)
		if !sleepContext(ctx, delay) {
			return nil, false
		}
		delay = min(delay*2, supervisor.retry.max)
	}
}

type startupProbeFailure struct{ checks []string }

func (failure startupProbeFailure) Error() string {
	return fmt.Sprintf("stream runner start-up probes failed: %v", failure.checks)
}

func (failure startupProbeFailure) Unwrap() error { return errStreamDependencyUnavailable }

// attempt opens storage when none is held yet, then runs every start-up
// probe. Storage that opened is kept across failed probes: its drivers
// reconnect on their own, so only a failed open is retried by reopening.
func (supervisor *streamConsumerSupervisor) attempt(ctx context.Context) (streamStorage, error) {
	storage := supervisor.current()
	if storage == nil {
		opened, err := supervisor.open(ctx, supervisor.cfg, supervisor.logger)
		if err != nil {
			return nil, err
		}
		if opened == nil {
			return nil, dependencyUnavailable("stream_storage_open_failed")
		}
		// Published before the probes run so the readiness checks and the
		// posture gauge reflect this storage's live state from now on;
		// Shutdown closes whatever is published.
		supervisor.storage.Store(&streamStorageRef{storage: opened})
		storage = opened
	}
	timeout := supervisor.cfg.HealthCheckTimeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	var failed []string
	for _, probe := range supervisor.startupChecks {
		probeCtx, cancel := context.WithTimeout(ctx, timeout)
		err := probe.check(probeCtx)
		cancel()
		if err != nil {
			failed = append(failed, probe.name)
		}
	}
	if len(failed) > 0 {
		return nil, startupProbeFailure{checks: failed}
	}
	return storage, nil
}

// build constructs the profile's consumers over storage. It runs once.
func (supervisor *streamConsumerSupervisor) build(storage streamStorage) ([]lifecycle.Component, error) {
	runners := make([]lifecycle.Component, 0, len(supervisor.specs))
	for _, spec := range supervisor.specs {
		runner, err := buildStreamRunner(storage, supervisor.registry, spec.kind, spec.config, supervisor.logger, supervisor.observer)
		if err != nil {
			// A runner that was built but never started only holds its
			// transport; Shutdown releases it without waiting.
			for _, built := range runners {
				_ = built.Shutdown(context.Background())
			}
			return nil, err
		}
		runners = append(runners, runner)
	}
	// Control components (the external recompute controller) exist only
	// once their handler is built, and start before the consumer feeding them.
	return append(storage.ControlComponents(), runners...), nil
}

func (supervisor *streamConsumerSupervisor) Shutdown(ctx context.Context) error {
	supervisor.mu.Lock()
	cancel, done := supervisor.cancel, supervisor.done
	supervisor.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	supervisor.consuming.Store(false)
	supervisor.mu.Lock()
	running := supervisor.running
	supervisor.running = nil
	supervisor.mu.Unlock()
	var errs []error
	for index := len(running) - 1; index >= 0; index-- {
		if err := running[index].Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("shutdown component %s: %w", running[index].Name(), err))
		}
	}
	if ref := supervisor.storage.Swap(nil); ref != nil {
		ref.storage.Close()
	}
	return errors.Join(errs...)
}

func (supervisor *streamConsumerSupervisor) fail(err error) {
	select {
	case supervisor.failures <- err:
	default:
	}
}

func (supervisor *streamConsumerSupervisor) log() *slog.Logger {
	if supervisor.logger == nil {
		return slog.New(slog.DiscardHandler)
	}
	return supervisor.logger
}

func (supervisor *streamConsumerSupervisor) current() streamStorage {
	if ref := supervisor.storage.Load(); ref != nil {
		return ref.storage
	}
	return nil
}

func (supervisor *streamConsumerSupervisor) clickHouseReady(ctx context.Context) error {
	storage := supervisor.current()
	if storage == nil {
		return errStreamDependencyUnavailable
	}
	return storage.ClickHouseReady(ctx)
}

func (supervisor *streamConsumerSupervisor) domainPostgresReady(ctx context.Context) error {
	storage := supervisor.current()
	if storage == nil {
		return errStreamDependencyUnavailable
	}
	return storage.DomainPostgresReady(ctx)
}

func (supervisor *streamConsumerSupervisor) valkeyReady(ctx context.Context) error {
	storage := supervisor.current()
	if storage == nil {
		return errStreamDependencyUnavailable
	}
	return storage.ValkeyReady(ctx)
}

func (supervisor *streamConsumerSupervisor) postureManifestLockstep(
	ctx context.Context, binaryDigest string,
) (postgres.PostureManifestLockstepResult, error) {
	storage := supervisor.current()
	if storage == nil {
		return postgres.PostureManifestLockstepResult{}, errStreamDependencyUnavailable
	}
	return storage.PostureManifestLockstep(ctx, binaryDigest)
}

func (supervisor *streamConsumerSupervisor) consumersReady(context.Context) error {
	if !supervisor.consuming.Load() {
		return errStreamDependencyUnavailable
	}
	return nil
}

func sleepContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
