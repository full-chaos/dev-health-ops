// Package externalrecompute owns the Go-side debounce and compatibility
// handoff for external-ingest recomputation. Metric planning and execution
// deliberately remain in the existing Python/Celery implementation during
// coexistence.
package externalrecompute

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/streamhandlers"
)

var ErrInvalidConfig = errors.New("invalid external recompute controller configuration")

type Claim struct {
	ID    string
	Scope streamhandlers.ExternalRecomputeScope

	ingestionIDs []string
}

type Store interface {
	Coalesce(context.Context, streamhandlers.ExternalRecomputeScope, time.Time, time.Duration) error
	ClaimDue(context.Context, time.Time, int, time.Duration) ([]Claim, error)
	Complete(context.Context, Claim) error
}

type CompatibilityDispatcher interface {
	Dispatch(context.Context, Claim) error
	PendingScopes(context.Context, int) ([]streamhandlers.ExternalRecomputeScope, error)
}

type Config struct {
	Debounce      time.Duration
	PollInterval  time.Duration
	InflightRetry time.Duration
	BatchSize     int
}

func DefaultConfig() Config {
	return Config{
		Debounce:      45 * time.Second,
		PollInterval:  5 * time.Second,
		InflightRetry: 30 * time.Second,
		BatchSize:     50,
	}
}

func (cfg Config) validate() error {
	if cfg.Debounce < time.Second || cfg.Debounce > 10*time.Minute ||
		cfg.PollInterval < 100*time.Millisecond || cfg.PollInterval > time.Minute ||
		cfg.InflightRetry < time.Second || cfg.InflightRetry > 10*time.Minute ||
		cfg.BatchSize < 1 || cfg.BatchSize > 1_000 {
		return ErrInvalidConfig
	}
	return nil
}

// Controller is both the ingestion handler's best-effort scheduler and a
// lifecycle-owned durable drain loop. A failed compatibility dispatch is
// non-fatal to ingestion: its claimed blob remains in the inflight set and is
// retried after InflightRetry.
type Controller struct {
	store      Store
	dispatcher CompatibilityDispatcher
	config     Config
	now        func() time.Time

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

func New(store Store, dispatcher CompatibilityDispatcher, cfg Config) (*Controller, error) {
	if store == nil || dispatcher == nil || cfg.validate() != nil {
		return nil, ErrInvalidConfig
	}
	return &Controller{store: store, dispatcher: dispatcher, config: cfg, now: time.Now}, nil
}

func (*Controller) Name() string { return "external-recompute-control" }

func (controller *Controller) Schedule(ctx context.Context, scope streamhandlers.ExternalRecomputeScope) error {
	if controller == nil {
		return ErrInvalidConfig
	}
	scope = canonicalScope(scope)
	if err := validateScope(scope); err != nil {
		return err
	}
	return controller.store.Coalesce(ctx, scope, controller.now().UTC(), controller.config.Debounce)
}

func (controller *Controller) Start(parent context.Context) error {
	if controller == nil || parent == nil || parent.Err() != nil {
		return ErrInvalidConfig
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if controller.done != nil {
		return ErrInvalidConfig
	}
	ctx, cancel := context.WithCancel(parent)
	controller.cancel = cancel
	controller.done = make(chan struct{})
	go controller.run(ctx, controller.done)
	return nil
}

func (controller *Controller) run(ctx context.Context, done chan struct{}) {
	defer close(done)
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			_ = controller.step(ctx)
			timer.Reset(controller.config.PollInterval)
		}
	}
}

func (controller *Controller) step(ctx context.Context) error {
	pending, pendingErr := controller.dispatcher.PendingScopes(ctx, controller.config.BatchSize)
	for _, scope := range pending {
		if err := controller.Schedule(ctx, scope); err != nil {
			pendingErr = errors.Join(pendingErr, fmt.Errorf("recover pending scope: %w", err))
		}
	}
	claims, claimErr := controller.store.ClaimDue(
		ctx,
		controller.now().UTC(),
		controller.config.BatchSize,
		controller.config.InflightRetry,
	)
	var dispatchErrors []error
	for _, claim := range claims {
		if err := controller.dispatcher.Dispatch(ctx, claim); err != nil {
			dispatchErrors = append(dispatchErrors, fmt.Errorf("dispatch compatibility claim: %w", err))
			continue
		}
		if err := controller.store.Complete(ctx, claim); err != nil {
			dispatchErrors = append(dispatchErrors, fmt.Errorf("complete compatibility claim: %w", err))
		}
	}
	return errors.Join(pendingErr, claimErr, errors.Join(dispatchErrors...))
}

func (controller *Controller) Shutdown(ctx context.Context) error {
	if controller == nil || ctx == nil {
		return ErrInvalidConfig
	}
	controller.mu.Lock()
	cancel, done := controller.cancel, controller.done
	controller.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func canonicalScope(scope streamhandlers.ExternalRecomputeScope) streamhandlers.ExternalRecomputeScope {
	scope.RepoIDs = sortedUnique(scope.RepoIDs)
	scope.TeamIDs = sortedUnique(scope.TeamIDs)
	scope.RecordKinds = sortedUnique(scope.RecordKinds)
	if scope.WindowStart != nil {
		value := scope.WindowStart.UTC()
		scope.WindowStart = &value
	}
	if scope.WindowEnd != nil {
		value := scope.WindowEnd.UTC()
		scope.WindowEnd = &value
	}
	return scope
}

func validateScope(scope streamhandlers.ExternalRecomputeScope) error {
	if scope.OrgID == "" || scope.SourceSystem == "" || scope.SourceInstance == "" ||
		scope.IngestionID.String() == "00000000-0000-0000-0000-000000000000" {
		return ErrInvalidConfig
	}
	if scope.WindowStart != nil && scope.WindowEnd != nil && scope.WindowEnd.Before(*scope.WindowStart) {
		return ErrInvalidConfig
	}
	return nil
}

func sortedUnique(values []string) []string {
	values = append([]string(nil), values...)
	slices.Sort(values)
	return slices.Compact(values)
}
