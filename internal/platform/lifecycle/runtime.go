// Package lifecycle coordinates startup, asynchronous failures, and ordered
// graceful shutdown for worker processes.
package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"
)

// Component starts synchronously and shuts down when the runtime exits.
// Components are stopped in reverse startup order.
type Component interface {
	Name() string
	Start(context.Context) error
	Shutdown(context.Context) error
}

// ErrorSource is optionally implemented by components that may fail after a
// successful Start.
type ErrorSource interface {
	Errors() <-chan error
}

// ShutdownBudgetSource reserves a reviewed part of the global shutdown
// deadline for a component. Later components are still attempted if it uses
// the remaining deadline.
type ShutdownBudgetSource interface {
	ShutdownBudget() time.Duration
}

type Options struct {
	Logger          *slog.Logger
	ShutdownTimeout time.Duration
	Components      []Component
}

type Runtime struct {
	logger          *slog.Logger
	shutdownTimeout time.Duration
	components      []Component
}

func New(options Options) (*Runtime, error) {
	if options.ShutdownTimeout <= 0 {
		return nil, fmt.Errorf("shutdown timeout must be positive")
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	seen := make(map[string]struct{}, len(options.Components))
	for _, component := range options.Components {
		if component == nil {
			return nil, fmt.Errorf("runtime component must not be nil")
		}
		if component.Name() == "" {
			return nil, fmt.Errorf("runtime component name must not be empty")
		}
		if _, exists := seen[component.Name()]; exists {
			return nil, fmt.Errorf("runtime component %q is duplicated", component.Name())
		}
		seen[component.Name()] = struct{}{}
	}
	return &Runtime{
		logger:          logger,
		shutdownTimeout: options.ShutdownTimeout,
		components:      append([]Component(nil), options.Components...),
	}, nil
}

// Run starts components in declaration order, waits for cancellation or an
// asynchronous component failure, and stops started components in reverse
// order under a fresh deadline that is not pre-canceled by the signal context.
func (r *Runtime) Run(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	started := make([]Component, 0, len(r.components))
	for _, component := range r.components {
		r.logger.InfoContext(ctx, "starting runtime component", "component", component.Name())
		if err := component.Start(ctx); err != nil {
			startErr := fmt.Errorf("start component %s: %w", component.Name(), err)
			return errors.Join(startErr, r.shutdown(ctx, started))
		}
		started = append(started, component)
	}

	watchCtx, stopWatching := context.WithCancel(context.Background())
	defer stopWatching()
	failures := make(chan error, len(started))
	for _, component := range started {
		source, ok := component.(ErrorSource)
		if !ok || source.Errors() == nil {
			continue
		}
		go watchErrors(watchCtx, component.Name(), source.Errors(), failures)
	}

	var runErr error
	select {
	case <-ctx.Done():
		r.logger.InfoContext(context.Background(), "shutdown requested")
	case runErr = <-failures:
		// The component name is a bounded, compile-time-known identifier
		// (never operator input), and the error is attached as a raw
		// attribute value rather than pre-stringified so the logging
		// handler's redacting ReplaceAttr still processes it (CHAOS-3873).
		attributes := []any{"error_category", "component_failure"}
		var failed *componentFailure
		if errors.As(runErr, &failed) {
			attributes = append(attributes, "component", failed.name, "error", failed.err)
		} else {
			attributes = append(attributes, "error", runErr)
		}
		r.logger.ErrorContext(context.Background(), "runtime component failed", attributes...)
	}
	stopWatching()
	return errors.Join(runErr, r.shutdown(ctx, started))
}

// componentFailure names the component whose asynchronous error triggered
// runtime shutdown, so callers can recover a bounded, safe identifier via
// errors.As instead of parsing it back out of formatted error text. It
// still wraps the underlying error so errors.As/errors.Is keep working for
// callers (e.g. a component-supplied DependencyReason) further up the chain.
type componentFailure struct {
	name string
	err  error
}

func (failure *componentFailure) Error() string {
	return fmt.Sprintf("component %s: %s", failure.name, failure.err)
}

func (failure *componentFailure) Unwrap() error { return failure.err }

func watchErrors(ctx context.Context, name string, source <-chan error, failures chan<- error) {
	select {
	case <-ctx.Done():
		return
	case err, ok := <-source:
		if !ok || err == nil {
			return
		}
		select {
		case failures <- &componentFailure{name: name, err: err}:
		case <-ctx.Done():
		}
	}
}

func (r *Runtime) shutdown(parent context.Context, started []Component) error {
	base := context.WithoutCancel(parent)
	shutdownCtx, shutdownCancel := context.WithTimeout(base, r.shutdownTimeout)
	defer shutdownCancel()
	shutdownDeadline, _ := shutdownCtx.Deadline()

	var shutdownErrs []error
	for index := len(started) - 1; index >= 0; index-- {
		component := started[index]
		r.logger.InfoContext(shutdownCtx, "stopping runtime component", "component", component.Name())

		// Reserve an equal share of the remaining global budget for every
		// component that still needs a shutdown attempt. A non-cooperative
		// component may outlive its attempt goroutine, but cannot prevent later
		// components from being attempted or extend Runtime.Run past the hard
		// shutdown deadline.
		remainingComponents := index + 1
		remainingBudget := time.Until(shutdownDeadline)
		attemptBudget := time.Duration(0)
		if remainingBudget > 0 {
			attemptBudget = remainingBudget / time.Duration(remainingComponents)
		}
		if source, ok := component.(ShutdownBudgetSource); ok && source.ShutdownBudget() > attemptBudget {
			attemptBudget = min(source.ShutdownBudget(), remainingBudget)
		}
		attemptCtx, attemptCancel := context.WithTimeout(shutdownCtx, attemptBudget)
		result, dispatched := dispatchShutdown(attemptCtx, component)
		<-dispatched

		select {
		case err := <-result:
			attemptCancel()
			if err != nil {
				shutdownErrs = append(shutdownErrs, fmt.Errorf("shutdown component %s: %w", component.Name(), err))
			}
		case <-attemptCtx.Done():
			attemptCancel()
			shutdownErrs = append(shutdownErrs, fmt.Errorf("shutdown component %s: %w", component.Name(), context.DeadlineExceeded))
		}
	}
	return errors.Join(shutdownErrs...)
}

func dispatchShutdown(ctx context.Context, component Component) (<-chan error, <-chan struct{}) {
	result := make(chan error, 1)
	dispatched := make(chan struct{})
	go func() {
		close(dispatched)
		result <- component.Shutdown(ctx)
	}()
	return result, dispatched
}
