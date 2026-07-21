// Package health implements dependency-extensible liveness and readiness.
package health

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var checkNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,62}$`)

// CheckFunc returns nil only when its dependency is ready for new work. Error
// text is deliberately never returned by the HTTP surface.
type CheckFunc func(context.Context) error

// Registry combines the process admission gate with required dependency
// checks. Any failed, missing, timed-out, or panicking check fails readiness.
type Registry struct {
	checkTimeout time.Duration
	startedAt    time.Time

	mu       sync.RWMutex
	required map[string]*requiredCheck
	ready    atomic.Bool
	live     atomic.Bool
}

type requiredCheck struct {
	check CheckFunc

	mu     sync.Mutex
	active *checkExecution
}

type checkExecution struct {
	done   chan struct{}
	passed bool
}

// Readiness is a sanitized snapshot suitable for logs, metrics, and HTTP.
type Readiness struct {
	Ready  bool
	Failed []string
}

func NewRegistry(checkTimeout time.Duration) *Registry {
	if checkTimeout <= 0 {
		checkTimeout = 2 * time.Second
	}
	registry := &Registry{
		checkTimeout: checkTimeout,
		startedAt:    time.Now(),
		required:     make(map[string]*requiredCheck),
	}
	registry.live.Store(true)
	return registry
}

// RegisterRequired adds a fail-closed readiness dependency. Names are bounded
// metric-safe identifiers, and duplicate registration is rejected.
func (r *Registry) RegisterRequired(name string, check CheckFunc) error {
	if !checkNamePattern.MatchString(name) {
		return fmt.Errorf("readiness check name must match %s", checkNamePattern.String())
	}
	if check == nil {
		return fmt.Errorf("readiness check %q must not be nil", name)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.required[name]; exists {
		return fmt.Errorf("readiness check %q is already registered", name)
	}
	r.required[name] = &requiredCheck{check: check}
	return nil
}

// SetReady opens or closes admission. It is opened only after every runtime
// component starts, and closed before ordered shutdown begins.
func (r *Registry) SetReady(ready bool) {
	r.ready.Store(ready)
}

// SetLive controls the liveness gate. Dependency failures do not affect
// liveness; an unrecoverable process-level failure does.
func (r *Registry) SetLive(live bool) {
	r.live.Store(live)
}

func (r *Registry) Live() bool {
	return r.live.Load()
}

func (r *Registry) Uptime() time.Duration {
	return time.Since(r.startedAt)
}

func (r *Registry) RequiredCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.required)
}

// Readiness runs a stable snapshot of all required checks concurrently. It
// fails closed and returns names only, never dependency error strings.
func (r *Registry) Readiness(ctx context.Context) Readiness {
	if !r.ready.Load() {
		return Readiness{Ready: false, Failed: []string{"runtime"}}
	}

	r.mu.RLock()
	checks := make(map[string]*requiredCheck, len(r.required))
	for name, check := range r.required {
		checks[name] = check
	}
	r.mu.RUnlock()
	if len(checks) == 0 {
		return Readiness{Ready: false, Failed: []string{"dependencies"}}
	}

	results := make(chan string, len(checks))
	for name, check := range checks {
		go func() {
			if !check.run(ctx, r.checkTimeout) {
				results <- name
				return
			}
			results <- ""
		}()
	}

	failed := make([]string, 0)
	for range checks {
		if name := <-results; name != "" {
			failed = append(failed, name)
		}
	}
	sort.Strings(failed)
	return Readiness{Ready: len(failed) == 0, Failed: failed}
}

// run shares a single in-flight execution across callers. A check that ignores
// cancellation can therefore strand at most one goroutine; every caller still
// has its own bounded wait and fails closed when that wait expires.
func (c *requiredCheck) run(parent context.Context, timeout time.Duration) bool {
	waitCtx, waitCancel := context.WithTimeout(parent, timeout)
	defer waitCancel()
	if waitCtx.Err() != nil {
		return false
	}

	c.mu.Lock()
	execution := c.active
	if execution == nil {
		execution = &checkExecution{done: make(chan struct{})}
		c.active = execution
		checkCtx, checkCancel := context.WithTimeout(context.Background(), timeout)
		go c.execute(checkCtx, checkCancel, execution)
	}
	c.mu.Unlock()

	select {
	case <-execution.done:
		return execution.passed
	case <-waitCtx.Done():
		return false
	}
}

func (c *requiredCheck) execute(
	ctx context.Context,
	cancel context.CancelFunc,
	execution *checkExecution,
) {
	defer cancel()
	passed := func() (passed bool) {
		defer func() {
			if recover() != nil {
				passed = false
			}
		}()
		return c.check(ctx) == nil
	}()

	c.mu.Lock()
	execution.passed = passed
	close(execution.done)
	if c.active == execution {
		c.active = nil
	}
	c.mu.Unlock()
}

// Gate is a lifecycle component that opens readiness after earlier components
// start and closes it before those components shut down.
type Gate struct {
	Registry *Registry
}

func (Gate) Name() string { return "readiness-gate" }

func (g Gate) Start(context.Context) error {
	if g.Registry == nil {
		return fmt.Errorf("readiness registry is required")
	}
	g.Registry.SetReady(true)
	return nil
}

func (g Gate) Shutdown(context.Context) error {
	if g.Registry != nil {
		g.Registry.SetReady(false)
	}
	return nil
}
