// Package health implements dependency-extensible liveness and readiness.
package health

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
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

// MetricsSource writes one complete Prometheus text-format fragment. Sources
// are registered at process construction time and must expose only bounded,
// pre-registered dimensions.
type MetricsSource interface {
	WritePrometheus(io.Writer) error
}

// Registry combines the process admission gate with required dependency
// checks. Any failed, missing, timed-out, or panicking check fails readiness.
type Registry struct {
	checkTimeout time.Duration
	startedAt    time.Time

	mu            sync.RWMutex
	required      map[string]*requiredCheck
	metricsSource map[string]MetricsSource
	ready         atomic.Bool
	live          atomic.Bool

	// refusals reports, to the process log, which required check refused
	// readiness and how (CHAOS-6883): the 503 body carries only the names and
	// kubelet keeps no body, so without a server-side line a refusal that
	// clears is undiagnosable afterwards.
	refusalMu  sync.Mutex
	refusalLog *slog.Logger
	refusing   map[string]*refusalState
	// The log writes happen off the readiness path (see reportRefusals): at most
	// refusalMaxInFlight run at once, extras are dropped and counted.
	refusalInFlight atomic.Int32
	refusalDropped  atomic.Int64
	refusalWG       sync.WaitGroup
}

// refusalMaxInFlight bounds the goroutines writing refusal log lines.
const refusalMaxInFlight = 4

// refusalState is one check's run of consecutive refusals.
type refusalState struct {
	since     time.Time
	count     int64
	lastLog   time.Time
	lastCause string
}

// refusalLogInterval bounds how often a check that KEEPS refusing is logged;
// the first refusal and the recovery are always logged.
const refusalLogInterval = 30 * time.Second

type requiredCheck struct {
	check CheckFunc

	mu     sync.Mutex
	active *checkExecution
}

type checkExecution struct {
	done   chan struct{}
	passed bool
	// timedOut is set only when passed is false, and only when the check
	// function itself returned an error wrapping context.DeadlineExceeded or
	// context.Canceled -- i.e. it noticed its own bounded context expire,
	// rather than answering with some other error. It is deliberately NOT
	// derived from a race between this execution finishing and a caller's
	// separate wait expiring (see requiredCheck.run): the caller's timeout
	// and the check's own internal timeout are frequently the same duration
	// started microseconds apart, which made "whichever context fires
	// first" decide the classification at random.
	timedOut bool
	// cause is the bounded class of a failure: "timeout", "canceled", "panic"
	// or "error". Never the error text, which can carry a DSN.
	cause string
}

// Readiness is a sanitized snapshot suitable for logs, metrics, and HTTP.
type Readiness struct {
	Ready  bool
	Failed []string
	// Checks is the per-check result for every required dependency, sorted by
	// name. It is populated only when CheckRequired actually ran the required
	// checks (the normal, gate-open path); the fail-closed sentinel paths
	// below ("runtime", "dependencies") leave it nil since there is no real
	// per-check data to report. Names come from RegisterRequired, which
	// rejects anything not matching checkNamePattern, so every Name here is
	// already a safe, unquoted-friendly label value.
	Checks []CheckStatus
}

// CheckStatus is one required check's pass/fail result. Name is always a
// pre-registered identifier matching checkNamePattern.
type CheckStatus struct {
	Name   string
	Failed bool
	// TimedOut reports that this result came from the caller's own wait
	// expiring (checkTimeout) before the check function returned anything at
	// all, as opposed to the check running to completion and returning an
	// error. A dependency that is merely slow right now -- contending for a
	// connection slot against a burst of other replicas starting at once --
	// looks exactly like this; a dependency that is definitively wrong (bad
	// credentials, a posture mismatch) answers quickly with an error instead.
	// Callers that want to retry only the former, not the latter, use this
	// bit to tell them apart without the check ever exposing its error text.
	TimedOut bool
	// Cause is the bounded failure class of a failed check -- "timeout", "canceled", "error",
	// "panic", or "wait_expired" (the caller's own wait ended before any answer) -- and "" for a
	// check that passed. It is never error text: a startup path that cannot serve the HTTP surface
	// (the worker's preclaim-readiness) logs it, so an exit names WHICH class of failure each check
	// hit instead of only which checks failed (CHAOS-6955).
	Cause string
}

func NewRegistry(checkTimeout time.Duration) *Registry {
	if checkTimeout <= 0 {
		checkTimeout = 2 * time.Second
	}
	registry := &Registry{
		checkTimeout:  checkTimeout,
		startedAt:     time.Now(),
		required:      make(map[string]*requiredCheck),
		metricsSource: make(map[string]MetricsSource),
	}
	registry.live.Store(true)
	return registry
}

// RegisterMetrics adds a named Prometheus fragment to the operator endpoint.
// Duplicate or unsafe names fail construction instead of silently replacing a
// collector.
func (r *Registry) RegisterMetrics(name string, source MetricsSource) error {
	if !checkNamePattern.MatchString(name) {
		return fmt.Errorf("metrics source name must match %s", checkNamePattern.String())
	}
	if source == nil {
		return fmt.Errorf("metrics source %q must not be nil", name)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.metricsSource[name]; exists {
		return &MetricsSourceRegisteredError{Name: name}
	}
	r.metricsSource[name] = source
	return nil
}

// MetricsSourceOutcome reports whether one registered source's fragment made
// it into a scrape. Err is nil when the fragment was written.
//
// Only Source is ever safe to expose: it is a pre-registered identifier
// matching checkNamePattern, whereas Err comes from arbitrary dependency code
// and has been observed to carry a database DSN. Callers rendering this to an
// HTTP surface must use the name alone.
type MetricsSourceOutcome struct {
	Source string
	Err    error
}

// WriteMetricsPartial writes every source that can be written and reports the
// per-source outcome, rather than abandoning the whole scrape at the first
// failure the way WriteMetrics does.
//
// This exists because the process-level gauges — live, ready, uptime — are most
// useful exactly when a dependency is down, which is precisely when a source
// backed by that dependency returns an error. Failing the endpoint then costs
// an operator the liveness signal at the moment they need it.
//
// Each source writes into its own buffer and is appended only on success, so a
// source that errors part-way through cannot leave a truncated fragment in the
// scrape and corrupt the sources that follow it.
func (r *Registry) WriteMetricsPartial(output io.Writer) ([]MetricsSourceOutcome, error) {
	if output == nil {
		return nil, fmt.Errorf("metrics output is required")
	}
	r.mu.RLock()
	sources := make(map[string]MetricsSource, len(r.metricsSource))
	for name, source := range r.metricsSource {
		sources[name] = source
	}
	r.mu.RUnlock()

	names := make([]string, 0, len(sources))
	for name := range sources {
		names = append(names, name)
	}
	sort.Strings(names)

	outcomes := make([]MetricsSourceOutcome, 0, len(names))
	for _, name := range names {
		var fragment bytes.Buffer
		if err := sources[name].WritePrometheus(&fragment); err != nil {
			outcomes = append(outcomes, MetricsSourceOutcome{
				Source: name,
				Err:    fmt.Errorf("write metrics source %q: %w", name, err),
			})
			continue
		}
		if _, err := output.Write(fragment.Bytes()); err != nil {
			return outcomes, fmt.Errorf("write metrics output: %w", err)
		}
		outcomes = append(outcomes, MetricsSourceOutcome{Source: name})
	}
	return outcomes, nil
}

// WriteMetrics writes registered sources in stable name order, failing on the
// first source that errors, with NOTHING reaching output unless every source
// succeeds. Prefer WriteMetricsPartial for anything serving a scrape; this
// remains for callers that genuinely want all-or-nothing, and for tests
// asserting a specific source's error.
//
// All sources are written into an internal buffer first and copied to output
// in one final write only after every source has succeeded (CHAOS-4175). The
// previous implementation wrote each source directly to output as it
// iterated in sorted name order, so a source that sorted BEFORE the one that
// ultimately failed had already landed real bytes in the caller's output —
// "all-or-nothing" that depended on registration/name order rather than
// holding unconditionally. See
// TestWriteMetricsFailsClosedWithNoPartialBytesEvenWhenAnEarlierSourceSucceeded.
func (r *Registry) WriteMetrics(output io.Writer) error {
	if output == nil {
		return fmt.Errorf("metrics output is required")
	}
	r.mu.RLock()
	sources := make(map[string]MetricsSource, len(r.metricsSource))
	for name, source := range r.metricsSource {
		sources[name] = source
	}
	r.mu.RUnlock()

	names := make([]string, 0, len(sources))
	for name := range sources {
		names = append(names, name)
	}
	sort.Strings(names)
	var buffer bytes.Buffer
	for _, name := range names {
		if err := sources[name].WritePrometheus(&buffer); err != nil {
			return fmt.Errorf("write metrics source %q: %w", name, err)
		}
	}
	if _, err := output.Write(buffer.Bytes()); err != nil {
		return fmt.Errorf("write metrics output: %w", err)
	}
	return nil
}

// SetRefusalLogger makes the registry log which required check refuses
// readiness, and why in bounded terms (CHAOS-6883). The HTTP body names the
// failing checks and nothing else, and a refusal that clears leaves no trace
// anywhere; this is the only durable record of it. Each check logs its first
// refusal, then at most once per refusalLogInterval while it keeps refusing,
// then once when it recovers. Only the check name, the bounded cause class and
// counters are logged, never the error text.
func (r *Registry) SetRefusalLogger(logger *slog.Logger) {
	r.refusalMu.Lock()
	defer r.refusalMu.Unlock()
	r.refusalLog = logger
}

func (r *Registry) reportRefusals(ctx context.Context, statuses []CheckStatus, causes map[string]string) {
	r.refusalMu.Lock()
	logger := r.refusalLog
	if logger == nil {
		r.refusalMu.Unlock()
		return
	}
	if r.refusing == nil {
		r.refusing = make(map[string]*refusalState, len(statuses))
	}
	type line struct {
		message string
		attrs   []any
		warn    bool
	}
	var lines []line
	now := time.Now()
	for _, status := range statuses {
		state := r.refusing[status.Name]
		if !status.Failed {
			if state != nil {
				lines = append(lines, line{"readiness check recovered", []any{
					"check", status.Name,
					"refused_for_ms", now.Sub(state.since).Milliseconds(),
					"consecutive_refusals", state.count,
					"last_cause", state.lastCause,
				}, false})
				delete(r.refusing, status.Name)
			}
			continue
		}
		cause := causes[status.Name]
		if cause == "" {
			cause = "error"
		}
		if state == nil {
			state = &refusalState{since: now}
			r.refusing[status.Name] = state
		}
		state.count++
		state.lastCause = cause
		if state.count == 1 || now.Sub(state.lastLog) >= refusalLogInterval {
			state.lastLog = now
			lines = append(lines, line{"readiness check refused", []any{
				"check", status.Name,
				"cause", cause,
				"timed_out", status.TimedOut,
				"refused_for_ms", now.Sub(state.since).Milliseconds(),
				"consecutive_refusals", state.count,
			}, true})
		}
	}
	r.refusalMu.Unlock()
	if len(lines) == 0 {
		return
	}
	// A log sink can block (a full stdout pipe) or panic; readiness must do
	// neither. The lines are written by a bounded background goroutine that
	// recovers, and dropped (counted) when too many writes are already stuck.
	if r.refusalInFlight.Add(1) > refusalMaxInFlight {
		r.refusalInFlight.Add(-1)
		r.refusalDropped.Add(int64(len(lines)))
		return
	}
	r.refusalWG.Add(1)
	go func() {
		defer r.refusalWG.Done()
		defer r.refusalInFlight.Add(-1)
		defer func() { _ = recover() }()
		for _, l := range lines {
			if l.warn {
				logger.WarnContext(context.WithoutCancel(ctx), l.message, l.attrs...)
			} else {
				logger.InfoContext(context.WithoutCancel(ctx), l.message, l.attrs...)
			}
		}
	}()
}

// flushRefusalLogs waits for in-flight refusal log writes (tests).
func (r *Registry) flushRefusalLogs() { r.refusalWG.Wait() }

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
	return r.CheckRequired(ctx)
}

// CheckRequired runs required dependency checks without opening the public
// readiness gate. Worker processes use it before starting River consumers so
// a replica cannot claim work before its dependencies pass.
func (r *Registry) CheckRequired(ctx context.Context) Readiness {
	r.mu.RLock()
	checks := make(map[string]*requiredCheck, len(r.required))
	for name, check := range r.required {
		checks[name] = check
	}
	r.mu.RUnlock()
	if len(checks) == 0 {
		return Readiness{Ready: false, Failed: []string{"dependencies"}}
	}

	type outcome struct {
		name   string
		result checkResult
	}
	results := make(chan outcome, len(checks))
	for name, check := range checks {
		go func() {
			results <- outcome{name: name, result: check.run(ctx, r.checkTimeout)}
		}()
	}

	failed := make([]string, 0)
	statuses := make([]CheckStatus, 0, len(checks))
	causes := make(map[string]string, len(checks))
	for range checks {
		result := <-results
		causes[result.name] = result.result.cause
		statuses = append(statuses, CheckStatus{
			Name:     result.name,
			Failed:   result.result.failed,
			TimedOut: result.result.timedOut,
			Cause:    failedCause(result.result),
		})
		if result.result.failed {
			failed = append(failed, result.name)
		}
	}
	sort.Strings(failed)
	sort.Slice(statuses, func(i, j int) bool { return statuses[i].Name < statuses[j].Name })
	r.reportRefusals(ctx, statuses, causes)
	return Readiness{Ready: len(failed) == 0, Failed: failed, Checks: statuses}
}

// checkResult is one caller's outcome from requiredCheck.run: whether the
// dependency is unready, and -- only when it is -- whether that failure is a
// timeout (the check itself hit its own bounded context, or the caller never
// got an answer at all before its own wait expired) as opposed to the check
// running to completion and reporting a real error.
// failedCause is the CheckStatus.Cause of a result: empty for a pass.
func failedCause(result checkResult) string {
	if !result.failed {
		return ""
	}
	if result.cause == "" {
		return "error"
	}
	return result.cause
}

type checkResult struct {
	failed   bool
	timedOut bool
	// cause is the bounded failure class (see checkExecution.cause), or
	// "wait_expired" when the caller's own wait ended before any answer.
	cause string
}

// run shares a single in-flight execution across callers. A check that ignores
// cancellation can therefore strand at most one goroutine; every caller still
// has its own bounded wait and fails closed when that wait expires.
func (c *requiredCheck) run(parent context.Context, timeout time.Duration) checkResult {
	waitCtx, waitCancel := context.WithTimeout(parent, timeout)
	defer waitCancel()
	if waitCtx.Err() != nil {
		return checkResult{failed: true, timedOut: true, cause: "wait_expired"}
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
		return checkResult{failed: !execution.passed, timedOut: !execution.passed && execution.timedOut, cause: execution.cause}
	case <-waitCtx.Done():
		// The caller's own wait expired with no answer at all -- whatever the
		// check eventually returns, THIS caller never saw it in time, which is
		// the definition of a timeout from its perspective.
		return checkResult{failed: true, timedOut: true, cause: "wait_expired"}
	}
}

func (c *requiredCheck) execute(
	ctx context.Context,
	cancel context.CancelFunc,
	execution *checkExecution,
) {
	defer cancel()
	passed, timedOut, cause := func() (passed, timedOut bool, cause string) {
		defer func() {
			if recover() != nil {
				// A panic is a bug in the check, never a transient timeout.
				passed, timedOut, cause = false, false, "panic"
			}
		}()
		err := c.check(ctx)
		if err == nil {
			return true, false, ""
		}
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			return false, true, "timeout"
		case errors.Is(err, context.Canceled):
			return false, true, "canceled"
		default:
			return false, false, "error"
		}
	}()

	c.mu.Lock()
	execution.passed = passed
	execution.timedOut = timedOut
	execution.cause = cause
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

// MetricsSourceRegisteredError is RegisterMetrics' refusal of a name that is
// already registered, so a caller that may run after another registration of
// the same source (a shell that installs a process-wide source and a service
// that also does) can tell that case from a real failure.
type MetricsSourceRegisteredError struct{ Name string }

func (e *MetricsSourceRegisteredError) Error() string {
	return fmt.Sprintf("metrics source %q is already registered", e.Name)
}
