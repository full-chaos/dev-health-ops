package reconcilejob

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/auth/pushreconcile"
)

const (
	// RunsCounterName counts finished probe runs per outcome.
	RunsCounterName = "devhealth_auth_push_reconcile_runs_total"
	// OutcomeLabel is the one label of the runs counter.
	OutcomeLabel = "outcome"
	// OutcomeOK and OutcomeFailed are the closed values of OutcomeLabel.
	OutcomeOK     = "ok"
	OutcomeFailed = "failed"
	// ProbeEnv switches the probe on when its value is ProbeOn. Unset or any other value: off.
	ProbeEnv = "PUSH_RECONCILE_PROBE"
	ProbeOn  = "on"

	// ProbePeriod is the time between two probe passes. A finding is confirmed against the pass before.
	ProbePeriod = 5 * time.Minute
	// ProbeBudget bounds one probe pass; it is below the period so passes never overlap.
	ProbeBudget = 4 * time.Minute
)

// Enabled reports whether the probe is switched on. It is off by default.
func Enabled(lookup func(string) (string, bool)) bool {
	if lookup == nil {
		return false
	}
	v, ok := lookup(ProbeEnv)
	return ok && v == ProbeOn
}

// Probe exports the reconcile drift of the worker. SigNoz cannot read
// Postgres, so the worker runs a pass every ProbePeriod and publishes the
// findings that two passes in a row agree on. A failed pass publishes no
// drift sample (never 0) and clears the memory of the pass before; the first
// pass after a start has nothing to confirm against, so it publishes none either.
type Probe struct {
	pass   PassFunc
	logger *slog.Logger
	runs   metric.Int64Counter

	mu       sync.Mutex
	prev     []pushreconcile.Finding
	havePrev bool
	counts   map[pushreconcile.Class]int
	hasData  bool

	cancel context.CancelFunc
	done   chan struct{}
}

// Register registers the drift gauge and the runs counter (both outcomes seeded at 0) on meter.
func Register(meter metric.Meter, pass PassFunc, logger *slog.Logger) (*Probe, error) {
	if meter == nil || pass == nil {
		return nil, errors.New("reconcilejob: the probe needs a meter and a pass")
	}
	if logger == nil {
		logger = slog.Default()
	}
	runs, err := meter.Int64Counter(RunsCounterName, metric.WithUnit("{run}"),
		metric.WithDescription("finished runs of the External Push projection reconcile probe, by outcome"))
	if err != nil {
		return nil, err
	}
	for _, outcome := range []string{OutcomeOK, OutcomeFailed} {
		runs.Add(context.Background(), 0, metric.WithAttributes(attribute.String(OutcomeLabel, outcome)))
	}
	p := &Probe{pass: pass, logger: logger, runs: runs}
	if err := p.registerDriftGauge(meter); err != nil {
		return nil, err
	}
	return p, nil
}

// MaybeStart registers and starts the probe when lookup switches it on. Off:
// it registers nothing and returns nil. A registration failure is returned for
// the caller to log; the worker never stops over it.
func MaybeStart(ctx context.Context, lookup func(string) (string, bool), meter metric.Meter, pass PassFunc, logger *slog.Logger) (*Probe, error) {
	if !Enabled(lookup) {
		return nil, nil
	}
	p, err := Register(meter, pass, logger)
	if err != nil {
		return nil, err
	}
	p.Start(ctx)
	return p, nil
}

// RunOnce runs one pass and updates the published state.
func (p *Probe) RunOnce(ctx context.Context) {
	runCtx, cancel := context.WithTimeout(ctx, ProbeBudget)
	defer cancel()
	result, err := p.pass(runCtx)
	p.mu.Lock()
	defer p.mu.Unlock()
	if err != nil {
		p.runs.Add(context.WithoutCancel(ctx), 1, metric.WithAttributes(attribute.String(OutcomeLabel, OutcomeFailed)))
		p.logger.WarnContext(ctx, "push reconcile pass failed", "error", err)
		p.hasData, p.havePrev, p.prev, p.counts = false, false, nil, nil
		return
	}
	p.runs.Add(context.WithoutCancel(ctx), 1, metric.WithAttributes(attribute.String(OutcomeLabel, OutcomeOK)))
	if p.havePrev {
		p.counts = CountByClass(pushreconcile.Confirm(p.prev, result.Findings))
		p.hasData = true
	}
	p.prev, p.havePrev = result.Findings, true
}

// Start runs a pass now and then every ProbePeriod until ctx ends or Stop is called.
func (p *Probe) Start(ctx context.Context) {
	loopCtx, cancel := context.WithCancel(ctx)
	p.cancel, p.done = cancel, make(chan struct{})
	go func() {
		defer close(p.done)
		ticker := time.NewTicker(ProbePeriod)
		defer ticker.Stop()
		for {
			p.RunOnce(loopCtx)
			select {
			case <-loopCtx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

// Stop ends the loop and waits for the pass in flight.
func (p *Probe) Stop() error {
	if p == nil || p.cancel == nil {
		return nil
	}
	p.cancel()
	<-p.done
	return nil
}
