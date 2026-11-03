package reconcilejob

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/metric"
)

const (
	DriftGaugeName  = "devhealth_auth_push_reconcile_drift"
	RunsCounterName = "devhealth_auth_push_reconcile_runs_total"
	ClassLabel      = "class"
	OutcomeLabel    = "outcome"
	OutcomeOK       = "ok"
	OutcomeFailed   = "failed"
	ProbeEnv        = "PUSH_RECONCILE_PROBE"
	ProbeOn         = "on"
	ProbePeriod     = 5 * time.Minute
	ProbeBudget     = 4 * time.Minute
)

func Enabled(func(string) (string, bool)) bool { return false }

type Probe struct{}

func Register(metric.Meter, PassFunc, *slog.Logger) (*Probe, error) { return &Probe{}, nil }

func MaybeStart(context.Context, func(string) (string, bool), metric.Meter, PassFunc, *slog.Logger) (*Probe, error) {
	return nil, nil
}

func (p *Probe) RunOnce(context.Context) {}
func (p *Probe) Start(context.Context)   {}
func (p *Probe) Stop() error             { return nil }
