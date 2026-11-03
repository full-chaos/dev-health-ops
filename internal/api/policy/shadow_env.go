package policy

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"

	"github.com/full-chaos/dev-health-go/pep"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// The shadow's environment switches.
const (
	// EnvShadowPDP turns the shadow on: true or false; unset is off.
	EnvShadowPDP = "AUTH_SHADOW_PDP"
	// EnvShadowPDPHighRisk lets high-risk rows ask the PDP, which writes an audit row per decision.
	EnvShadowPDPHighRisk = "AUTH_SHADOW_PDP_HIGH_RISK"
	// EnvShadowPDPSample is the sample percent, 1 to 100; unset is 100.
	EnvShadowPDPSample = "AUTH_SHADOW_PDP_SAMPLE"
	// EnvShadowPDPInFlight bounds the calls in flight, 1 or more; unset is 32.
	EnvShadowPDPInFlight = "AUTH_SHADOW_PDP_MAX_IN_FLIGHT"
)

// ShadowInputError is the error a NewDecider returns when a named environment input is missing or bad.
// Variable names the input or inputs (comma separated), never a value or a path.
type ShadowInputError struct{ Variable string }

func (e *ShadowInputError) Error() string { return "shadow input: " + e.Variable }

// ShadowEnvDeps are what a service brings to ShadowFromEnv.
type ShadowEnvDeps struct {
	// Rows is the service's action map.
	Rows map[string]ShadowRow
	// NewDecider builds the service's PEP client. It is called only when the switch is on; its
	// error text is never logged.
	NewDecider func() (pep.Decider, error)
	Logger     *slog.Logger
}

// ShadowFromEnv builds the shadow from the environment. It never fails a start: off, or any
// missing input or bad value, is a nil Shadow; a bad value logs an ERROR naming the variable
// (never its value) and sets the config-invalid gauge.
func ShadowFromEnv(lookup func(string) (string, bool), deps ShadowEnvDeps) *Shadow {
	ctx := context.Background()
	logger := deps.Logger
	if logger == nil {
		logger = slog.Default()
	}
	disabled := func(variable string) *Shadow {
		logger.ErrorContext(ctx, "api shadow pdp disabled: a missing input or a bad value", slog.String("variable", variable))
		recordShadowStateSafe(ctx, false, true)
		return nil
	}
	if lookup == nil {
		return disabled("lookup")
	}
	read := func(key string) string {
		value, _ := lookup(key)
		return strings.TrimSpace(value)
	}
	on := false
	if raw := read(EnvShadowPDP); raw != "" {
		parsed, err := strconv.ParseBool(raw)
		if err != nil {
			return disabled(EnvShadowPDP)
		}
		on = parsed
	}
	if !on {
		recordShadowStateSafe(ctx, false, false)
		return nil
	}
	cfg := ShadowConfig{Rows: deps.Rows, Logger: logger}
	if raw := read(EnvShadowPDPHighRisk); raw != "" {
		parsed, err := strconv.ParseBool(raw)
		if err != nil {
			return disabled(EnvShadowPDPHighRisk)
		}
		cfg.HighRisk = parsed
	}
	if raw := read(EnvShadowPDPSample); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 100 {
			return disabled(EnvShadowPDPSample)
		}
		cfg.SamplePercent = parsed
	}
	if raw := read(EnvShadowPDPInFlight); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 {
			return disabled(EnvShadowPDPInFlight)
		}
		cfg.MaxInFlight = parsed
	}
	if len(deps.Rows) == 0 {
		return disabled("rows")
	}
	if deps.NewDecider == nil {
		return disabled("decider")
	}
	decider, err := deps.NewDecider()
	if err != nil || decider == nil {
		var input *ShadowInputError
		if errors.As(err, &input) && input.Variable != "" {
			return disabled(input.Variable)
		}
		return disabled("decider")
	}
	cfg.Decider = decider
	shadow, err := NewShadow(cfg)
	if err != nil {
		return disabled("config")
	}
	recordShadowStateSafe(ctx, true, false)
	return shadow
}

func recordShadowStateSafe(ctx context.Context, enabled, configInvalid bool) {
	defer func() { _ = recover() }()
	if recordShadowState != nil {
		recordShadowState(ctx, enabled, configInvalid)
	}
}

// recordShadowState sets the two gauges; tests replace it.
var recordShadowState = newShadowStateRecorder(otel.Meter(shadowMeterName))

func newShadowStateRecorder(meter metric.Meter) func(ctx context.Context, enabled, configInvalid bool) {
	enabledGauge, err1 := meter.Int64Gauge(shadowEnabledMetric, metric.WithDescription("1 while the shadow PDP call is built and on, else 0"))
	invalidGauge, err2 := meter.Int64Gauge(shadowConfigInvalidMetric, metric.WithDescription("1 when the shadow was asked for but a missing input or bad value disabled it"))
	if err1 != nil || err2 != nil {
		return func(context.Context, bool, bool) {}
	}
	return func(ctx context.Context, enabled, configInvalid bool) {
		enabledGauge.Record(ctx, boolGauge(enabled))
		invalidGauge.Record(ctx, boolGauge(configInvalid))
	}
}

func boolGauge(v bool) int64 {
	if v {
		return 1
	}
	return 0
}
