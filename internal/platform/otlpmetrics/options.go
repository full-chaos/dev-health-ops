// Package otlpmetrics pushes a Go process's metrics over OTLP/gRPC to the
// collector its traces already use (OTEL_EXPORTER_OTLP_ENDPOINT).
//
// Two kinds of content sit behind a Go binary's /metrics: the OTel
// instruments its code declares (the process MeterProvider, see
// internal/api/apimetrics) and the hand-written Prometheus-text fragments
// registered on the health registry (pools, probes, reconcilers, schedulers,
// stream collectors). The instruments reach OTLP through a periodic reader on
// the same MeterProvider; the fragments reach it through one bridge that
// parses each fragment into Prometheus families and hands them to the OTel
// Prometheus bridge as a producer of the same reader. The /metrics pull
// endpoint is unchanged.
package otlpmetrics

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

const (
	defaultServiceName = "dev-health-ops"
	defaultEnvironment = "production"
	defaultEndpoint    = "localhost:4317"
	defaultInterval    = 60 * time.Second
	// maxIntervalMillis bounds OTEL_METRIC_EXPORT_INTERVAL at one day: a larger
	// value overflows time.Duration (a huge positive count of milliseconds
	// becomes a negative interval, which panics the periodic reader).
	maxIntervalMillis = int64(24 * time.Hour / time.Millisecond)
)

// Options is the push configuration, read from the same OTEL_* environment as
// tracing so one deployment setting configures traces and metrics.
type Options struct {
	// Enabled is false when OTEL_ENABLED or OTEL_METRICS_ENABLED turns it off.
	Enabled bool
	// Endpoint is OTEL_EXPORTER_OTLP_ENDPOINT: a bare host:port or a URL.
	Endpoint    string
	Interval    time.Duration
	ServiceName string
	Environment string
	// InstanceID separates replicas of one service (the pod name).
	InstanceID string
}

// LookupFunc reads one environment variable (secrets.ProcessLookup in the
// binaries).
type LookupFunc func(string) (string, bool)

// OptionsFromEnv reads Options. defaultServiceName is the binary's own name,
// used when OTEL_SERVICE_NAME is unset exactly as tracing does. A malformed
// OTEL_METRIC_EXPORT_INTERVAL returns an error with Enabled false: the caller
// logs it and runs without metric push, as a bad OTEL_* value never stops a
// process.
func OptionsFromEnv(defaultName string, lookup LookupFunc) (Options, error) {
	if lookup == nil {
		lookup = secrets.ProcessLookup
	}
	get := func(name, fallback string) string {
		if value, ok := lookup(name); ok {
			return value
		}
		return fallback
	}
	options := Options{
		Enabled:     enabled(get("OTEL_ENABLED", "true")) && enabled(get("OTEL_METRICS_ENABLED", "true")),
		Endpoint:    get("OTEL_EXPORTER_OTLP_ENDPOINT", defaultEndpoint),
		ServiceName: get("OTEL_SERVICE_NAME", firstNonEmpty(defaultName, defaultServiceName)),
		Environment: get("OTEL_ENVIRONMENT", defaultEnvironment),
		Interval:    defaultInterval,
	}
	if host, err := os.Hostname(); err == nil {
		options.InstanceID = host
	}
	if raw, ok := lookup("OTEL_METRIC_EXPORT_INTERVAL"); ok {
		millis, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
		if err != nil || millis <= 0 || millis > maxIntervalMillis {
			options.Enabled = false
			return options, fmt.Errorf("OTEL_METRIC_EXPORT_INTERVAL must be between 1 and %d milliseconds, got %q", maxIntervalMillis, raw)
		}
		options.Interval = time.Duration(millis) * time.Millisecond
	}
	return options, nil
}

func enabled(value string) bool {
	switch strings.ToLower(value) {
	case "false", "0", "no":
		return false
	default:
		return true
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
