// Package apimetrics exports the dho api's OTel instruments as Prometheus
// text on the operator /metrics endpoint (internal/platform/health), the
// Go api's scrape surface.
//
// The Python api served its prometheus_client counters on /metrics. Its
// Go port declares each counter with otel.Meter under the Python name and
// labels (for example devhealth_integration_credential_decrypt_failed_total
// {provider}); Source installs the process's MeterProvider so those
// instruments record, and writes them for the health registry. The
// exporter drops the OTel scope labels, target_info and unit suffixes, so
// a sample reads exactly as the Python api wrote it.
package apimetrics

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/otlpmetrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
	"go.opentelemetry.io/otel"
	otelprometheus "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// Source writes the process's OTel instruments as Prometheus text; it is a
// health.MetricsSource.
type Source struct {
	gatherer   prometheus.Gatherer
	pushReader sdkmetric.Reader
	pushStats  *otlpmetrics.Stats
}

var (
	installOnce sync.Once
	installed   *Source
	installErr  error
)

// Install sets the process's global MeterProvider to one that exports to a
// dedicated Prometheus registry, and returns the Source that writes it.
// It runs once per process: otel's global delegation binds every
// package-level otel.Meter instrument to the first provider set, so a
// second provider would never see them.
func Install() (*Source, error) {
	return InstallWith(PushOptions{})
}

// PushOptions turns on the OTLP push of the same provider. Zero means no push.
type PushOptions struct {
	// Options is the push configuration (otlpmetrics.OptionsFromEnv). The
	// push runs only when Options.Enabled and Registry is set.
	Options otlpmetrics.Options
	// Registry supplies the hand-written Prometheus-text fragments the bridge
	// pushes beside the instruments. Service and Version are the operator
	// server's, for the runtime block it also writes on /metrics.
	Registry *health.Registry
	Service  string
	Version  string
	Logger   *slog.Logger
}

// InstallWith is Install with the OTLP push. Only the first call in a process
// takes effect (see Install).
func InstallWith(push PushOptions) (*Source, error) {
	installOnce.Do(func() {
		provider, source, err := buildProvider(push)
		if err != nil {
			installErr = err
			return
		}
		otel.SetMeterProvider(provider)
		installed = source
	})
	return installed, installErr
}

// buildProvider is InstallWith without the process-global state, so a test can
// build a provider of its own.
func buildProvider(push PushOptions) (*sdkmetric.MeterProvider, *Source, error) {
	registry := prometheus.NewRegistry()
	exporter, err := otelprometheus.New(
		otelprometheus.WithRegisterer(registry),
		otelprometheus.WithoutScopeInfo(),
		otelprometheus.WithoutTargetInfo(),
		otelprometheus.WithoutUnits(),
	)
	if err != nil {
		return nil, nil, err
	}
	providerOptions := []sdkmetric.Option{sdkmetric.WithReader(exporter)}
	source := &Source{gatherer: registry}
	if push.Options.Enabled && push.Registry != nil {
		pipeline, err := otlpmetrics.NewPipeline(context.Background(), push.Options, health.Scrape{Registry: push.Registry, Service: push.Service, Version: push.Version}, pushSkip(), push.Logger)
		if err != nil {
			// Fails soft like tracing: the pull endpoint still works.
			if push.Logger != nil {
				push.Logger.Warn("otlp metrics push unavailable", "error", err)
			}
		} else {
			providerOptions = append(providerOptions, sdkmetric.WithReader(pipeline.Reader), sdkmetric.WithResource(pipeline.Resource))
			source.pushStats = pipeline.Stats
			source.pushReader = pipeline.Reader
		}
	}
	return sdkmetric.NewMeterProvider(providerOptions...), source, nil
}

// pushSkip is the registry sources the bridge must not read: the instruments'
// own fragment, because they reach OTLP natively through the provider's OTLP
// reader and would otherwise be sent twice.
func pushSkip() map[string]bool { return map[string]bool{SourceName: true} }

// Shutdown flushes and stops the OTLP push (a no-op without one). Only the
// push reader stops: the provider and the Prometheus reader keep serving
// /metrics and recording, as they do for a process that never pushed.
func (s *Source) Shutdown(ctx context.Context) error {
	if s == nil || s.pushReader == nil {
		return nil
	}
	return s.pushReader.Shutdown(ctx)
}

// WritePrometheus writes every recorded instrument in the Prometheus text
// format.
func (s *Source) WritePrometheus(w io.Writer) error {
	if s == nil {
		return nil
	}
	families, err := s.gatherer.Gather()
	if err != nil {
		return err
	}
	encoder := expfmt.NewEncoder(w, expfmt.NewFormat(expfmt.TypeTextPlain))
	for _, family := range families {
		if err := encoder.Encode(family); err != nil {
			return err
		}
	}
	return nil
}

// SourceName is the health registry name the process's OTel instruments are
// registered under.
const SourceName = "api_instruments"

// Register installs the process's OTel MeterProvider (Install) and puts its
// Prometheus text on registry's /metrics under SourceName. It is idempotent per
// registry: the operator shell registers it for every binary at start, and a
// service that registers it again (the api) is not an error.
func Register(registry *health.Registry) error {
	source, err := Install()
	if err != nil {
		return err
	}
	return registerSource(registry, source)
}

// RegisterWithPush is Register with the OTLP push of push.Options on, reading
// registry's fragments. The push counters are registered on registry too.
func RegisterWithPush(registry *health.Registry, push otlpmetrics.Options, service, version string, logger *slog.Logger) (*Source, error) {
	source, err := InstallWith(PushOptions{Options: push, Registry: registry, Service: service, Version: version, Logger: logger})
	if err != nil {
		return nil, err
	}
	if err := registerSource(registry, source); err != nil {
		return source, err
	}
	if source != nil && source.pushStats != nil {
		if err := registry.RegisterMetrics(otlpmetrics.SourceName, source.pushStats); err != nil {
			var registered *health.MetricsSourceRegisteredError
			if !errors.As(err, &registered) {
				return source, err
			}
		}
	}
	return source, nil
}

func registerSource(registry *health.Registry, source *Source) error {
	if err := registry.RegisterMetrics(SourceName, source); err != nil {
		var registered *health.MetricsSourceRegisteredError
		if errors.As(err, &registered) {
			return nil
		}
		return err
	}
	return nil
}
