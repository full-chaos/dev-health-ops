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
	"io"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
	"go.opentelemetry.io/otel"
	otelprometheus "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// Source writes the process's OTel instruments as Prometheus text; it is a
// health.MetricsSource.
type Source struct {
	gatherer prometheus.Gatherer
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
	installOnce.Do(func() {
		registry := prometheus.NewRegistry()
		exporter, err := otelprometheus.New(
			otelprometheus.WithRegisterer(registry),
			otelprometheus.WithoutScopeInfo(),
			otelprometheus.WithoutTargetInfo(),
			otelprometheus.WithoutUnits(),
		)
		if err != nil {
			installErr = err
			return
		}
		otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter)))
		installed = &Source{gatherer: registry}
	})
	return installed, installErr
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
