package reconcilejob

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/auth/pushreconcile"
)

const (
	// DriftGaugeName is the confirmed drift per class. The alert rules of the
	// group auth_push_projection read this name and the label ClassLabel.
	DriftGaugeName = "devhealth_auth_push_reconcile_drift"
	// ClassLabel is the one label of the drift gauge: a closed set (pushreconcile.Classes).
	ClassLabel = "class"
)

// The gauge is observable, so the label census (internal/platform/otlpmetrics) cannot read its labels from an
// Add or Record site: it lists this file as a manual instrument with the one key class.
func (p *Probe) registerDriftGauge(meter metric.Meter) error {
	_, err := meter.Int64ObservableGauge(DriftGaugeName, metric.WithUnit("{credential}"),
		metric.WithDescription("External Push credentials whose projection differs from the domain store in two reconcile passes, by class; no sample when the last run failed or has no pass to confirm against"),
		metric.WithInt64Callback(p.observe))
	return err
}

func (p *Probe) observe(_ context.Context, observer metric.Int64Observer) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.hasData {
		return nil
	}
	for _, class := range pushreconcile.Classes {
		observer.Observe(int64(p.counts[class]), metric.WithAttributes(attribute.String("class", string(class))))
	}
	return nil
}
