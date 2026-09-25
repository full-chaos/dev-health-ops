//go:build integration

package billingvenue

import (
	"context"
	"sync"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// refundMeter is the one meter provider of this test process: the refund
// counters live in package billing and bind to the first provider set, so
// every test reads deltas from the same reader.
var refundMeter struct {
	once   sync.Once
	reader *sdkmetric.ManualReader
}

// decisionCounts is the refund decision and unapplied-event counters now,
// keyed "<counter>/<reason or event type>".
func decisionCounts(t *testing.T) map[string]int64 {
	t.Helper()
	refundMeter.once.Do(func() {
		refundMeter.reader = sdkmetric.NewManualReader()
		otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(refundMeter.reader)))
	})
	var collected metricdata.ResourceMetrics
	if err := refundMeter.reader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	out := map[string]int64{}
	for _, scope := range collected.ScopeMetrics {
		for _, series := range scope.Metrics {
			if series.Name != "dev_health_api_stripe_refund_event_decisions_total" && series.Name != "dev_health_api_stripe_webhook_unhandled_events_total" {
				continue
			}
			for _, point := range series.Data.(metricdata.Sum[int64]).DataPoints {
				for _, key := range []string{"reason", "event_type"} {
					if label, ok := point.Attributes.Value(attribute.Key(key)); ok {
						out[series.Name+"/"+label.AsString()] += point.Value
					}
				}
			}
		}
	}
	return out
}

// decisionDelta is the counts that moved between two readings.
func decisionDelta(before, after map[string]int64) map[string]int64 {
	delta := map[string]int64{}
	for key, value := range after {
		if value != before[key] {
			delta[key] = value - before[key]
		}
	}
	return delta
}

const decisionPrefix = "dev_health_api_stripe_refund_event_decisions_total/"
