package identity

import (
	"context"
	"fmt"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// A password write refused because its reset link was used, expired or
// replaced after the first read counts as link_unusable, not as an error.
func TestALateResetLinkRefusalCountsAsLinkUnusable(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	counter, err := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test").Int64Counter("auth.identity.external_links")
	if err != nil {
		t.Fatal(err)
	}
	s := &Store{links: counter}
	refused := fmt.Errorf("%w: applying the state mutation: %w", audit.ErrMutationFailed, ErrLinkUnusable)
	s.countLink(context.Background(), "set_password", s.localErr(context.Background(), "set password hash", refused))
	s.countLink(context.Background(), "set_password", fmt.Errorf("%w: committing", audit.ErrMutationFailed))

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	got := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok || m.Name != "auth.identity.external_links" {
				continue
			}
			for _, dp := range sum.DataPoints {
				action, _ := dp.Attributes.Value(attribute.Key("action"))
				outcome, _ := dp.Attributes.Value(attribute.Key("outcome"))
				got[action.AsString()+"/"+outcome.AsString()] += dp.Value
			}
		}
	}
	want := map[string]int64{"set_password/link_unusable": 1, "set_password/error": 1}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("counted %v, want %v", got, want)
	}
}
