package authapi

import (
	"context"
	"net/http"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
)

func legacyRefreshTotal(t *testing.T, reader *sdkmetric.ManualReader) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			if m.Name != accessverify.RefreshAcceptedMetric || !ok {
				continue
			}
			for _, point := range sum.DataPoints {
				service, _ := point.Attributes.Value(attribute.Key("service"))
				class, _ := point.Attributes.Value(attribute.Key("class"))
				if service.AsString() == AccessService && class.AsString() == accessverify.ClassLegacyRefresh {
					total += point.Value
				}
			}
		}
	}
	return total
}

func TestCompatRefreshCountsAnAcceptedGoAPIRefreshTokenOnly(t *testing.T) {
	f := newCompatFixture(t)
	reader := sdkmetric.NewManualReader()
	counter, err := accessverify.NewRefreshCounter(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test"), AccessService)
	if err != nil {
		t.Fatal(err)
	}
	SetRefreshAccepted(counter)
	t.Cleanup(func() { SetRefreshAccepted(nil) })
	f.sessions.legacyResult = accepted(f.record)

	other := `{"refresh_token":"` + legacyToken(t, f, f.org.String(), "another-key-0123456789abcdef-0123456") + `"}`
	f.do(http.MethodPost, PatternCompatRefresh, other, nil)
	f.do(http.MethodPost, PatternCompatRefresh, `{"refresh_token":"`+credential()+`"}`, nil)
	if got := legacyRefreshTotal(t, reader); got != 0 {
		t.Fatalf("an unverifiable token or a native credential moved the counter to %d, want 0", got)
	}
	response := f.do(http.MethodPost, PatternCompatRefresh, `{"refresh_token":"`+legacyToken(t, f, f.org.String(), testSecret)+`"}`, nil)
	if response.Code != http.StatusOK {
		t.Fatalf("status %d: %s", response.Code, response.Body)
	}
	if got := legacyRefreshTotal(t, reader); got != 1 {
		t.Fatalf("after one accepted go-api refresh token the counter is %d, want 1", got)
	}
}
