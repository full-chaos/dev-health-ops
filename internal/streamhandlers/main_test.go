package streamhandlers

import (
	"os"
	"testing"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// testMetricReader is the ONE real MeterProvider this test binary installs, so a test can
// read the package's process-global counters back. The counter is created at package load
// against the ambient provider; otel resolves that delegation against the first real
// provider only, hence one, installed before any test runs.
var testMetricReader = sdkmetric.NewManualReader()

func TestMain(m *testing.M) {
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(testMetricReader)))
	os.Exit(m.Run())
}
