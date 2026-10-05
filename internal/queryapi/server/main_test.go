package server

import (
	"os"
	"testing"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// testMeterReader is the ONE real OTel SDK meter reader for this test binary, following the one-time
// global delegation discipline internal/queryapi/analytics/main_test.go documents: every package-level
// instrument here is created via the global otel.Meter(...) proxy at package-init time, and the
// process-wide delegateMeterOnce binds it to whichever provider FIRST calls otel.SetMeterProvider.
var testMeterReader *sdkmetric.ManualReader

func TestMain(m *testing.M) {
	testMeterReader = sdkmetric.NewManualReader()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(testMeterReader)))
	os.Exit(venueoracle.RunTests(m))
}
