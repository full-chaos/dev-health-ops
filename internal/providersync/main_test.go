package providersync

import (
	"os"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// meterReader is the one real OTel SDK reader of this test binary: otel's
// global meter-provider delegation binds package-level instruments to the
// first provider set, so every counter test here reads from this one.
var meterReader *sdkmetric.ManualReader

// TestMain fails the run when a test of this package that ran did not use its
// golden: a frozen answer no test compares is a comparison that stopped.
func TestMain(m *testing.M) {
	meterReader = sdkmetric.NewManualReader()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(meterReader)))
	os.Exit(venueoracle.RunTests(m))
}
