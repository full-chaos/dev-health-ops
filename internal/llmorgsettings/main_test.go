package llmorgsettings

import (
	"os"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// realMeterReader is the ONE real OTel SDK meter reader for this whole test
// binary run -- otel's global meter-provider delegation is a process-wide
// sync.Once (see internal/queryapi/analytics/main_test.go's own doc
// comment for the full mechanics), so every "RecordsToRealMeter" test in
// this package must read from this same shared reader.
var realMeterReader *sdkmetric.ManualReader

// TestMain also fails the run when a test of this package that ran did not use
// its golden: a frozen answer no test compares is a comparison that stopped.
func TestMain(m *testing.M) {
	realMeterReader = sdkmetric.NewManualReader()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(realMeterReader)))
	os.Exit(venueoracle.RunTests(m))
}
