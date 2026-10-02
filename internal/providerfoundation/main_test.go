package providerfoundation

import (
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// meterReader is the one real OTel SDK reader of this test binary: otel's
// global meter-provider delegation binds package-level instruments to the
// first provider set, so the counters under test read from this one.
var meterReader *sdkmetric.ManualReader

// InitMeterReader sets the one reader up. The test binary's TestMain is in the
// external test package (it runs the tests through the opened-golden check,
// which the package under test cannot import), and calls this first.
func InitMeterReader() {
	meterReader = sdkmetric.NewManualReader()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(meterReader)))
}
