package sync

import (
	"os"
	"testing"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// meterReader is the one real OTel SDK reader of this test binary: otel's
// global meter-provider delegation binds package-level instruments to the
// first provider set, so every counter test here reads from this one.
var meterReader *sdkmetric.ManualReader

func TestMain(m *testing.M) {
	meterReader = sdkmetric.NewManualReader()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(meterReader)))
	os.Exit(m.Run())
}
