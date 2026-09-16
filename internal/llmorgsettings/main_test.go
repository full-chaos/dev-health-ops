package llmorgsettings

import (
	"os"
	"testing"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// realMeterReader is the ONE real OTel SDK meter reader for this whole test
// binary run -- otel's global meter-provider delegation is a process-wide
// sync.Once (see cmd/query-api/internal/analytics/main_test.go's own doc
// comment for the full mechanics), so every "RecordsToRealMeter" test in
// this package must read from this same shared reader.
var realMeterReader *sdkmetric.ManualReader

func TestMain(m *testing.M) {
	realMeterReader = sdkmetric.NewManualReader()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(realMeterReader)))
	os.Exit(m.Run())
}
