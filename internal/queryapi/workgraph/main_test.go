package workgraph

import (
	"os"
	"testing"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// realMeterReader is the ONE real OTel SDK meter reader for this whole
// test binary run -- same one-time-delegation rationale
// analytics/main_test.go documents: go.opentelemetry.io/otel's global
// meter-provider delegation binds every package-level instrument
// (created via the global otel.Meter(...) proxy at package-init time,
// before any test runs) to whichever provider FIRST calls
// otel.SetMeterProvider -- a process-wide sync.Once, not a per-call
// rebind. A second, independent SetMeterProvider+ManualReader pair in a
// second "real meter" test in THIS package would silently lose the
// one-time delegation the same way analytics/main_test.go's doc comment
// describes happening there. This TestMain performs that delegation
// exactly once, before any test runs, so every real-meter assertion in
// this package (currently just
// teamattribution_integration_test.go's truncation-counter check) reads
// from the SAME shared reader.
var realMeterReader *sdkmetric.ManualReader

func TestMain(m *testing.M) {
	realMeterReader = sdkmetric.NewManualReader()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(realMeterReader)))
	os.Exit(m.Run())
}
