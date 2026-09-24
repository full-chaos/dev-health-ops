package analytics

import (
	"os"
	"testing"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// realMeterReader is the ONE real OTel SDK meter reader for this whole
// test binary run.
//
// go.opentelemetry.io/otel's global meter-provider delegation binds every
// package-level instrument (created via the global otel.Meter(...) proxy
// at package-init time, before any test runs) to whichever provider
// FIRST calls otel.SetMeterProvider -- internal/global/state.go's
// delegateMeterOnce is a process-wide sync.Once, not a per-call rebind.
// A second, independent SetMeterProvider+ManualReader pair in a second
// "RecordsToRealMeter"-style test silently loses: the one-time
// delegation already happened for whichever test's provider ran first,
// and every subsequent SetMeterProvider call only changes what FUTURE
// otel.Meter() callers see, never where already-bound instruments
// write. Found adding investmentargmaxtransitionguard_test.go's own
// real-meter test (CHAOS-4759): running the package suite made
// investmentmembershiptelemetry_test.go's pre-existing real-meter test
// fail, because the new test's SetMeterProvider call (alphabetically
// first) won the one-time delegation and the older test's own
// provider+reader never saw the package's counters again.
//
// This TestMain performs that one-time delegation exactly once, before
// any test runs, so every "RecordsToRealMeter" test in this package
// reads from the SAME shared reader and asserts only on its own metric
// name and attribute values -- safe as long as no two such tests record
// to the same instrument with the same attribute set (each currently
// does not).
var realMeterReader *sdkmetric.ManualReader

func TestMain(m *testing.M) {
	realMeterReader = sdkmetric.NewManualReader()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(realMeterReader)))
	os.Exit(m.Run())
}
