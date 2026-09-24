package principal

import (
	"os"
	"testing"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// sharedTestMetricReader is the ONE real OTel MeterProvider this test
// binary ever installs, for every telemetry test in this package
// (envelope and edge alike).
//
// Why one, package-wide, rather than one per test: this package's
// counters (verifyOutcomeCounter, envelopeRejectedCounter,
// edgeVerifyOutcomeCounter, edgeRejectedCounter) are process-global
// singletons created once at package load against whatever the ambient
// (delegating, no-op) MeterProvider was at that time. go.opentelemetry.io/
// otel's global package only resolves that delegation against the FIRST
// real provider a process ever installs via otel.SetMeterProvider --
// every later SetMeterProvider call only affects instruments created
// afterwards, not ones already resolved. With two (or more) telemetry
// tests each installing and restoring their OWN provider, whichever runs
// first permanently wins the real delegation for every counter in the
// package, and every other test's own reader is left collecting from a
// provider nothing ever writes to -- reproduced: adding this package's
// second such test (the edge access token's own rejection/counter proof)
// made the FIRST one fail with "not found ... the reader consumed
// nothing", for exactly this reason. A single TestMain-installed
// provider, shared by every telemetry test via this reader, removes the
// ordering dependency: whichever test runs first, everything after it
// already points at the SAME reader.
//
// Safe for exact-count assertions (`want exactly N data points`) because
// no OTHER test in this package touches these four counters except the
// tests that read this reader themselves, and each of those reads
// (Collect) happens synchronously at the end of its own test function,
// before the next test function starts -- so a later test's own calls
// can never retroactively pollute an earlier test's already-completed
// collection. The only pre-existing calls to Verify()/EdgeVerifier.Verify
// outside these dedicated telemetry tests (envelopemint_compat_test.go,
// live_python_envelope_oracle_test.go, edge_verifier_test.go's own
// accept/reject cases, verifier_test.go, verifier_toctou_test.go) either
// run after the telemetry tests that share this reader (this package's
// test files sort alphabetically: edge_rejection_telemetry_test.go and
// rejection_telemetry_test.go each precede the other files that reuse
// their respective verifier), or -- for the two files that run between
// the edge and envelope telemetry tests -- only ever assert success,
// never a rejection, so they cannot add a data point to either rejected-
// reason counter at all.
var sharedTestMetricReader = sdkmetric.NewManualReader()

// TestMain installs sharedTestMetricReader as this process's ONE real
// MeterProvider before any test runs. See sharedTestMetricReader's own
// doc comment for why this must be package-wide rather than per-test.
func TestMain(m *testing.M) {
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(sharedTestMetricReader)))
	os.Exit(m.Run())
}
