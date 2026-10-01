package tracing

import (
	"sync/atomic"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// localRate is the OTEL_SAMPLE_RATE the installed provider was built with
// (parsed once, by InitWithServiceName). Nil means no provider of this package
// is installed, so nothing is recorded.
var localRate atomic.Pointer[float64]

func recordLocalRate(rate float64) { localRate.Store(&rate) }

func clearLocalRate() { localRate.Store(nil) }

// LocalSamplingDecision reports whether this process's ROOT sampler (the same
// sampler(rate) the provider wraps in ParentBased) samples a trace with this id.
// A listener that must not let a caller force recording (a sampled traceparent
// from an outside client) uses it to replace the remote parent's sampled flag
// with the local decision, so the caller keeps its trace id but not the vote.
// It is false when no provider of this package is installed.
func LocalSamplingDecision(traceID oteltrace.TraceID) bool {
	rate := localRate.Load()
	if rate == nil {
		return false
	}
	result := sampler(*rate).ShouldSample(sdktrace.SamplingParameters{TraceID: traceID})
	return result.Decision == sdktrace.RecordAndSample
}
