package tracing

import (
	"context"
	"io"
	"sync/atomic"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// spanMetric is the one counter family that says where this process's spans
// went. A span a process never exported leaves no trace anywhere, so without
// it an absent span cannot be told from a request that never happened
// (CHAOS-9106). The outcomes:
//
//	sampled         the sampler chose to record a span
//	not_sampled     the sampler chose not to (the ratio of the two is the effective rate)
//	ended           a recorded span ended and was handed to the exporter queue
//	exported        spans the collector accepted
//	export_failed   spans in an export call that failed
const spanMetric = "dev_health_otel_spans_total"

// spanUnaccountedMetric is a gauge: recorded spans that ended and are neither
// exported nor in a failed export. Spans waiting in the queue or in flight
// are in it for a moment; a value that stays high after traffic stops is the
// number the exporter queue dropped (the batch queue holds 2048 spans and
// drops the rest without a word).
const spanUnaccountedMetric = "dev_health_otel_spans_unaccounted"

// spanCounters counts span outcomes in this process.
type spanCounters struct {
	sampled      atomic.Uint64
	notSampled   atomic.Uint64
	ended        atomic.Uint64
	exported     atomic.Uint64
	exportFailed atomic.Uint64
}

var spanOutcomes = &spanCounters{}

// unaccounted is ended - exported - failed, never below zero (the three are
// read one after the other while spans move).
func (c *spanCounters) unaccounted() uint64 {
	ended, done := c.ended.Load(), c.exported.Load()+c.exportFailed.Load()
	if done >= ended {
		return 0
	}
	return ended - done
}

// WritePrometheus writes every series, so the families are present at zero on
// a process that has not started a span (health.MetricsSource).
func (c *spanCounters) WritePrometheus(w io.Writer) error {
	lines := []string{
		"# HELP " + spanMetric + " Spans in this process by outcome: sampled and not_sampled count the sampler's decisions; ended, exported and export_failed count recorded spans.\n",
		"# TYPE " + spanMetric + " counter\n",
		spanMetric + "{outcome=\"sampled\"} " + uitoa(c.sampled.Load()) + "\n",
		spanMetric + "{outcome=\"not_sampled\"} " + uitoa(c.notSampled.Load()) + "\n",
		spanMetric + "{outcome=\"ended\"} " + uitoa(c.ended.Load()) + "\n",
		spanMetric + "{outcome=\"exported\"} " + uitoa(c.exported.Load()) + "\n",
		spanMetric + "{outcome=\"export_failed\"} " + uitoa(c.exportFailed.Load()) + "\n",
		"# HELP " + spanUnaccountedMetric + " Recorded spans that ended and are neither exported nor in a failed export: queued, in flight, or dropped by a full exporter queue (a value that stays high after traffic stops is the drop count).\n",
		"# TYPE " + spanUnaccountedMetric + " gauge\n",
		spanUnaccountedMetric + " " + uitoa(c.unaccounted()) + "\n",
	}
	for _, line := range lines {
		if _, err := io.WriteString(w, line); err != nil {
			return err
		}
	}
	return nil
}

// SpansSource is the process-wide span outcome counters for
// health.Registry.RegisterMetrics.
func SpansSource() *spanCounters { return spanOutcomes }

// countingSampler counts the decision of the whole sampler chain (the parent's
// decision included), so sampled / (sampled + not_sampled) is the rate this
// process really records at.
type countingSampler struct {
	inner    sdktrace.Sampler
	counters *spanCounters
}

func (s countingSampler) ShouldSample(parameters sdktrace.SamplingParameters) sdktrace.SamplingResult {
	result := s.inner.ShouldSample(parameters)
	if result.Decision == sdktrace.RecordAndSample {
		s.counters.sampled.Add(1)
	} else {
		s.counters.notSampled.Add(1)
	}
	return result
}

func (s countingSampler) Description() string { return s.inner.Description() }

// endCounter counts the recorded spans that end. It never holds a span.
type endCounter struct{ counters *spanCounters }

func (endCounter) OnStart(context.Context, sdktrace.ReadWriteSpan) {}
func (e endCounter) OnEnd(span sdktrace.ReadOnlySpan) {
	if span.SpanContext().IsSampled() {
		e.counters.ended.Add(1)
	}
}
func (endCounter) Shutdown(context.Context) error   { return nil }
func (endCounter) ForceFlush(context.Context) error { return nil }

// countingExporter counts what each export call carried and how it ended.
type countingExporter struct {
	sdktrace.SpanExporter
	counters *spanCounters
}

func (e countingExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	err := e.SpanExporter.ExportSpans(ctx, spans)
	if err != nil {
		e.counters.exportFailed.Add(uint64(len(spans)))
		return err
	}
	e.counters.exported.Add(uint64(len(spans)))
	return nil
}
