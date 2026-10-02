package otlpmetrics

import (
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
)

// Stats counts what the push did, so a push that fails is visible on /metrics
// (and, once the collector is reachable again, in the backend), not only in a
// log line. It is a health.MetricsSource.
type Stats struct {
	exports        atomic.Int64
	exportFailures atomic.Int64

	mu             sync.Mutex
	sourceFailures map[string]int64
}

func (s *Stats) exported()     { s.exports.Add(1) }
func (s *Stats) exportFailed() { s.exportFailures.Add(1) }

func (s *Stats) bridgeSourceFailed(source string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sourceFailures == nil {
		s.sourceFailures = map[string]int64{}
	}
	s.sourceFailures[source]++
}

// Exports and ExportFailures are the counts so far.
func (s *Stats) Exports() int64        { return s.exports.Load() }
func (s *Stats) ExportFailures() int64 { return s.exportFailures.Load() }

// SourceFailures is the per-source count of fragments the bridge skipped.
func (s *Stats) SourceFailures() map[string]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]int64, len(s.sourceFailures))
	for source, count := range s.sourceFailures {
		out[source] = count
	}
	return out
}

// SourceName is the health registry name the push counters are registered under.
const SourceName = "otlp_metrics_push"

// WritePrometheus implements health.MetricsSource.
func (s *Stats) WritePrometheus(w io.Writer) error {
	if _, err := fmt.Fprintf(w,
		"# HELP dev_health_otlp_metrics_exports_total OTLP metric exports that succeeded.\n"+
			"# TYPE dev_health_otlp_metrics_exports_total counter\n"+
			"dev_health_otlp_metrics_exports_total %d\n"+
			"# HELP dev_health_otlp_metrics_export_failures_total OTLP metric exports that failed.\n"+
			"# TYPE dev_health_otlp_metrics_export_failures_total counter\n"+
			"dev_health_otlp_metrics_export_failures_total %d\n"+
			"# HELP dev_health_otlp_metrics_bridge_source_failures_total Metrics sources the OTLP bridge skipped because they failed to write or parse.\n"+
			"# TYPE dev_health_otlp_metrics_bridge_source_failures_total counter\n",
		s.Exports(), s.ExportFailures()); err != nil {
		return err
	}
	failures := s.SourceFailures()
	sources := make([]string, 0, len(failures))
	for source := range failures {
		sources = append(sources, source)
	}
	sort.Strings(sources)
	if len(sources) == 0 {
		_, err := io.WriteString(w, "dev_health_otlp_metrics_bridge_source_failures_total{source=\"\"} 0\n")
		return err
	}
	for _, source := range sources {
		// Registry source names match checkNamePattern, so they are safe as a
		// label value without escaping.
		if _, err := fmt.Fprintf(w, "dev_health_otlp_metrics_bridge_source_failures_total{source=%q} %d\n", source, failures[source]); err != nil {
			return err
		}
	}
	return nil
}
