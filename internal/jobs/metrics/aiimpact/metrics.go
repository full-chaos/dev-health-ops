package aiimpact

import (
	"fmt"
	"io"
	"sync/atomic"
)

// linkageMetrics is a process-wide singleton counting how many partitions
// this family computed WITHOUT commit-linkage data (codex round
// chaos-4280-r1, finding 5).
//
// Python's own build wraps the linkage query in a bare try/except and
// degrades to has_test_change=None on ANY failure -- a deliberate, documented
// choice this port mirrors exactly (CHAOS-2183: an unavailable linkage must
// null test_gap_rate, never read as a 100% gap). That degrade-and-continue
// behavior is correct and stays; the finding was that when it fires, the
// OUTER handler still records outcome=computed with no distinguishing
// signal, so a genuine, ongoing ClickHouse problem on the linkage query looks
// identical to a healthy day forever. This counter is the distinguishing
// signal: it does not change what gets written, only what an operator can
// see. A nonzero, GROWING rate here (not a one-off) is the thing worth
// paging on; a lone increment is exactly the transient blip Python already
// tolerated.
var linkageMetrics = &LinkageMetrics{}

// LinkageMetricsSource returns the process-wide singleton, mirroring
// internal/jobs/metrics/daily/cicd.RowsWrittenMetricsSource's registration
// shape.
func LinkageMetricsSource() *LinkageMetrics { return linkageMetrics }

// LinkageMetrics is exported only so a binary's health registry can hold a
// typed reference to the singleton; construct it via LinkageMetricsSource,
// never directly.
type LinkageMetrics struct {
	unavailable atomic.Uint64
}

// RecordLinkageUnavailable is called once per partition whose commit-linkage
// build failed. A no-op is deliberately not provided for the success case --
// this is a counter of an ABSENCE, not a rate that needs a denominator series
// to divide against; the family's own outcome=computed count already serves
// as that denominator.
func RecordLinkageUnavailable() { linkageMetrics.unavailable.Add(1) }

// WritePrometheus implements health.MetricsSource.
func (m *LinkageMetrics) WritePrometheus(output io.Writer) error {
	if m == nil {
		return nil
	}
	if _, err := io.WriteString(output,
		"# HELP dev_health_ai_impact_linkage_unavailable_total Partitions the ai_impact native family computed with commit-linkage data unavailable (test_gap_rate null, not a real zero).\n"+
			"# TYPE dev_health_ai_impact_linkage_unavailable_total counter\n"); err != nil {
		return err
	}
	_, err := fmt.Fprintf(output, "dev_health_ai_impact_linkage_unavailable_total %d\n", m.unavailable.Load())
	return err
}
