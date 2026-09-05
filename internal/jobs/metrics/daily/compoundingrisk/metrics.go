package compoundingrisk

import (
	"fmt"
	"io"
	"sync/atomic"
)

// writerRowsWritten is a process-wide singleton counting rows this package's
// Writer has appended to compounding_risk_daily, split by whether the row
// carried the partition's real org_id. WriteRecords fails closed on an empty
// orgID (see its doc comment), so orgScoped only ever advances today -- the
// unscoped bucket exists so a future regression that bypasses or weakens that
// guard shows up as a nonzero
// dev_health_compounding_risk_rows_written_total{org_scoped="false"} instead of
// silently writing org_id="" again. Mirrors cicd's and repouser's CHAOS-4341
// pattern.
var writerRowsWritten = &RowsWrittenMetrics{}

// RowsWrittenMetricsSource returns the process-wide singleton, mirroring
// internal/jobs/metrics/daily/cicd.RowsWrittenMetricsSource.
func RowsWrittenMetricsSource() *RowsWrittenMetrics { return writerRowsWritten }

// RowsWrittenMetrics is exported only so a binary's health registry can hold a
// typed reference to the singleton; construct it via RowsWrittenMetricsSource,
// never directly.
type RowsWrittenMetrics struct {
	orgScoped atomic.Uint64
	unscoped  atomic.Uint64
}

func recordRowsWritten(rows int, orgScoped bool) {
	if rows <= 0 {
		return
	}
	if orgScoped {
		writerRowsWritten.orgScoped.Add(uint64(rows))
	} else {
		writerRowsWritten.unscoped.Add(uint64(rows))
	}
}

// WritePrometheus implements health.MetricsSource.
func (m *RowsWrittenMetrics) WritePrometheus(output io.Writer) error {
	if m == nil {
		return nil
	}
	if _, err := io.WriteString(output,
		"# HELP dev_health_compounding_risk_rows_written_total compounding_risk_daily rows written by the native compounding_risk writer, by whether the row carried the partition's real org_id.\n"+
			"# TYPE dev_health_compounding_risk_rows_written_total counter\n"); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(output, "dev_health_compounding_risk_rows_written_total{org_scoped=\"true\"} %d\n", m.orgScoped.Load()); err != nil {
		return err
	}
	_, err := fmt.Fprintf(output, "dev_health_compounding_risk_rows_written_total{org_scoped=\"false\"} %d\n", m.unscoped.Load())
	return err
}
