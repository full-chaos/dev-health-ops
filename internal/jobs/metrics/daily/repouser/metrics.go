package repouser

import (
	"fmt"
	"io"
	"sync/atomic"
)

// writerRowsWritten is a process-wide singleton counting rows this
// package's Writer has appended to repo_metrics_daily/user_metrics_daily/
// commit_metrics, split by whether the row carried the partition's real
// org_id (CHAOS-4341). Writer.WriteResult fails closed on an empty orgID
// (see its doc comment), so orgScoped only ever advances today -- the
// unscoped bucket exists so a future regression that bypasses or weakens
// that guard shows up as a nonzero dev_health_repo_user_commit_rows_
// written_total{org_scoped="false"} instead of silently writing org_id=""
// again, exactly the failure mode this ticket fixed.
var writerRowsWritten = &RowsWrittenMetrics{}

// RowsWrittenMetricsSource returns the process-wide singleton, mirroring
// internal/synccoverage.ScopeIntentMetricsSource() -- a binary's health
// registry registers it directly (health.MetricsSource is satisfied
// structurally by WritePrometheus(io.Writer) error, so this package does not
// need to import internal/platform/health).
func RowsWrittenMetricsSource() *RowsWrittenMetrics { return writerRowsWritten }

// RowsWrittenMetrics is exported only so a binary's health registry can hold
// a typed reference to the singleton; construct it via
// RowsWrittenMetricsSource, never directly.
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
		"# HELP dev_health_repo_user_commit_rows_written_total repo_metrics_daily/user_metrics_daily/commit_metrics rows written by the native repo_user_commit writer, by whether the row carried the partition's real org_id (CHAOS-4341).\n"+
			"# TYPE dev_health_repo_user_commit_rows_written_total counter\n"); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(output, "dev_health_repo_user_commit_rows_written_total{org_scoped=\"true\"} %d\n", m.orgScoped.Load()); err != nil {
		return err
	}
	_, err := fmt.Fprintf(output, "dev_health_repo_user_commit_rows_written_total{org_scoped=\"false\"} %d\n", m.unscoped.Load())
	return err
}
