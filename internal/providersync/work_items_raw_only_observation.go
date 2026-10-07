package providersync

import (
	"log/slog"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// observeWorkItemDerivedTablesLeftToDailyJob is the one place a work-items
// unit reports that it stored raw rows and derived nothing (CHAOS-8811): one
// scalar log line and one bounded counter per unit. The nine tables computed
// from stored work-item rows have one writer, the daily job; this line is how
// an operator sees which units left them to it.
func observeWorkItemDerivedTablesLeftToDailyJob(
	metrics *providerfoundation.Metrics,
	claim Claim,
	workItems int,
) {
	metrics.RecordWorkItemDerivedTablesLeftToDailyJob(claim.Provider)
	slog.Info("providersync.work_items.derived_tables_left_to_daily_job",
		"org_id", claim.OrgID, "unit_id", claim.ID, "provider", claim.Provider,
		"dataset", claim.Dataset, "mode", claim.Mode, "work_items", workItems)
}
