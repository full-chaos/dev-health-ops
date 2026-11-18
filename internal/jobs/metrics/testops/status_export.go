package testops

// NormalizeStatus is normalizePipelineStatus for readers outside this package
// (CHAOS-8513). The query API groups CI job runs at read time by the same
// status vocabulary the pipeline rollup uses; its test pins its own SQL lists
// to this function, so the two cannot drift.
func NormalizeStatus(status string) string {
	return normalizePipelineStatus(&status)
}

// StatusDeleted is the stored status of a pipeline run the provider no longer
// has: a re-read of an open run answered "not found", so its row stays but says
// nothing about the run's outcome. Every reader that counts pipeline rows
// leaves it out through ExcludedFromPipelineCounts, the single list.
const StatusDeleted = "deleted"

// The stored statuses of a run that has not finished. A provider sync copies its
// last update time into finished_at for such a run, so a finish timestamp alone
// does not make a run finished.
const (
	StatusRunning = "running"
	StatusQueued  = "queued"
)

// IsOpenRun reports whether a stored ci_pipeline_runs status is a run that has
// not finished.
func IsOpenRun(status string) bool {
	switch trimLower(status) {
	case StatusRunning, StatusQueued:
		return true
	}
	return false
}

// NotOpenRunSQL is IsOpenRun as a ClickHouse predicate on ci_pipeline_runs.
const NotOpenRunSQL = "lowerUTF8(trimBoth(ifNull(status, ''))) NOT IN ('" + StatusRunning + "', '" + StatusQueued + "')"

// ExcludedFromPipelineCounts reports whether a stored ci_pipeline_runs status
// is left out of pipeline counts and rates.
func ExcludedFromPipelineCounts(status string) bool {
	return trimLower(status) == StatusDeleted
}
