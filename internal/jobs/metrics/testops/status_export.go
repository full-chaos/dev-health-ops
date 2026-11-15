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

// ExcludedFromPipelineCounts reports whether a stored ci_pipeline_runs status
// is left out of pipeline counts and rates.
func ExcludedFromPipelineCounts(status string) bool {
	return trimLower(status) == StatusDeleted
}
