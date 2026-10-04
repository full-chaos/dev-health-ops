package testops

// NormalizeStatus is normalizePipelineStatus for readers outside this package
// (CHAOS-8513). The query API groups CI job runs at read time by the same
// status vocabulary the pipeline rollup uses; its test pins its own SQL lists
// to this function, so the two cannot drift.
func NormalizeStatus(status string) string {
	return normalizePipelineStatus(&status)
}
