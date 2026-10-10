package daily

// The test seed of the reader tests (internal/testsupport/retractionseed)
// stores measured rows and retraction rows of the team-keyed tables by plain
// INSERT, in a throwaway ClickHouse. It is a fixture of a test-support
// package: no production binary imports it, and it needs no stale-key rule.
// It is declared here, in its own file, so the declaration of the reader
// tests does not edit the writer census.
func init() {
	const seed = "internal/testsupport/retractionseed/retractionseed.go"
	for _, table := range []string{
		"work_item_metrics_daily", "work_item_state_durations_daily", "estimate_coverage_metrics_daily",
		"team_metrics_daily", "ai_impact_metrics_daily", "ai_governance_coverage_daily",
		"compounding_risk_daily", "issue_type_metrics_daily", "investment_metrics_daily",
		"team_cognitive_load_daily", "ic_landscape_rolling_30d",
	} {
		teamKeyTableWriters[table][seed] = teamKeyWriterFixture
	}
}
