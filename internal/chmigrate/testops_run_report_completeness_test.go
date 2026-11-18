package chmigrate_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

const testopsRunReportCompletenessVersion = "153_testops_run_report_completeness.sql"

// 153 is the completeness table keyed (org_id, repo_id, run_id, run_attempt) with the verdict as its version, and the three
// Nullable daily counts; nothing else.
func TestMigration153IsTheCompletenessTableAndTheThreeDailyCounts(t *testing.T) {
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	var migration *chmigrate.ChainFile
	for index := range chain {
		if chain[index].Version == testopsRunReportCompletenessVersion {
			migration = &chain[index]
		}
	}
	if migration == nil {
		t.Fatalf("the live chain does not hold %s", testopsRunReportCompletenessVersion)
	}
	var got []string
	for _, statement := range chmigrate.SplitStatements(migration.SQL) {
		got = append(got, strings.Join(strings.Fields(statement), " "))
	}
	want := []string{
		"CREATE TABLE IF NOT EXISTS testops_run_report_completeness ( `org_id` LowCardinality(String), `repo_id` UUID, `run_id` String, " +
			"`run_attempt` UInt32, `provider` LowCardinality(String), `complete` UInt8, `reasons` Array(LowCardinality(String)), " +
			"`reports_expected` UInt32, `reports_admitted` UInt32, `last_synced` DateTime64(3, 'UTC'), `verdict_version` UInt64 ) " +
			"ENGINE = ReplacingMergeTree(verdict_version) ORDER BY (org_id, repo_id, run_id, run_attempt)",
		"ALTER TABLE testops_test_metrics_daily ADD COLUMN IF NOT EXISTS report_runs Nullable(UInt32)",
		"ALTER TABLE testops_test_metrics_daily ADD COLUMN IF NOT EXISTS partial_report_runs Nullable(UInt32)",
		"ALTER TABLE testops_test_metrics_daily ADD COLUMN IF NOT EXISTS unmarked_report_runs Nullable(UInt32)",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("%s is\n  %s\nwant\n  %s", testopsRunReportCompletenessVersion, strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}
