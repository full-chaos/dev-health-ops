package chmigrate_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

const testopsReportSlotVersion = "151_testops_report_slot_attempt.sql"

// 151 adds exactly the four defaulted columns the test-report writer and the testops readers name, and nothing else.
func TestMigration151IsTheFourDefaultedColumns(t *testing.T) {
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	var migration *chmigrate.ChainFile
	for index := range chain {
		if chain[index].Version == testopsReportSlotVersion {
			migration = &chain[index]
		}
	}
	if migration == nil {
		t.Fatalf("the live chain does not hold %s", testopsReportSlotVersion)
	}
	var got []string
	for _, statement := range chmigrate.SplitStatements(migration.SQL) {
		got = append(got, strings.Join(strings.Fields(statement), " "))
	}
	want := []string{
		"ALTER TABLE test_suite_results ADD COLUMN IF NOT EXISTS report_slot LowCardinality(String) DEFAULT ''",
		"ALTER TABLE test_suite_results ADD COLUMN IF NOT EXISTS run_attempt UInt32 DEFAULT 0",
		"ALTER TABLE coverage_snapshots ADD COLUMN IF NOT EXISTS report_slot LowCardinality(String) DEFAULT ''",
		"ALTER TABLE coverage_snapshots ADD COLUMN IF NOT EXISTS run_attempt UInt32 DEFAULT 0",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("%s is\n  %s\nwant\n  %s", testopsReportSlotVersion, strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}
