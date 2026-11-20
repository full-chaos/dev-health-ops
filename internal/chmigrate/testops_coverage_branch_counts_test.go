package chmigrate_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

const testopsCoverageBranchCountsVersion = "168_testops_coverage_branch_counts.sql"

// 168 adds exactly the two Nullable branch count columns of testops_coverage_metrics_daily, with no default, and nothing else.
func TestMigration168IsTheTwoNullableBranchCountColumns(t *testing.T) {
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	var migration *chmigrate.ChainFile
	for index := range chain {
		if chain[index].Version == testopsCoverageBranchCountsVersion {
			migration = &chain[index]
		}
	}
	if migration == nil {
		t.Fatalf("the live chain does not hold %s", testopsCoverageBranchCountsVersion)
	}
	var got []string
	for _, statement := range chmigrate.SplitStatements(migration.SQL) {
		got = append(got, strings.Join(strings.Fields(statement), " "))
	}
	want := []string{
		"ALTER TABLE testops_coverage_metrics_daily ADD COLUMN IF NOT EXISTS branches_total Nullable(UInt32) AFTER lines_covered",
		"ALTER TABLE testops_coverage_metrics_daily ADD COLUMN IF NOT EXISTS branches_covered Nullable(UInt32) AFTER branches_total",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("%s is\n  %s\nwant\n  %s", testopsCoverageBranchCountsVersion, strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}
