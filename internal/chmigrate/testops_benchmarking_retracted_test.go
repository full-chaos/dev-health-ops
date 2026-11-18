package chmigrate_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

const benchmarkingRetractedVersion = "156_testops_benchmarking_retracted.sql"

// 156 adds exactly the retraction marker of the six benchmarking output tables, defaulted to 0, and nothing else.
func TestMigration156IsTheOneDefaultedMarkerColumnOfTheBenchmarkingOutputs(t *testing.T) {
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	var migration *chmigrate.ChainFile
	for index := range chain {
		if chain[index].Version == benchmarkingRetractedVersion {
			migration = &chain[index]
		}
	}
	if migration == nil {
		t.Fatalf("the live chain does not hold %s", benchmarkingRetractedVersion)
	}
	var got []string
	for _, statement := range chmigrate.SplitStatements(migration.SQL) {
		got = append(got, strings.Join(strings.Fields(statement), " "))
	}
	var want []string
	for _, table := range []string{
		"testops_metric_baselines", "testops_maturity_bands", "testops_metric_anomalies",
		"testops_period_comparisons", "testops_metric_correlations", "testops_benchmark_insights",
	} {
		want = append(want, "ALTER TABLE "+table+" ADD COLUMN IF NOT EXISTS retracted UInt8 DEFAULT 0")
	}
	if !slices.Equal(got, want) {
		t.Fatalf("%s is\n  %s\nwant\n  %s", benchmarkingRetractedVersion, strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}
