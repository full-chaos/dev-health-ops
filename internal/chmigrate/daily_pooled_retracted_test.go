package chmigrate_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

const dailyPooledRetractedVersion = "164_daily_pooled_retracted.sql"

var dailyPooledRetractedTables = []string{
	"incident_metrics_daily", "deploy_metrics_daily", "repo_metrics_daily", "testops_pipeline_metrics_daily",
}

// 164 adds exactly the retraction marker of the four pooled daily tables, defaulted to 0, and nothing else.
func TestMigration164IsTheOneDefaultedMarkerColumnOfTheFourPooledDailyTables(t *testing.T) {
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	var migration *chmigrate.ChainFile
	for index := range chain {
		if chain[index].Version == dailyPooledRetractedVersion {
			migration = &chain[index]
		}
	}
	if migration == nil {
		t.Fatalf("the live chain does not hold %s", dailyPooledRetractedVersion)
	}
	var got []string
	for _, statement := range chmigrate.SplitStatements(migration.SQL) {
		got = append(got, strings.Join(strings.Fields(statement), " "))
	}
	var want []string
	for _, table := range dailyPooledRetractedTables {
		want = append(want, "ALTER TABLE "+table+" ADD COLUMN IF NOT EXISTS retracted UInt8 DEFAULT 0")
	}
	if !slices.Equal(got, want) {
		t.Fatalf("%s is\n  %s\nwant\n  %s", dailyPooledRetractedVersion, strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}
