package chmigrate_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

const testopsRetractedVersion = "162_testops_test_metrics_retracted.sql"

// 162 adds exactly the retraction marker of testops_test_metrics_daily, defaulted to 0, and nothing else.
func TestMigration162IsTheOneDefaultedMarkerColumn(t *testing.T) {
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	var migration *chmigrate.ChainFile
	for index := range chain {
		if chain[index].Version == testopsRetractedVersion {
			migration = &chain[index]
		}
	}
	if migration == nil {
		t.Fatalf("the live chain does not hold %s", testopsRetractedVersion)
	}
	var got []string
	for _, statement := range chmigrate.SplitStatements(migration.SQL) {
		got = append(got, strings.Join(strings.Fields(statement), " "))
	}
	want := []string{
		"ALTER TABLE testops_test_metrics_daily ADD COLUMN IF NOT EXISTS retracted UInt8 DEFAULT 0",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("%s is\n  %s\nwant\n  %s", testopsRetractedVersion, strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}
