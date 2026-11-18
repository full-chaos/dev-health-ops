package chmigrate_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

const cicdRetractedVersion = "159_cicd_metrics_daily_retracted.sql"

// 159 adds exactly the retraction marker of cicd_metrics_daily, defaulted to 0, and nothing else.
func TestMigration159IsTheOneDefaultedMarkerColumn(t *testing.T) {
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	var migration *chmigrate.ChainFile
	for index := range chain {
		if chain[index].Version == cicdRetractedVersion {
			migration = &chain[index]
		}
	}
	if migration == nil {
		t.Fatalf("the live chain does not hold %s", cicdRetractedVersion)
	}
	var got []string
	for _, statement := range chmigrate.SplitStatements(migration.SQL) {
		got = append(got, strings.Join(strings.Fields(statement), " "))
	}
	want := []string{"ALTER TABLE cicd_metrics_daily ADD COLUMN IF NOT EXISTS retracted UInt8 DEFAULT 0"}
	if !slices.Equal(got, want) {
		t.Fatalf("%s is\n  %s\nwant\n  %s", cicdRetractedVersion, strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}
