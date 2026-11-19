package chmigrate_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

const testopsRiskRetractedVersion = "163_testops_risk_retracted.sql"

var testopsRiskRetractedTables = []string{"testops_release_confidence", "testops_quality_drag", "testops_pipeline_stability"}

// 163 adds exactly the retraction marker of the three testops_risk tables, defaulted to 0, and nothing else.
func TestMigration158IsTheThreeDefaultedMarkerColumns(t *testing.T) {
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	var migration *chmigrate.ChainFile
	for index := range chain {
		if chain[index].Version == testopsRiskRetractedVersion {
			migration = &chain[index]
		}
	}
	if migration == nil {
		t.Fatalf("the live chain does not hold %s", testopsRiskRetractedVersion)
	}
	var got, want []string
	for _, statement := range chmigrate.SplitStatements(migration.SQL) {
		got = append(got, strings.Join(strings.Fields(statement), " "))
	}
	for _, table := range testopsRiskRetractedTables {
		want = append(want, "ALTER TABLE "+table+" ADD COLUMN IF NOT EXISTS retracted UInt8 DEFAULT 0")
	}
	if !slices.Equal(got, want) {
		t.Fatalf("%s is\n  %s\nwant\n  %s", testopsRiskRetractedVersion, strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}
