package chmigrate_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

const testSuiteResultsRetractedVersion = "171_test_suite_results_retracted.sql"

// 171 adds exactly the defaulted retracted flag of test_suite_results, and nothing else: no other table, no key change.
func TestMigration171IsTheOneDefaultedRetractedColumn(t *testing.T) {
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	var migration *chmigrate.ChainFile
	for index := range chain {
		if chain[index].Version == testSuiteResultsRetractedVersion {
			migration = &chain[index]
		}
	}
	if migration == nil {
		t.Fatalf("the live chain does not hold %s", testSuiteResultsRetractedVersion)
	}
	var got []string
	for _, statement := range chmigrate.SplitStatements(migration.SQL) {
		got = append(got, strings.Join(strings.Fields(statement), " "))
	}
	want := []string{"ALTER TABLE test_suite_results ADD COLUMN IF NOT EXISTS retracted UInt8 DEFAULT 0"}
	if !slices.Equal(got, want) {
		t.Fatalf("%s is\n  %s\nwant\n  %s", testSuiteResultsRetractedVersion, strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}
