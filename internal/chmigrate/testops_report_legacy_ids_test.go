package chmigrate_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

const testopsLegacyIDsVersion = "152_testops_report_legacy_ids.sql"

// 152 adds exactly the two defaulted Array(String) columns the slotted test-report writer fills, and nothing else.
func TestMigration152IsTheTwoDefaultedArrayColumns(t *testing.T) {
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	var migration *chmigrate.ChainFile
	for index := range chain {
		if chain[index].Version == testopsLegacyIDsVersion {
			migration = &chain[index]
		}
	}
	if migration == nil {
		t.Fatalf("the live chain does not hold %s", testopsLegacyIDsVersion)
	}
	var got []string
	for _, statement := range chmigrate.SplitStatements(migration.SQL) {
		got = append(got, strings.Join(strings.Fields(statement), " "))
	}
	want := []string{
		"ALTER TABLE test_suite_results ADD COLUMN IF NOT EXISTS legacy_suite_ids Array(String) DEFAULT []",
		"ALTER TABLE coverage_snapshots ADD COLUMN IF NOT EXISTS legacy_snapshot_ids Array(String) DEFAULT []",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("%s is\n  %s\nwant\n  %s", testopsLegacyIDsVersion, strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}
