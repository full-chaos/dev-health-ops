package chmigrate_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

const failureRecurrenceCountsVersion = "168_testops_test_failure_recurrence_counts.sql"

// 168 adds exactly the two Nullable count columns of failure_recurrence_score, and nothing else.
func TestMigration168IsTheTwoNullableRecurrenceCountColumns(t *testing.T) {
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	var migration *chmigrate.ChainFile
	for index := range chain {
		if chain[index].Version == failureRecurrenceCountsVersion {
			migration = &chain[index]
		}
	}
	if migration == nil {
		t.Fatalf("the live chain does not hold %s", failureRecurrenceCountsVersion)
	}
	var got []string
	for _, statement := range chmigrate.SplitStatements(migration.SQL) {
		got = append(got, strings.Join(strings.Fields(statement), " "))
	}
	want := []string{
		"ALTER TABLE testops_test_metrics_daily ADD COLUMN IF NOT EXISTS recurrent_failed_names Nullable(UInt32)",
		"ALTER TABLE testops_test_metrics_daily ADD COLUMN IF NOT EXISTS distinct_failed_names Nullable(UInt32)",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("%s is\n  %s\nwant\n  %s", failureRecurrenceCountsVersion, strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}
