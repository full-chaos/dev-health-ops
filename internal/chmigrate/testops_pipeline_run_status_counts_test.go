package chmigrate_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

const pipelineRunStatusCountsVersion = "155_testops_pipeline_run_status_counts.sql"

// pipelineRunStatusCountsColumns are the columns 155 adds, in order.
var pipelineRunStatusCountsColumns = []string{"skipped_count", "unfinished_count", "other_count", "unfinished_stale_count"}

// 155 adds exactly the four Nullable(UInt32) bucket columns with no default, and nothing else.
func TestMigration155IsTheFourNullableCountColumns(t *testing.T) {
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	var migration *chmigrate.ChainFile
	for index := range chain {
		if chain[index].Version == pipelineRunStatusCountsVersion {
			migration = &chain[index]
		}
	}
	if migration == nil {
		t.Fatalf("the live chain does not hold %s", pipelineRunStatusCountsVersion)
	}
	var got, want []string
	for _, statement := range chmigrate.SplitStatements(migration.SQL) {
		got = append(got, strings.Join(strings.Fields(statement), " "))
	}
	for _, column := range pipelineRunStatusCountsColumns {
		want = append(want, "ALTER TABLE testops_pipeline_metrics_daily ADD COLUMN IF NOT EXISTS "+column+" Nullable(UInt32)")
	}
	if !slices.Equal(got, want) {
		t.Fatalf("%s is\n  %s\nwant\n  %s", pipelineRunStatusCountsVersion, strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}
