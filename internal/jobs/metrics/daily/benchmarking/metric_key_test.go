package benchmarking

import (
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

var sortingKeyOf = regexp.MustCompile(`(?is)CREATE TABLE\s+(?:IF NOT EXISTS\s+)?` + "`?" + `(\w+)` + "`?" + `\s*\(.*?\)\s*ENGINE\s*=.*?\bORDER BY\s+\(([^)]*)\)`)

// modifiedSortingKeyOf is an ALTER TABLE that rewrites the sorting key (migration 141 appends row_view).
var modifiedSortingKeyOf = regexp.MustCompile(`(?is)ALTER TABLE\s+(\w+)\s[^;]*?MODIFY ORDER BY\s*\(([^)]*)\)`)

// Every metric reads one value per key of its table: InnerGroupColumns is the
// table's sorting key less org_id, the day and a column the metric selects by
// a filter. A value column in the group (the team of a testops row) splits a
// key into one group per value its rows were computed with, so an older row
// under a superseded team id is a sample of its own.
func TestEveryMetricGroupsByTheKeyOfItsTable(t *testing.T) {
	keys := map[string][]string{}
	baseline, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatalf("load the schema baseline: %v", err)
	}
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatalf("load the migrations after the head: %v", err)
	}
	sources := []string{}
	for _, object := range baseline.Objects {
		sources = append(sources, object.Create)
	}
	for _, file := range chain {
		sources = append(sources, file.SQL)
	}
	for _, source := range sources {
		for _, match := range sortingKeyOf.FindAllStringSubmatch(source, -1) {
			var columns []string
			for _, column := range strings.Split(match[2], ",") {
				columns = append(columns, strings.TrimSpace(column))
			}
			keys[match[1]] = columns
		}
		for _, match := range modifiedSortingKeyOf.FindAllStringSubmatch(source, -1) {
			var columns []string
			for _, column := range strings.Split(match[2], ",") {
				columns = append(columns, strings.TrimSpace(column))
			}
			keys[match[1]] = columns
		}
	}
	filtered := map[string][]string{"dora_metrics_daily": {"metric_name"}}

	if len(metricDefinitions) < 15 {
		t.Fatalf("%d metric definitions: the test did not measure", len(metricDefinitions))
	}
	for name, definition := range metricDefinitions {
		key, known := keys[definition.Table]
		if !known {
			t.Errorf("%s: no sorting key read for table %s", name, definition.Table)
			continue
		}
		var want []string
		for _, column := range key {
			if column != "org_id" && column != "day" && !contains(filtered[definition.Table], column) {
				want = append(want, column)
			}
		}
		got := append([]string(nil), definition.InnerGroupColumns...)
		sort.Strings(want)
		sort.Strings(got)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s groups %s by %v, the key of the table is %v", name, definition.Table, got, want)
		}
	}
}

func contains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}
