package providersync

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"
)

// The three tests in this file pin facts that live in PYTHON source, not in Go:
// which columns the live ClickHouse sink actually writes for each destination,
// and which columns the live migration made the sorting key. Mirroring either
// as a Go constant would let production drift away from the port with both
// sides still green, so both are READ from the checked-in source at test time.
//
// Every parse below fails loudly when it finds nothing. A source-reading test
// that quietly matches zero things and passes is the worst outcome available
// here: it reads as coverage of the exact drift it stopped detecting.

func githubWorkItemMetricRepoRoot(t *testing.T) string {
	t.Helper()
	_, currentFile, _, _ := runtime.Caller(0)
	root, err := filepath.Abs(filepath.Join(filepath.Dir(currentFile), "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func githubWorkItemMetricReadSource(t *testing.T, relative string) string {
	t.Helper()
	source, err := os.ReadFile(filepath.Join(githubWorkItemMetricRepoRoot(t), relative))
	if err != nil {
		t.Fatal(err)
	}
	if len(source) == 0 {
		t.Fatalf("%s is empty -- nothing could be parsed from it", relative)
	}
	return string(source)
}

// pythonSinkInsertColumns parses every `self._insert_rows("<table>", [...])`
// call in the live ClickHouse sink and returns table -> ordered column list.
func pythonSinkInsertColumns(t *testing.T, source string) map[string][]string {
	t.Helper()
	const marker = "self._insert_rows("
	result := map[string][]string{}
	for offset := 0; ; {
		index := strings.Index(source[offset:], marker)
		if index < 0 {
			break
		}
		cursor := offset + index + len(marker)
		offset = cursor
		table, next, ok := githubWorkItemMetricQuoted(source, cursor)
		if !ok {
			continue
		}
		open := strings.Index(source[next:], "[")
		closed := strings.Index(source[next:], "]")
		if open < 0 || closed < open {
			continue
		}
		block := source[next+open+1 : next+closed]
		columns := []string{}
		for position := 0; ; {
			column, after, found := githubWorkItemMetricQuoted(block, position)
			if !found {
				break
			}
			columns = append(columns, column)
			position = after
		}
		if len(columns) > 0 {
			result[table] = columns
		}
	}
	if len(result) == 0 {
		t.Fatal("parsed zero _insert_rows column lists from the live ClickHouse sink -- " +
			"the parser no longer matches production, so this test is measuring nothing")
	}
	return result
}

// githubWorkItemMetricQuoted reads the next double-quoted token at or after
// start, returning it plus the offset just past its closing quote.
func githubWorkItemMetricQuoted(source string, start int) (string, int, bool) {
	open := strings.Index(source[start:], `"`)
	if open < 0 {
		return "", 0, false
	}
	open += start + 1
	closed := strings.Index(source[open:], `"`)
	if closed < 0 {
		return "", 0, false
	}
	return source[open : open+closed], open + closed + 1, true
}

// TestGitHubWorkItemMetricPersistenceProjectionsMatchThePythonSink is the live
// proof of the narrowing this lane had to reproduce: compute emits twenty
// cycle-time fields, the sink writes sixteen. Writing the other three would put
// values into columns Python leaves at their DEFAULT 0, and readback would then
// report a conflict on every re-run of a unit Python had already written.
func TestGitHubWorkItemMetricPersistenceProjectionsMatchThePythonSink(t *testing.T) {
	source := githubWorkItemMetricReadSource(t,
		"src/dev_health_ops/metrics/sinks/clickhouse/work_graph.py")
	byTable := pythonSinkInsertColumns(t, source)
	tests := []struct {
		destination string
		row         any
	}{
		{githubWorkItemMetricsDailyDestination, githubWorkItemMetricsDailyRow{}},
		{githubWorkItemUserMetricsDailyDestination, githubWorkItemUserMetricsDailyRow{}},
		{githubWorkItemCycleTimesDestination, githubWorkItemCycleTimePersistenceRow{}},
	}
	for _, test := range tests {
		t.Run(test.destination, func(t *testing.T) {
			want, exists := byTable[test.destination]
			if !exists {
				t.Fatalf("the live sink has no _insert_rows call for %q", test.destination)
			}
			got := githubWorkItemMetricJSONFieldNames(reflect.TypeOf(test.row))
			if !slices.Equal(got, want) {
				t.Fatalf("persisted projection drifted:\n python=%v\n     go=%v", want, got)
			}
		})
	}
	// The compute record is deliberately WIDER than what is persisted. Asserting
	// the difference by name keeps "the sink drops three flow fields" a checked
	// statement rather than a comment: if Python starts writing them, the
	// per-table assertion above fails AND this one stops describing reality.
	compute := githubWorkItemMetricJSONFieldNames(reflect.TypeOf(githubWorkItemCycleTimeRecord{}))
	persisted := githubWorkItemMetricJSONFieldNames(reflect.TypeOf(githubWorkItemCycleTimePersistenceRow{}))
	var dropped []string
	for _, field := range compute {
		if !slices.Contains(persisted, field) {
			dropped = append(dropped, field)
		}
	}
	want := []string{"active_time_hours", "wait_time_hours", "flow_efficiency"}
	if !slices.Equal(dropped, want) {
		t.Fatalf("compute-only cycle-time fields = %v, want %v", dropped, want)
	}
}

func githubWorkItemMetricJSONFieldNames(structType reflect.Type) []string {
	names := make([]string, 0, structType.NumField())
	for index := 0; index < structType.NumField(); index++ {
		field := structType.Field(index)
		if field.PkgPath != "" {
			continue
		}
		name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		if name == "" {
			name = field.Name
		}
		names = append(names, name)
	}
	return names
}

// TestGitHubWorkItemMetricReadbacksFenceTheRealSortingKey reads the sorting keys
// migration 027 actually installed and requires every readback to fence exactly
// those columns. A readback that omits one of them can return a DIFFERENT
// logical row than the one being verified and call the effect Exact; a readback
// that adds a column outside the key can miss the row that ReplacingMergeTree
// will collapse it into.
func TestGitHubWorkItemMetricReadbacksFenceTheRealSortingKey(t *testing.T) {
	source := githubWorkItemMetricReadSource(t,
		"src/dev_health_ops/migrations/clickhouse/027_add_org_id_to_sorting_keys.py")
	adapters := githubWorkItemMetricReadSource(t,
		"internal/providersync/github_work_item_metric_triplet_effects_clickhouse.go")
	for _, destination := range []string{
		githubWorkItemMetricsDailyDestination,
		githubWorkItemUserMetricsDailyDestination,
		githubWorkItemCycleTimesDestination,
	} {
		t.Run(destination, func(t *testing.T) {
			key := githubWorkItemMetricSortingKey(t, source, destination)
			statement := githubWorkItemMetricSelectFrom(t, adapters, destination)
			if !strings.Contains(statement, "FINAL") {
				t.Fatalf("%s readback does not use FINAL; ReplacingMergeTree(computed_at) "+
					"leaves duplicate versions visible until a background merge runs", destination)
			}
			predicates := githubWorkItemMetricPredicateColumns(statement)
			// The required fence is the sorting key UNION the partition key,
			// not the sorting key alone.
			//
			// FINAL collapses one natural key across two partitions only when
			// the server says so: measured on a real server, the default
			// returns a single row while
			// `do_not_merge_across_partitions_select_final = 1` returns both.
			// This code sets no ClickHouse settings, so it inherits whatever
			// the server profile chose. Fencing the partition column makes the
			// verdict the same under either, which is the only version of this
			// readback worth having. For the two daily rollups the partition
			// column is already inside the sorting key and this adds nothing.
			required := append(slices.Clone(key),
				githubWorkItemMetricPartitionColumns(t, destination)...)
			// Compared as SETS: ClickHouse does not care in which order a WHERE
			// clause names the columns, only that every required one is fenced
			// and none outside the required set is.
			fenced, expected := slices.Clone(predicates), slices.Compact(slices.Sorted(slices.Values(required)))
			slices.Sort(fenced)
			if !slices.Equal(fenced, expected) {
				t.Fatalf("%s readback fences %v; sorting key (migration 027) is %v and the "+
					"partition key adds %v", destination, predicates, key,
					githubWorkItemMetricPartitionColumns(t, destination))
			}
			if key[0] != "org_id" || !slices.Contains(predicates, "org_id") {
				t.Fatalf("%s must be tenant-fenced on org_id, got key=%v predicates=%v",
					destination, key, predicates)
			}
		})
	}
}

// githubWorkItemMetricPartitionColumns reads a table's PARTITION BY expression
// out of the live migration that created it and returns the column names inside
// it. Like the sorting key, this is PARSED rather than mirrored: a partition
// scheme that changed without the readback following would otherwise leave the
// fence silently short.
func githubWorkItemMetricPartitionColumns(t *testing.T, table string) []string {
	t.Helper()
	source := githubWorkItemMetricReadSource(t,
		"src/dev_health_ops/migrations/clickhouse/001_metrics_v2.sql")
	index := strings.Index(source, "CREATE TABLE IF NOT EXISTS "+table+" (")
	if index < 0 {
		t.Fatalf("migration 001 does not create %q", table)
	}
	statement := source[index:]
	if end := strings.Index(statement, ";"); end >= 0 {
		statement = statement[:end]
	}
	marker := strings.Index(statement, "PARTITION BY ")
	if marker < 0 {
		t.Fatalf("%s has no PARTITION BY clause in migration 001 -- if the table is "+
			"genuinely unpartitioned this helper needs to say so deliberately, not "+
			"return an empty fence by accident", table)
	}
	clause := statement[marker+len("PARTITION BY "):]
	if end := strings.IndexAny(clause, "\n"); end >= 0 {
		clause = clause[:end]
	}
	open := strings.Index(clause, "(")
	closed := strings.LastIndex(clause, ")")
	if open < 0 || closed <= open {
		t.Fatalf("%s: could not read the partition expression from %q", table, clause)
	}
	columns := []string{}
	for _, column := range strings.Split(clause[open+1:closed], ",") {
		if trimmed := strings.TrimSpace(column); trimmed != "" {
			columns = append(columns, trimmed)
		}
	}
	if len(columns) == 0 {
		t.Fatalf("%s: parsed an empty partition key from %q", table, clause)
	}
	return columns
}

func githubWorkItemMetricSortingKey(t *testing.T, source, table string) []string {
	t.Helper()
	marker := fmt.Sprintf("%q: \"(", table)
	index := strings.Index(source, marker)
	if index < 0 {
		t.Fatalf("migration 027 declares no sorting key for %q", table)
	}
	rest := source[index+len(marker):]
	end := strings.Index(rest, ")")
	if end < 0 {
		t.Fatalf("migration 027's sorting key for %q is unterminated", table)
	}
	columns := []string{}
	for _, column := range strings.Split(rest[:end], ",") {
		if trimmed := strings.TrimSpace(column); trimmed != "" {
			columns = append(columns, trimmed)
		}
	}
	if len(columns) == 0 {
		t.Fatalf("parsed an empty sorting key for %q", table)
	}
	return columns
}

// githubWorkItemMetricSelectFrom returns the readback statement that reads the
// given table, i.e. the SELECT whose FROM names it. Anchoring on `FROM <table>`
// rather than on a method name keeps the test tied to the SQL that runs.
func githubWorkItemMetricSelectFrom(t *testing.T, source, table string) string {
	t.Helper()
	index := strings.Index(source, "FROM "+table+" FINAL")
	if index < 0 {
		t.Fatalf("no `FROM %s FINAL` readback found in the adapter source", table)
	}
	rest := source[index:]
	end := strings.Index(rest, "`")
	if end < 0 {
		t.Fatalf("the readback statement for %s is unterminated", table)
	}
	return rest[:end]
}

// githubWorkItemMetricPredicateColumns pulls the column names out of a
// `WHERE a = ? AND b = ?` tail, in the order they are fenced.
func githubWorkItemMetricPredicateColumns(statement string) []string {
	index := strings.Index(statement, "WHERE ")
	if index < 0 {
		return nil
	}
	columns := []string{}
	for _, clause := range strings.Split(statement[index+len("WHERE "):], " AND ") {
		name, _, found := strings.Cut(strings.TrimSpace(clause), " = ?")
		if !found {
			continue
		}
		if name = strings.TrimSpace(name); name != "" {
			columns = append(columns, name)
		}
	}
	return columns
}

// TestGitHubWorkItemMetricTripletOwnsExactlyThreeDerivedDestinations keeps this
// lane inside its declared boundary. The composite route requires all NINE
// derived destinations before it will build effects, so a lane that silently
// grew a fourth would be claiming a destination another lane still owes.
func TestGitHubWorkItemMetricTripletOwnsExactlyThreeDerivedDestinations(t *testing.T) {
	if len(githubWorkItemMetricTripletDestinations) != 3 {
		t.Fatalf("triplet destinations = %v", githubWorkItemMetricTripletDestinations)
	}
	for _, destination := range githubWorkItemMetricTripletDestinations {
		if !slices.Contains(githubWorkItemDerivedDestinations, destination) {
			t.Fatalf("%q is not one of the route's derived destinations", destination)
		}
		// The GITHUB list, on a github path. Behaviour-neutral TODAY -- the
		// metric triplet is a subset of the shared family, so Contains answers
		// the same against either -- but the shared list is the wrong source
		// for a github assertion, and a future github-only surface here would
		// fail silently against it.
		if !slices.Contains(githubWorkItemRouteDestinations(), destination) {
			t.Fatalf("%q is not a canonical work-item route destination", destination)
		}
	}
	if !slices.IsSorted(githubWorkItemMetricTripletDestinations) {
		t.Fatalf("triplet destinations are not in canonical order: %v",
			githubWorkItemMetricTripletDestinations)
	}
}
