package rowview

import (
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// The census of the readers of the three tables of the view column. A new file that names one of the tables
// fails here until it is listed in readerRoles with a role: a reader that sums across work scopes then has to
// say that it takes OrgPredicate, and a reader that does not has to say why (CHAOS-8863).
func TestEveryFileThatNamesAViewTableIsInTheReaderAllowlist(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	for _, dir := range []string{"internal", "cmd"} {
		walkErr := filepath.WalkDir(filepath.Join(root, dir), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() && (entry.Name() == "testdata" || entry.Name() == ".git") {
				return filepath.SkipDir
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			body, readErr := os.ReadFile(path)
			if readErr != nil {
				return readErr
			}
			for _, table := range Tables() {
				if strings.Contains(string(body), table) {
					relative, _ := filepath.Rel(root, path)
					found[filepath.ToSlash(relative)] = true
					break
				}
			}
			return nil
		})
		if walkErr != nil {
			t.Fatal(walkErr)
		}
	}
	if len(found) < 50 {
		t.Fatalf("the scan found %d files that name a view table: it read too little", len(found))
	}
	var unlisted, stale []string
	for file := range found {
		if _, ok := readerRoles[file]; !ok {
			unlisted = append(unlisted, file)
		}
	}
	for file, entry := range readerRoles {
		if !found[file] {
			stale = append(stale, file)
		}
		switch entry.role {
		case "reader-sum", "reader-nosum", "writer", "names":
		default:
			t.Errorf("%s: role %q is not one of reader-sum, reader-nosum, writer, names", file, entry.role)
		}
		if entry.why == "" {
			t.Errorf("%s: no reason", file)
		}
	}
	sort.Strings(unlisted)
	sort.Strings(stale)
	for _, file := range unlisted {
		t.Errorf("%s names a table of the view column and is not in readerRoles: list it with its role", file)
	}
	for _, file := range stale {
		t.Errorf("%s is in readerRoles and names no table of the view column any more: remove it", file)
	}
}

// viewReaders are the reader files that already take their view (CHAOS-8863, the work_item_metrics_daily and work_item_state_durations_daily readers):
// each must call the helper, and a file that groups by key must carry row_view in the group. A reader that drops the
// predicate or the group column fails here; the golden statements in the reader packages hold the SQL text.
var viewReaders = map[string][]string{
	"internal/jobs/metrics/remaining/capacity_native_clickhouse.go":        {"rowview.OrgPredicate("},
	"internal/jobs/metrics/remaining/recommendations_loader.go":            {"rowview.OrgPredicate(", "GROUP BY day, provider, work_scope_id, row_view"},
	"internal/jobs/metrics/remaining/recommendations_native_clickhouse.go": {"rowview.OrgPredicate("},
	"internal/jobs/metrics/daily/benchmarking/clickhouse.go":               {"rowview.OrgPredicate(", `"row_view"`},
	"internal/queryapi/capacityforecast/clickhouse.go":                     {"rowview.OrgPredicate("},
	"internal/queryapi/throughputforecast/clickhouse.go":                   {"rowview.OrgPredicate(", "GROUP BY day, provider, work_scope_id, team_id, row_view", "GROUP BY provider, work_scope_id, team_id, row_view", "GROUP BY team_id, work_scope_id, provider, row_view"},
	"internal/queryapi/aggflame/clickhouse.go":                             {"rowview.OrgPredicate(", "GROUP BY day, provider, work_scope_id, team_id, status, row_view"},
	"internal/queryapi/aianalytics/flow_opportunities.go":                  {"rowview.OrgPredicate("},
	"internal/queryapi/sankey/queries.go":                                  {"rowview.OrgPredicate("},
	"internal/queryapi/home/queries_metrics.go":                            {"rowview.OrgPredicate(", "GROUP BY day, provider, work_scope_id, team_id, status, row_view"},
	"internal/queryapi/explain/metrics.go":                                 {"rowview.OrgPredicate("},
	"internal/queryapi/operatingreview/operatingreview.go":                 {"rowview.OrgPredicate(", "GROUP BY day, provider, work_scope_id, row_view", "GROUP BY day, provider, work_scope_id, status, row_view"},
	"internal/queryapi/quadrant/quadrant.go":                               {"rowview.OrgPredicate(", "rowview.ScopePredicate("},
}

func TestEveryViewReaderCallsTheHelper(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	for file, needles := range viewReaders {
		body, readErr := os.ReadFile(filepath.Join(root, file))
		if readErr != nil {
			t.Errorf("%s: %v", file, readErr)
			continue
		}
		if entry, listed := readerRoles[file]; !listed || entry.role != "reader-sum" {
			t.Errorf("%s is a view reader and must be a reader-sum file in readerRoles", file)
		}
		for _, needle := range needles {
			if !strings.Contains(string(body), needle) {
				t.Errorf("%s lost %q: the reader no longer takes its view of the table", file, needle)
			}
		}
	}
}
